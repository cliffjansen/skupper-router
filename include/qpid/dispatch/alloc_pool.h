#ifndef __dispatch_alloc_h__
#define __dispatch_alloc_h__ 1
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/threading.h"

#include <stdint.h>
#include <stdlib.h>

/**
 * @file
 * Memory Allocation
 *
 * Allocate memory in per-thread, per-type memory pools.

 * @internal
 */

/** Allocation pool */
typedef struct qd_alloc_pool_t qd_alloc_pool_t;

DEQ_DECLARE(qd_alloc_pool_t, qd_alloc_pool_list_t);

/** Allocation configuration. */
typedef struct {
    int  transfer_batch_size;
    int  local_free_list_max;
    int  global_free_list_max;  ///< -1 means unlimited
} qd_alloc_config_t;

/** Allocation statistics. */
typedef struct {
    uint64_t total_alloc_from_heap;
    uint64_t total_free_to_heap;
    uint64_t held_by_threads;
    uint64_t batches_rebalanced_to_threads;
    uint64_t batches_rebalanced_to_global;
} qd_alloc_stats_t;

/** Allocation type descriptor. */
typedef struct qd_alloc_type_desc_t {
    // note: keep most frequently accessed fields at the top
    sys_mutex_t              lock;
    qd_alloc_pool_t         *global_pool;
    const qd_alloc_config_t *config;
    size_t                   total_size;
    qd_alloc_stats_t         stats;
    qd_alloc_pool_list_t     tpool_list;
    size_t                   type_size;
    const char              *type_name;
    const size_t            *additional_size;
    void                    *debug;
    DEQ_LINKS(struct qd_alloc_type_desc_t);
} qd_alloc_type_desc_t;

typedef struct {
    void     *ptr;
    uint32_t  seq;
} qd_alloc_safe_ptr_t;

/** Allocate in a thread pool. Use via ALLOC_DECLARE */
void *qd_alloc(qd_alloc_type_desc_t *desc, qd_alloc_pool_t **tpool);
/** De-allocate from a thread pool. Use via ALLOC_DECLARE */
void qd_dealloc(qd_alloc_type_desc_t *desc, qd_alloc_pool_t **tpool, char *p);
uint32_t qd_alloc_sequence(void *p);

// generic safe pointer api for any alloc pool item

#define QD_SAFE_PTR_INIT(p) { .ptr = (void*)(p), .seq = qd_alloc_sequence(p) }

static inline void qd_nullify_safe_ptr(qd_alloc_safe_ptr_t *sp)
{
    sp->ptr = 0;
}

static inline void qd_alloc_set_safe_ptr(qd_alloc_safe_ptr_t *sp, void *p)
{
    sp->ptr = p;
    sp->seq = qd_alloc_sequence(p);
}

static inline void *qd_alloc_deref_safe_ptr(const qd_alloc_safe_ptr_t *sp)
{
    return sp->seq == qd_alloc_sequence(sp->ptr) ? sp->ptr : (void*) 0;
}

/**
 * Declare functions new_T and alloc_T
 */
#define ALLOC_DECLARE(T)             \
    T               *new_##T(void);  \
    void             free_##T(T *p); \
    qd_alloc_stats_t alloc_stats_##T(void)

#define ALLOC_DECLARE_SAFE(T) \
    ALLOC_DECLARE(T); \
    typedef qd_alloc_safe_ptr_t T##_sp; \
    void set_safe_ptr_##T(T *p, T##_sp *sp); \
    T *safe_deref_##T(T##_sp sp)

/**
 * Define allocator configuration.
 *@internal
 */
void qd_alloc_desc_init(const char *name, qd_alloc_type_desc_t *desc, size_t size, const size_t *additional_size,
                        const qd_alloc_config_t *config);
qd_alloc_stats_t qd_alloc_desc_stats(const qd_alloc_type_desc_t *desc);  // thread safe
// clang-format off
#define ALLOC_DEFINE_CONFIG(T,S,A,C)                                    \
    qd_alloc_type_desc_t __desc_##T  __attribute__((aligned(64)));      \
    __thread qd_alloc_pool_t *__local_pool_##T = 0;                     \
    T *new_##T(void) { return (T*) qd_alloc(&__desc_##T, &__local_pool_##T); } \
    void free_##T(T *p) { qd_dealloc(&__desc_##T, &__local_pool_##T, (char*) p); } \
    qd_alloc_stats_t alloc_stats_##T(void) { return qd_alloc_desc_stats(&__desc_##T); } \
    __attribute__((constructor)) void init_##T(void) {                  \
        qd_alloc_desc_init(#T, &__desc_##T, S, A, C);                   \
    }                                                                   \
    void *unused##T

#define ALLOC_DEFINE_CONFIG_SAFE(T,S,A,C)                                \
    ALLOC_DEFINE_CONFIG(T,S,A,C); \
    void set_safe_ptr_##T(T *p, T##_sp *sp) { qd_alloc_set_safe_ptr(sp, (void*)p); } \
    T *safe_deref_##T(T##_sp sp) { return (T*) qd_alloc_deref_safe_ptr((qd_alloc_safe_ptr_t*) &(sp)); } \
    void *unused##T
// clang-format on

/**
 * Define functions new_T and alloc_T
 */
#define ALLOC_DEFINE(T) ALLOC_DEFINE_CONFIG(T, sizeof(T), 0, 0)

#define ALLOC_DEFINE_SAFE(T) ALLOC_DEFINE_CONFIG_SAFE(T, sizeof(T), 0, 0)

void qd_alloc_initialize(void);
void qd_alloc_debug_dump(const char *file);
void qd_alloc_finalize(void);
size_t qd_alloc_type_size(const qd_alloc_type_desc_t *desc);  // thread safe
#endif
