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

#include <netinet/in.h>
#include <arpa/inet.h>
#include <liburing.h>
#include <sys/mman.h>
#include <unistd.h>
#include <netinet/tcp.h>
#include <sys/socket.h>


// Type safe version of containerof used to find parent structs from contained structs
#define containerof(ptr, type, member) ((type *)((char *)(1 ? (ptr) : &((type *)0)->member) - offsetof(type, member)))


typedef struct eloop_t eloop_t;

typedef void (*eloop_work_cb_t)(eloop_t *l, void* context);
typedef struct eloop_work_t {
    eloop_work_cb_t cb;
    void *context;
    struct eloop_work_t *next;
} eloop_work_t ;

// Buffer pools
// io_uring "provided buffers" require us to
//   - not underflow the ring and cause outstanding kernel read submissions to fail
//   - not have two buffer ids as active read targets in a given ring/pool at once
//   - remember which buffer ring/pool is used for an io submission (not given in CQE)
//   - for each pool have a known mapping for the CQE buffer id to actual buffer data and metadata
//
// The sample from liburing uses a static mapping between fixed size chunks and pointer
// arithmetic to find the memory.  Same used here assuming buffer mem recycled real soon.
// In general any mapping would do, static reuse is not a requirement, and arbitrary meta
// data could be maintained per buffer.
// Different buffer pools might have different policies, eg a "small buffer" pool for overcommitment
// and low latency data streams versus a "large buffer" pool never overcommitted and only used on
// reads that are known not to block.  Another buffer pool might also leave a fixed area blank at the
// front of a provided buffer.
//
// Assumption: No router single buffer is more than 64k-1, and probably much smaller for
// latency purposes.  hic sunt dracones, never assume, etc.

typedef struct eloop_buf_pool_t {
    struct io_uring_buf_ring *br;
    unsigned char *pool_base;
    int br_mask;
    uint16_t pool_id;
    uint16_t buf_size;
    uint16_t buf_count;
    uint16_t recycle_ops;
    uint16_t in_use;
    bool registered;
} eloop_buf_pool_t;

// This lives at the front of the buffer, allowing buffer_size - sizeof(buf_desc_t) payload.
typedef struct buf_desc_t {
    uint8_t  pool_id;
    uint8_t  more;
    uint16_t buf_id;
    uint16_t len;
    uint16_t offset;
    struct buf_desc_t *next;
} buf_desc_t ;

static inline unsigned char* buf_payload(buf_desc_t *bd) {
    return ((unsigned char *) bd) + sizeof(buf_desc_t);
}

typedef struct eloop_t {
    struct io_uring ring;
    sys_mutex_t lock;
    footp_module_t *module;
    uint32_t ios_pending; // ios submitted - completions processed
    uint32_t max_ready_cqe; // lifetime max number of unprocessed completions
    eloop_buf_pool_t buf_pools[2];  // 0 = normal, 1 = large buffers
//    unsigned char *buf_pool_base;
//    size_t buf_size;
//    eloop_buf_pool_t *default_pool;
    eloop_work_t *ts_work_head; // protected by lock
//    struct io_uring_buf_ring *buf_pool_ring;
//    int buf_pool_ring_mask;
//    int buf_pool_recycle_ops;   // Number of buf_ring_add ops waiting for buf_ring advance
    int eventfd;
    bool notified;
    bool running;
    // park less frequently accessed mem at the back
//    size_t buf_pool_mem_size;
//    size_t buf_count;  // sensible size TBD.  must be power-of-2
    size_t staged_output;
    pthread_t thread;
    int loop_id;
//    bool buf_pool_registered;
} eloop_t;

static inline unsigned char *get_buffer_mem(eloop_buf_pool_t *bp, int idx) {
    // convert index idx to physical memory from the pre-allocated buffer pool
    return bp->pool_base + (idx * bp->buf_size);
}

static inline buf_desc_t *get_buf_desc(eloop_t *loop, int pool_id, int idx) {
    assert(pool_id <= 1);
    eloop_buf_pool_t *bp = &loop->buf_pools[pool_id];
    // buf descriptor is at front
    return (buf_desc_t *) get_buffer_mem(bp, idx);
}

// ------------------------------------------------------------------------
// Adapter interface

#define IRL_ID 0
#define TCP_ID 1

// eloop ring op completion callbacks
typedef void (*eloop_accept_done_t)(void* user_data, int sockfd);
typedef void (*eloop_connect_done_t)(void* user_data, int sys_errno);
typedef void (*eloop_close_done_t)(void* user_data, int sys_errno);
typedef void (*eloop_socket_move_done_t)(void* user_data, int sockfd, eloop_t *loop);
typedef void (*eloop_recv_done_t)(void* user_data, uint8_t pool_id, uint16_t buf_id, int sys_errno, bool immediate);
typedef void (*eloop_send_done_t)(void* user_data, int sys_errno);
typedef void (*eloop_shutwr_done_t)(void* user_data, int sys_errno);

typedef struct eloop_callbacks_t {
    const eloop_accept_done_t accept_done;
    const eloop_connect_done_t connect_done;
    const eloop_close_done_t close_done;
    const eloop_socket_move_done_t socket_move_done;
    const eloop_recv_done_t recv_done;
    const eloop_send_done_t send_done;
    const eloop_shutwr_done_t shutwr_done;
} eloop_callbacks_t;
    

// ------------------------------------------------------------------------
// io and io pairs

// General notes:
// There is an lside first, then a cside
// Call to create a pair always succeeds.  Potential setup failure is communicated later.
// Lifecycle is immediate closed (remote connection failed) or
//   credit -> (read and write close, either order) -> closed(errnos)
//   closed means can never call other_io after call returns.

typedef struct foo_io_t {
    foo_io_t *other_io;
    bool lside;
    uint8_t adapter_id;
} foo_io_t;

// Credit: end to end is bytes, no knowledge of splitting and combining chunks on journey.
// End to end credit is communicated as "this many additional bytes" and decremented by sender.

typedef void (*foo_io_make_routed_pair_t)(void *path_ctx, foo_io_t *first_io, const char *rk, eloop_t *loop);
typedef void (*foo_io_fwd_buffer_t)(foo_io_t *io, buf_desc_t *bd);
typedef void (*foo_io_fwd_ended_t)(foo_io_t *io);
typedef void (*foo_io_ending_t)(foo_io_t *io);

typedef uint32_t (*foo_io_get_credit_t)(foo_io_t *io);
typedef void (*foo_io_closed_t)(foo_io_t *io, uint32_t router_errno, uint32_t sys_errno);
typedef void (*foo_io_write_close_t)(foo_io_t *io);

typedef void (*foo_io_recycle_buffer_t)(foo_io_t *io, buf_desc_t *bd);
typedef void (*foo_io_credit_updated_t)(foo_io_t *io, uint32_t new_val);

// TBD: useful possibly for IRL to throttle inbound TCP buffers from firehose sockets.  TCP<->TCP?
typedef void (*foo_io_fwd_pause_t)(foo_io_t *io);
typedef void (*foo_io_fwd_resume_t)(foo_io_t *io);

typedef struct foo_io_fn_t {
    const foo_io_closed_t closed;
    const foo_io_credit_updated_t credit_updated;
    const foo_io_make_routed_pair_t make_routed_pair;
    const foo_io_fwd_buffer_t fwd_buffer;
    const foo_io_fwd_ended_t fwd_ended;
    const foo_io_recycle_buffer_t recycle_buffer;
    const foo_io_ending_t ending;
    int ZZZfubar;
} foo_io_fn_t;

// IRL foo_io_t functions
static void irl_closed_ZZZ(foo_io_t *io, uint32_t router_errno, uint32_t sys_errno) {
    fprintf(stderr, "irl_open_failed_ZZZ called in error\n"); abort();
}
static void irl_credit_updated_ZZZ(foo_io_t *io, uint32_t new_val) {
    fprintf(stderr, "irl_credit_updated_ZZZ called in error\n"); abort();
}
static void irl_make_routed_pair_ZZZ(void *path_ctx, foo_io_t *first_io, const char *rk, eloop_t *loop) {
    fprintf(stderr, "irl_make_routed_pair_ZZZ called in error\n"); abort();
}
static void irl_fwd_buffer_ZZZ(foo_io_t *io, buf_desc_t *bd) {
    fprintf(stderr, "irl_fwd_buffer_ZZZ called in error\n"); abort();
}
static void irl_fwd_ended_ZZZ(foo_io_t *io) {
    fprintf(stderr, "irl_fwd_ended_ZZZ called in error\n"); abort();
}
static void irl_recycle_buffer_ZZZ(foo_io_t *io, buf_desc_t *bd) {
    fprintf(stderr, "irl_recycle_buffer_ZZZ called in error\n"); abort();
}
static void irl_ending_ZZZ(foo_io_t *io) {
    fprintf(stderr, "irl_ending_ZZZ called in error\n"); abort();
}
//IRL adapter_t functions
static void irl_accept_done_ZZZ(void *user_data, int sockfd) {
    fprintf(stderr, "irl_accept_done_ZZZ called in error\n"); abort();
}
static void irl_connect_done_ZZZ(void *user_data, int sys_errno) {
    fprintf(stderr, "irl_connect_done_ZZZ called in error\n"); abort();
}
static void irl_close_done_ZZZ(void *user_data, int sys_errno) {
    fprintf(stderr, "irl_close_done_ZZZ called in error\n"); abort();
}
static void irl_socket_move_done_ZZZ(void *user_data, int sockfd, eloop_t *loop) {
    fprintf(stderr, "irl_move_socket_done_ZZZ called in error\n"); abort();
}
static void irl_recv_done_ZZZ(void *user_data, uint8_t pool_id, uint16_t buf_id, int sys_errno, bool immediate) {
    fprintf(stderr, "irl_recv_done_ZZZ called in error\n"); abort();
}
static void irl_send_done_ZZZ(void* user_data, int sys_errno) {
    fprintf(stderr, "irl_send_done_ZZZ called in error\n"); abort();
}
static void irl_shutwr_done_ZZZ(void* user_data, int sys_errno) {
    fprintf(stderr, "irl_shutwr_done_ZZZ called in error\n"); abort();
}


static void tcp_closed(foo_io_t *io, uint32_t router_errno, uint32_t sys_errno);
static void tcp_credit_updated(foo_io_t *io, uint32_t delta);
static void tcp_make_routed_pair(void *path_ctx, foo_io_t *first_io, const char *rk, eloop_t *loop);
static void tcp_fwd_buffer(foo_io_t *io, buf_desc_t *bd);
static void tcp_fwd_ended(foo_io_t *io);
static void tcp_recycle_buffer(foo_io_t *io, buf_desc_t *bd);
static void tcp_ending(foo_io_t *io);

static const foo_io_fn_t io_impl[2] = {
    {
        irl_closed_ZZZ,
        irl_credit_updated_ZZZ,
        irl_make_routed_pair_ZZZ,
        irl_fwd_buffer_ZZZ,
        irl_fwd_ended_ZZZ,
        irl_recycle_buffer_ZZZ,
        irl_ending_ZZZ
    },
    {
        tcp_closed,
        tcp_credit_updated,
        tcp_make_routed_pair,
        tcp_fwd_buffer,
        tcp_fwd_ended,
        tcp_recycle_buffer,
        tcp_ending
    }
};


typedef enum {
    RNG_EVENTFD = 0,
    RNG_LISTEN,
    RNG_ACCEPT,
    RNG_CONNECT,
    RNG_SEND,
    RNG_RECV,
    RNG_SHUTWR,
    RNG_CLOSE,
    RNG_TO_RNG,
    RNG_SOCK_MOV
} ringio_type_t;

// A single ringio struct can stand in for one or many io submissions.
// The loop requires type and adapter id correctly set.
// The other data may be used by the adapaters exclusively.
typedef struct base_ringio_t {
    uint8_t io_type;
    uint8_t adapter_id;
    uint16_t data16;
    uint32_t data32;
} base_ringio_t ;

typedef struct rcv_io_t {
    base_ringio_t base_io;
    uint16_t eloop_private;
} rcv_io_t;

typedef struct snd_io_t {
    base_ringio_t base_io;
    buf_desc_t *buf_desc;
    uint16_t eloop_private;
} snd_io_t;


// Buffers always on the move except at last sender, possibly with slow consumer.  Few buffers per
// connection should pile up anywhere in router network except at the last endpoint, but "few" might
// not be very few for low priority traffic at a congested spot.  Pathological case is one byte per
// buffer, with just enough time between bytes for kernel to trigger a recv per byte without a
// "more" flag, limited only by end-to-end credit.
// Future: fast treatment of short chains with graceful transition to arbitrary long chains.


#ifdef ZZZ_perhaps
struct bce_t {
    void *address;
    uint size;
    uint offset;
    buf_desc_t bd;
    bce_t *next;
};
struct bchain_t {
    uint count;
    uint tot_size;
    bool more;
    bce_t *first;
    bce_t *last;
};
struct foo_buf_t {
    poolp;
    bool more;
    pool_idx;
    offset;
    len;
};
#endif



// ------------------------------------------------------------------------
// Event loop calls

void eloop_post_accept(eloop_t *loop, base_ringio_t *io, int listenfd, int priority);
void eloop_post_connect(eloop_t *loop, base_ringio_t *io, int outbound_sock, struct sockaddr *addr, socklen_t addr_len, int priority);
void eloop_post_socket_move(eloop_t *loop, base_ringio_t *io, eloop_t *otherloop, int sockfd, int priority);
void eloop_post_close(eloop_t *loop, base_ringio_t *io, int sockfd, int priority);
void eloop_add_work(eloop_t *loop, eloop_work_cb_t cb, void *context);

void eloop_recycle_buffer(eloop_t *loop, buf_desc_t *bd);
void eloop_post_recv(eloop_t *loop, rcv_io_t *rcv_io, int sockfd, uint32_t maxcount, bool immediate, int priority);
void eloop_post_send(eloop_t *loop, snd_io_t *snd_io, int sockfd, int priority);
void eloop_post_shutwr(eloop_t *loop, base_ringio_t *io, int sockfd, int priority);
// ------------------------------------------------------------------------
// Temporary nonsense

// Buffer pool test setup uses these default buffer sizes.  tweak via env
static uint16_t sp_buf_size = 4096;
static uint16_t lp_buf_size = 0;
