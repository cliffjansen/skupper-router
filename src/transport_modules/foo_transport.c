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

 // A sample plugin transport.  io_uring based.
 // Overall structure is a mishmash of general abstraction and premature optimization.
 //
 // Modules: "tcp" (like tcpAdapter), "irl" (inter router link), "loop" an event loop instance (uring)
 //
 // Chief concepts that differ from existing router:
 //   - more than one loop, cooperate to provide a global scheduler
 //   - one thread per loop, minimal use of locks
 //   - routed stream segments (raw<->raw, raw<->irl, irl<->irl) are bound to a loop
 //   - each loop owns and manages separate inbound buffer pools
 //   - "more" flag preserved always
 //   - event callbacks MUST be very short for latency goals
 //      - long maintenance activities must self time and reschedule if needed
 //      - kernel has deferred some work assuming io_uring behavior is not too lumpy
 //        see IORING_SETUP_COOP_TASKRUN
 //   - assumes other threads (core, vanflow, proactor) have separate cpu resources (latency again).
 //      - perhaps someday ALL activity scheduled by loop (incl core, vanflow, proactor).
 //   - batches of io work provided by the kernel can be given priority (QOS/latency)
 //     resulting IRL traffic can be given the same or different priority.
 //   - expect backpressure from at least two sources.  Slow end consumer (end to end credit) and
 //     busy/slow IRL (competing traffic volume, or many incomplete writes). 3rd: rate limiting QOS.
 //
 // This design structure could also be implemented via epoll with extra ioctls to know "more"
 // status and unwritten bytes pending.  Benefits of uring-controlled buffers could be emulated.
 //
 // Buffer pools could have prepended empty space for IRL metadata (spurious for OpenSSL?)
 // Maintaining even workloads per ring an issue (opportunity?)
 // Queues of deferred writes and reads (with priorities) need maintaining.

 // In general TCP tries to optimize what it can see per connection, IRL tries for its bundle of
 // funneled dodads, and the eloop defers/expedites based on its total ring traffic

 // Consider multiple rings per thread.  Each with different priority.  This way, the kernel
 // "sorts" the current workload in advance of each loop iteration.

 // Misc: try two pass of completions, first priority schedule and estimate overall busy state
 //    (free cpu? writes to IRLs that remain pending?) then process in priority order.
 // full end to end state info, n hops, credit each direction, memory pressure (all connections)
 //    at router endpoints
 // Full backpressure at edge and pending buf storage, zero within router network
 // Track slow consumers and adjust credit accordingly?
 // Credit squishy/inexact?  receiver may refuse to post a read for partial buffer if node busy,
 //   and wait for future credit increase?
 // Try huge pages
 // Try simpler buffer pool designs to see performance impact vs code maintenance
 //   current: 256 pools x 64k entries = 16M individual buffers (could be per ring).
 

// For cpu_set, include first
#define _GNU_SOURCE
#include <sched.h>
#include <sys/sysinfo.h>
#include <assert.h>


#include <qpid/dispatch/ctools.h>
#include <qpid/dispatch/enum.h>
#include <qpid/dispatch/alloc_pool.h>
#include <qpid/dispatch/io_module.h>
#include <qpid/dispatch/protocol_adaptor.h>
#include <qpid/dispatch/log.h>
#include <qpid/dispatch/adaptor_common.h>
#include <qpid/dispatch/service_transport.h>
#include <qpid/dispatch/server.h>
#include "dispatch_private.h"

//===========================================================================================
// Settings and Definitions
//===========================================================================================

static const char *module_name = "fooTransport";

//===========================================================================================
// State Definitions
//===========================================================================================

//
// Service Listener
//
typedef struct footp_tcp_listener_t {
    DEQ_LINKS(struct footp_tcp_listener_t);
} footp_tcp_listener_t;

ALLOC_DECLARE(footp_tcp_listener_t);
ALLOC_DEFINE(footp_tcp_listener_t);
DEQ_DECLARE(footp_tcp_listener_t, footp_tcp_listener_list_t);

//
// Service Connector
//
typedef struct footp_tcp_connector_t {
    DEQ_LINKS(struct footp_tcp_connector_t);
} footp_tcp_connector_t;

ALLOC_DECLARE(footp_tcp_connector_t);
ALLOC_DEFINE(footp_tcp_connector_t);
DEQ_DECLARE(footp_tcp_connector_t, footp_tcp_connector_list_t);

//
// Inter-Router Listener
//

//
// Inter-Router Connector
//

//
// Inter-Router Link
//

//
// Module State
//

typedef struct eloop_t eloop_t;
typedef struct foo_rkey_watcher_t foo_rkey_watcher_t;
typedef struct foo_rkey_provider_t foo_rkey_provider_t;

typedef struct {
    footp_tcp_listener_list_t  tcpListeners;
    footp_tcp_connector_list_t tcpConnectors;
    sys_mutex_t lock;
    int eloop_count;
    eloop_t **eloops;

    qdr_core_t                *core;
    qd_dispatch_t             *qd;
    qd_server_t               *server;
    pn_proactor_t             *proactor;

    // routing key watchers and providers
    foo_rkey_watcher_t        *watchers_head;
    foo_rkey_provider_t       *providers_head;
} footp_module_t;

static footp_module_t *footp_module_context;

//===========================================================================================
// Transport API Handlers
//===========================================================================================


//===========================================================================================
// Management API Handlers - TCP Listeners
//===========================================================================================


static void *configure_foo_tcp_listener(qd_dispatch_t *qd, qd_entity_t *entity);

static void *configure_tcp_listener(qd_dispatch_t *qd, qd_entity_t *entity)
{
    return configure_foo_tcp_listener(qd, entity);
}

static void *update_tcp_listener(qd_dispatch_t *qd, qd_entity_t *entity, void *impl)
{
    fprintf(stderr, "update_tcp_listener ignored\n");
    return 0;
}

static void delete_tcp_listener(qd_dispatch_t *qd, void *impl)
{
    fprintf(stderr, "delete_tcp_listener ignored\n");
}

static qd_error_t refresh_tcp_listener(qd_entity_t* entity, void *impl)
{
    fprintf(stderr, "refresh_tcp_listener ignored\n");
    return QD_ERROR_NONE;
}

//===========================================================================================
// Management API Handlers - TCP Connectors
//===========================================================================================

static void *configure_foo_tcp_connector(qd_dispatch_t *qd, qd_entity_t *entity);

static void *configure_tcp_connector(qd_dispatch_t *qd, qd_entity_t *entity)
{
    return configure_foo_tcp_connector(qd, entity);
}

static void delete_tcp_connector(qd_dispatch_t *qd, void *impl)
{
    fprintf(stderr, "ZZZ delete_tcp_connector\n");
}

static qd_error_t refresh_tcp_connector(qd_entity_t* entity, void *impl)
{
    fprintf(stderr, "ZZZ refresh_tcp_connector\n");
    return QD_ERROR_NONE;
}

//===========================================================================================
// Module API Handlers
//===========================================================================================
//
// This initialization function is invoked once at router startup if this module is
// the one transport module enabled in the router configuration.
//

void footp_startup(footp_module_t *module);
void footp_shutdown(footp_module_t *module);

static void TRANSPORT_init(qdr_core_t *core, void **adaptor_context)
{
    qd_log(LOG_ROUTER, QD_LOG_INFO, "Transport Module Initialized: %s", module_name);

    qd_register_tcp_management_handlers(
        configure_tcp_listener,
        configure_tcp_connector,
        update_tcp_listener,
        delete_tcp_listener,
        delete_tcp_connector,
        refresh_tcp_listener,
        refresh_tcp_connector
    );

    footp_module_t *module = NEW(footp_module_t);
    ZERO(module);
    *adaptor_context = module;
    footp_module_context = module;  // for API calls that do not get the adaptor context
    module->core   = core;
    module->qd     = qdr_core_dispatch(core);
    module->server = module->qd->server;
    module->proactor = qd_server_proactor(module->server);

    // create the loops (urings and worker threads)
    footp_startup(module);
}

//
// This finalization function is invoked once at router shut-down only if it was earlier initialized.
//
static void TRANSPORT_final(void *module_context)
{
    qd_log(LOG_ROUTER, QD_LOG_INFO, "Transport Module Finalized: %s", module_name);
    footp_module_t *module = (footp_module_t *) module_context;
    footp_shutdown(module);
    free(module_context);
}

/**
 * Declare the module so that it will self-register on process startup.
 */
QDR_TRANSPORT_MODULE_DECLARE(module_name, TRANSPORT_init, TRANSPORT_final)

// The goal of this module is to activate two adapter instances and make a routed pair out of them.
//
// foo_io_t ... the adapter bit that does io (i.e. raw connection, irl channel)
// foo_io_path_t ... a provider of foo_io_t instances for a routing key (keys)
//     i.e. a connector or IRL equivalent for the next hop/leg towards the final rk destination



typedef struct foo_io_t foo_io_t;

typedef void (*make_routed_pair_fn_t)(void *path_ctx, foo_io_t *first_io, const char *rk, eloop_t *loop);
typedef struct foo_path_t {
    make_routed_pair_fn_t make_routed_pair;
} foo_path_t;

// Fake core.  For a handful of addresses, no smarts.
// Listeners pair up with the first promissing connector (tcp/irl)
// Empty address matches everything and means IRL.

typedef void (*foo_watcher_cb_t)(void* context, const char *rk, foo_path_t *s, void *path_ctx);
typedef struct foo_rkey_watcher_t {
    const char *rk;
    foo_watcher_cb_t cb;
    void *watcher_context;
    struct foo_rkey_watcher_t *next;
    bool watched;
} foo_rkey_watcher_t;

typedef struct foo_rkey_provider_t {
    const char *rk;
    foo_path_t *path;
    void *path_context;
    struct foo_rkey_provider_t *next;
} foo_rkey_provider_t;

// Remember watcher and see if there is a matching provider
static void fake_core_add_watcher(char *rk, foo_watcher_cb_t cb, void *watcher_context) {
    footp_module_t *module = footp_module_context;
    foo_rkey_watcher_t *w = malloc(sizeof(foo_rkey_watcher_t));
    ZERO(w);
    assert(rk);
    w->rk = strdup(rk);
    w->cb = cb;
    w->watcher_context = watcher_context;
    if (!module->watchers_head)
        module->watchers_head = w;
    else {
        foo_rkey_watcher_t *w2 = module->watchers_head;
        while (!!w2->next) w2 = w2->next;
        w2->next = w;
    }
    foo_rkey_provider_t *p = module->providers_head;
    while (p) {
        if (!p->rk || !strcmp(p->rk, rk)) {
            w->watched = true;
            w->cb(w->watcher_context, w->rk, p->path, p->path_context);
            break;
        }
        p = p->next;
    }
}

// Remember provider and see if there is a matching watcher
static void fake_core_add_provider(char *rk, foo_path_t *path, void *path_context) {
    footp_module_t *module = footp_module_context;
    foo_rkey_provider_t *p = malloc(sizeof(foo_rkey_provider_t));
    ZERO(p);
    if (!!rk) p->rk = strdup(rk);
    p->path = path;
    p->path_context = path_context;
    if (!module->providers_head)
        module->providers_head = p;
    else {
        foo_rkey_provider_t *p2 = module->providers_head;
        while (!!p2->next) p2 = p2->next;
        p2->next = p;
    }
    foo_rkey_watcher_t *w = module->watchers_head;
    while (w) {
        if (!p->rk || !strcmp(p->rk, w->rk)) {
            if (!w->watched) {
                w->watched = true;
                w->cb(w->watcher_context, w->rk, p->path, p->path_context);
                break;
            }
        }
        w = w->next;
    }
}

static void eloop_fatal(int errno, const char *str) {
    fprintf(stderr, "eloop internal fatal error %s: %s\n", str, strerror(errno));
    fflush(stderr);
    abort();
}

#include "foo_transport_common.h"
#include "foo_transport_tcp.c"
#include "foo_transport_eloop.c"
