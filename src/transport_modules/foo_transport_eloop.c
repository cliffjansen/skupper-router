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

#include <sys/eventfd.h>

// "depth" values must be power-of-2
// goal is "big enough" to avoid ring overflow.
// user space controls size of max pending batched submissions but not kernel generated completions.
#define SUBMISSION_Q_DEPTH 64
#define COMPLETION_Q_DEPTH (8 * SUBMISSION_Q_DEPTH)

// hardwire the two known adapters: 0 = IRL, 1 = tcp
static const eloop_callbacks_t impl[2] = {
    {
        irl_accept_done_ZZZ,
        irl_connect_done_ZZZ,
        irl_close_done_ZZZ,
        irl_socket_move_done_ZZZ,
        irl_recv_done_ZZZ,
        irl_send_done_ZZZ,
        irl_shutwr_done_ZZZ,
    } ,
    {
        tcp_accept_done,
        tcp_connect_done,
        tcp_connection_close_done,
        tcp_socket_move_done,
        tcp_recv_done,
        tcp_send_done,
        tcp_shutwr_done
    }
};

// reusable completion user data
static base_ringio_t eventfd_io = { .io_type = RNG_EVENTFD };
static base_ringio_t rng_to_rng_io = { .io_type = RNG_TO_RNG };
static uint64_t eventfd_read_buf;  // where to place the read data, shared by threads and not checked.

static bool create_io_ring(eloop_t *loop) {
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    // SINGLE_ISSUER recommended for performance.  Ring creator must also be the sq issuer.
    params.flags |= IORING_SETUP_SINGLE_ISSUER;
    params.flags |= IORING_SETUP_SUBMIT_ALL | IORING_SETUP_CQSIZE;
    params.cq_entries = COMPLETION_Q_DEPTH;
    // DEFER_TASKRUN recommended.  COOP worth benchmarking.
    if (!!getenv("SKRFOO_COOP"))
        params.flags |= IORING_SETUP_DEFER_TASKRUN;
    else
        params.flags |= IORING_SETUP_COOP_TASKRUN;
    // TODO: try IORING_FEAT_NODROP again.  Got EINVAL on Fedora 41.
    // Consider MAP_HUGETLB|MAP_HUGE_2MB mmap buffers with IORING_SETUP_NO_MMAP and elevated perms.

    int ret = io_uring_queue_init_params(SUBMISSION_Q_DEPTH, &loop->ring, &params);
    if (ret < 0) {
        qd_log(LOG_ROUTER, QD_LOG_ERROR, "foo transport uring creation failure: %s", strerror(-ret));
        return false;
    }
    return true;
}

static bool create_buffer_pool(eloop_t *loop, eloop_buf_pool_t *bp) {
    // Someday perhaps, use a buffer pool manager that grows and shrinks
    // collections of pools of different sizes per ring.
    // Fixed for now.
    assert(!bp->registered);

    size_t buf_pool_mem_size = (sizeof(struct io_uring_buf) + bp->buf_size) * bp->buf_count;
    void *mapped = mmap(NULL, buf_pool_mem_size, PROT_READ | PROT_WRITE,
                  MAP_ANONYMOUS | MAP_PRIVATE, 0, 0);
    if (mapped == MAP_FAILED) {
        qd_log(LOG_ROUTER, QD_LOG_ERROR, "foo transport buffer pool init failure: %s", strerror(errno));
        return false;
    }
    // front is uring lib data struct, tail is the base of the pool buffer memory
    bp->br = (struct io_uring_buf_ring *)mapped;
    bp->pool_base = (unsigned char *)bp->br + bp->buf_count * sizeof(struct io_uring_buf);

    struct io_uring_buf_reg reg;
    ZERO(&reg);
    reg.ring_addr = (unsigned long)bp->br;
    reg.ring_entries = bp->buf_count;
    reg.bgid = bp->pool_id;
    int ret = io_uring_register_buf_ring(&loop->ring, &reg, 0);
    if (ret) {
        qd_log(LOG_ROUTER, QD_LOG_ERROR, "foo transport buffer pool register failure: %s", strerror(-ret));
        // release mapped mem!
        return false;
    }
    bp->br_mask = io_uring_buf_ring_mask(reg.ring_entries);
    bp->registered = true; // registered in kernel
    io_uring_buf_ring_init(bp->br); // user space buffer ring can now be used

    // Reserve sizeof(buf_desc_t) bytes at the front of each buffer
    for (int i = 0; i < bp->buf_count; i++) {
        io_uring_buf_ring_add(bp->br, get_buffer_mem(bp, i) + sizeof(buf_desc_t),
                              bp->buf_size - sizeof(buf_desc_t), i,
                              io_uring_buf_ring_mask(bp->buf_count), i);
    }
    io_uring_buf_ring_advance(bp->br, bp->buf_count);
    return true;
}

void eloop_recycle_buffer(eloop_t *loop, buf_desc_t *bd) {
    assert(bd->pool_id <= 1);
    eloop_buf_pool_t *bp = &loop->buf_pools[bd->pool_id];
    io_uring_buf_ring_add(bp->br, buf_payload(bd),
                          bp->buf_size - sizeof(buf_desc_t), bd->buf_id,
                          bp->br_mask, bp->recycle_ops++);
    // needs io_uring_buf_ring_advance() per pool before next call into kernel.
}

static void footp_cleanup(footp_module_t *module) {
    // TODO
}

/* ------------------------------------------------------------------------ */
// General IO submissions

void eloop_post_accept(eloop_t *loop, base_ringio_t *io, int listenfd, int priority) {
    assert(io->io_type == RNG_ACCEPT && (io->adapter_id <= 1));
    assert(((__u64)io & 0x7) == 0);
    struct io_uring_sqe *sqe = io_uring_get_sqe(&loop->ring);
    io_uring_prep_accept(sqe, listenfd, NULL, NULL, 0);
    sqe->user_data = (__u64) io;
    loop->ios_pending++;
}

void eloop_post_connect(eloop_t *loop, base_ringio_t *io, int outbound_sock, struct sockaddr *addr, socklen_t addr_len, int priority) {
    assert(io->io_type == RNG_CONNECT && (io->adapter_id <= 1));
    assert(((__u64)io & 0x7) == 0);
    struct io_uring_sqe *sqe = io_uring_get_sqe(&loop->ring);
    io_uring_prep_connect(sqe, outbound_sock, addr, addr_len);
    sqe->user_data = (__u64) io;
    loop->ios_pending++;
}

void eloop_post_close(eloop_t *loop, base_ringio_t *io, int sockfd, int priority) {
    assert(io->io_type == RNG_CLOSE && (io->adapter_id <= 1));
    assert(((__u64)io & 0x7) == 0);
    struct io_uring_sqe *sqe = io_uring_get_sqe(&loop->ring);
    io_uring_prep_close(sqe, sockfd);
    sqe->user_data = (__u64) io;
    loop->ios_pending++;
}

void eloop_post_recv(eloop_t *loop, rcv_io_t *rcv_io, int sockfd, uint32_t maxcount, bool immediate, int priority) {
    assert(rcv_io->base_io.io_type == RNG_RECV && (rcv_io->base_io.adapter_id <= 1));
    assert(((__u64)rcv_io & 0x7) == 0);
    assert(maxcount != 0);
    // Pick a pool.
    eloop_buf_pool_t *bp = &loop->buf_pools[0];
    if (immediate && lp_buf_size && loop->buf_pools[1].in_use < loop->buf_pools[1].buf_count) {
        bp = &loop->buf_pools[1];
        loop->buf_pools[1].in_use++;
    }
    uint16_t pool_id = bp->pool_id;
    uint32_t count = bp->buf_size - sizeof(buf_desc_t);
    if (count > maxcount)
        count = maxcount;
    struct io_uring_sqe *sqe = io_uring_get_sqe(&loop->ring);
    io_uring_prep_recv(sqe, sockfd, NULL, count, 0);
    //  sqe->flags |= IOSQE_FIXED_FILE; // ZZZZZZZ
    sqe->flags |= IOSQE_BUFFER_SELECT;
    sqe->buf_group = pool_id;
    // If socket fully drained on previous recv, go straight to poll
//    if (!immediate)
//        sqe->flags |= IORING_RECVSEND_POLL_FIRST;
//  revisit: why does this flag fail and result in EBADF?  Interaction with other ring flags?

    sqe->user_data = (__u64) rcv_io;
    rcv_io->eloop_private = pool_id;
//    fprintf(stderr, "ZZZ recv debug, %d %d %d %p\n", (int)sockfd, (int)count, (int)immediate, (void *)rcv_io);
    loop->ios_pending++;
}

// This function may choose to defer and reorder sends.
// May also choose to send less than the full payload (throttle)base_io.
// Undefined behaviour if caller provides more than one pending send
void eloop_post_send(eloop_t *loop, snd_io_t *snd_io, int sockfd, int priority) {
    assert(snd_io->base_io.io_type == RNG_SEND && (snd_io->base_io.adapter_id <= 1));
    assert(((__u64)snd_io & 0x7) == 0);
    struct io_uring_sqe *sqe = io_uring_get_sqe(&loop->ring);
    buf_desc_t *bd = snd_io->buf_desc;

    int flags = MSG_WAITALL;
    if (bd->more) 
        flags |= MSG_MORE;
    unsigned char *bytes = buf_payload(bd) + bd->offset;
    uint16_t count = bd->len;
    // TODO: throttle? if (output_is_saturated) count = max(count, max_hog_bytes);
    io_uring_prep_send(sqe, sockfd, bytes, count, flags);
    sqe->user_data = (__u64) snd_io;
    snd_io->eloop_private = count;
    loop->staged_output += count;
    loop->ios_pending++;
}

void eloop_post_shutwr(eloop_t *loop, base_ringio_t *io, int sockfd, int priority) {
    assert(io->io_type == RNG_SHUTWR && (io->adapter_id <= 1));
    assert(((__u64)io & 0x7) == 0);
    struct io_uring_sqe *sqe = io_uring_get_sqe(&loop->ring);
    io_uring_prep_shutdown(sqe, sockfd, SHUT_WR);
    sqe->user_data = (__u64) io;
    loop->ios_pending++;
}

void eloop_post_socket_move(eloop_t *loop, base_ringio_t *io, eloop_t *otherloop, int sockfd, int priority) {
    assert(io->io_type == RNG_SOCK_MOV && (io->adapter_id <= 1));
    assert(((__u64)io & 0x7) == 0);
    struct io_uring_sqe *sqe = io_uring_get_sqe(&loop->ring);
    // ring to ring msg generates two CQEs, io goes to other loop and this one gets rng_to_rng_io
    io_uring_prep_msg_ring(sqe, otherloop->ring.ring_fd, sockfd, (__u64) io, 0);
    sqe->user_data = (__u64) &rng_to_rng_io;
    loop->ios_pending++;
}    

// TODO: FIFO of reuseable work_t's, maybe global or per loop, DEQ_DECLARE etc.
void eloop_add_work(eloop_t *loop, eloop_work_cb_t cb, void *context) {
    eloop_work_t *w = (eloop_work_t *) malloc(sizeof(eloop_work_t));
    w->cb = cb;
    w->context = context;
    w->next = NULL;
    sys_mutex_lock(&loop->lock);
    if (!loop->ts_work_head)
        loop->ts_work_head = w;
    else {
        eloop_work_t *w2 = loop->ts_work_head;
        while (w2->next) w2 = w2->next;
        w2->next = w;
    }
    sys_mutex_unlock(&loop->lock);
    uint64_t increment = 1;
    write(loop->eventfd, &increment, sizeof(uint64_t));
}

/* ------------------------------------------------------------------------ */

#define BATCH_SIZE 16

static void process_completion(eloop_t *loop, struct io_uring_cqe *cqe) {
    struct io_uring *rng = &loop->ring;
    struct io_uring_sqe *sqe;
    uint64_t data = (uint64_t) io_uring_cqe_get_data(cqe);
//    int priority = data & 0x7;
    base_ringio_t *io = (base_ringio_t *)(data & ~0x7);
    ringio_type_t io_type = (ringio_type_t) io->io_type;
    const unsigned id = io->adapter_id;
    loop->ios_pending--;
//    fprintf(stderr, "ZZZ **cqe** %d %p %p %d %d %d\n", loop->loop_id, (void *) data, (void *) io, io_type, id, loop->ios_pending);
    switch (io_type) {
    case RNG_EVENTFD: {
        loop->notified = true;
        // rearm, even if shutting down.
        sqe = io_uring_get_sqe(rng);
        io_uring_prep_read(sqe, loop->eventfd, &eventfd_read_buf, sizeof(uint64_t), 0);
        sqe->user_data = (__u64) &eventfd_io;
        loop->ios_pending++;
        break;
    }
    case RNG_LISTEN: {
        // TODO
        break;
    }
    case RNG_ACCEPT: {
        int sockfd_or_errno = cqe->res;
        impl[id].accept_done(io, sockfd_or_errno);
        break;
    }
    case RNG_CONNECT: {
        int sockfd_or_errno = cqe->res;
        impl[id].connect_done(io, sockfd_or_errno);
        break;
    }
    case RNG_RECV: {
        rcv_io_t *rcv_io = (rcv_io_t *) io;
        uint16_t buf_id = 0;
        uint16_t pool_id = 0;
        bool immediate_read = false;
        int count_or_errno = cqe->res;
        if (count_or_errno > 0) {
            assert(cqe->flags & IORING_CQE_F_BUFFER);
            buf_id = cqe->flags >> 16;
            if (cqe->flags & IORING_CQE_F_SOCK_NONEMPTY)
                immediate_read = true;
            pool_id = rcv_io->eloop_private & 0xFF;
            // TODO track pool use count
        }
        impl[id].recv_done(io, pool_id, buf_id, count_or_errno, immediate_read);
        break;
    }
    case RNG_SEND: {
        snd_io_t *snd_io = (snd_io_t *) io;
        uint16_t unstaged = snd_io->eloop_private;
        loop->staged_output -= unstaged;
        int count_or_errno = cqe->res;
        if (count_or_errno > 0 && count_or_errno != unstaged)
            fprintf(stderr, "ZZZ snd cqe short: %d of %d\n", count_or_errno, (int) unstaged);
        impl[id].send_done(io, count_or_errno);
        break;
    }
    case RNG_SHUTWR: {
        int sys_errno = cqe->res;
        impl[id].shutwr_done(io, sys_errno);
        break;
    }
    case RNG_CLOSE: {
        int sys_errno = cqe->res;
        impl[id].close_done(io, sys_errno);
        break;
    }
    case RNG_SOCK_MOV: {
        int sockfd = cqe->res;
        // submit was from other ring, fix accounting
        loop->ios_pending++;
        impl[id].socket_move_done(io, sockfd, loop);
        break;
    }
    case RNG_TO_RNG: {
        if (cqe->res) {
            qd_log(LOG_ROUTER, QD_LOG_ERROR, "foo transport eloop ring to ring error from loop%d: %s", loop->loop_id, strerror(-cqe->res));
        }
        break;
    }
    default: {
            qd_log(LOG_ROUTER, QD_LOG_ERROR, "foo transport main loop internal error loop%d: %d", loop->loop_id, io_type);
            break;
    }
    }
}

static void * footp_worker_thread(void *thr_arg) {
    eloop_t *loop = (eloop_t *) thr_arg;
    struct io_uring *rng = &loop->ring;
    struct io_uring_cqe *cqe_batch[BATCH_SIZE];
    struct io_uring_sqe *sqe;
//    struct io_uring_cqe *cqe;
    uint64_t tot_loops = 0;
    uint64_t tot_cqes = 0;
    uint64_t tot_batches = 0;
    int ret;
    unsigned int count;

    if (!create_io_ring(loop)) {
        fprintf(stderr, "fatal startup snafoo ring creation\n");
        fflush(stderr);
        abort();
    }
    int pool_count = 0;
    if (sp_buf_size) {
        eloop_buf_pool_t *bp = &loop->buf_pools[0];
        bp->pool_id = 0;
        bp->buf_count = 256;  // sensible sizing TBD.  must be power-of-2
        bp->buf_size = sp_buf_size;
        if (!create_buffer_pool(loop, bp)) {
            fprintf(stderr, "fatal startup snafoo buf pool creation\n");
            fflush(stderr);
            abort();
        }
        pool_count++;
    }
    if (lp_buf_size) {
        eloop_buf_pool_t *bp = &loop->buf_pools[1];
        bp->pool_id = 1;
        bp->buf_count = 32;  // sensible sizing TBD.  must be power-of-2
        bp->buf_size = lp_buf_size;
        if (!create_buffer_pool(loop, bp)) {
            fprintf(stderr, "fatal startup snafoo buf pool creation\n");
            fflush(stderr);
            abort();
        }
        pool_count++;
    }
    if (pool_count == 0) abort();

    // Caller has locked module->lock until all ring structs are initialized.
    // Pause thread here to get all inter-ring static memory in sync
    // In particular otherloop->ring->ring_fd
    footp_module_t *module = footp_module_context;
    sys_mutex_lock(&module->lock);
    sys_mutex_unlock(&module->lock);

    // pin CPUs if requested.  "07B" assigns loops 0,1,2 to cpus 0,7,11
    const char* str = getenv("SKRFOO_PINCPU");
    if (str && strlen(str) == loop->module->eloop_count) {
        char ehx = str[loop->loop_id];
        int cpu = -1;
        if (ehx >= '0' && ehx <= '9')
            cpu = ehx - '0';
        else if (ehx >= 'A' && ehx <= 'Z')
            cpu = 10 + (ehx - 'A');
        if (cpu >= 0) {
            cpu_set_t cs; // TODO: _S generic
            CPU_ZERO(&cs);
            CPU_SET(cpu, &cs);
            // Pin this thread to the cpu.
            if (sched_setaffinity(0, sizeof(cs), &cs))
                qd_log(LOG_ROUTER, QD_LOG_ERROR, "foo transport cpu affinity error: eloop %d to cpu %d: %s", loop->loop_id, cpu, strerror(errno));
            else
                qd_log(LOG_ROUTER, QD_LOG_INFO, "foo transport eloop %d pinned to cpu %d", loop->loop_id, cpu);
            // Consider: io_uring_register_iowq_aff(this_rng, sizeof(cs), &cs).
        }
    }

    // Always arm the eventfd
    sqe = io_uring_get_sqe(rng);
    io_uring_prep_read(sqe, loop->eventfd, &eventfd_read_buf, sizeof(uint64_t), 0);
    sqe->user_data = (__u64) &eventfd_io;
    loop->ios_pending++;

    loop->running = true;
    // The loop.
    // Post newest SQEs.
    //   Allow the kernel to do ready io operations based on previous and new SQEs.
    //     This is the TASK_RUN efficiency dance between user space and kernel.
    // Gather a batch of CQEs.
    // Schedule work based on priorities or dwindling resources (buffer pools?).
    // Do high priority work and defer lower priority work if busy
    //   This will generate a new batch of SQEs
    //   Deferred CQEs can be copied somewhere for later processing.
    //   Keep an eye on SQE/CQE/buf_pool rings and prevent overflows
    // Repeat.

    do {
        for (int i = 0; i < 2; i++) {
            eloop_buf_pool_t *bp = &loop->buf_pools[i];
            if (bp->recycle_ops) {
                // let kernel see newly available recycled buffers.  Fast memory op in user space.
                io_uring_buf_ring_advance(bp->br, bp->recycle_ops);
                if (bp->pool_id == 1)
                    bp->in_use -= bp->recycle_ops;
                bp->recycle_ops = 0;
            }
        }
        unsigned wait_nr = 1;  // set to zero if we already have (deferred) CQEs pending.
        // The kernel may do a lot of queued work in the following call.  Hold no locks.
        ret = io_uring_submit_and_wait(rng, wait_nr);
        // maybe ret = io_uring_submit_and_wait_timeout(rng, &some_cqe_ptr_array, wait_nr, ts, sigask);
        if (ret == -EINTR)
            continue;
        if (ret < 0) {
            fprintf(stderr, "submit and wait failed %d\n", ret);
            abort();
        }
        tot_loops++;

        bool work_prep = true;
        while (work_prep) {
            count = io_uring_peek_batch_cqe(rng, &cqe_batch[0], BATCH_SIZE);
            if (count) {
                tot_batches++;
                tot_cqes += count;
            }
            if (count < BATCH_SIZE)
                work_prep = false;  // we got them all
#ifdef coming_soon
            for (int i = 0; i < count; i++) {
                cqe = cqe_batch[i];
                // At this point we only look at the pointer value just copied by the Kernel
                // into the cqe, not the data poihted to, which may be very cold.
                uint64_t data = io_uring_cqe_get_data(cqe);
                int pri = data & 0x7;
                // sort here based on priority
            }
#endif
            // Do high priority completions right away (currently all of them).
            // Possibly set "work_prep = false" if we need to get the kernel to flush io
            // before completing mini batches or low priority work.
            for (int i = 0; i < count; i++) {
                process_completion(loop, cqe_batch[i]);
            }

            // track how close we are to a kernel cqe overflow, including new un-peeked cqes
            unsigned ncqe = io_uring_cq_ready(rng);
            if (ncqe > loop->max_ready_cqe)
                loop->max_ready_cqe = ncqe;
            // move ring past the batch.  We need to make a local copy of each *cqe being deferred
            io_uring_cq_advance(rng, count);
        }

        // TODO: process lower priority cqes here, or a subset before going back to kernel.

        if (loop->notified) {
            // work_queue functions so far are rare and brief, so just do it all, rethink scheduling later
            loop->notified = false;
            eloop_work_t *wrk = NULL;
            sys_mutex_lock(&loop->lock);
            wrk = loop->ts_work_head;
            loop->ts_work_head = NULL;
            sys_mutex_unlock(&loop->lock);
            while (wrk) {
                wrk->cb(loop, wrk->context);
                eloop_work_t *next = wrk->next;
                free(wrk);
                wrk = next;
            }
        }
    } while (loop->running);

    // Router should have gracefully closed all listeners and connections at this point.
    // TODO: log stats, proper cleanup of ring, buffers, eventfd... all the things
    // Just exit thread for now.
    return NULL;
}

static void loop_shutdown(eloop_t *l, void *ignored) {
    l->running = false;
}

static void startupZZZ(eloop_t *l, void *ctx) {
    fprintf(stderr, "ZZZ test string from %d %d\n", l->loop_id, (int) (int64_t) ctx);
    fflush(stderr);
}     


void footp_startup(footp_module_t *module) {
    // TODO: real config.  use getenv for now.
    sys_mutex_init(&module->lock);
    module->eloop_count = 2;
    const char* str = getenv("SKRFOO_NRINGS");
    if (str && *str) {
        int nr = atoi(str);
        if (nr > 0 && nr <= 32)
            module->eloop_count = nr;
    }
    str = getenv("SKRFOO_BSZ");
    if (str && *str) {
        int sz = atoi(str);
        if (sz > 1024 && sz < 65536)
            sp_buf_size = sz;
        if (sp_buf_size % 256)
            fprintf(stderr, "ZZZ main pool buffer size choice has dubious alignment\n");
    }
    str = getenv("SKRFOO_LBSZ");
    if (str && *str) {
        int sz = atoi(str);
        if (sz > 1024 && sz < 65536)
            lp_buf_size = sz;
        if (lp_buf_size % 256)
            fprintf(stderr, "ZZZ large buffer pool buffer size choice has dubious alignment\n");
    }

    module->eloops = calloc(module->eloop_count, sizeof(eloop_t *));
    sys_mutex_lock(&module->lock);
    int i = 0;
    for (; i < module->eloop_count; i++) {
        eloop_t *loop = calloc(1, sizeof(eloop_t));
        module->eloops[i] = loop;
        loop->loop_id = i;
        loop->module = module;
        sys_mutex_init(&loop->lock);
        loop->eventfd = eventfd(0, 0);  // TODO: err check
        int rc = pthread_create(&loop->thread, 0, footp_worker_thread, (void *) loop);
        (void) rc;
        assert(rc == 0);
        char name[10];
        uint8_t tid = i & 0x3F;
        snprintf(name, 10, "footp%02d", tid);
        pthread_setname_np(loop->thread, name);
    }
    sys_mutex_unlock(&module->lock);
    if (i < module->eloop_count) {
        fprintf(stderr, "ZZZ debug multi loop setup fail\n"); fflush(stderr);
        footp_cleanup(module);
        return;
    }
    fprintf(stderr, "ZZZ debug try 2\n"); fflush(stderr);
    // ZZZ startup should wait for sync from all (all can see loop0 mem ok)
    // So all can signal event
    // last loop can see all earlier ones and eventfds, so can force sync.
    // ...or just use the global lock to sync up.
    eloop_add_work(module->eloops[0], startupZZZ, (void *)11);
}

void footp_shutdown(footp_module_t *module) {
    // TODO...
    // ensure full cleanup, *module about to be free()'d
    sys_mutex_lock(&module->lock);
    sys_mutex_unlock(&module->lock);
    for (int i = 0; i < module->eloop_count; i++) {
        eloop_add_work(module->eloops[i], loop_shutdown, NULL);
    }
    for (int i = 0; i < module->eloop_count; i++) {
        fprintf(stderr, "ZZZ joining %d\n", i); fflush(stderr);
        pthread_join(module->eloops[i]->thread, 0);
    }
    // This cleanup bit belongs elsewhere and generalized
    foo_rkey_watcher_t *w = module->watchers_head;
    while (w) {
        foo_tcp_listener_t *li = (foo_tcp_listener_t *) w->watcher_context;
        w = w->next;
        listener_cleanup(li);
    }
    foo_rkey_provider_t *p = module->providers_head;
    while (p) {
        foo_tcp_connector_t *ctor = (foo_tcp_connector_t *) p->path_context;
        p = p->next;
        connector_cleanup(ctor);
    }
    fprintf(stderr, "ZZZ shutdn complete\n"); fflush(stderr);
    fprintf(stderr, "ZZZ debug %d %d %d\n", (int) sizeof(struct io_uring_cqe), (int) sizeof(struct io_uring_sqe),
            (int) sizeof(base_ringio_t)); fflush(stderr);
}
