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

typedef struct foo_tcp_listener_t {
    eloop_t *loop;
    qd_adaptor_config_t *adaptor_config;
    int sockfd;
    foo_path_t *path;
    void *path_context;
    base_ringio_t accept_io;
    base_ringio_t socket_move_io;
    int accept_loop_idx;  // round robin counter
    bool closing;
    bool close_posted;
    bool wclose_needed;
    bool wclose_sent;
    bool read_closed;
} foo_tcp_listener_t;

typedef struct foo_tcp_connector_t {
    qd_adaptor_config_t *adaptor_config;
    struct sockaddr_in connect_addr;
} foo_tcp_connector_t;

typedef enum {
    TCP_SETUP = 0,
    TCP_RUNNING,
    TCP_CLOSED
} tcp_connection_state_t;

typedef struct foo_tcp_connection_t {
    foo_io_t foo_io;
    eloop_t *loop;
    foo_tcp_connector_t *connector;
    qd_adaptor_config_t *adaptor_config;
    base_ringio_t connect_io;
    base_ringio_t close_io;
    base_ringio_t shutwr_io;
    
    rcv_io_t rcv_io;        // can ony be one in progress at a time
    uint64_t rcv_ops;
    uint64_t rcv_total;
    uint32_t rcv_buffered;
    uint32_t rcv_buffered_max;
    uint32_t rcv_hogcount;
    bool rcvop_posted;
    uint8_t priority;
    uint64_t credit_stalls;
    uint64_t hog_stalls;

    snd_io_t snd_io;       // One at a time
    buf_desc_t *sndbuf_first;
    buf_desc_t *sndbuf_last;
    bool sndop_posted;
    uint64_t snd_ops;
    uint64_t snd_total;
    uint64_t rcv_more;
    uint64_t snd_more;

    int32_t sockfd;
    uint32_t rcv_credit;         // provided by remote router for backpressure
    uint32_t max_send_credit;    // provided to remote router
    uint32_t new_send_credit;

    unsigned ops_pending;
    unsigned tracked_bufs;       // Both incoming (recv) and outgoing (fwd_buf)
    bool ended;                  // Both io's in the pair are inactive... cleanup possible.
    bool immediate_rcv;
    bool closing;
    bool shutwr_needed;
    bool shutwr_sent;
    bool read_closed;
    bool write_closed;
    bool other_ending;            // other_io will never contact us again
    bool ending;                  // reverse
    bool cleanup_scheduled;
    bool error_reported;          // max once in io pair.  Stop all buffer flows immediately and cleanup
    tcp_connection_state_t state; // revisit ZZZ
} foo_tcp_connection_t;


// ------------------------------------------------------------------------
// Listener

static void listener_init(foo_tcp_listener_t *li) {
    li->sockfd = -1;
    footp_module_t *module = footp_module_context;
    li->loop = module->eloops[0];  // arbitrary for now
    ZERO(&li->accept_io);
    li->accept_io.io_type = RNG_ACCEPT;
    li->accept_io.adapter_id = TCP_ID;
    ZERO(&li->socket_move_io);
    li->socket_move_io.io_type = RNG_SOCK_MOV;
    li->socket_move_io.adapter_id = TCP_ID;
}

static void listener_cleanup(foo_tcp_listener_t *li) {
    qd_free_adaptor_config(li->adaptor_config);
    // TODO: full cleanup
}

static foo_tcp_connection_t *new_foo_tcp_connection(bool is_lside, int sockfd, eloop_t *thisloop);

static void new_lside_connection(foo_tcp_listener_t *li, int sockfd, eloop_t *thisloop) {
    fprintf(stderr, "ZZZ got us a new socket %d on loop %d\n", sockfd, thisloop->loop_id);
    // Create lside connection object which contains the foo_io_t
    // then hand it over to the routing key path instance to make the second io and join them up.
    foo_tcp_connection_t *conn = new_foo_tcp_connection(true, sockfd, thisloop);
    assert(conn->foo_io.lside == true);
    assert(conn->foo_io.adapter_id == TCP_ID);
    const char *rk = li->adaptor_config->address;
    li->path->make_routed_pair(li->path_context, &conn->foo_io, rk, thisloop);
    // other_io will notify when to start reading or writing (or setup failure).
}

static void tcp_make_routed_pair(void *path_ctx, foo_io_t *first_io, const char *rk, eloop_t *loop) {
    foo_tcp_connector_t *ctor = (foo_tcp_connector_t *) path_ctx;
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) eloop_fatal(sockfd, "outbound socket");
    foo_tcp_connection_t *conn = new_foo_tcp_connection(false, sockfd, loop);
    assert(conn->foo_io.lside == false);
    assert(conn->foo_io.adapter_id == TCP_ID);
    foo_io_t *second_io = &conn->foo_io;
    first_io->other_io = second_io;
    second_io->other_io = first_io;
    // Routed pair is configured, next hop is the outbound tcp connection
    // first_io waits for credit or "connect fail" information for its next steps.
    eloop_post_connect(conn->loop, &conn->connect_io, conn->sockfd, (struct sockaddr *) &ctor->connect_addr, sizeof(struct sockaddr_in), 0);
    conn->ops_pending++;
}

static void post_accept(foo_tcp_listener_t *li) {
    eloop_post_accept(li->loop, &li->accept_io, li->sockfd, 0);
}

static void tcp_accept_done(void *user_data, int sockfd) {
    foo_tcp_listener_t *li = containerof(user_data, foo_tcp_listener_t, accept_io);
    footp_module_t *module = footp_module_context;
    if (sockfd >= 0) { 
        // Choose a target loop for the new socket
        // Simple round robin for now, in future based on priority and even distributed load
        eloop_t *thisloop, *otherloop;
        thisloop = otherloop = li->loop;
        if (module->eloop_count) {
            li->accept_loop_idx = (li->accept_loop_idx + 1) % module->eloop_count;
            otherloop = module->eloops[li->accept_loop_idx];
        }
        if (thisloop == otherloop)
            new_lside_connection(li, sockfd, thisloop);
        else {
            // logic resumes at tcp_socket_move_done callback on otherloop's thread
            eloop_post_socket_move(thisloop, &li->socket_move_io, otherloop, sockfd, 0);
        }
    } else {
        qd_log(LOG_ROUTER, QD_LOG_ERROR, "footp tcp listener accept failure : %s:%s: %s", li->adaptor_config->host, li->adaptor_config->port, strerror(errno));
    }
    if (!li->closing)
        post_accept(li);
}

static void tcp_socket_move_done(void *user_data, int sockfd, eloop_t *thisloop) {
    assert(sockfd >= 0);
    foo_tcp_listener_t *li = containerof(user_data, foo_tcp_listener_t, socket_move_io);
    new_lside_connection(li, sockfd, thisloop);
}


static void foo_tcp_start_listener(eloop_t *loop, void *ctx) {
    foo_tcp_listener_t *li = (foo_tcp_listener_t *) ctx;
    assert(li->loop == loop);
    static int option_on = 1;
    uint16_t port = atoi(li->adaptor_config->port);
    struct sockaddr_in addr = (struct sockaddr_in) {
        .sin_family = AF_INET,
        .sin_addr.s_addr = inet_addr(li->adaptor_config->host),
        .sin_port = htons(port)
    };

    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock >= 0) {
        if (!setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR, &option_on, sizeof(option_on)) &&
            !bind(server_sock, (const struct sockaddr*) &addr, sizeof(addr)) &&
            !listen(server_sock, li->adaptor_config->backlog)) {
            li->sockfd = server_sock;
            eloop_post_accept(li->loop, &li->accept_io, li->sockfd, 0);
            qd_log(LOG_ROUTER, QD_LOG_INFO, "footp tcp listener started: %s:%s", li->adaptor_config->host, li->adaptor_config->port);
            post_accept(li);
            return;
        }
        close (server_sock);
    }

    qd_log(LOG_ROUTER, QD_LOG_ERROR, "footp tcp listener init failure : %s:%s: %s", li->adaptor_config->host, li->adaptor_config->port, strerror(errno));
    // de-register, self-delete
}

static void listener_rk_watch_cb(void *ctx, const char *rk, foo_path_t *s, void *path_ctx) {
    foo_tcp_listener_t *l = (foo_tcp_listener_t *) ctx;
    l->path = s;
    l->path_context = path_ctx;
    fprintf(stderr, "rk watch... time to listen, next hop %p\n", l->path_context);
    assert(!strcmp(rk,l->adaptor_config->address));
    eloop_add_work(l->loop, foo_tcp_start_listener, l);
}

// ------------------------------------------------------------------------
// Connector/connection

// Max bytes to write into a socket without completion callback in case of slow consumer.
// Value TBD based on runtime experience.
#define TCP_MAX_WR (256 * 1024)

static void tcp_configure_socket(int sock) {
    // TODO: do this via io_uring calls
    int tcp_nodelay = 1;
    (void)setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (void*) &tcp_nodelay, sizeof(tcp_nodelay));
}

static foo_tcp_connection_t *new_foo_tcp_connection(bool is_lside, int sockfd, eloop_t *thisloop) {
    foo_tcp_connection_t *c = malloc(sizeof(foo_tcp_connection_t));
    ZERO(c);
    c->foo_io.lside = is_lside;
    c->foo_io.adapter_id = TCP_ID;
    c->sockfd = sockfd;
    c->loop = thisloop;
    c->connect_io.io_type = RNG_CONNECT;
    c->connect_io.adapter_id = TCP_ID;
    c->close_io.io_type = RNG_CLOSE;
    c->close_io.adapter_id = TCP_ID;
    c->shutwr_io.io_type = RNG_SHUTWR;
    c->shutwr_io.adapter_id = TCP_ID;
    c->rcv_io.base_io.io_type = RNG_RECV;
    c->rcv_io.base_io.adapter_id = TCP_ID;
    c->snd_io.base_io.io_type = RNG_SEND;
    c->snd_io.base_io.adapter_id = TCP_ID;
    tcp_configure_socket(sockfd);
    // bogus guess for hog amount.  Configurable? want hogcount + 1 big buf < too_much
    // where too_much suggests inbound bytes are comming faster than they get processed
    // and we need some backpressure.
    c->rcv_hogcount = 22222;  // ZZZ
    fprintf(stderr, "ZZZ new tcp connection  (%d,%d): %p\n",
           c->sockfd, c->foo_io.lside, (void *) c);
    return c;
}

static void connector_cleanup(foo_tcp_connector_t *ctor) {
    qd_free_adaptor_config(ctor->adaptor_config);
    // TODO: full cleanup
}

static void connection_cleanup(eloop_t *l, void *ctx) {
    foo_tcp_connection_t *conn = (foo_tcp_connection_t *) ctx;
    assert(conn->sockfd == -1);
    fprintf(stderr, "ZZZ foo cleanup %p\n", (void *) conn);    
    foo_tcp_connection_t *c = conn; // ZZZ
    fprintf(stderr, "    ZZZ %d  %d %d %d . %d %d  :  %d %d  M %d %d\n",
            c->foo_io.lside,
            (int) c->rcv_ops, (int) c->rcv_total, (int) c->rcv_buffered_max,
            (int)c->credit_stalls, (int) c->hog_stalls,
            (int) c->snd_ops, (int) c->snd_total,
            (int) c->rcv_more, (int) c->snd_more);
    // TODO
    free(conn);
}

static void schedule_cleanup(foo_tcp_connection_t *conn) {
    // create low priority work item to delete mem when loop isn't busy
    eloop_add_work(conn->loop, connection_cleanup, (void *) conn);
}

static void tcp_maybe_cleanup(foo_tcp_connection_t *conn) {
    assert(conn->closing);
    if (conn->sockfd != -1 || conn->ops_pending  || conn->tracked_bufs)
        return;
    if (!conn->ending) {
        conn->ending = true;
        foo_io_t *other_io = conn->foo_io.other_io;
        io_impl[other_io->adapter_id].ending(other_io);
        if (conn->other_ending)
            conn->ended = true;
    }
    if (conn->ended && !conn->cleanup_scheduled) {
        conn->cleanup_scheduled = true;
        schedule_cleanup(conn);
        return;
    }
}

static void tcp_op_completed(foo_tcp_connection_t *conn) {
    conn->ops_pending--;
    if (conn->closing)
        tcp_maybe_cleanup(conn);
}

static void start_socket_close(foo_tcp_connection_t *conn, int sys_errno) {
    // Post an async close and start reaping any un-completed ops
    conn->write_closed = conn->read_closed = true;
    conn->rcv_credit = 0;
    if (sys_errno) {
        if (!conn->error_reported) {
            conn->error_reported = true;
            foo_io_t *other_io = conn->foo_io.other_io;
            io_impl[other_io->adapter_id].closed(other_io, 0, sys_errno);
        }
    }

    // unwritten buffers?
    buf_desc_t *bd = conn->sndbuf_first;
    if (bd) {
        if (conn->sndop_posted) {
            // leave first buf for the send_done callback
            bd = bd->next;
            conn->sndbuf_last = conn->sndbuf_first;
            conn->sndbuf_first->next = NULL;
        } else {
            conn->sndbuf_last = conn->sndbuf_first = NULL;
        }
    }
    foo_io_t *other_io = conn->foo_io.other_io;
    while (bd) {
        assert(conn->tracked_bufs > 0);
        conn->tracked_bufs--;
        buf_desc_t *bdnext = bd->next;
        bd->next = NULL;
        io_impl[other_io->adapter_id].recycle_buffer(other_io, bd);
        bd = bdnext;
    }

    if (!conn->closing) {
        conn->closing = true;
        if (conn->sockfd >= 0) {
            eloop_post_close(conn->loop, &conn->close_io, conn->sockfd, 0);
            conn->ops_pending++;
        }
    }
}

static void tcp_ending(foo_io_t *io) {
    foo_tcp_connection_t *conn = containerof(io, foo_tcp_connection_t, foo_io);
    conn->other_ending = true;
    if (conn->ending)
        conn->ended = true;
    if (!conn->closing) {
        start_socket_close(conn, 0);
    } else {
        tcp_maybe_cleanup(conn);
    }
}

static void tcp_connect_done(void *user_data, int sys_errno) {
    foo_tcp_connection_t *c = containerof(user_data, foo_tcp_connection_t, connect_io);
    fprintf(stderr, "ZZZ connect, time for credit %d\n", sys_errno);
    foo_io_t *other_io = c->foo_io.other_io;
    if (!sys_errno) {
        c->state = TCP_RUNNING;
        // Credit should be configurable.  Should be prepared to buffer all of it locally
        // if slow consumer.  The more hops on the journey, the more credit required to
        // prevent stalls.  Replenishment should not be too chatty.
        // max credit perhaps should take socket send buffer size into account.
        c->max_send_credit = 131072; // early guestimate for iperf3 over 0 hops.  TBD.
        io_impl[other_io->adapter_id].credit_updated(other_io, c->max_send_credit);
    } else {
        qd_log(LOG_ROUTER, QD_LOG_ERROR, "foo tcp connect fail: %s", strerror(-sys_errno));
        c->state = TCP_CLOSED;
        start_socket_close(c, -sys_errno);
    }
    tcp_op_completed(c);
}

static void tcp_connection_close_done(void *user_data, int res) {
    foo_tcp_connection_t *c = containerof(user_data, foo_tcp_connection_t, close_io);
    if (res < 0) {
        qd_log(LOG_ROUTER, QD_LOG_INFO, "TCP close failure: socket %d : %s", c->sockfd, strerror(-res));
    }
    fprintf(stderr, "ZZZ close done %d %d\n", c->sockfd, (int) c->ops_pending);
    c->sockfd = -1;
    tcp_op_completed(c);
}

static void tcp_post_shutwr(foo_tcp_connection_t *conn) {
    assert(!conn->shutwr_sent);
    conn->shutwr_sent = true;
    fprintf(stderr, "ZZZ shutwr posted %d\n", conn->sockfd);
    eloop_post_shutwr(conn->loop, &conn->shutwr_io, conn->sockfd, conn->priority);
    conn->ops_pending++;
}

static void tcp_post_send(foo_tcp_connection_t *conn) {
    assert(!conn->sndop_posted);
    buf_desc_t *bd = conn->sndbuf_first;
    conn->snd_io.buf_desc = bd;
    conn->sndop_posted = true;
    if (bd->more)
        conn->snd_more++;
    eloop_post_send(conn->loop, &conn->snd_io, conn->sockfd, conn->priority);
    conn->ops_pending++;
}

static void tcp_try_sending(foo_tcp_connection_t *conn) {
    if (!conn->sndop_posted && conn->sndbuf_first) {
        tcp_post_send(conn);
        return;
    }
    // No pending outbound buffers.  Check if write side closing.
    if (conn->shutwr_needed && !conn->shutwr_sent && !conn->write_closed) {
        tcp_post_shutwr(conn);
    }
}

static void tcp_fwd_buffer(foo_io_t *io, buf_desc_t *bd) {
    foo_tcp_connection_t *conn = containerof(io, foo_tcp_connection_t, foo_io);
    foo_io_t *other_io = conn->foo_io.other_io;
    conn->tracked_bufs++;
    if (conn->write_closed) {
        // We may have closed since we last gave credit to other_io
        assert(conn->tracked_bufs > 0);
        conn->tracked_bufs--;
        io_impl[other_io->adapter_id].recycle_buffer(other_io, bd);
        return;
    }

    // Add to outgoing buffer chain
    if (!conn->sndbuf_first) {
        conn->sndbuf_first = bd;
        conn->sndbuf_last = bd;
    } else {
        conn->sndbuf_last->more = true;
        conn->sndbuf_last->next = bd;
        conn->sndbuf_last = bd;
    }

    if (!conn->sndop_posted) {
        tcp_try_sending(conn);
    }
}

static void tcp_fwd_ended(foo_io_t *io) {
    foo_tcp_connection_t *conn = containerof(io, foo_tcp_connection_t, foo_io);
    conn->shutwr_needed = true;
    tcp_try_sending(conn);
}

static void tcp_shutwr_done(void *user_data, int count_or_error) {
    foo_tcp_connection_t *conn = containerof(user_data, foo_tcp_connection_t, shutwr_io);
    int shutwr_errno = -count_or_error;
    if (shutwr_errno != 0) {
        // Skip error report if socket closed for other reason
        if (!conn->closing) {
            qd_log(LOG_ROUTER, QD_LOG_ERROR, "foo tcp connection shutwr fail: %s", strerror(shutwr_errno));
            start_socket_close(conn, shutwr_errno);
        }
    }

    conn->write_closed = true;
    if (conn->read_closed)
        start_socket_close(conn, 0);

    tcp_op_completed(conn);
}

static void tcp_send_done(void *user_data, int count_or_error) {
    foo_tcp_connection_t *conn = containerof(user_data, foo_tcp_connection_t, snd_io);
    buf_desc_t *bd = conn->snd_io.buf_desc;
    assert(bd == conn->sndbuf_first);
    foo_io_t *other_io = conn->foo_io.other_io;
    uint16_t remaining = 0;

//    fprintf(stderr, "ZZZ snd done,   %d %d     %d %d\n", conn->sockfd, count_or_error, bd->pool_id, bd->buf_id);

    if (count_or_error < 0) {
        foo_io_t *other_io = conn->foo_io.other_io;
        qd_log(LOG_ROUTER, QD_LOG_ERROR, "foo tcp connection write fail (%d,%d): %s",
               conn->sockfd, conn->foo_io.lside, strerror(-count_or_error));
        io_impl[other_io->adapter_id].closed(other_io, 0, -count_or_error);
        start_socket_close(conn, 0);
    } else if (count_or_error == 0) {
        foo_io_t *other_io = conn->foo_io.other_io;
        qd_log(LOG_ROUTER, QD_LOG_ERROR, "foo tcp connection unexpected zero byte write");
        io_impl[other_io->adapter_id].closed(other_io, 0, 0);
        start_socket_close(conn, 0);
    } else {
        assert(count_or_error < (1 << 16));
        uint16_t count = (uint16_t) count_or_error;
        assert(count <= bd->len);

        remaining = bd->len - count;
        if (remaining) {
            // resubmit buffer
            bd->len -= count;
            bd->offset += count;
        }
        conn->new_send_credit += count;
        conn->snd_ops++;
        conn->snd_total += bd->len;
    }
    
    // This must remain true until last possible call to start_socket_close() within this funtion
    // and become false before the first possible call to tcp_try_sending()
    conn->sndop_posted = false;

    if (remaining == 0) {
        // Done with current buffer
        conn->sndbuf_first = conn->sndbuf_first->next;
        if (!conn->sndbuf_first)
            conn->sndbuf_last = NULL;
        assert(conn->tracked_bufs > 0);
        conn->tracked_bufs--;
        bd->next = NULL;
        io_impl[other_io->adapter_id].recycle_buffer(other_io, bd);
    }

    if (!conn->write_closed) {
        // Update credit frequently but not always
        if ((conn->new_send_credit * 4) >= conn->max_send_credit) {
            io_impl[other_io->adapter_id].credit_updated(other_io, conn->new_send_credit);
            conn->new_send_credit = 0;
        }
        // See if more write side activity.
        tcp_try_sending(conn);
    }

    tcp_op_completed(conn);
}

static void post_recv(foo_tcp_connection_t *conn, uint32_t credit) {
    assert(!conn->rcvop_posted);
    assert(credit > 0);
    conn->rcvop_posted = true;
    eloop_post_recv(conn->loop, &conn->rcv_io, conn->sockfd, credit, conn->immediate_rcv, 0);
    conn->ops_pending++;
}

// Return true if a new recv op is posted.
static inline bool try_recv(foo_tcp_connection_t *conn) {
    if (!conn->rcvop_posted) {
        // checking credit also covers read_closed case
        if (conn->rcv_credit > 0 && conn->rcv_buffered < conn->rcv_hogcount) {
            // What if credit < 5% of a buffer?  wait for new credit? same answer for low latency?
            post_recv(conn, conn->rcv_credit);
            return true;
        }
    }
    return false;
}

static void tcp_recycle_buffer(foo_io_t *io, buf_desc_t *bd) {
    foo_tcp_connection_t *conn = containerof(io, foo_tcp_connection_t, foo_io);
    assert(conn->tracked_bufs > 0);
    conn->tracked_bufs--;
    conn->rcv_buffered -= (bd->len + bd->offset);
    eloop_recycle_buffer(conn->loop, bd);
    try_recv(conn);
}

static void tcp_recv_done(void *user_data, uint8_t pool_id, uint16_t buf_id, int count_or_error, bool more) {
    foo_tcp_connection_t *conn = containerof(user_data, foo_tcp_connection_t, rcv_io);
    assert(conn->rcvop_posted);
    conn->rcvop_posted = false;
    if (more)
        conn->rcv_more++;
    foo_io_t *other_io = conn->foo_io.other_io;
//    fprintf(stderr, "ZZZ recv done, %d %d   %d %d\n", conn->sockfd, count_or_error, (int)pool_id, (int)buf_id);

    if (count_or_error > 0) {
        int count = count_or_error;
        conn->tracked_bufs++;
        // pass content to other_io, launch a new recv op if appropriate
        conn->immediate_rcv = more;
        buf_desc_t *bd = get_buf_desc(conn->loop, pool_id, buf_id);
        bd->pool_id = pool_id;
        bd->buf_id = buf_id;
        bd->len = count;
        bd->offset = 0;
        bd->more = more;
        if (!conn->read_closed) {
            io_impl[other_io->adapter_id].fwd_buffer(other_io, bd);
            assert(conn->rcv_credit > count);
            conn->rcv_credit -= count;
            conn->rcv_buffered += count;
            if (conn->rcv_buffered > conn->rcv_buffered_max)
                conn->rcv_buffered_max = conn->rcv_buffered;
            conn->rcv_ops++;
            conn->rcv_total += count;
            if (!try_recv(conn)) {
                if (conn->rcv_credit == 0)
                    conn->credit_stalls++;
                else {
                    if (conn->rcv_buffered < conn->rcv_hogcount)
                        conn->hog_stalls++;
                }
            }
        } else {
            // read_closed and not gracefully, pass buffer back to self
            assert(conn->tracked_bufs > 0);
            tcp_recycle_buffer(&conn->foo_io, bd);
        }
    } else if (count_or_error == 0) {
        // EOF
        io_impl[other_io->adapter_id].fwd_ended(other_io);
        conn->read_closed = true;
        conn->rcv_credit = 0;
        if (conn->write_closed)
            start_socket_close(conn, 0);
    } else {
        // Unexpected error.  Log and force close
        qd_log(LOG_ROUTER, QD_LOG_ERROR, "foo transport connection read fail: %s", strerror(-count_or_error));
        io_impl[other_io->adapter_id].closed(other_io, 0, -count_or_error);
        start_socket_close(conn, 0);
    }
    tcp_op_completed(conn);
}

static void tcp_credit_updated(foo_io_t *io, uint32_t delta) {
    foo_tcp_connection_t *c = containerof(io, foo_tcp_connection_t, foo_io);
    if (c->state == TCP_SETUP) {
        c->state = TCP_RUNNING;
        fprintf(stderr, "we is runningZZZ %d\n", delta);
        assert(c->foo_io.lside);
        if (c->max_send_credit == 0) {
            c->max_send_credit = 131072; // see comment for other_io
            foo_io_t *other_io = c->foo_io.other_io;
            io_impl[other_io->adapter_id].credit_updated(other_io, c->max_send_credit);
        }
    }
    if (c->read_closed) 
        return;
    c->rcv_credit += delta;
    try_recv(c);
}

static void tcp_closed(foo_io_t *io, uint32_t router_errno, uint32_t sys_errno) {
    foo_tcp_connection_t *c = containerof(io, foo_tcp_connection_t, foo_io);
    if (!c->error_reported) {
        c->error_reported = true;
        qd_log(LOG_ROUTER, QD_LOG_ERROR, "footp tcp connection other io failure : %s", strerror(sys_errno));
    }
    start_socket_close(c, 0);
    tcp_maybe_cleanup(c);
}


// ------------------------------------------------------------------------
// Config

static foo_path_t foo_tcp_path_data  = {
    .make_routed_pair = tcp_make_routed_pair
};

static void *configure_foo_tcp_listener(qd_dispatch_t *qd, qd_entity_t *entity) {
    fprintf(stderr, "ZZZ configure_tcp_listener\n");
    footp_module_t *module = footp_module_context;
    foo_tcp_listener_t *listener = calloc(1, sizeof(foo_tcp_listener_t));
    listener_init(listener);

    listener->adaptor_config = new_qd_adaptor_config_t();
    ZERO(listener->adaptor_config);
    if (qd_load_adaptor_config(module->core, listener->adaptor_config, entity) != QD_ERROR_NONE) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "Unable to create footp tcp listener: %s", qd_error_message());
        qd_free_adaptor_config(listener->adaptor_config);
        free(listener);
        return 0;
    }

    
    fake_core_add_watcher(listener->adaptor_config->address, listener_rk_watch_cb, listener);

    return listener;
}

static void *configure_foo_tcp_connector(qd_dispatch_t *qd, qd_entity_t *entity) {
    fprintf(stderr, "ZZZ configure_tcp_ctor\n");
    footp_module_t *module = footp_module_context;
    foo_tcp_connector_t *ctor = calloc(1, sizeof(foo_tcp_connector_t));

    ctor->adaptor_config = new_qd_adaptor_config_t();
    ZERO(ctor->adaptor_config);

    if (qd_load_adaptor_config(module->core, ctor->adaptor_config, entity) != QD_ERROR_NONE) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "Unable to create tcp connector: %s", qd_error_message());
        qd_free_adaptor_config(ctor->adaptor_config);
        free(ctor);
        return 0;
    }

    // This step will eventually require a successful async nslookup before "providing" on this addr
    uint16_t port = atoi(ctor->adaptor_config->port);
    ctor->connect_addr.sin_family = AF_INET;
    ctor->connect_addr.sin_addr.s_addr = inet_addr(ctor->adaptor_config->host),
    ctor->connect_addr.sin_port = htons(port);
    fake_core_add_provider(ctor->adaptor_config->address, &foo_tcp_path_data, ctor);

    return ctor;
}
