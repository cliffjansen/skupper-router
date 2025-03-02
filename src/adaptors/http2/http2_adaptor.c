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
#include "http2_adaptor.h"

#include "adaptors/adaptor_common.h"
#include "adaptors/http_common.h"

#include "qpid/dispatch/buffer.h"
#include "qpid/dispatch/connection_manager.h"
#include "qpid/dispatch/dispatch.h"
#include "qpid/dispatch/protocol_adaptor.h"

#include <proton/condition.h>
#include <proton/listener.h>
#include <proton/netaddr.h>
#include <proton/proactor.h>
#include <proton/raw_connection.h>

#include <inttypes.h>
#include <nghttp2/nghttp2.h>
#include <pthread.h>
#include <stdio.h>

const char *PATH = ":path";
const char *METHOD = ":method";
const char *STATUS = ":status";
const char *CONTENT_TYPE = "content-type";
const char *CONTENT_ENCODING = "content-encoding";

#define DEFAULT_CAPACITY   250
#define NUM_ALPN_PROTOCOLS 1
#define WRITE_BUFFERS 4
#define ARRLEN(x) (sizeof(x) / sizeof(x[0]))

ALLOC_DEFINE_SAFE(qdr_http2_stream_data_t);
ALLOC_DEFINE(qdr_http2_connection_t);

typedef struct qdr_http2_adaptor_t {
    qdr_core_t                  *core;
    qdr_protocol_adaptor_t      *adaptor;
    qd_http_listener_list_t      listeners;   // A list of all http2 listeners
    qd_http_connector_list_t     connectors;  // A list of all http2 connectors
    void                        *callbacks;
    qdr_http2_connection_list_t  connections;
    sys_mutex_t                  lock;  // protects connections, connectors, listener lists
} qdr_http2_adaptor_t;


static qdr_http2_adaptor_t *http2_adaptor;
const int32_t WINDOW_SIZE = 65536;
const int32_t MAX_FRAME_SIZE = 16384;
const char *protocols[] = {"h2"};

static void handle_connection_event(pn_event_t *e, qd_server_t *qd_server, void *context);
static void _http_record_request(qdr_http2_connection_t *conn, qdr_http2_stream_data_t *stream_data);
static void free_http2_stream_data(qdr_http2_stream_data_t *stream_data, bool on_shutdown);
static void handle_raw_connected_event(qdr_http2_connection_t *conn);
static void encrypt_outgoing_tls(qdr_http2_connection_t *conn, qd_adaptor_buffer_t *unencrypted_buff,
                                 bool write_buffers);
static bool schedule_activation(qdr_http2_connection_t *conn, qd_duration_t msec);
static void cancel_activation(qdr_http2_connection_t *conn);

static void grant_read_buffers(qdr_http2_connection_t *conn, const char *msg)
{
    if (IS_ATOMIC_FLAG_SET(&conn->raw_closed_read))
        return;
    int buffers = qd_raw_connection_grant_read_buffers(conn->pn_raw_conn);
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "] grant_read_buffers(%s) granted %i read buffers to proton raw api", conn->conn_id, msg,
           buffers);
}

/**
 * If an ALPN protocol was detected by the TLS API, we make sure that the protocol matches the "h2" (short for http2) protocol.
 * The connection is simply closed if any other protocol (other than h2) was detected.
 * It is ok for no protocol to be detected which means that the other side might not be doing ALPN. If this is the case,
 * we still continue sending http2 frames and close the connection if the response to those http2 frames is non-http2.
 *
 */
static bool is_alpn_protocol_match(qdr_http2_connection_t *http_conn)
{
    const char *protocol_name;
    size_t protocol_name_length;
    bool        alpn_protocol_match = true;
    if (pn_tls_get_alpn_protocol(qd_tls_get_pn_tls_session(http_conn->tls), &protocol_name, &protocol_name_length)) {
        //
        // An ALPN protocol was present. We want to match it to "h2" protocol.
        //
        char *alpn_protocol = qd_calloc(protocol_name_length + 1, sizeof(char));
        memmove(alpn_protocol, protocol_name, protocol_name_length);

        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] Using protocol %s obtained via ALPN",
               http_conn->conn_id, alpn_protocol);

        if (strcmp(alpn_protocol, protocols[0])) {
            // The protocol received from ALPN is not h2, we will log an error and close this connection.
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR,
                   "[C%" PRIu64 "] conn->ingress=%i, Error in ALPN: was expecting protocol %s but got %s",
                   http_conn->conn_id, http_conn->ingress, protocols[0], alpn_protocol);
            nghttp2_submit_goaway(http_conn->session, 0, 0, NGHTTP2_PROTOCOL_ERROR, (uint8_t *)"TLS Protocol Error", 18);
            alpn_protocol_match = false;
        }
        free(alpn_protocol);
    } else {
        //
        // No protocol was received via ALPN. This could mean that the other side does not do ALPN and that is ok.
        //
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] No ALPN protocol was returned",
               http_conn->conn_id);
    }
    http_conn->alpn_check_complete = true;
    return alpn_protocol_match;
}

// invoked once when tls session handshake completes successfully
static void on_tls_connection_secured(qd_tls_t *tls, void *user_context)
{
    qdr_http2_connection_t *conn = (qdr_http2_connection_t *) user_context;
    assert(conn);
    if (conn->qdr_conn && conn->qdr_conn->connection_info) {
        qd_tls_update_connection_info(conn->tls, conn->qdr_conn->connection_info);
    }
}

static void free_all_connection_streams(qdr_http2_connection_t *http_conn, bool on_shutdown)
{
    // Free all the stream data associated with this connection/session.
    qdr_http2_stream_data_t *stream_data = DEQ_HEAD(http_conn->streams);
    while (stream_data) {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][S%" PRId32 "] Freeing stream in free_qdr_http2_connection", stream_data->conn->conn_id,
               stream_data->stream_id);
        free_http2_stream_data(stream_data, on_shutdown);
        stream_data = DEQ_HEAD(http_conn->streams);
    }
}

/**
 * All streams with id greater than the last_stream_id will be freed.
 */
static void free_unprocessed_streams(qdr_http2_connection_t *http_conn, int32_t last_stream_id)
{
    qdr_http2_stream_data_t *stream_data = DEQ_HEAD(http_conn->streams);
    while (stream_data) {
        int32_t stream_id = stream_data->stream_id;

        //
        // This stream_id is greater that the last_stream_id, this stream will not be processed by the http server
        // and hence needs to be freed.
        //
        if (stream_id > last_stream_id) {
            qdr_http2_stream_data_t *next_stream_data = DEQ_NEXT(stream_data);
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32 "] Freeing stream in free_last_id_streams", stream_data->conn->conn_id,
                   stream_data->stream_id);
            free_http2_stream_data(stream_data, false);
            stream_data = next_stream_data;
        }
        else {
            stream_data = DEQ_NEXT(stream_data);
        }
    }
}

static void set_stream_data_delivery_flags(qdr_http2_stream_data_t * stream_data, qdr_delivery_t *dlv) {
    if (dlv == stream_data->in_dlv) {
        stream_data->in_dlv_decrefed = true;
    }
    if (dlv == stream_data->out_dlv) {
        stream_data->out_dlv_decrefed = true;
    }
}

static void advance_stream_status(qdr_http2_stream_data_t *stream_data)
{
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][S%" PRId32 "] Trying to move stream status",
           stream_data->conn->conn_id, stream_data->stream_id);
    if (stream_data->status == QD_STREAM_OPEN) {
        stream_data->status = QD_STREAM_HALF_CLOSED;
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][S%" PRId32 "] Moving stream status to QD_STREAM_HALF_CLOSED", stream_data->conn->conn_id,
               stream_data->stream_id);
    }
    else if (stream_data->status == QD_STREAM_HALF_CLOSED) {
        stream_data->status = QD_STREAM_FULLY_CLOSED;
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][S%" PRId32 "] Moving stream status to QD_STREAM_FULLY_CLOSED",
               stream_data->conn->conn_id, stream_data->stream_id);
    }
    else if (stream_data->status == QD_STREAM_FULLY_CLOSED) {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][S%" PRId32 "] Not moving stream status, stream is already QD_STREAM_FULLY_CLOSED",
               stream_data->conn->conn_id, stream_data->stream_id);
    }
    else {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][S%" PRId32 "] Unknown stream status",
               stream_data->conn->conn_id, stream_data->stream_id);
    }
}

// Per-message callback to resume receiving after Q2 is unblocked on the
// incoming link (to HTTP2 app).  This routine runs on another I/O thread so it
// must be thread safe and hence we use the server activation lock
//
static void qdr_http2_q2_unblocked_handler(const qd_alloc_safe_ptr_t context)
{
    // prevent the conn from being deleted while running:
    sys_mutex_lock(qd_server_get_activation_lock(http2_adaptor->core->qd->server));

    qdr_http2_connection_t *conn = (qdr_http2_connection_t*)qd_alloc_deref_safe_ptr(&context);
    if (conn && conn->pn_raw_conn) {
        SET_ATOMIC_FLAG(&conn->q2_restart);
        pn_raw_connection_wake(conn->pn_raw_conn);
    }

    sys_mutex_unlock(qd_server_get_activation_lock(http2_adaptor->core->qd->server));
}

/**
 * HTTP :path is mapped to the AMQP 'to' field.
 */
qd_composed_field_t *qd_message_compose_amqp(qdr_http2_connection_t *conn,
                                             qd_message_t           *msg,
                                             const char             *to,
                                             const char             *subject,
                                             const char             *reply_to,
                                             const char             *content_type,
                                             const char             *content_encoding,
                                             int32_t                 correlation_id,
                                             const char             *group_id)
{
    qd_composed_field_t  *field   = qd_compose(QD_PERFORMATIVE_HEADER, 0);
    qd_message_content_t *content = MSG_CONTENT(msg);
    if (!content) {
        qd_compose_free(field);
        return 0;
    }
    //
    // Header
    //
    qd_compose_start_list(field);
    qd_compose_insert_bool(field, 0);     // durable
    qd_compose_insert_null(field);        // priority
    //qd_compose_insert_null(field);        // ttl
    //qd_compose_insert_bool(field, 0);     // first-acquirer
    //qd_compose_insert_uint(field, 0);     // delivery-count
    qd_compose_end_list(field);

    //
    // Properties
    //
    field = qd_compose(QD_PERFORMATIVE_PROPERTIES, field);
    qd_compose_start_list(field);
    qd_compose_insert_null(field);          // message-id
    qd_compose_insert_null(field);          // user-id
    if (to) {
        qd_compose_insert_string(field, to);    // to
    }
    else {
        qd_compose_insert_null(field);
    }

    if (subject) {
        qd_compose_insert_string(field, subject);      // subject
    }
    else {
        qd_compose_insert_null(field);
    }

    if (reply_to) {
        qd_compose_insert_string(field, reply_to); // reply-to
    }
    else {
        qd_compose_insert_null(field);
    }

    if (correlation_id > 0) {
        qd_compose_insert_int(field, correlation_id);
    }
    else {
        qd_compose_insert_null(field);          // correlation-id
    }

    if (content_type) {
        qd_compose_insert_string(field, content_type);        // content-type
    }
    else {
        qd_compose_insert_null(field);
    }
    if (content_encoding) {
        qd_compose_insert_string(field, content_encoding);               // content-encoding
    }
    else {
        qd_compose_insert_null(field);
    }
    qd_compose_insert_null(field);                      // absolute-expiry-time
    qd_compose_insert_null(field);                      // creation-time
    if (group_id) {
        qd_compose_insert_string(field, group_id);      // group-id
    } else {
        qd_compose_insert_null(field);
    }
    qd_compose_end_list(field);

    qd_alloc_safe_ptr_t conn_sp = QD_SAFE_PTR_INIT(conn);
    qd_message_set_q2_unblocked_handler(msg, qdr_http2_q2_unblocked_handler, conn_sp);

    return field;
}

static void free_http2_stream_data(qdr_http2_stream_data_t *stream_data, bool on_shutdown)
{
    if (!stream_data)
        return;

    qdr_http2_connection_t *conn = stream_data->conn;

    // Record the request just before freeing the stream.
    _http_record_request(conn, stream_data);

    if (!on_shutdown) {
        if (conn->qdr_conn && stream_data->in_link) {
            qdr_link_set_context(stream_data->in_link, 0);
            qdr_link_detach(stream_data->in_link, QD_CLOSED, 0);
        }
        if (conn->qdr_conn && stream_data->out_link) {
            qdr_link_set_context(stream_data->out_link, 0);
            qdr_link_detach(stream_data->out_link, QD_CLOSED, 0);
        }
    }
    free(stream_data->reply_to);
    qd_compose_free(stream_data->app_properties);
    qd_buffer_list_free_buffers(&stream_data->body_buffers);
    qd_compose_free(stream_data->footer_properties);
    if (DEQ_SIZE(conn->streams) > 0) {
        DEQ_REMOVE(conn->streams, stream_data);
        nghttp2_session_set_stream_user_data(conn->session, stream_data->stream_id, NULL);
    }
    free(stream_data->method);
    free(stream_data->remote_site);
    free(stream_data->request_status);

    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "][S%" PRId32 "] Freeing stream_data in free_http2_stream_data (%lx)", conn->conn_id,
           stream_data->stream_id, (long) stream_data);

    // If the httpConnector was deleted, a client request has nowhere to go because of lack of receiver and hence credit.
    // No delivery was created. The message that was created for such a hanging request must be freed here..
    if (!stream_data->in_dlv && stream_data->message) {
        qd_message_clear_q2_unblocked_handler(stream_data->message);
        qd_message_free(stream_data->message);
    }

    //
    // If the client/server closed the connection abruptly, we need to release the stream_data->curr_stream_data and
    // stream_data->next_stream_data.
    // This final decref of the delivery is going to free the associated message but before this message can be freed
    // all stream data (body data) objects need to be freed. We do this here.
    //
    if (stream_data->in_dlv && !stream_data->in_dlv_decrefed) {
        qd_message_stream_data_release(stream_data->curr_stream_data);
        qd_iterator_free(stream_data->curr_stream_data_iter);
        stream_data->curr_stream_data = 0;
        stream_data->curr_stream_data_iter = 0;

        qd_message_stream_data_release(stream_data->next_stream_data);
        stream_data->next_stream_data = 0;

        set_stream_data_delivery_flags(stream_data, stream_data->in_dlv);
        qdr_delivery_decref(http2_adaptor->core, stream_data->in_dlv, "HTTP2 adaptor in_dlv - free_http2_stream_data");
    }

    if (stream_data->out_dlv && !stream_data->out_dlv_decrefed) {
        qd_message_stream_data_release(stream_data->curr_stream_data);
        qd_iterator_free(stream_data->curr_stream_data_iter);
        stream_data->curr_stream_data = 0;
        stream_data->curr_stream_data_iter = 0;

        qd_message_stream_data_release(stream_data->next_stream_data);
        stream_data->next_stream_data = 0;
        set_stream_data_delivery_flags(stream_data, stream_data->in_dlv);
        qdr_delivery_decref(http2_adaptor->core, stream_data->out_dlv,
                            "HTTP2 adaptor out_dlv - free_http2_stream_data");
    }

    // End the vanflow record for the stream level vanflow.
    vflow_end_record(stream_data->vflow);
    free_qdr_http2_stream_data_t(stream_data);
}

void free_qdr_http2_connection(qdr_http2_connection_t* http_conn, bool on_shutdown)
{
    // Free all the stream data associated with this connection/session.
    free_all_connection_streams(http_conn, on_shutdown);
    qd_adaptor_buffer_list_free_buffers(&http_conn->out_buffs);

    if(http_conn->remote_address) {
        free(http_conn->remote_address);
        http_conn->remote_address = 0;
    }
    if (http_conn->activate_timer) {
        qd_timer_free(http_conn->activate_timer);
        http_conn->activate_timer = 0;
    }

    http_conn->context.context = 0;

    if (http_conn->session) {
        nghttp2_session_del(http_conn->session);
    }

    sys_mutex_lock(&http2_adaptor->lock);
    DEQ_REMOVE(http2_adaptor->connections, http_conn);
    sys_mutex_unlock(&http2_adaptor->lock);

    sys_atomic_destroy(&http_conn->activate_scheduled);
    sys_atomic_destroy(&http_conn->raw_closed_read);
    sys_atomic_destroy(&http_conn->raw_closed_write);
    sys_atomic_destroy(&http_conn->q2_restart);
    sys_atomic_destroy(&http_conn->delay_buffer_write);

    // Free tls related stuff if need be.
    if (http_conn->tls) {
        qd_tls_free(http_conn->tls);
    }

    //
    // We are about to free the qdr_http2_connection_t object. We need to decref the listener/connector, so they can be freed.
    //
    if (http_conn->listener) {
        qd_http_listener_decref(http_conn->listener);
    }
    else if (http_conn->connector) {
        // Note here that decrefing the connector also frees the config. The http_conn->config remains unfreed and accessible on the qdr_http2_connection_t object
        // until we call the following qd_http_connector_decref which *might* then free the config and then free connector itself.
        qd_http_connector_decref(http_conn->connector);
    }

    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "] Freeing http2 connection in free_qdr_http2_connection", http_conn->conn_id);
    // End the vanflow record for the connection level vanflow.
    vflow_end_record(http_conn->vflow);
    free_qdr_http2_connection_t(http_conn);
}

static qdr_http2_stream_data_t *create_qdr_http2_stream_data(qdr_http2_connection_t *conn, int32_t stream_id)
{
    qdr_http2_stream_data_t *stream_data = new_qdr_http2_stream_data_t();

    ZERO(stream_data);
    stream_data->stream_id = stream_id;

    //
    // Start a vanflow record for the http2 stream. The parent of this vanflow is
    // its connection's vanflow record.
    //
    stream_data->vflow = vflow_start_record(VFLOW_RECORD_FLOW, conn->vflow);
    vflow_set_uint64(stream_data->vflow, VFLOW_ATTRIBUTE_OCTETS, 0);
    vflow_add_rate(stream_data->vflow, VFLOW_ATTRIBUTE_OCTETS, VFLOW_ATTRIBUTE_OCTET_RATE);

    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][S%" PRId32 "] Created new stream_data (%lx)",
           conn->conn_id, stream_id, (long) stream_data);

    stream_data->message = qd_message();
    qd_message_set_streaming_annotation(stream_data->message);
    stream_data->conn = conn;
    stream_data->app_properties = qd_compose(QD_PERFORMATIVE_APPLICATION_PROPERTIES, 0);
    stream_data->status = QD_STREAM_OPEN;
    DEQ_INIT(stream_data->body_buffers);
    stream_data->start = qd_timer_now();
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "][S%" PRId32
           "] Creating new stream_data->app_properties=QD_PERFORMATIVE_APPLICATION_PROPERTIES",
           conn->conn_id, stream_id);
    qd_compose_start_map(stream_data->app_properties);
    //
    // Insert the flow id into the message's application property.
    // The http2 adaptor sends a message per stream.
    //
    qd_compose_insert_symbol(stream_data->app_properties, QD_AP_FLOW_ID);
    vflow_serialize_identity(stream_data->vflow, stream_data->app_properties);
    nghttp2_session_set_stream_user_data(conn->session, stream_id, stream_data);
    DEQ_INSERT_TAIL(conn->streams, stream_data);
    stream_data->out_msg_has_body = true;

    //
    // Start latency timer for this http2  stream.
    // This stream can be on an ingress connection or an egress connection.
    //
    vflow_latency_start(stream_data->vflow);
    return stream_data;
}


/**
 * This callback function  is invoked when the nghttp2 library tells the application about the error code, and error message.
 */
static int on_error_callback(nghttp2_session *session, int lib_error_code, const char *msg, size_t len, void *user_data)
{
    qdr_http2_connection_t *conn = (qdr_http2_connection_t *) user_data;
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR,
           "[C%" PRIu64 "] Error generated in the on_error_callback, lib_error_code=%i, error_msg=%s", conn->conn_id,
           lib_error_code, msg);
    return 0;
}

static int on_frame_send_callback(nghttp2_session *session, const nghttp2_frame *frame, void *user_data)
{
    qdr_http2_connection_t *conn = (qdr_http2_connection_t *) user_data;
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "][S%" PRId32 "] on_frame_send_callback, frame size=%zu", conn->conn_id, frame->hd.stream_id,
           frame->hd.length);
    return 0;
}

static int on_frame_not_send_callback(nghttp2_session *session, const nghttp2_frame *frame, int lib_error_code,
                                      void *user_data)
{
    qdr_http2_connection_t *conn      = (qdr_http2_connection_t *) user_data;
    int32_t                 stream_id = frame->hd.stream_id;
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "][S%" PRId32 "] on_frame_not_send_callback, lib_error_code=%s", conn->conn_id, stream_id,
           nghttp2_strerror(lib_error_code));
    return 0;
}

static int on_stream_close_callback(nghttp2_session *session, int32_t stream_id, uint32_t error_code, void *user_data)
{
    qdr_http2_connection_t *conn = (qdr_http2_connection_t *) user_data;
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "][S%" PRId32 "] on_stream_close_callback, lib_error_code=%s", conn->conn_id, stream_id,
           nghttp2_http2_strerror(error_code));
    return 0;
}

/**
 * Callback function invoked by nghttp2_session_recv() and nghttp2_session_mem_recv() when an invalid non-DATA frame is received
 */
static int on_invalid_frame_recv_callback(nghttp2_session *session, const nghttp2_frame *frame, int lib_error_code, void *user_data)
{
    qdr_http2_connection_t *conn = (qdr_http2_connection_t *)user_data;
    int32_t stream_id = frame->hd.stream_id;
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "][S%" PRId32 "] on_invalid_frame_recv_callback, lib_error_code=%s", conn->conn_id, stream_id,
           nghttp2_strerror(lib_error_code));

    if (lib_error_code == NGHTTP2_ERR_FLOW_CONTROL) {
        const char*str_error = nghttp2_http2_strerror(lib_error_code);
        nghttp2_submit_goaway(session, 0, 0, NGHTTP2_FLOW_CONTROL_ERROR, (uint8_t *)str_error, strlen(str_error));
        nghttp2_session_send(conn->session);
        qd_raw_connection_write_buffers(conn->pn_raw_conn, &conn->out_buffs);
    }

    return lib_error_code;
}


static int on_data_chunk_recv_callback(nghttp2_session *session,
                                       uint8_t flags,
                                       int32_t stream_id,
                                       const uint8_t *data,
                                       size_t len,
                                       void *user_data)
{
    qdr_http2_connection_t *conn = (qdr_http2_connection_t *)user_data;
    qdr_http2_stream_data_t *stream_data = nghttp2_session_get_stream_user_data(conn->session, stream_id);

    if (!conn->pn_raw_conn)
        return 0;

    if (!stream_data)
        return 0;

    if(stream_data->stream_force_closed)
        return 0;

    stream_data->bytes_in += len;
    conn->bytes_in += len;

    // Flows are unidirectional. Let's just send in the bytes_in
    vflow_set_uint64(conn->vflow, VFLOW_ATTRIBUTE_OCTETS, conn->bytes_in);
    vflow_set_uint64(stream_data->vflow, VFLOW_ATTRIBUTE_OCTETS, stream_data->bytes_in);

    //
    // DISPATCH-1868: If an in_dlv is present it means that the qdr_link_deliver() has already been called (delivery has already been routed)
    // in which case qd_message_stream_data_append can be called to append buffers to the message body
    // If stream_data->in_dlv = 0 but stream_data->header_and_props_composed is true, it means that the message has not been routed yet
    // but the message already has headers and properties
    // in which case the qd_message_stream_data_append() can be called to add body data to the message.
    // In many cases when the response message is streamed by a server, the entire message body can arrive before we get credit to route it.
    // We want to be able to keep collecting the incoming DATA in the message object so we can ultimately route it when the credit does ultimately arrive.
    //
    if (stream_data->in_dlv || stream_data->header_and_props_composed) {
        qd_buffer_list_t buffers;
        DEQ_INIT(buffers);
        qd_buffer_list_append(&buffers, (uint8_t *)data, len);
        // DISPATCH-1868: Part of the HTTP2 message body arrives *before* we can route the delivery. So we accumulated the body buffers
        // in the stream_data->body_buffers. But before the rest of the HTTP2 data arrives, we got credit to send the delivery
        // and we have an in_dlv object now. Now, we take the buffers that were added previously to stream_data->body_buffers and call qd_message_stream_data_append
        bool q2_blocked1 = false;
        if (DEQ_SIZE(stream_data->body_buffers) > 0) {
            if (!stream_data->body_data_added_to_msg) {
                qd_message_stream_data_append(stream_data->message, &stream_data->body_buffers, &q2_blocked1);
            }
        }
        bool q2_blocked2 = false;
        qd_message_stream_data_append(stream_data->message, &buffers, &q2_blocked2);
        stream_data->body_data_added_to_msg = true;
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][S%" PRId32
               "] HTTP2 DATA on_data_chunk_recv_callback qd_compose_insert_binary_buffers into stream_data->message",
               conn->conn_id, stream_id);

        if ((q2_blocked1 || q2_blocked2) && !conn->q2_blocked) {
            conn->q2_blocked = true;
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] q2 is blocked on this connection",
                   conn->conn_id);
        }
    }
    else {
        // Keep inserting buffers to stream_data->body_buffers.
        qd_buffer_list_append(&stream_data->body_buffers, (uint8_t *)data, len);
        qd_log(
            LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
            "[C%" PRIu64 "][S%" PRId32
            "] HTTP2 DATA on_data_chunk_recv_callback qd_compose_insert_binary_buffers into stream_data->body_buffers",
            conn->conn_id, stream_id);
    }

    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "][S%" PRId32 "] HTTP2 DATA on_data_chunk_recv_callback data length %zu", conn->conn_id,
           stream_id, len);

    // Calling this here to send out any WINDOW_UPDATE frames that might be necessary.
    // The only function that nghttp2 calls if it wants to send data is the send_callback.
    // The only function that calls send_callback is nghttp2_session_send
    nghttp2_session_send(conn->session);

    //Returning zero means success.
    return 0;
}


static int send_data_callback(nghttp2_session *session,
                             nghttp2_frame *frame,
                             const uint8_t *framehd,
                             size_t length,
                             nghttp2_data_source *source,
                             void *user_data) {
    // The frame is a DATA frame to send. The framehd is the serialized frame header (9 bytes).
    // The length is the length of application data to send (this does not include padding)
    qdr_http2_connection_t *conn = (qdr_http2_connection_t *)user_data;
    qdr_http2_stream_data_t *stream_data = (qdr_http2_stream_data_t *) source->ptr;
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][S%" PRId32 "] send_data_callback length=%zu",
           conn->conn_id, stream_data->stream_id, length);

    //
    // local_buffs is used in the case when TLS encryption is required before sending the data out.
    // All the http2 data is gathered into local_buffs and the local_buffs is sent to qd_tls_encrypt
    // where the outgoing data is encrypted.
    //
    qd_adaptor_buffer_list_t local_buffs;
    DEQ_INIT(local_buffs);
    bool require_tls = conn->require_tls;

    int bytes_sent = 0; // This should not include the header length of 9.
    bool write_buffs = false;
    if (length) {
        qd_adaptor_buffer_t *tail_buff = 0;
        if (require_tls) {
            tail_buff = qd_adaptor_buffer();
            memcpy(qd_adaptor_buffer_cursor(tail_buff), framehd, HTTP2_DATA_FRAME_HEADER_LENGTH);
            qd_adaptor_buffer_insert(tail_buff, HTTP2_DATA_FRAME_HEADER_LENGTH);
            DEQ_INSERT_TAIL(local_buffs, tail_buff);
        }
        else {
            qd_adaptor_buffer_list_append(&(conn->out_buffs), framehd, HTTP2_DATA_FRAME_HEADER_LENGTH);
            tail_buff = DEQ_TAIL(conn->out_buffs);
        }
        size_t tail_buff_capacity = qd_adaptor_buffer_capacity(tail_buff);
        if (tail_buff_capacity == 0) {
            tail_buff = qd_adaptor_buffer();
            if (require_tls) {
                DEQ_INSERT_TAIL(local_buffs, tail_buff);
            }
            else {
                DEQ_INSERT_TAIL(conn->out_buffs, tail_buff);
            }
            tail_buff_capacity = qd_adaptor_buffer_capacity(tail_buff);
        }
        size_t bytes_to_write = length;
        while (bytes_to_write > 0) {
            uint32_t octets_remaining = qd_iterator_remaining(stream_data->curr_stream_data_iter);
            size_t len = MIN(tail_buff_capacity, bytes_to_write);
            len = MIN(len, octets_remaining);
            int copied =
                qd_iterator_ncopy(stream_data->curr_stream_data_iter, qd_adaptor_buffer_cursor(tail_buff), len);
            assert(copied == len);
            qd_adaptor_buffer_insert(tail_buff, len);
            octets_remaining -= copied;
            bytes_sent += copied;
            qd_iterator_trim_view(stream_data->curr_stream_data_iter, octets_remaining);
            bytes_to_write -= len;
            if (bytes_to_write > 0 && qd_adaptor_buffer_capacity(tail_buff) == 0) {
                tail_buff = qd_adaptor_buffer();
                if (require_tls) {
                    DEQ_INSERT_TAIL(local_buffs, tail_buff);
                }
                else {
                    DEQ_INSERT_TAIL(conn->out_buffs, tail_buff);
                }
                tail_buff_capacity = qd_adaptor_buffer_capacity(tail_buff);
            }
        }
    }
    else if (length == 0 && stream_data->out_msg_data_flag_eof) {
        if (require_tls) {
            qd_adaptor_buffer_t *http2_buff = qd_adaptor_buffer();
            DEQ_INSERT_TAIL(local_buffs, http2_buff);
            memcpy(qd_adaptor_buffer_cursor(http2_buff), framehd, HTTP2_DATA_FRAME_HEADER_LENGTH);
            qd_adaptor_buffer_insert(http2_buff, HTTP2_DATA_FRAME_HEADER_LENGTH);
        }
        else {
            qd_adaptor_buffer_list_append(&(conn->out_buffs), framehd, HTTP2_DATA_FRAME_HEADER_LENGTH);
        }
    }

    //
    // If the message has a footer, don't flush the buffers now. Flush them after you write out the footer.
    //
    if (!stream_data->out_msg_has_footer) {
        write_buffs = true;
    }

    if (require_tls) {
        qd_adaptor_buffer_t *local_adaptor_buff = DEQ_HEAD(local_buffs);
        while (local_adaptor_buff) {
            // local_adaptor_buff will be freed once you call encrypt_outgoing_tls with it
            // so get the next buff right now.
            qd_adaptor_buffer_t *next_adaptor_buff = DEQ_NEXT(local_adaptor_buff);
            encrypt_outgoing_tls(conn, local_adaptor_buff, true);
            local_adaptor_buff = next_adaptor_buff;
        }
    }

    if (stream_data->full_payload_handled) {
        if (stream_data->curr_stream_data) {
            if (stream_data->curr_stream_data_result == QD_MESSAGE_STREAM_DATA_FOOTER_OK) {
                stream_data->footer_stream_data = stream_data->curr_stream_data;
                stream_data->footer_stream_data_iter = stream_data->curr_stream_data_iter;
            }
            else {
                qd_message_stream_data_release(stream_data->curr_stream_data);
                qd_iterator_free(stream_data->curr_stream_data_iter);
            }
            stream_data->curr_stream_data_iter = 0;
            stream_data->curr_stream_data = 0;
        }
        stream_data->payload_handled = 0;
    }
    else {
        stream_data->payload_handled += bytes_sent;
    }

    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "][S%" PRId32 "] HTTP2 send_data_callback finished, length=%zu, bytes_sent=%i, stream_data=%p",
           conn->conn_id, stream_data->stream_id, length, bytes_sent, (void *) stream_data);

    if (length) {
        assert(bytes_sent == length);
    }

    if (write_buffs) {
        qd_raw_connection_write_buffers(conn->pn_raw_conn, &conn->out_buffs);
    }

    return 0;

}

static ssize_t send_callback(nghttp2_session *session,
                             const uint8_t *data,
                             size_t length,
                             int flags,
                             void *user_data) {
    qdr_http2_connection_t *conn = (qdr_http2_connection_t *)user_data;
    bool require_tls = conn->require_tls;
    if (require_tls) {
        qd_adaptor_buffer_t *adaptor_buffer = qd_adaptor_buffer();
        memcpy(qd_adaptor_buffer_base(adaptor_buffer), data, length);
        qd_adaptor_buffer_insert(adaptor_buffer, length);
        //
        // This data is being sent over a TLS session. It needs to be encrypted before it is sent out on the wire.
        //
        encrypt_outgoing_tls(conn, adaptor_buffer, false);
    }
    else {
        //
        // Data not being sent over a TLS session, just stick it at the end of the last buffer of conn->out_buffs
        //
        qd_adaptor_buffer_list_append(&(conn->out_buffs), (uint8_t *) data, length);
    }
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] HTTP2 send_callback data length %zu",
           conn->conn_id, length);
    if (! IS_ATOMIC_FLAG_SET(&conn->delay_buffer_write)) {
        qd_raw_connection_write_buffers(conn->pn_raw_conn, &conn->out_buffs);
    }
    return (ssize_t)length;
}

/**
 * This callback function is invoked with the reception of header block in HEADERS or PUSH_PROMISE is started.
 * The HEADERS frame can arrive from a client or server. We start building a new AMQP message (qd_message_t) in this callback and create the two links per stream.
 *
 * Return zero if function succeeds.
 */
static int on_begin_headers_callback(nghttp2_session *session,
                                     const nghttp2_frame *frame,
                                     void *user_data)
{
    qdr_http2_connection_t *conn = (qdr_http2_connection_t *)user_data;
    qdr_http2_stream_data_t *stream_data = 0;

    // For the client applications, frame->hd.type is either NGHTTP2_HEADERS or NGHTTP2_PUSH_PROMISE
    // TODO - deal with NGHTTP2_PUSH_PROMISE
    if (frame->hd.type == NGHTTP2_HEADERS) {
        if(frame->headers.cat == NGHTTP2_HCAT_REQUEST && conn->ingress) {
            if (!conn->qdr_conn) {
                return 0;
            }

            int32_t stream_id = frame->hd.stream_id;
            qdr_terminus_t *target = qdr_terminus(0);
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] Processing incoming HTTP2 stream with id %" PRId32 "", conn->conn_id, stream_id);
            stream_data = create_qdr_http2_stream_data(conn, stream_id);
            //
            // Capture the stream id on the ingress side.
            // This will help vflow correlate the ingress and egress streams.
            //
            vflow_set_uint64(stream_data->vflow, VFLOW_ATTRIBUTE_STREAM_ID, stream_data->stream_id);

            //
            // For every single stream in the same connection, create  -
            // 1. sending link with the configured address as the target
            //
            qdr_terminus_set_address(target, conn->config->adaptor_config->address);
            stream_data->in_link = qdr_link_first_attach(conn->qdr_conn,
                                                         QD_INCOMING,
                                                         qdr_terminus(0),  //qdr_terminus_t   *source,
                                                         target,           //qdr_terminus_t   *target,
                                                         "http.ingress.in",         //const char       *name,
                                                         0,                //const char       *terminus_addr,
                                                         false,
                                                         NULL,
                                                         &(stream_data->incoming_id));
            qdr_link_set_context(stream_data->in_link, stream_data);

            //
            // 2. dynamic receiver on which to receive back the response data for that stream.
            //
            qdr_terminus_t *dynamic_source = qdr_terminus(0);
            qdr_terminus_set_dynamic(dynamic_source);
            stream_data->out_link = qdr_link_first_attach(conn->qdr_conn,
                                                          QD_OUTGOING,   //Receiver
                                                          dynamic_source,   //qdr_terminus_t   *source,
                                                          qdr_terminus(0),  //qdr_terminus_t   *target,
                                                          "http.ingress.out",        //const char       *name,
                                                          0,                //const char       *terminus_addr,
                                                          false,
                                                          NULL,
                                                          &(stream_data->outgoing_id));
            qdr_link_set_context(stream_data->out_link, stream_data);
        } else if (!conn->ingress) {
            //
            // The on_begin_headers_callback() is called only once just before the first response header header arrives.
            // We will end the vanflow latency here.
            //
            int32_t                  stream_id   = frame->hd.stream_id;
            qdr_http2_stream_data_t *stream_data = nghttp2_session_get_stream_user_data(conn->session, stream_id);
            vflow_latency_end(stream_data->vflow);
        }
    }

    return 0;
}

/**
 *  nghttp2_on_header_callback: Called when nghttp2 library emits
 *  single header name/value pair.
 *  Collects all headers in the application properties map of the AMQP
 *
 *  @return zero if function succeeds.
 */
static int on_header_callback(nghttp2_session *session,
                              const nghttp2_frame *frame,
                              const uint8_t *name,
                              size_t namelen,
                              const uint8_t *value,
                              size_t valuelen,
                              uint8_t flags,
                              void *user_data)
{
    int32_t stream_id = frame->hd.stream_id;
    qdr_http2_connection_t *conn = (qdr_http2_connection_t *)user_data;
    qdr_http2_stream_data_t *stream_data = nghttp2_session_get_stream_user_data(conn->session, stream_id);

    switch (frame->hd.type) {
        case NGHTTP2_HEADERS: {
            if (stream_data->use_footer_properties) {
                if (!stream_data->footer_properties) {
                    stream_data->footer_properties = qd_compose(QD_PERFORMATIVE_FOOTER, 0);
                    qd_compose_start_map(stream_data->footer_properties);
                }

                qd_compose_insert_string_n(stream_data->footer_properties, (const char *)name, namelen);
                qd_compose_insert_string_n(stream_data->footer_properties, (const char *)value, valuelen);

                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32 "] HTTP2 FOOTER Incoming [%s=%s]", conn->conn_id,
                       stream_data->stream_id, (char *) name, (char *) value);
            }
            else {
                if (strcmp(METHOD, (const char *)name) == 0) {
                    stream_data->method = qd_strdup((const char *)value);
                    // Set the http method (GET, POST, PUT, DELETE etc) on the stream's vflow object.
                    vflow_set_string(stream_data->vflow, VFLOW_ATTRIBUTE_METHOD, stream_data->method);
                }
                if (strcmp(STATUS, (const char *)name) == 0) {
                    stream_data->request_status = qd_strdup((const char *)value);
                    // Set the http response status (200, 404 etc) on the stream's vflow object.
                    vflow_set_string(stream_data->vflow, VFLOW_ATTRIBUTE_RESULT, stream_data->request_status);
                }
                qd_compose_insert_string_n(stream_data->app_properties, (const char *)name, namelen);
                qd_compose_insert_string_n(stream_data->app_properties, (const char *)value, valuelen);

                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32 "] HTTP2 HEADER Incoming [%s=%s]", conn->conn_id,
                       stream_data->stream_id, (char *) name, (char *) value);
            }

        }
        break;
        default:
            break;
    }
    return 0;
}


static bool compose_and_deliver(qdr_http2_connection_t *conn, qdr_http2_stream_data_t *stream_data, bool receive_complete)
{
    if (!stream_data->header_and_props_composed) {
        qd_composed_field_t  *header_and_props = 0;
        if (conn->ingress) {
            header_and_props = qd_message_compose_amqp(conn,
                                                       stream_data->message,
                                                       conn->config->adaptor_config->address,  // const char *to
                                                       stream_data->method,    // const char *subject
                                                       stream_data->reply_to,  // const char *reply_to
                                                       0,                      // const char *content_type
                                                       0,                      // const char *content_encoding
                                                       0,                      // int32_t  correlation_id
                                                       conn->config->adaptor_config->site_id);
        }
        else {
            header_and_props = qd_message_compose_amqp(conn,
                                                       stream_data->message,
                                                       stream_data->reply_to,        // const char *to
                                                       stream_data->request_status,  // const char *subject
                                                       0,                            // const char *reply_to
                                                       0,                            // const char *content_type
                                                       0,                            // const char *content_encoding
                                                       0,                            // int32_t  correlation_id
                                                       conn->config->adaptor_config->site_id);
        }

        if (receive_complete) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32 "][L%" PRIu64 "] receive_complete = true in compose_and_deliver",
                   conn->conn_id, stream_data->stream_id, stream_data->in_link->identity);
            bool q2_blocked;
            if (stream_data->footer_properties) {
                qd_message_compose_3(stream_data->message, header_and_props, stream_data->app_properties, receive_complete);
                qd_message_stream_data_append(stream_data->message, &stream_data->body_buffers, &q2_blocked);
                stream_data->body_data_added_to_msg = true;

                qd_buffer_list_t existing_buffers;
                DEQ_INIT(existing_buffers);
                qd_compose_take_buffers(stream_data->footer_properties, &existing_buffers);
                qd_message_stream_data_footer_append(stream_data->message, &existing_buffers);
            }
            else {
                qd_message_compose_3(stream_data->message, header_and_props, stream_data->app_properties, receive_complete);
                qd_message_stream_data_append(stream_data->message, &stream_data->body_buffers, &q2_blocked);
                stream_data->body_data_added_to_msg = true;
            }
            if (q2_blocked && !conn->q2_blocked) {
                conn->q2_blocked = true;
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] q2 is blocked on this connection",
                       conn->conn_id);
            }
        }
        else {
            if (DEQ_SIZE(stream_data->body_buffers) > 0) {
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32 "][L%" PRIu64
                       "] receive_complete = false and has stream_data->body_buffers in compose_and_deliver",
                       conn->conn_id, stream_data->stream_id, stream_data->in_link->identity);
                bool q2_blocked;
                if (stream_data->footer_properties) {
                    if (!stream_data->entire_footer_arrived) {
                        qd_compose_free(header_and_props);
                        return false;
                    }

                    qd_message_compose_3(stream_data->message, header_and_props, stream_data->app_properties, receive_complete);
                    qd_message_stream_data_append(stream_data->message, &stream_data->body_buffers, &q2_blocked);
                    qd_buffer_list_t existing_buffers;
                    DEQ_INIT(existing_buffers);
                    qd_compose_take_buffers(stream_data->footer_properties, &existing_buffers);
                    qd_message_stream_data_footer_append(stream_data->message, &existing_buffers);
                }
                else {
                    qd_message_compose_3(stream_data->message, header_and_props, stream_data->app_properties,
                                         receive_complete);
                    qd_message_stream_data_append(stream_data->message, &stream_data->body_buffers, &q2_blocked);
                }
                stream_data->body_data_added_to_msg = true;
                if (q2_blocked && !conn->q2_blocked) {
                    conn->q2_blocked = true;
                    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] q2 is blocked on this connection",
                           conn->conn_id);
                }
            }
            else {
                if (stream_data->footer_properties) {
                    if (!stream_data->entire_footer_arrived) {
                        qd_compose_free(header_and_props);
                        return false;
                    }

                    //
                    // The footer has already arrived but there was no body. Insert an empty body
                    //
                    qd_message_compose_3(stream_data->message, header_and_props, stream_data->app_properties, receive_complete);
                    qd_message_stream_data_append(stream_data->message, &stream_data->body_buffers, 0);

                    qd_buffer_list_t existing_buffers;
                    DEQ_INIT(existing_buffers);
                    qd_compose_take_buffers(stream_data->footer_properties, &existing_buffers);
                    qd_message_stream_data_footer_append(stream_data->message, &existing_buffers);
                    stream_data->body_data_added_to_msg = true;
                }
                else {
                    qd_message_compose_3(stream_data->message, header_and_props, stream_data->app_properties, receive_complete);
                    stream_data->body_data_added_to_msg = false;
                }
            }
        }

        // The header and properties have been added. Now we can start adding BODY DATA to this message.
        stream_data->header_and_props_composed = true;
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][S%" PRId32 "][L%" PRIu64
               "] stream_data->header_and_props_composed = true in compose_and_deliver",
               conn->conn_id, stream_data->stream_id, stream_data->in_link->identity);
        qd_compose_free(header_and_props);
    }

    if (stream_data->in_link_credit == 0) {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] compose_and_deliver stream_data->in_link_credit is zero", conn->conn_id);
    }

    if (!stream_data->in_dlv && stream_data->in_link_credit > 0) {
        //
        // Not doing an incref here since the qdr_link_deliver increfs the delivery twice
        //
        stream_data->in_dlv = qdr_link_deliver(stream_data->in_link, stream_data->message, 0, false, 0, 0, 0, 0);
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][S%" PRId32 "] Routed delivery in compose_and_deliver (conn->ingress=%i) " DLV_FMT,
               conn->conn_id, stream_data->stream_id, conn->ingress, DLV_ARGS(stream_data->in_dlv));
        qdr_delivery_set_context(stream_data->in_dlv, stream_data);
        stream_data->in_link_credit -= 1;
        return true;
    }
    return false;
}

static bool route_delivery(qdr_http2_stream_data_t *stream_data, bool receive_complete)
{
    qdr_http2_connection_t *conn  = stream_data->conn;
    if (stream_data->in_dlv) {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][S%" PRId32 "] in_dlv already present, delivery already routed", conn->conn_id,
               stream_data->stream_id);
        return false;
    }

    bool delivery_routed = false;

    if (conn->ingress) {
        if (stream_data->reply_to && stream_data->entire_header_arrived && !stream_data->in_dlv) {
            delivery_routed = compose_and_deliver(conn, stream_data, receive_complete);
        }
        if (!stream_data->reply_to) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32 "][L%" PRIu64
                   "] stream_data->reply_to is unavailable, did not route delivery in route_delivery",
                   conn->conn_id, stream_data->stream_id, stream_data->in_link->identity);
        }
    }
    else {
        if (stream_data->entire_header_arrived && !stream_data->in_dlv) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32 "] Calling compose_and_deliver, routing delivery", conn->conn_id,
                   stream_data->stream_id);
            delivery_routed = compose_and_deliver(conn, stream_data, receive_complete);
        }
    }

    return delivery_routed;
}

static void create_settings_frame(qdr_http2_connection_t *conn)
{
    nghttp2_settings_entry iv[4] = {{NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 100},
                                    {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, WINDOW_SIZE},
                                    {NGHTTP2_SETTINGS_MAX_FRAME_SIZE, MAX_FRAME_SIZE},
                                    {NGHTTP2_SETTINGS_ENABLE_PUSH, 0}};

    // You must call nghttp2_session_send after calling nghttp2_submit_settings
    int rv = nghttp2_submit_settings(conn->session, NGHTTP2_FLAG_NONE, iv, ARRLEN(iv));
    if (rv != 0) {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR, "[C%" PRIu64 "] Fatal error sending settings frame, rv=%i",
               conn->conn_id, rv);
        return;
    }
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] Initial SETTINGS frame sent on conn=%p",
           conn->conn_id, (void *) conn);
}

static void send_settings_frame(qdr_http2_connection_t *conn)
{
    if (!conn->initial_settings_frame_sent) {
        create_settings_frame(conn);
        nghttp2_session_send(conn->session);
        conn->initial_settings_frame_sent = true;
    }
    // qd_raw_connection_write_buffers(conn->pn_raw_conn, &conn->out_buffs);
}

static void _http_record_request(qdr_http2_connection_t *conn, qdr_http2_stream_data_t *stream_data)
{
    stream_data->stop = qd_timer_now();

    bool free_remote_addr = false;
    char *remote_addr;
    if (conn->ingress) {
        remote_addr = qd_get_host_from_host_port(conn->remote_address);
        if (remote_addr) {
            free_remote_addr = true;
        } else {
            remote_addr = conn->remote_address;
        }
    } else {
        remote_addr = conn->config?conn->config->adaptor_config->host:0;
    }
    qd_http_record_request(http2_adaptor->core,
                           stream_data->method,
                           stream_data->request_status?atoi(stream_data->request_status):0,
                           conn->config?conn->config->adaptor_config->address:0,
                           remote_addr, conn->config?conn->config->adaptor_config->site_id:0,
                           stream_data->remote_site,
                           conn->ingress, stream_data->bytes_in, stream_data->bytes_out,
                           stream_data->stop && stream_data->start ? stream_data->stop - stream_data->start : 0);
    if (free_remote_addr) {
        free(remote_addr);
    }
}

static int on_frame_recv_callback(nghttp2_session *session,
                                  const nghttp2_frame *frame,
                                  void *user_data)
{
    qdr_http2_connection_t *conn = (qdr_http2_connection_t *)user_data;
    int32_t stream_id = frame->hd.stream_id;
    qdr_http2_stream_data_t *stream_data = nghttp2_session_get_stream_user_data(conn->session, stream_id);

    switch (frame->hd.type) {
    case NGHTTP2_GOAWAY: {
        //
        // A GOAWAY frame has been received from the HTTP2 server. Usually a server sends a GOAWAY but nothing prevents the client from sending one.
        //
        // "The GOAWAY frame is used to initiate shutdown of a connection or to signal serious error conditions.  GOAWAY allows an
        // endpoint to gracefully stop accepting new streams while still
        // finishing processing of previously established streams.  This enables administrative actions, like server maintenance.
        // Receivers of a GOAWAY frame MUST NOT open additional streams on the connection, although a new connection can be established for new streams."
        //
        // We will close any unprocessed streams on the connection. In doing so, all the outstanding deliveries on that connection will be PN_RELEASED which will in turn release all the peer
        // deliveries on the client side which will enable us to send a GOAWAY frame to the client. This is how we propagate a GOAWAY received from the server side to the client side.
        //
        // We will also close the pn_raw_connection (we will not close the qdr_connection_t and the qdr_http2_connection_t, those will still remain). This will close the TCP connection to the server
        // and will enable creation  of a new connection to the server since we are not allowed to create any more streams on the connection that received the GOAWAY frame.
        //
        int32_t last_stream_id = frame->goaway.last_stream_id;
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR,
               "[C%" PRIu64 "][S%" PRId32 "] GOAWAY frame received, last_stream_id=[%" PRId32 "]", conn->conn_id,
               stream_id, last_stream_id);
        // Free all streams that are greater that the last_stream_id because the server is not going to process those streams.
        free_unprocessed_streams(conn, last_stream_id);
        conn->goaway_received = true;
        pn_raw_connection_close(conn->pn_raw_conn);
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR,
               "[C%" PRIu64 "][S%" PRId32 "] pn_raw_connection closed after GOAWAY frame received", conn->conn_id,
               stream_id);
        return 0;
    }
    break;
    case NGHTTP2_PUSH_PROMISE: {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][S%" PRId32 "] HTTP2 NGHTTP2_PUSH_PROMISE frame received", conn->conn_id, stream_id);
    } break;
    case NGHTTP2_RST_STREAM: {
        if (stream_data) {
                if (stream_data->out_dlv) {
                    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                           "[C%" PRIu64 "][S%" PRId32 "] HTTP2 NGHTTP2_RST_STREAM frame received, rejecting out_dlv",
                           conn->conn_id, stream_id);
                    //
                    // The client sent an RST_STREAM frame which means it does not want to hear from the router on this
                    // stream anymore. We will reject this delivery which is already in progress/streaming. Rejecting
                    // this delivery will free it and its peer delivery.
                    //
                    stream_data->out_dlv_local_disposition = PN_REJECTED;
                    qdr_delivery_remote_state_updated(http2_adaptor->core, stream_data->out_dlv,
                                                      stream_data->out_dlv_local_disposition, true, 0, false);
                }
                if (stream_data->in_dlv && !stream_data->in_dlv_decrefed) {
                    // The stream_data->in_dlv could sometimes be freed from underneath when there
                    // is a race between the connection close and the handling of the RST_STREAM
                    // Procced to set the context only if the stream_data->in_dlv_decrefed has not already been
                    // decrefed.
                    // Fix for https://github.com/skupperproject/skupper-router/issues/1106
                    if (stream_data->in_dlv_decrefed) {
                        stream_data->in_dlv = 0;
                    } else {
                        qdr_delivery_set_context(stream_data->in_dlv, 0);
                    }
                }
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32 "] HTTP2 NGHTTP2_RST_STREAM frame received, freeing stream data",
                       conn->conn_id, stream_id);

                // Free the stream data object since it is no longer needed.
                free_http2_stream_data(stream_data, false);
        }
    } break;
    case NGHTTP2_PING: {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][S%" PRId32 "] HTTP2 PING frame received",
               conn->conn_id, stream_id);
    }
    break;
    case NGHTTP2_PRIORITY: {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][S%" PRId32 "] HTTP2 PRIORITY frame received",
               conn->conn_id, stream_id);
    }
    break;
    case NGHTTP2_SETTINGS: {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][S%" PRId32 "] HTTP2 SETTINGS frame received",
               conn->conn_id, stream_id);
    }
    break;
    case NGHTTP2_WINDOW_UPDATE:
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][S%" PRId32 "] HTTP2 WINDOW_UPDATE frame received", conn->conn_id, stream_id);
        break;
    case NGHTTP2_DATA: {

        if (!stream_data)
            return 0;

        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][S%" PRId32 "] NGHTTP2_DATA frame received",
               conn->conn_id, stream_id);

        if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) {
            if (!stream_data->stream_force_closed) {
                qd_message_set_receive_complete(stream_data->message);
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32
                       "] NGHTTP2_DATA NGHTTP2_FLAG_END_STREAM flag received, setting receive_complete = true",
                       conn->conn_id, stream_id);
            }
            advance_stream_status(stream_data);
        }

        if (stream_data->in_dlv && !stream_data->stream_force_closed) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32 "] NGHTTP2_DATA frame received, qdr_delivery_continue " DLV_FMT,
                   conn->conn_id, stream_id, DLV_ARGS(stream_data->in_dlv));
            qdr_delivery_continue(http2_adaptor->core, stream_data->in_dlv, false);
        }

        if (stream_data->out_dlv && !stream_data->disp_updated && !stream_data->out_dlv_decrefed && stream_data->status == QD_STREAM_FULLY_CLOSED ) {
            stream_data->disp_updated = true;
            qdr_delivery_remote_state_updated(http2_adaptor->core, stream_data->out_dlv, stream_data->out_dlv_local_disposition, true, 0, false);
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32
                   "] In on_frame_recv_callback NGHTTP2_DATA QD_STREAM_FULLY_CLOSED, "
                   "qdr_delivery_remote_state_updated(stream_data->out_dlv)",
                   conn->conn_id, stream_data->stream_id);
        }
    }
    break;
    case NGHTTP2_HEADERS:
    case NGHTTP2_CONTINUATION: {
        if (!stream_data)
            return 0;
        if (frame->hd.type == NGHTTP2_CONTINUATION) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32 "] HTTP2 CONTINUATION frame received", conn->conn_id, stream_id);
        }
        else {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32 "] HTTP2 HEADERS frame received", conn->conn_id, stream_id);
        }

        if (frame->hd.flags & NGHTTP2_FLAG_END_HEADERS) {
            /* All the headers have been received. Send out the AMQP message */
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32
                   "] HTTP2 NGHTTP2_FLAG_END_HEADERS flag received, all headers have arrived",
                   conn->conn_id, stream_id);
            stream_data->entire_header_arrived = true;

            if (stream_data->use_footer_properties) {
                qd_compose_end_map(stream_data->footer_properties);
                stream_data->entire_footer_arrived = true;
                qd_message_extend(stream_data->message, stream_data->footer_properties, 0);
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32 "] Closing footer map, extending message with footer", conn->conn_id,
                       stream_id);
            }
            else {
                //
                // All header fields have been received. End the application properties map.
                //
                stream_data->use_footer_properties = true;
                qd_compose_end_map(stream_data->app_properties);
            }

            bool receive_complete = false;
            if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) {
                if (stream_data->entire_footer_arrived) {
                        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                               "[C%" PRIu64 "][S%" PRId32
                               "] HTTP2 NGHTTP2_FLAG_END_HEADERS and NGHTTP2_FLAG_END_STREAM flag received (footer), "
                               "receive_complete=true",
                               conn->conn_id, stream_id);
                }
                else {
                        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                               "[C%" PRIu64 "][S%" PRId32
                               "] HTTP2 NGHTTP2_FLAG_END_HEADERS and NGHTTP2_FLAG_END_STREAM flag received, "
                               "receive_complete=true",
                               conn->conn_id, stream_id);
                }
                qd_message_set_receive_complete(stream_data->message);
                advance_stream_status(stream_data);
                receive_complete = true;
            }

            if (stream_data->entire_footer_arrived) {
                if (stream_data->in_dlv) {
                    qdr_delivery_continue(http2_adaptor->core, stream_data->in_dlv, false);
                    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                           "[C%" PRIu64 "][S%" PRId32 "] Entire footer arrived, qdr_delivery_continue " DLV_FMT,
                           conn->conn_id, stream_id, DLV_ARGS(stream_data->in_dlv));
                }
                else {
                    if (route_delivery(stream_data, receive_complete)) {
                        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                               "[C%" PRIu64 "][S%" PRId32
                               "] Entire footer arrived, delivery routed successfully (on_frame_recv_callback)",
                               conn->conn_id, stream_id);
                    }
                }
            }
            else {
                //
                // All headers have arrived, send out the delivery with just the headers,
                // if/when the body arrives later, we will call the qdr_delivery_continue()
                //
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32
                       "] All headers arrived, trying to route delivery (on_frame_recv_callback)",
                       conn->conn_id, stream_id);
                if (route_delivery(stream_data, receive_complete)) {
                    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                           "[C%" PRIu64 "][S%" PRId32
                           "] All headers arrived, delivery routed successfully (on_frame_recv_callback)",
                           conn->conn_id, stream_id);
                }
            }

            if (stream_data->out_dlv && !stream_data->disp_updated && !stream_data->out_dlv_decrefed && stream_data->status == QD_STREAM_FULLY_CLOSED) {
                qdr_delivery_remote_state_updated(http2_adaptor->core, stream_data->out_dlv, stream_data->out_dlv_local_disposition, true, 0, false);
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32
                       "] In on_frame_recv_callback NGHTTP2_HEADERS QD_STREAM_FULLY_CLOSED, "
                       "qdr_delivery_remote_state_updated(stream_data->out_dlv)",
                       conn->conn_id, stream_data->stream_id);
                stream_data->disp_updated = true;
            }
        }
    }
    break;
    default:
        break;
  }
    return 0;
}

ssize_t read_data_callback(nghttp2_session *session,
                      int32_t stream_id,
                      uint8_t *buf,
                      size_t length,
                      uint32_t *data_flags,
                      nghttp2_data_source *source,
                      void *user_data)
{
    qdr_http2_connection_t *conn = (qdr_http2_connection_t *)user_data;
    qdr_http2_stream_data_t *stream_data = (qdr_http2_stream_data_t *)source->ptr;
    qd_message_t *message = qdr_delivery_message(stream_data->out_dlv);
    qd_message_depth_status_t status = qd_message_check_depth(message, QD_DEPTH_BODY);

    CHECK_PROACTOR_RAW_CONNECTION(conn->pn_raw_conn);

    // This flag tells nghttp2 that the data is not being copied into the buffer supplied by nghttp2 (uint8_t *buf).
    *data_flags |= NGHTTP2_DATA_FLAG_NO_COPY;

    switch (status) {
    case QD_MESSAGE_DEPTH_OK: {
        //
        // At least one complete body performative has arrived.  It is now safe to switch
        // over to the per-message extraction of body-data segments.
        //
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][S%" PRId32 "] read_data_callback QD_MESSAGE_DEPTH_OK", conn->conn_id,
               stream_data->stream_id);

        if (stream_data->next_stream_data) {
            stream_data->curr_stream_data = stream_data->next_stream_data;
            qd_iterator_free(stream_data->curr_stream_data_iter);
            stream_data->curr_stream_data_iter = qd_message_stream_data_iterator(stream_data->curr_stream_data);
            stream_data->curr_stream_data_result = stream_data->next_stream_data_result;
            stream_data->next_stream_data = 0;
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32 "] read_data_callback Use next_stream_data", conn->conn_id,
                   stream_data->stream_id);
        }

        if (!stream_data->curr_stream_data) {
            stream_data->curr_stream_data_result = qd_message_next_stream_data(message, &stream_data->curr_stream_data);
            if (stream_data->curr_stream_data) {
                qd_iterator_free(stream_data->curr_stream_data_iter);
                stream_data->curr_stream_data_iter = qd_message_stream_data_iterator(stream_data->curr_stream_data);
            }
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32 "] read_data_callback No body data, get qd_message_next_stream_data",
                   conn->conn_id, stream_data->stream_id);
        }

        if (stream_data->next_stream_data == 0 &&
            (stream_data->next_stream_data_result == QD_MESSAGE_STREAM_DATA_NO_MORE ||
             stream_data->next_stream_data_result == QD_MESSAGE_STREAM_DATA_ABORTED ||
             stream_data->next_stream_data_result == QD_MESSAGE_STREAM_DATA_INVALID)) {
            stream_data->curr_stream_data_result = stream_data->next_stream_data_result;
        }

        switch (stream_data->curr_stream_data_result) {
        case QD_MESSAGE_STREAM_DATA_BODY_OK: {
            //
            // We have a new valid body-data segment.  Handle it
            //
            size_t pn_buffs_write_capacity = pn_raw_connection_write_buffers_capacity(conn->pn_raw_conn);
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32
                   "] read_data_callback QD_MESSAGE_STREAM_DATA_BODY_OK pn_raw_connection_write_buffers_capacity=%zu",
                   conn->conn_id, stream_data->stream_id, pn_buffs_write_capacity);

            if (pn_buffs_write_capacity == 0) {
                //
                // Proton capacity is zero, we will come back later to write this stream, return for now.
                //
                    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                           "[C%" PRIu64 "][S%" PRId32
                           "] Exiting read_data_callback, QD_MESSAGE_STREAM_DATA_BODY_OK pn_buffs_write_capacity=0, "
                           "pausing stream, returning NGHTTP2_ERR_DEFERRED",
                           conn->conn_id, stream_data->stream_id);
                    stream_data->out_dlv_local_disposition = 0;
                    return NGHTTP2_ERR_DEFERRED;
            }

            // total length of the payload (across all qd_buffers in the current body data)
            size_t payload_length = qd_message_stream_data_payload_length(stream_data->curr_stream_data);

            if (payload_length == 0) {
                    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                           "[C%" PRIu64 "][S%" PRId32 "] read_data_callback, payload_length=0", conn->conn_id,
                           stream_data->stream_id);

                    // The payload length is zero on this body data. Look ahead one body data to see if it is
                    // QD_MESSAGE_STREAM_DATA_NO_MORE
                    stream_data->next_stream_data_result =
                        qd_message_next_stream_data(message, &stream_data->next_stream_data);
                    if (stream_data->next_stream_data_result == QD_MESSAGE_STREAM_DATA_NO_MORE) {
                        if (!stream_data->out_msg_has_footer) {
                            qd_message_stream_data_release(stream_data->curr_stream_data);
                            qd_iterator_free(stream_data->curr_stream_data_iter);
                            stream_data->curr_stream_data_iter = 0;
                            stream_data->curr_stream_data      = 0;
                        }

                        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
                        stream_data->out_msg_data_flag_eof = true;
                        stream_data->out_msg_body_sent     = true;
                        stream_data->full_payload_handled  = true;
                        if (stream_data->next_stream_data) {
                            qd_message_stream_data_release(stream_data->next_stream_data);
                            stream_data->next_stream_data = 0;
                        }
                        stream_data->out_dlv_local_disposition = PN_ACCEPTED;
                        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                               "[C%" PRIu64 "][S%" PRId32
                               "] read_data_callback, payload_length=0 and "
                               "next_stream_data=QD_MESSAGE_STREAM_DATA_NO_MORE",
                               conn->conn_id, stream_data->stream_id);
                }
                else if (stream_data->next_stream_data_result == QD_MESSAGE_STREAM_DATA_FOOTER_OK) {
                    stream_data->full_payload_handled = true;
                    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                           "[C%" PRIu64 "][S%" PRId32
                           "] read_data_callback, payload_length=0 and "
                           "next_stream_data_result=QD_MESSAGE_STREAM_DATA_FOOTER_OK",
                           conn->conn_id, stream_data->stream_id);
                }
                else {
                    qd_message_stream_data_release(stream_data->curr_stream_data);
                    qd_iterator_free(stream_data->curr_stream_data_iter);
                    stream_data->curr_stream_data_iter = 0;
                    stream_data->curr_stream_data = 0;
                }

                //
                // The payload length on this body data is zero. Nothing to do, just return zero to move on to the next body data. Usually, zero length body datas are a result of programmer error.
                //
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32 "] Exiting read_data_callback, payload_length=0, returning 0",
                       conn->conn_id, stream_data->stream_id);
                return 0;
            }

            size_t bytes_to_send = 0;
            if (payload_length) {
                int remaining_payload_length = payload_length - stream_data->payload_handled;

                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32 "] read_data_callback remaining_payload_length=%i, length=%zu",
                       conn->conn_id, stream_data->stream_id, remaining_payload_length, length);

                if (remaining_payload_length <= QD_ADAPTOR_MAX_BUFFER_SIZE) {
                    if (length < remaining_payload_length) {
                        bytes_to_send                     = length;
                        stream_data->full_payload_handled = false;
                    } else {
                        bytes_to_send                     = remaining_payload_length;
                        stream_data->full_payload_handled = true;
                        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                               "[C%" PRIu64 "][S%" PRId32
                               "] read_data_callback remaining_payload_length (%i) <= qd_adaptor_buffer_size, "
                               "bytes_to_send=%zu",
                               conn->conn_id, stream_data->stream_id, remaining_payload_length, bytes_to_send);

                        // Look ahead one body data
                        stream_data->next_stream_data_result = qd_message_next_stream_data(message, &stream_data->next_stream_data);
                        if (stream_data->next_stream_data_result == QD_MESSAGE_STREAM_DATA_NO_MORE) {
                            *data_flags |= NGHTTP2_DATA_FLAG_EOF;
                            stream_data->out_msg_data_flag_eof = true;
                            stream_data->out_msg_body_sent = true;
                            stream_data->out_dlv_local_disposition = PN_ACCEPTED;
                            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                                   "[C%" PRIu64 "][S%" PRId32
                                   "] read_data_callback, looking ahead one body data QD_MESSAGE_STREAM_DATA_NO_MORE",
                                   conn->conn_id, stream_data->stream_id);
                        }
                        else if (stream_data->next_stream_data_result == QD_MESSAGE_STREAM_DATA_FOOTER_OK) {
                            stream_data->out_msg_has_footer = true;
                            stream_data->out_msg_body_sent = true;
                            qd_log(
                                LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                                "[C%" PRIu64 "][S%" PRId32
                                "] read_data_callback, looking ahead one body data, QD_MESSAGE_STREAM_DATA_FOOTER_OK",
                                conn->conn_id, stream_data->stream_id);
                        }
                    }
                } else {
                    // This means that there is more that 16k worth of payload in one body data.
                    // We want to send only 16k or less of data per read_data_callback.
                    // We can only send what nghttp2 allows us to send. nghttp2 might be doing http2 flow control and
                    // we abide by it.
                    if (length < QD_ADAPTOR_MAX_BUFFER_SIZE) {
                        bytes_to_send = length;
                    } else {
                        bytes_to_send = QD_ADAPTOR_MAX_BUFFER_SIZE;
                    }

                    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                           "[C%" PRIu64 "][S%" PRId32
                           "] read_data_callback remaining_payload_length <= qd_adaptor_buffer_size ELSE "
                           "bytes_to_send=%zu",
                           conn->conn_id, stream_data->stream_id, bytes_to_send);
                    stream_data->full_payload_handled = false;
                }
            }

            stream_data->bytes_out += bytes_to_send;
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32 "] read_data_callback returning bytes_to_send=%zu", conn->conn_id,
                   stream_data->stream_id, bytes_to_send);
            return bytes_to_send;
        }

        case QD_MESSAGE_STREAM_DATA_FOOTER_OK:
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32 "] read_data_callback QD_MESSAGE_STREAM_DATA_FOOTER_OK", conn->conn_id,
                   stream_data->stream_id);
            if (!stream_data->out_msg_has_footer) {
                stream_data->out_msg_has_footer = true;
                stream_data->next_stream_data_result =
                    qd_message_next_stream_data(message, &stream_data->next_stream_data);
            }

            if (stream_data->next_stream_data) {
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32
                       "] read_data_callback QD_MESSAGE_STREAM_DATA_FOOTER_OK, we have a next_stream_data",
                       conn->conn_id, stream_data->stream_id);
            }
            if (stream_data->next_stream_data_result == QD_MESSAGE_STREAM_DATA_INVALID ||
                stream_data->next_stream_data_result == QD_MESSAGE_STREAM_DATA_ABORTED) {
                stream_data->out_msg_has_footer = false;
                if (stream_data->next_stream_data) {
                    qd_message_stream_data_release(stream_data->next_stream_data);
                    stream_data->next_stream_data = 0;
                }
            }
            break;

        case QD_MESSAGE_STREAM_DATA_INCOMPLETE:
            //
            // A new segment has not completely arrived yet.  Check again later.
            //
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32
                   "] read_data_callback QD_MESSAGE_STREAM_DATA_INCOMPLETE, returning NGHTTP2_ERR_DEFERRED",
                   conn->conn_id, stream_data->stream_id);
            stream_data->out_dlv_local_disposition = 0;
            return NGHTTP2_ERR_DEFERRED;

        case QD_MESSAGE_STREAM_DATA_NO_MORE: {
            //
            // We have already handled the last body-data segment for this delivery.
            //
            size_t pn_buffs_write_capacity = pn_raw_connection_write_buffers_capacity(conn->pn_raw_conn);
            if (pn_buffs_write_capacity == 0) {
                stream_data->out_dlv_local_disposition = 0;
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32
                       "] read_data_callback QD_MESSAGE_STREAM_DATA_NO_MORE - pn_buffs_write_capacity=0 send is not "
                       "complete",
                       conn->conn_id, stream_data->stream_id);
                return NGHTTP2_ERR_DEFERRED;
            }
            else {
                *data_flags |= NGHTTP2_DATA_FLAG_EOF;
                stream_data->out_msg_data_flag_eof = true;
                if (stream_data->out_msg_has_footer) {
                    //
                    // We have to send the trailer fields.
                    // You cannot send trailer fields after sending frame with END_STREAM
                    // set.  To avoid this problem, one can set
                    // NGHTTP2_DATA_FLAG_NO_END_STREAM along with
                    // NGHTTP2_DATA_FLAG_EOF to signal the library not to set
                    // END_STREAM in DATA frame.
                    //
                    *data_flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;
                    qd_log(
                        LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                        "[C%" PRIu64 "][S%" PRId32
                        "] read_data_callback stream_data->out_msg_has_footer, setting NGHTTP2_DATA_FLAG_NO_END_STREAM",
                        conn->conn_id, stream_data->stream_id);
                }
                stream_data->full_payload_handled = true;
                stream_data->out_msg_body_sent = true;
                stream_data->out_dlv_local_disposition = PN_ACCEPTED;
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32
                       "] read_data_callback QD_MESSAGE_STREAM_DATA_NO_MORE - stream_data->out_dlv_local_disposition = "
                       "PN_ACCEPTED - send_complete=true, setting NGHTTP2_DATA_FLAG_EOF",
                       conn->conn_id, stream_data->stream_id);
            }

            break;
        }

        case QD_MESSAGE_STREAM_DATA_INVALID:
        case QD_MESSAGE_STREAM_DATA_ABORTED:
            //
            // The body-data is corrupt or the sender aborted the message (incomplete).  Stop handling the delivery and
            // reject it.
            //
            *data_flags |= NGHTTP2_DATA_FLAG_EOF;
            stream_data->out_msg_data_flag_eof = true;
            if (stream_data->curr_stream_data) {
                qd_message_stream_data_release(stream_data->curr_stream_data);
                qd_iterator_free(stream_data->curr_stream_data_iter);
                stream_data->curr_stream_data_iter = 0;
                stream_data->curr_stream_data      = 0;
            }
            stream_data->out_dlv_local_disposition = PN_REJECTED;
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR,
                   "[C%" PRIu64 "][S%" PRId32 "] read_data_callback QD_MESSAGE_STREAM_DATA_%s", conn->conn_id,
                   stream_data->stream_id,
                   stream_data->curr_stream_data_result == QD_MESSAGE_STREAM_DATA_ABORTED ? "ABORTED" : "INVALID");
            break;
        }
        break;
    }

    case QD_MESSAGE_DEPTH_INVALID:
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR,
               "[C%" PRIu64 "][S%" PRId32 "] read_data_callback QD_MESSAGE_DEPTH_INVALID", conn->conn_id,
               stream_data->stream_id);
        stream_data->out_dlv_local_disposition = PN_REJECTED;
        break;

    case QD_MESSAGE_DEPTH_INCOMPLETE:
        stream_data->out_dlv_local_disposition = 0;
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][S%" PRId32 "] read_data_callback QD_MESSAGE_DEPTH_INCOMPLETE", conn->conn_id,
               stream_data->stream_id);
        return NGHTTP2_ERR_DEFERRED;
    }

    return 0;
}

qdr_http2_connection_t *qdr_http_connection_ingress(qd_http_listener_t* listener)
{
    qdr_http2_connection_t* ingress_http_conn = new_qdr_http2_connection_t();
    ZERO(ingress_http_conn);

    ingress_http_conn->conn_id         = qd_server_allocate_connection_id(listener->server);
    ingress_http_conn->ingress = true;
    ingress_http_conn->require_tls     = !!listener->tls_domain;
    ingress_http_conn->context.context = ingress_http_conn;
    ingress_http_conn->context.handler = &handle_connection_event;
    ingress_http_conn->listener = listener;

    // Incref the ref count on the listener since the qdr_http2_connection_t object is holding a ref to the listener
    sys_atomic_inc(&listener->ref_count);

    ingress_http_conn->config = listener->config;
    ingress_http_conn->server = listener->server;
    ingress_http_conn->pn_raw_conn = pn_raw_connection();
    sys_atomic_init(&ingress_http_conn->activate_scheduled, 0);
    sys_atomic_init(&ingress_http_conn->raw_closed_read, 0);
    sys_atomic_init(&ingress_http_conn->raw_closed_write, 0);
    sys_atomic_init(&ingress_http_conn->q2_restart, 0);
    sys_atomic_init(&ingress_http_conn->delay_buffer_write, 0);
    DEQ_INIT(ingress_http_conn->out_buffs);
    DEQ_INIT(ingress_http_conn->streams);
    ingress_http_conn->data_prd.read_callback = read_data_callback;

    //
    // Start an ingress connection level vanflow record. The parent of the connection level
    // vanflow record is the listener's vanflow record.
    //
    ingress_http_conn->vflow = vflow_start_record(VFLOW_RECORD_FLOW, listener->vflow);
    vflow_set_uint64(ingress_http_conn->vflow, VFLOW_ATTRIBUTE_OCTETS, 0);
    vflow_add_rate(ingress_http_conn->vflow, VFLOW_ATTRIBUTE_OCTETS, VFLOW_ATTRIBUTE_OCTET_RATE);
    vflow_set_uint64(ingress_http_conn->vflow, VFLOW_ATTRIBUTE_WINDOW_SIZE, WINDOW_SIZE);

    sys_mutex_lock(&http2_adaptor->lock);
    DEQ_INSERT_TAIL(http2_adaptor->connections, ingress_http_conn);
    sys_mutex_unlock(&http2_adaptor->lock);

    nghttp2_session_server_new(&(ingress_http_conn->session), (nghttp2_session_callbacks*)http2_adaptor->callbacks, ingress_http_conn);
    pn_raw_connection_set_context(ingress_http_conn->pn_raw_conn, ingress_http_conn);
    return ingress_http_conn;
}

static void qdr_http_detach(void *context, qdr_link_t *link, qdr_error_t *error, bool first, bool close)
{
}


static void qdr_http_flow(void *context, qdr_link_t *link, int credit)
{
    if (credit > 0) {
        qdr_http2_stream_data_t *stream_data = qdr_link_get_context(link);
        if (! stream_data)
            return;
        stream_data->in_link_credit += credit;
        if (!stream_data->in_dlv) {
            if (route_delivery(stream_data, qd_message_receive_complete(stream_data->message))) {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][S%" PRId32 "] qdr_http_flow, delivery routed successfully", stream_data->conn->conn_id,
               stream_data->stream_id);
            }
        }
    }
}


static void qdr_http_offer(void *context, qdr_link_t *link, int delivery_count)
{
}


static void qdr_http_drained(void *context, qdr_link_t *link)
{
}


static void qdr_http_drain(void *context, qdr_link_t *link, bool mode)
{
}

static int qdr_http_get_credit(void *context, qdr_link_t *link)
{
    return 10;
}


ssize_t error_read_callback(nghttp2_session *session,
                      int32_t stream_id,
                      uint8_t *buf,
                      size_t length,
                      uint32_t *data_flags,
                      nghttp2_data_source *source,
                      void *user_data)
{
    size_t len = 0;
    char *error_msg = (char *) source->ptr;
    if (error_msg) {
        len  = strlen(error_msg);
        if (len > 0)
            memcpy(buf, error_msg, len);
    }
    *data_flags |= NGHTTP2_DATA_FLAG_EOF;
    return len;
}

static void qdr_http_delivery_update(void *context, qdr_delivery_t *dlv, uint64_t disp, bool settled)
{
    qdr_http2_stream_data_t* stream_data = qdr_delivery_get_context(dlv);
    if (!stream_data)
        return;

    qdr_http2_connection_t *conn = stream_data->conn;

    //
    // DISPATCH-1849: In the case of large messages, the final DATA frame arriving from the server may or may not
    // contain the END_STREAM flag. In the cases when the final DATA frame does not contain the END_STREAM flag,
    // the router ends up forwarding all the data to the curl client without sending the END_STREAM to the client. The END_STREAM does arrive from the server
    // but not before the curl client closes the client connection after receiving all the data. The curl client
    // does not wait for the router to send an END_STREAM flag to close the connection. The client connection closure
    // triggers the link cleanup on the ingress connection, in turn freeing up all deliveries and its peer deliveries.
    // The peer delivery is released while it is still receiving the END_STREAM frame and the router crashes when we try to set receive complete
    // on the message because the message has already been freed. To solve this issue,
    // the stream_data->stream_force_closed flag is set to true when the peer delivery is released and this flag is
    // check when performing further actions on the delivery. No action on the peer delivery is performed
    // if this flag is set because the delivery and its underlying message have been freed.
    //
    if (settled && !conn->ingress && (disp == PN_RELEASED || disp == PN_MODIFIED || disp == PN_REJECTED)) {
        stream_data->stream_force_closed = true;
    }

    if (settled) {
        nghttp2_nv hdrs[3];
        if (conn->ingress && (disp == PN_RELEASED || disp == PN_MODIFIED || disp == PN_REJECTED)) {
            if (disp == PN_RELEASED || disp == PN_MODIFIED) {
                hdrs[0].name = (uint8_t *)":status";
                hdrs[0].value = (uint8_t *)"503";
                hdrs[0].namelen = 7;
                hdrs[0].valuelen = 3;
                hdrs[0].flags = NGHTTP2_NV_FLAG_NONE;
            }
            else if (disp == PN_REJECTED) {
                hdrs[0].name = (uint8_t *)":status";
                hdrs[0].value = (uint8_t *)"400";
                hdrs[0].namelen = 7;
                hdrs[0].valuelen = 3;
                hdrs[0].flags = NGHTTP2_NV_FLAG_NONE;
            }

            hdrs[1].name = (uint8_t *)"content-type";
            hdrs[1].value = (uint8_t *)"text/html; charset=utf-8";
            hdrs[1].namelen = 12;
            hdrs[1].valuelen = 24;
            hdrs[1].flags = NGHTTP2_NV_FLAG_NONE;

            hdrs[2].name = (uint8_t *)"content-length";
            hdrs[2].value = (uint8_t *)"0";
            hdrs[2].namelen = 14;
            hdrs[2].valuelen = 1;
            hdrs[2].flags = NGHTTP2_NV_FLAG_NONE;

            nghttp2_submit_headers(stream_data->conn->session, NGHTTP2_FLAG_END_HEADERS | NGHTTP2_FLAG_END_STREAM, stream_data->stream_id, NULL, hdrs, 3, 0);
        }

        if (!conn->ingress && (disp == PN_RELEASED || disp == PN_MODIFIED || disp == PN_REJECTED)) {
            //
            // On the server side connection, send a DATA frame with an END_STREAM flag thus closing the particular stream. We
            // don't want to close the entire connection like we did not the client side.
            //
            nghttp2_submit_data(conn->session, NGHTTP2_FLAG_END_STREAM, stream_data->stream_id, &conn->data_prd);
            // On the ingress side, there is a possibility that the client immediately closes the connection as soon as it receives the
            // required data frames. The client does not need to wait to receive the END_STREAM flag.
            // When that happens, the delivery on the ingress side is freed which in turn releases its peer delivery which means we can close
            // the stream on the egress side.
            // The stream status can be set to QD_STREAM_FULLY_CLOSED and freed.
            advance_stream_status(stream_data);
            stream_data->out_msg_send_complete = true;
        }

        nghttp2_session_send(stream_data->conn->session);

        qdr_delivery_set_context(dlv, 0);
        if (stream_data->in_dlv == dlv) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32 "] qdr_http_delivery_update, stream_data->in_dlv == dlv",
                   stream_data->conn->conn_id, stream_data->stream_id);
        }
        else if (stream_data->out_dlv == dlv) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32 "] qdr_http_delivery_update, stream_data->out_dlv == dlv",
                   stream_data->conn->conn_id, stream_data->stream_id);
        }

        if (stream_data->status == QD_STREAM_FULLY_CLOSED) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32
                   "] qdr_http_delivery_update, stream_data->status == QD_STREAM_FULLY_CLOSED",
                   stream_data->conn->conn_id, stream_data->stream_id);
        }
        else {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32
                   "] qdr_http_delivery_update, stream_data->status != QD_STREAM_FULLY_CLOSED",
                   stream_data->conn->conn_id, stream_data->stream_id);
        }

        bool send_complete = stream_data->out_msg_send_complete;
        if (send_complete) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32 "] qdr_http_delivery_update, send_complete=true",
                   stream_data->conn->conn_id, stream_data->stream_id);
        }
        else {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32 "] qdr_http_delivery_update, send_complete=false",
                   stream_data->conn->conn_id, stream_data->stream_id);
        }

        set_stream_data_delivery_flags(stream_data, dlv);
        qdr_delivery_decref(http2_adaptor->core, dlv, "HTTP2 adaptor  - qdr_http_delivery_update");

        if (send_complete && stream_data->status == QD_STREAM_FULLY_CLOSED) {
            // When all the necessary HTTP2 frames have been sent and the stream is fully closed, free the stream.
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32
                   "] qdr_http_delivery_update, stream_data->status == QD_STREAM_FULLY_CLOSED, calling "
                   "free_http2_stream_data, send_complete(dlv)=%i",
                   stream_data->conn->conn_id, stream_data->stream_id, stream_data->out_msg_send_complete);
            free_http2_stream_data(stream_data, false);
        }
        else {
            stream_data->disp_applied = true;
        }
    }
}


static void qdr_http_conn_close(void *context, qdr_connection_t *qdr_conn, qdr_error_t *error)
{
    if (qdr_conn) {
        qdr_http2_connection_t *http_conn = qdr_connection_get_context(qdr_conn);
        assert(http_conn);
        if (http_conn) {
            //
            // When the pn_raw_connection_close() is called, the
            // PN_RAW_CONNECTION_READ and PN_RAW_CONNECTION_WRITTEN events to be emitted so
            // the application can clean up buffers given to the raw connection. After that a
            // PN_RAW_CONNECTION_DISCONNECTED event will be emitted which will in turn call handle_disconnected().
            //
            pn_raw_connection_close(http_conn->pn_raw_conn);
        }
    }
}


static void qdr_http_conn_trace(void *context, qdr_connection_t *conn, bool trace)
{
}


static void qdr_http_first_attach(void *context, qdr_connection_t *conn, qdr_link_t *link,
                                 qdr_terminus_t *source, qdr_terminus_t *target,
                                 qd_session_class_t session_class)
{
}


static void qdr_copy_reply_to(qdr_http2_stream_data_t* stream_data, qd_iterator_t* reply_to)
{
    int length = qd_iterator_length(reply_to);
    stream_data->reply_to = malloc(length + 1);
    qd_iterator_strncpy(reply_to, stream_data->reply_to, length + 1);
}


static void qdr_http_second_attach(void *context, qdr_link_t *link,
                                  qdr_terminus_t *source, qdr_terminus_t *target)
{
    qdr_http2_stream_data_t *stream_data =  (qdr_http2_stream_data_t*)qdr_link_get_context(link);
    if (stream_data) {
        if (qdr_link_direction(link) == QD_OUTGOING && source->dynamic) {
            if (stream_data->conn->ingress) {
                qdr_copy_reply_to(stream_data, qdr_terminus_get_address(source));
                if (route_delivery(stream_data, qd_message_receive_complete(stream_data->message))) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] Reply-to available now, delivery routed successfully", stream_data->conn->conn_id);
                }
                else {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] Reply-to available but delivery not routed (qdr_http_second_attach)",
                   stream_data->conn->conn_id);
                }
            }
            qdr_link_flow(http2_adaptor->core, link, DEFAULT_CAPACITY, false);
        }
    }
}

static void qdr_http_activate(void *notused, qdr_connection_t *c)
{
    sys_mutex_lock(qd_server_get_activation_lock(http2_adaptor->core->qd->server));
    qdr_http2_connection_t* conn = (qdr_http2_connection_t*) qdr_connection_get_context(c);
    if (conn) {
        if (conn->pn_raw_conn && !(IS_ATOMIC_FLAG_SET(&conn->raw_closed_read) && IS_ATOMIC_FLAG_SET(&conn->raw_closed_write))) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] Activation triggered, calling pn_raw_connection_wake()", conn->conn_id);
            pn_raw_connection_wake(conn->pn_raw_conn);
        }
        else if (conn->activate_timer) {
            schedule_activation(conn, 0);
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_INFO,
                   "[C%" PRIu64 "] Activation triggered, no socket yet so scheduled timer", conn->conn_id);
        } else {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR, "[C%" PRIu64 "] Cannot activate", conn->conn_id);
        }
    }
    sys_mutex_unlock(qd_server_get_activation_lock(http2_adaptor->core->qd->server));
}

static int qdr_http_push(void *context, qdr_link_t *link, int limit)
{
    return qdr_link_process_deliveries(http2_adaptor->core, link, limit);
}


static void http_connector_establish(qdr_http2_connection_t *conn)
{
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_INFO, "[C%" PRIu64 "] Connecting to %s", conn->conn_id,
           conn->config->adaptor_config->host_port);
    sys_mutex_lock(qd_server_get_activation_lock(http2_adaptor->core->qd->server));
    if (conn->require_tls) {
        // Create the qd_tls_t object
        assert(!conn->tls);
        conn->tls = qd_tls(conn->connector->tls_domain, conn, conn->conn_id, on_tls_connection_secured);
        if (conn->tls) {
            // Call pn_raw_connection() only if we were successfully able to configure TLS
            // with the information provided in the sslProfile.
            conn->pn_raw_conn = pn_raw_connection();
            pn_raw_connection_set_context(conn->pn_raw_conn, conn);
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64
                   "] Success setting up sslProfile on connector %s to %s, now initiating actual connection via "
                   "pn_proactor_raw_connect()",
                   conn->conn_id, conn->config->adaptor_config->name, conn->config->adaptor_config->host_port);
            pn_proactor_raw_connect(qd_server_proactor(conn->server), conn->pn_raw_conn, conn->config->adaptor_config->host_port);
        } else {
            // TLS was not configured successfully using the details in the connector and SSLProfile. See the logs for
            // additional detail
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR,
                   "[C%" PRIu64 "] Error setting up TLS on connector %s to %s", conn->conn_id,
                   conn->config->adaptor_config->name, conn->config->adaptor_config->host_port);
        }
    }
    else {
        // This is just a regular connection, no TLS involved.
        conn->pn_raw_conn = pn_raw_connection();
        pn_raw_connection_set_context(conn->pn_raw_conn, conn);
        pn_proactor_raw_connect(qd_server_proactor(conn->server), conn->pn_raw_conn, conn->config->adaptor_config->host_port);
    }
    sys_mutex_unlock(qd_server_get_activation_lock(http2_adaptor->core->qd->server));
}

/**
 * Converts the AMQP message into a HTTP request or response
 */
uint64_t handle_outgoing_http(qdr_http2_stream_data_t *stream_data)
{
    qdr_http2_connection_t *conn = stream_data->conn;

    if (IS_ATOMIC_FLAG_SET(&conn->raw_closed_write))
        return 0;

    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] Starting to handle_outgoing_http", conn->conn_id);
    if (stream_data->out_dlv) {

        qd_message_t *message = qdr_delivery_message(stream_data->out_dlv);

        if (stream_data->out_msg_send_complete) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32 "] handle_outgoing_http send is already complete, returning " DLV_FMT,
                   conn->conn_id, stream_data->stream_id, DLV_ARGS(stream_data->out_dlv));
            return 0;
        }

        if (!stream_data->out_msg_header_sent) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] Header not sent yet", conn->conn_id);

            qd_iterator_t *group_id_itr = qd_message_field_iterator(message, QD_FIELD_GROUP_ID);
            stream_data->remote_site = (char*) qd_iterator_copy(group_id_itr);
            qd_iterator_free(group_id_itr);

#ifndef NDEBUG
            qd_iterator_t *subject_itr = qd_message_field_iterator(message, QD_FIELD_SUBJECT);
            // Make sure there is a non-zero subject field iterator
            assert(subject_itr != 0);
            qd_iterator_free(subject_itr);
#endif
            qd_iterator_t *app_properties_iter = qd_message_field_iterator(message, QD_FIELD_APPLICATION_PROPERTIES);
            qd_parsed_field_t *app_properties_fld = qd_parse(app_properties_iter);

            uint32_t count = qd_parse_sub_count(app_properties_fld);
            int      actual_count = count - 1;  // Ignore the QD_AP_FLOW_ID

            nghttp2_nv hdrs[actual_count];
            int        index         = 0;
            bool       flow_id_found = false;
            for (uint32_t idx = 0; idx < count; idx++) {
                qd_parsed_field_t *key = qd_parse_sub_key(app_properties_fld, idx);
                qd_parsed_field_t *val = qd_parse_sub_value(app_properties_fld, idx);
                qd_iterator_t *key_raw = qd_parse_raw(key);
                qd_iterator_t *val_raw = qd_parse_raw(val);

                if (!flow_id_found && !!key_raw && qd_iterator_equal(key_raw, (const unsigned char *) QD_AP_FLOW_ID)) {
                    vflow_set_ref_from_parsed(stream_data->vflow, VFLOW_ATTRIBUTE_COUNTERFLOW, val);
                    flow_id_found = true;
                    continue;
                }
                char *name           = (char *) qd_iterator_copy(key_raw);
                hdrs[index].name     = (uint8_t *) name;
                hdrs[index].value    = (uint8_t *) qd_iterator_copy(val_raw);
                hdrs[index].namelen  = qd_iterator_length(key_raw);
                hdrs[index].valuelen = qd_iterator_length(val_raw);
                hdrs[index].flags    = NGHTTP2_NV_FLAG_NONE;

                if (strcmp(METHOD, name) == 0) {
                    stream_data->method = qd_strdup((const char *) hdrs[index].value);
                }
                if (strcmp(STATUS, name) == 0) {
                    stream_data->request_status = qd_strdup((const char *) hdrs[index].value);
                }
                index += 1;
            }

            int stream_id = stream_data->conn->ingress?stream_data->stream_id: -1;

            send_settings_frame(conn);

            uint8_t flags                        = NGHTTP2_FLAG_END_HEADERS;
            stream_data->curr_stream_data_result = qd_message_next_stream_data(message, &stream_data->curr_stream_data);
            if (stream_data->curr_stream_data_result == QD_MESSAGE_STREAM_DATA_BODY_OK) {
                size_t payload_length = qd_message_stream_data_payload_length(stream_data->curr_stream_data);

                if (payload_length == 0) {
                    stream_data->next_stream_data_result = qd_message_next_stream_data(message, &stream_data->next_stream_data);

                    if (stream_data->next_stream_data_result == QD_MESSAGE_STREAM_DATA_NO_MORE) {
                        if (stream_data->next_stream_data) {
                            qd_message_stream_data_release(stream_data->next_stream_data);
                            stream_data->next_stream_data = 0;
                        }

                        qd_message_stream_data_release(stream_data->curr_stream_data);

                        stream_data->curr_stream_data = 0;
                        flags = NGHTTP2_FLAG_END_STREAM;
                        stream_data->out_msg_has_body = false;
                        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                               "[C%" PRIu64
                               "] Message has no body, sending NGHTTP2_FLAG_END_STREAM with nghttp2_submit_headers",
                               conn->conn_id);
                    }
                }
                else {
                    stream_data->curr_stream_data_iter = qd_message_stream_data_iterator(stream_data->curr_stream_data);
                }
            }

            // There is a body for this message, set the delay_buffer_write so the buffers are not immediately
            // pushed out on header submission.
            if (stream_data->out_msg_has_body) {
                SET_ATOMIC_FLAG(&conn->delay_buffer_write);
            }

            int32_t ret_val = nghttp2_submit_headers(conn->session, flags, stream_id, NULL, hdrs, actual_count, stream_data);

            // The call to nghttp2_submit_headers can return 3 possible values.
            // 1. The new stream id > 0 if this is a request and the passed in stream id was -1.
            // 2. zero if this is response and there was no error when submitting the response headers
            // 3. returns an nghttp2 specific error code less than zero if there was some error calling nghttp2_submit_headers.
            if (ret_val < 0) {
                // An error code was returned by nghttp2 when calling nghttp2_submit_headers. This was a failure in submitting the headers
                // Log the failure code returned by nghttp2 and do not proceed
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR,
                       "[C%" PRIu64 "] nghttp2_submit_headers failed, ret_val=%" PRId32 ", closing connection",
                       conn->conn_id, ret_val);
                // Since there was an error calling nghttp2_submit_headers, we cannot proceed further, we will have to close the connection
                nghttp2_submit_goaway(conn->session, 0, stream_id, ret_val, (uint8_t *)"Error while submitting header", 29);
                vflow_set_uint64(stream_data->vflow, VFLOW_ATTRIBUTE_RESULT, ret_val);
                vflow_set_string(stream_data->vflow, VFLOW_ATTRIBUTE_REASON, nghttp2_strerror(ret_val));

                pn_raw_connection_close(conn->pn_raw_conn);
                return 0;
            } else if (ret_val == 0) {
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "] nghttp2_submit_headers successful, ret_val=%" PRId32 "", conn->conn_id, ret_val);
            } else {
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "] nghttp2_submit_headers successful, new stream id=[S%" PRId32 "]", conn->conn_id,
                       ret_val);
                stream_data->stream_id = ret_val;
            }

            //
            // We have just submitted a request on the egress connection.
            // Capture the stream id on the egress side.
            // This will help vflow correlate the input and output streams.
            //
            vflow_set_uint64(stream_data->vflow, VFLOW_ATTRIBUTE_STREAM_ID, stream_data->stream_id);

            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32
                   "] handle_outgoing_http, out_dlv before sending Outgoing headers " DLV_FMT,
                   conn->conn_id, stream_data->stream_id, DLV_ARGS(stream_data->out_dlv));

            for (uint32_t idx = 0; idx < actual_count; idx++) {
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32 "] HTTP2 HEADER Outgoing [%s=%s]", conn->conn_id,
                       stream_data->stream_id, (char *) hdrs[idx].name, (char *) hdrs[idx].value);
            }

            ret_val = nghttp2_session_send(conn->session);
            if (ret_val == NGHTTP2_ERR_NOMEM || ret_val == NGHTTP2_ERR_CALLBACK_FAILURE) {
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR,
                       "[C%" PRIu64 "][S%" PRId32 "] Error submitting header ret_val=%i", conn->conn_id,
                       stream_data->stream_id, ret_val);
            }

            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][S%" PRId32 "] Headers submitted",
                   conn->conn_id, stream_data->stream_id);

            qd_iterator_free(app_properties_iter);
            qd_parse_free(app_properties_fld);

            for (uint32_t idx = 0; idx < actual_count; idx++) {
                free(hdrs[idx].name);
                free(hdrs[idx].value);
            }
        }
        else {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32 "] Headers already submitted, Proceeding with the body", conn->conn_id,
                   stream_data->stream_id);
        }

        if (stream_data->out_msg_has_body) {
            if (stream_data->out_msg_header_sent) {
                // This is usually called if there are many AMQP data streams objects in a delivery. These data streams were created on the inbound AMQP side using the qdr_delivery_continue() function.
                nghttp2_session_resume_data(conn->session, stream_data->stream_id);
                nghttp2_session_send(conn->session);
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32 "] nghttp2_session_send - write_buffers done for resumed stream",
                       conn->conn_id, stream_data->stream_id);
            }
            else {
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][S%" PRId32 "] Processing message body",
                       conn->conn_id, stream_data->stream_id);
                conn->data_prd.source.ptr = stream_data;

                int rv = 0;
                rv = nghttp2_submit_data(conn->session, NGHTTP2_FLAG_END_STREAM, stream_data->stream_id, &conn->data_prd);
                if (rv != 0) {
                    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR,
                           "[C%" PRIu64 "][S%" PRId32 "] Error submitting data rv=%i", conn->conn_id,
                           stream_data->stream_id, rv);
                }
                else {
                    if (conn->session) {
                        nghttp2_session_send(conn->session);
                        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                               "[C%" PRIu64 "][S%" PRId32 "] nghttp2_session_send - done", conn->conn_id,
                               stream_data->stream_id);
                    }
                }
            }
        }
        else {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][S%" PRId32 "] Message has no body",
                   conn->conn_id, stream_data->stream_id);
        }
        stream_data->out_msg_header_sent = true;
        CLEAR_ATOMIC_FLAG(&conn->delay_buffer_write);

        if (stream_data->out_msg_has_footer) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][S%" PRId32 "] Message has a footer",
                   conn->conn_id, stream_data->stream_id);
            bool send_footer = false;
            if (stream_data->out_msg_has_body && !stream_data->out_msg_body_sent) {
                if (stream_data->out_msg_body_sent) {
                    send_footer = true;
                    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][S%" PRId32 "] send_footer = true",
                           conn->conn_id, stream_data->stream_id);
                }
                else {
                    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][S%" PRId32 "] send_footer = false",
                           conn->conn_id, stream_data->stream_id);
                }
            }
            else {
                send_footer = true;
            }

            //
            // We have a footer and are ready to send it.
            //
            if (send_footer) {
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][S%" PRId32 "] Starting to send footer",
                       conn->conn_id, stream_data->stream_id);
                // Send the properties in the footer as a HEADERS frame.
                qd_iterator_t     *footer_properties_iter = stream_data->footer_stream_data_iter;
                qd_parsed_field_t *footer_properties_fld = qd_parse(footer_properties_iter);

                uint32_t count = qd_parse_sub_count(footer_properties_fld);

                nghttp2_nv hdrs[count];

                for (uint32_t idx = 0; idx < count; idx++) {
                    qd_parsed_field_t *key = qd_parse_sub_key(footer_properties_fld, idx);
                    qd_parsed_field_t *val = qd_parse_sub_value(footer_properties_fld, idx);
                    qd_iterator_t *key_raw = qd_parse_raw(key);
                    qd_iterator_t *val_raw = qd_parse_raw(val);

                    hdrs[idx].name = (uint8_t *)qd_iterator_copy(key_raw);
                    hdrs[idx].value = (uint8_t *)qd_iterator_copy(val_raw);
                    hdrs[idx].namelen = qd_iterator_length(key_raw);
                    hdrs[idx].valuelen = qd_iterator_length(val_raw);
                    hdrs[idx].flags = NGHTTP2_NV_FLAG_NONE;
                }

                nghttp2_submit_headers(conn->session,
                        NGHTTP2_FLAG_END_STREAM,
                        stream_data->stream_id,
                        NULL,
                        hdrs,
                        count,
                        stream_data);

                for (uint32_t idx = 0; idx < count; idx++) {
                    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                           "[C%" PRIu64 "][S%" PRId32 "] HTTP2 HEADER(footer) Outgoing [%s=%s]", conn->conn_id,
                           stream_data->stream_id, (char *) hdrs[idx].name, (char *) hdrs[idx].value);
                }

                nghttp2_session_send(conn->session);
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32 "] Headers(from footer) submitted", conn->conn_id,
                       stream_data->stream_id);

                qd_iterator_free(footer_properties_iter);
                qd_parse_free(footer_properties_fld);
                if (stream_data->footer_stream_data) {
                    qd_message_stream_data_release(stream_data->footer_stream_data);
                }
                if (stream_data->curr_stream_data) {
                    qd_message_stream_data_release(stream_data->curr_stream_data);
                    qd_iterator_free(stream_data->curr_stream_data_iter);
                    stream_data->curr_stream_data_iter = 0;
                    stream_data->curr_stream_data = 0;
                }
                if (stream_data->next_stream_data) {
                    qd_message_stream_data_release(stream_data->next_stream_data);
                    stream_data->next_stream_data = 0;
                }

                for (uint32_t idx = 0; idx < count; idx++) {
                    free(hdrs[idx].name);
                    free(hdrs[idx].value);
                }
            }
        }
        else {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][S%" PRId32 "] Message has no footer",
                   conn->conn_id, stream_data->stream_id);
        }

        if (stream_data->out_msg_header_sent) {
            if (stream_data->out_msg_has_body) {
                if (stream_data->out_msg_body_sent) {
                    qd_message_set_send_complete(qdr_delivery_message(stream_data->out_dlv));
                    stream_data->out_msg_send_complete = true;
                    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                           "[C%" PRIu64 "][S%" PRId32 "] handle_outgoing_http, out_dlv send_complete " DLV_FMT,
                           conn->conn_id, stream_data->stream_id, DLV_ARGS(stream_data->out_dlv));
                }
            }
            else {
                qd_message_set_send_complete(qdr_delivery_message(stream_data->out_dlv));
                stream_data->out_msg_send_complete = true;
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32 "] handle_outgoing_http, out_dlv send_complete " DLV_FMT,
                       conn->conn_id, stream_data->stream_id, DLV_ARGS(stream_data->out_dlv));
            }
        }

        if (qd_message_send_complete(qdr_delivery_message(stream_data->out_dlv))) {
            advance_stream_status(stream_data);
            if (!stream_data->disp_updated && stream_data->status == QD_STREAM_FULLY_CLOSED) {
                qdr_delivery_remote_state_updated(http2_adaptor->core, stream_data->out_dlv, stream_data->out_dlv_local_disposition, true, 0, false);
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32
                       "] In handle_outgoing_http, qdr_delivery_remote_state_updated(stream_data->out_dlv)",
                       conn->conn_id, stream_data->stream_id);
                stream_data->disp_updated = true;
                set_stream_data_delivery_flags(stream_data, stream_data->out_dlv);
                qdr_delivery_decref(http2_adaptor->core, stream_data->out_dlv,
                                    "HTTP2 adaptor out_dlv - handle_outgoing_http");
                stream_data->out_dlv = 0;
            }
        }
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] Finished handle_outgoing_http", conn->conn_id);
    }
    else {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] No out_dlv, no handle_outgoing_http",
               conn->conn_id);
    }
    return 0;
}

static uint64_t qdr_http_deliver(void *context, qdr_link_t *link, qdr_delivery_t *delivery, bool settled)
{
    qdr_http2_stream_data_t *stream_data =  (qdr_http2_stream_data_t*)qdr_link_get_context(link);

    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, DLV_FMT " qdr_http_deliver", DLV_ARGS(delivery));

    if (!stream_data) {
        return 0;
    }

    qdr_http2_connection_t *conn = stream_data->conn;

    if (conn->require_tls) {
        if (!qd_tls_is_secure(conn->tls)) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   DLV_FMT " qdr_http_deliver - delivery cannot be sent, connection is not secure yet, returning",
                   DLV_ARGS(delivery));
            return 0;
        }
        send_settings_frame(conn);
    }

    if (link == stream_data->conn->stream_dispatcher) {
        qdr_http2_connection_t *conn = stream_data->conn;

        qdr_http2_stream_data_t *stream_data = create_qdr_http2_stream_data(conn, 0);
        if (!stream_data->out_dlv) {
            stream_data->out_dlv = delivery;
            qdr_delivery_incref(delivery, "egress out_dlv referenced by HTTP2 adaptor");
        }
        qdr_terminus_t *source = qdr_terminus(0);
        qdr_terminus_set_address(source, conn->config->adaptor_config->address);

        // Receiving link.
        stream_data->out_link = qdr_link_first_attach(conn->qdr_conn,
                                                     QD_OUTGOING,
                                                     source,            // qdr_terminus_t   *source,
                                                     qdr_terminus(0),   // qdr_terminus_t   *target,
                                                     "http.egress.out", // const char       *name,
                                                     0,                 // const char       *terminus_addr,
                                                     true,
                                                     delivery,
                                                     &(stream_data->outgoing_id));
        qdr_link_set_context(stream_data->out_link, stream_data);
        qd_iterator_t *fld_iter = qd_message_field_iterator(qdr_delivery_message(delivery), QD_FIELD_REPLY_TO);
        stream_data->reply_to = (char *)qd_iterator_copy(fld_iter);
        qd_iterator_free(fld_iter);

        // Sender link.
        qdr_terminus_t *target = qdr_terminus(0);
        qdr_terminus_set_address(target, stream_data->reply_to);
        stream_data->in_link = qdr_link_first_attach(conn->qdr_conn,
                                                     QD_INCOMING,
                                                     qdr_terminus(0),  //qdr_terminus_t   *source,
                                                     target, //qdr_terminus_t   *target,
                                                     "http.egress.in",  //const char       *name,
                                                     0,                //const char       *terminus_addr,
                                                     false,
                                                     0,
                                                     &(stream_data->incoming_id));
        qdr_link_set_context(stream_data->in_link, stream_data);
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               DLV_FMT " qdr_http_deliver, returning QD_DELIVERY_MOVED_TO_NEW_LINK", DLV_ARGS(delivery));

        // Set vanflow stuff
        vflow_set_trace(stream_data->vflow, delivery->msg);
        return QD_DELIVERY_MOVED_TO_NEW_LINK;
    }

    if (conn->ingress) {
        if (!stream_data->out_dlv) {
            stream_data->out_dlv = delivery;
            qdr_delivery_incref(delivery, "ingress out_dlv referenced by HTTP2 adaptor");
            //
            // On an ingress connection, the response qdr_delivery_t is being received for a particular stream.
            // This is the time we call the vflow_latency_end and we do it only once.
            //
            vflow_latency_end(stream_data->vflow);
        }
    }
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "][S%" PRId32 "] qdr_http_deliver - call handle_outgoing_http", conn->conn_id,
           stream_data->stream_id);

    uint64_t disp = handle_outgoing_http(stream_data);
    if (stream_data->status == QD_STREAM_FULLY_CLOSED && disp == PN_ACCEPTED) {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][S%" PRId32 "] qdr_http_deliver - calling free_http2_stream_data", conn->conn_id,
               stream_data->stream_id);
        free_http2_stream_data(stream_data, false);
    }
    return disp;
}


/**
 * Takes read buffers from proton raw connection and feeds the binary http2 frame data
 * to nghttp2 via the nghttp2_session_mem_recv() function.
 * All pertinent nghttp2 callbacks are called before the call to nghttp2_session_mem_recv() completes.
 */
static bool push_rx_buffer_to_nghttp2(qdr_http2_connection_t *conn, uint8_t *buf, size_t size)
{
    // send a buffer to nghttp2
    // return true if error was detected and logged, and connection should close
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64
           "] handle_incoming_http - Calling nghttp2_session_mem_recv "
           "qd_adaptor_buffer of size %zu",
           conn->conn_id, size);
    bool close_conn = false; // return result
    if (!conn->buffers_pushed_to_nghttp2)
        conn->buffers_pushed_to_nghttp2 = true;

    int rv = nghttp2_session_mem_recv(conn->session, buf, size);

    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] nghttp2_session_mem_recv rv=%i", conn->conn_id,
           rv);

    if (rv < 0) {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR, "[C%" PRIu64 "] Error in nghttp2_session_mem_recv rv=%i",
               conn->conn_id, rv);
        if (rv == NGHTTP2_ERR_FLOODED) {
            // Flooding was detected in this HTTP/2 session, and it must be closed.
            // This is most likely caused by misbehavior of peer.
            // If the client magic is bad, we need to close the connection.
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR, "[C%" PRIu64 "] HTTP NGHTTP2_ERR_FLOODED", conn->conn_id);
            nghttp2_submit_goaway(conn->session, 0, 0,
                                  NGHTTP2_PROTOCOL_ERROR, (uint8_t *)"Protocol Error", 14);
        }
        else if (rv == NGHTTP2_ERR_CALLBACK_FAILURE) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR, "[C%" PRIu64 "] HTTP NGHTTP2_ERR_CALLBACK_FAILURE",
                   conn->conn_id);
            nghttp2_submit_goaway(conn->session, 0, 0,
                                  NGHTTP2_PROTOCOL_ERROR, (uint8_t *)"Internal Error", 14);
        }
        else if (rv == NGHTTP2_ERR_BAD_CLIENT_MAGIC) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR,
                   "[C%" PRIu64 "] HTTP2 NGHTTP2_ERR_BAD_CLIENT_MAGIC, closing connection", conn->conn_id);
            nghttp2_submit_goaway(conn->session, 0, 0,
                                  NGHTTP2_PROTOCOL_ERROR, (uint8_t *)"Bad Client Magic", 16);
        } else if (rv == NGHTTP2_ERR_FRAME_SIZE_ERROR) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR,
                   "[C%" PRIu64 "] HTTP2 NGHTTP2_ERR_FRAME_SIZE_ERROR, closing connection", conn->conn_id);
            nghttp2_submit_goaway(conn->session, 0, 0, NGHTTP2_ERR_FRAME_SIZE_ERROR, (uint8_t *) "Bad Frame Size", 14);
        }

        else {
            nghttp2_submit_goaway(conn->session, 0, 0,
                                  NGHTTP2_PROTOCOL_ERROR, (uint8_t *)"Protocol Error", 14);
        }
        nghttp2_session_send(conn->session);

        //
        // An error was received from nghttp2, the connection needs to be closed.
        //
        close_conn = true;
    }
    return close_conn;
}

static int handle_incoming_http(qdr_http2_connection_t *conn)
{
    if (!conn->pn_raw_conn) {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] In handle_incoming_http, no pn raw connection, returning", conn->conn_id);
        return 0;
    }
    CHECK_PROACTOR_RAW_CONNECTION(conn->pn_raw_conn);

    int  count      = 0;
    bool close_conn = false;
    if (conn->require_tls) {
        int                      encrypted_bytes_in = 0;
        qd_adaptor_buffer_list_t decrypted_buffs;
        DEQ_INIT(decrypted_buffs);
        encrypted_bytes_in = qd_tls_decrypt(conn->tls, conn->pn_raw_conn, &decrypted_buffs);
        if (encrypted_bytes_in == QD_TLS_ERROR) {
            pn_raw_connection_close(conn->pn_raw_conn);
            return 0;
        } else if (DEQ_SIZE(decrypted_buffs) > 0 && qd_tls_is_secure(conn->tls)) {
            if (!conn->alpn_check_complete)
                close_conn = !is_alpn_protocol_match(conn);
            if (!close_conn) {
                send_settings_frame(conn);
            }
            qd_adaptor_buffer_t *adaptor_buff = DEQ_HEAD(decrypted_buffs);
            size_t               buffer_size  = qd_adaptor_buffer_size(adaptor_buff);
            count += buffer_size;

            while (adaptor_buff) {
                if (!close_conn) {
                    close_conn = push_rx_buffer_to_nghttp2(conn, qd_adaptor_buffer_base(adaptor_buff), buffer_size);
                }
                DEQ_REMOVE_HEAD(decrypted_buffs);
                qd_adaptor_buffer_free(adaptor_buff);
                adaptor_buff = DEQ_HEAD(decrypted_buffs);
                if (adaptor_buff)
                    buffer_size = qd_adaptor_buffer_size(adaptor_buff);
            }
        }
        conn->encrypted_bytes_in += encrypted_bytes_in;
    } else {
        // No TLS case, no need to decrypt data first, directly hand data to nghttp2.

        //
        // This fix is a for nodejs server (router acting as client).

        // This is what happens -
        // 1. nodejs sends a SETTINGS frame immediately after we open the connection. (this is allowed)
        // 2. Router sends -
        //     2a. Client magic
        //     2b. SETTINGS frame with ack=true (here the router is responding to the SETTINGS frame from nodejs in step
        //     2c. SETTINGS frame ack=false(this is the router's inital settings frame)
        //     2d. GET request
        // 3. Nodejs responds with GOAWAY. Not sure why
        // To remedy this problem, when nodejs sends the initial SETTINGS frame, we don't tell nghttp2 about it. So step
        // 2c happens before step 2b and nodejs is now happy
        //
        if (!IS_ATOMIC_FLAG_SET(&conn->raw_closed_read) && !conn->ingress && !conn->tls) {
            if (!conn->initial_settings_frame_sent) {
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "] In handle_incoming_http, initial settings frame not sent, returning",
                       conn->conn_id);
                return 0;
            }
        }

        pn_raw_buffer_t raw_buffers[RAW_BUFFER_BATCH];
        size_t          n;

        while ((n = pn_raw_connection_take_read_buffers(conn->pn_raw_conn, raw_buffers, RAW_BUFFER_BATCH))) {
            for (size_t i = 0; i < n && raw_buffers[i].bytes; ++i) {
                qd_adaptor_buffer_t *buf           = (qd_adaptor_buffer_t *) raw_buffers[i].context;
                uint32_t             raw_buff_size = raw_buffers[i].size;
                qd_adaptor_buffer_insert(buf, raw_buff_size);
                count += raw_buff_size;

                if (raw_buff_size > 0 && !close_conn) {
                    // no tls, just raw bytes. Push the bytes to nghttp2
                    if (!conn->buffers_pushed_to_nghttp2)
                        conn->buffers_pushed_to_nghttp2 = true;
                    close_conn =
                        push_rx_buffer_to_nghttp2(conn, qd_adaptor_buffer_base(buf), qd_adaptor_buffer_size(buf));
                }
                // Free the wire buffer
                qd_adaptor_buffer_free(buf);
            }
        }
    }

    if (close_conn) {
        pn_raw_connection_close(conn->pn_raw_conn);
    }
    else {
        if (!IS_ATOMIC_FLAG_SET(&conn->raw_closed_read)) {
            grant_read_buffers(conn, "handle_incoming_http");
        }
    }

    if (conn->buffers_pushed_to_nghttp2)
        nghttp2_session_send(conn->session);

    return count;
}

qdr_http2_connection_t *qdr_http_connection_ingress_accept(qdr_http2_connection_t* ingress_http_conn)
{
    ingress_http_conn->remote_address = qd_raw_conn_get_address(ingress_http_conn->pn_raw_conn);
    qdr_connection_info_t *info = qdr_connection_info(false, //bool             is_encrypted,
                                                      false, //bool             is_authenticated,
                                                      true,  //bool             opened,
                                                      "",   //char            *sasl_mechanisms,
                                                      QD_INCOMING, //qd_direction_t   dir,
                                                      ingress_http_conn->remote_address,    //const char      *host,
                                                      "",    //const char      *ssl_proto,
                                                      "",    //const char      *ssl_cipher,
                                                      "",    //const char      *user,
                                                      "HttpAdaptor",    //const char      *container,
                                                      0,     //pn_data_t       *connection_properties,
                                                      0,     //int              ssl_ssf,
                                                      false, //bool             ssl,
                                                      "",                  // peer router version,
                                                      false,               // streaming links
                                                      false);              // connection trunking

    qdr_connection_t *conn = qdr_connection_opened(http2_adaptor->core,
                                                   http2_adaptor->adaptor,
                                                   true,
                                                   QDR_ROLE_NORMAL,
                                                   1,
                                                   ingress_http_conn->conn_id,
                                                   0,
                                                   0,
                                                   false,
                                                   false,
                                                   250,
                                                   0,
                                                   info,
                                                   0,
                                                   0);

    ingress_http_conn->qdr_conn = conn;
    qdr_connection_set_context(conn, ingress_http_conn);
    ingress_http_conn->connection_established = true;
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "] qdr_http_connection_ingress_accept, qdr_connection_t object created ",
           ingress_http_conn->conn_id);
    return ingress_http_conn;
}


static void restart_streams(qdr_http2_connection_t *http_conn)
{
    qdr_http2_stream_data_t *stream_data = DEQ_HEAD(http_conn->streams);
    if (!stream_data) {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] In restart_streams, no stream_data, returning",
               http_conn->conn_id);
        return;
    }

    DEQ_REMOVE_HEAD(http_conn->streams);
    DEQ_INSERT_TAIL(http_conn->streams, stream_data);
    stream_data = DEQ_HEAD(http_conn->streams);
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "][S%" PRId32 "] In restart_streams swapped head and tail streams", http_conn->conn_id,
           stream_data->stream_id);
    while (stream_data) {
        if (stream_data->status == QD_STREAM_FULLY_CLOSED) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][S%" PRId32 "] In restart_streams QD_STREAM_FULLY_CLOSED, not restarting stream",
                   http_conn->conn_id, stream_data->stream_id);

            if (stream_data->out_dlv && !stream_data->disp_updated && !stream_data->out_dlv_decrefed && stream_data->status == QD_STREAM_FULLY_CLOSED ) {
                // A call to qdr_delivery_remote_state_updated will free the out_dlv
                qdr_delivery_remote_state_updated(http2_adaptor->core, stream_data->out_dlv, stream_data->out_dlv_local_disposition, true, 0, false);
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32
                       "] In restart_streams QD_STREAM_FULLY_CLOSED, "
                       "qdr_delivery_remote_state_updated(stream_data->out_dlv)",
                       http_conn->conn_id, stream_data->stream_id);
                stream_data->disp_updated = true;
            }
            qdr_http2_stream_data_t *next_stream_data = 0;
            next_stream_data = DEQ_NEXT(stream_data);
            if(stream_data->out_msg_send_complete && stream_data->disp_applied) {
                free_http2_stream_data(stream_data, false);
            }
            stream_data = next_stream_data;
        }
        else {
            if (stream_data->out_dlv_local_disposition != PN_ACCEPTED) {
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][S%" PRId32 "] Restarting stream in restart_streams()", http_conn->conn_id,
                       stream_data->stream_id);
                handle_outgoing_http(stream_data);
            }
            stream_data = DEQ_NEXT(stream_data);
        }
    }
}


static void qdr_del_http2_connection_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    //
    // DISPATCH-1996: discard is true in the case where this action is called from qdr_core_free()
    // This means that the qdr_adaptors_finalize has already been called and the connection in question has already been freed.
    // No need to do anything now, if discard, just return.
    //
    if (discard)
        return;

    qdr_http2_connection_t *conn = (qdr_http2_connection_t*) action->args.general.context_1;
    free_qdr_http2_connection(conn, false);
}


static void close_connections(qdr_http2_connection_t* conn)
{
    if (conn->dummy_link) {
        qdr_link_detach(conn->dummy_link, QD_CLOSED, 0);
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] Detaching dummy link on egress connection",
               conn->conn_id);
        conn->dummy_link = 0;
    }

    qdr_connection_set_context(conn->qdr_conn, 0);
    if (conn->qdr_conn) {
        qdr_connection_closed(conn->qdr_conn);
        conn->qdr_conn = 0;
    }
    qdr_action_t *action = qdr_action(qdr_del_http2_connection_CT, "delete_http2_connection");
    action->args.general.context_1 = conn;
    qdr_action_enqueue(http2_adaptor->core, action);
}

static void clean_http2_conn(qdr_http2_connection_t* conn)
{
    free_all_connection_streams(conn, false);

    //
    // This closes the nghttp2 session. Next time when a new connection is opened, a new nghttp2 session
    // will be created by calling nghttp2_session_client_new
    //
    nghttp2_session_del(conn->session);
    conn->session = 0;
    qd_adaptor_buffer_list_free_buffers(&conn->out_buffs);

    // Free tls related stuff if need be.
    if (conn->tls) {
        qd_tls_free(conn->tls);
        conn->tls = 0;
    }
}


static void handle_disconnected(qdr_http2_connection_t* conn)
{
    sys_mutex_lock(qd_server_get_activation_lock(http2_adaptor->core->qd->server));
    if (conn->pn_raw_conn) {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] handle_disconnected Setting conn->pn_raw_conn=0", conn->conn_id);
        pn_raw_connection_set_context(conn->pn_raw_conn, 0);
        conn->pn_raw_conn = 0;
    }

    if (conn->ingress) {
        clean_http2_conn(conn);
        close_connections(conn);
    }
    else {
        if (conn->stream_dispatcher) {
            qdr_http2_stream_data_t *stream_data = qdr_link_get_context(conn->stream_dispatcher);
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] Detaching stream dispatcher link on egress connection, freed associated stream data",
                   conn->conn_id);
            qdr_link_detach(conn->stream_dispatcher, QD_CLOSED, 0);
            qdr_link_set_context(conn->stream_dispatcher, 0);
            conn->stream_dispatcher = 0;
            if (stream_data) {
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "] Freeing stream_data (stream_dispatcher, handle_disconnected) (%p)",
                       conn->conn_id, (void *) stream_data);
                free_qdr_http2_stream_data_t(stream_data);
            }
            conn->stream_dispatcher_stream_data = 0;

        }

        if (conn->delete_egress_connections) {
            clean_http2_conn(conn);
            close_connections(conn);
        }
        else {
            clean_http2_conn(conn);
        }
    }
    sys_mutex_unlock(qd_server_get_activation_lock(http2_adaptor->core->qd->server));
}


static void egress_conn_timer_handler(void *context)
{
    ASSERT_PROACTOR_MODE(SYS_THREAD_PROACTOR_MODE_TIMER);

    qdr_http2_connection_t* conn = (qdr_http2_connection_t*) context;
    CLEAR_ATOMIC_FLAG(&conn->activate_scheduled);

    // Protect with the lock when accessing conn->pn_raw_conn
    sys_mutex_lock(qd_server_get_activation_lock(http2_adaptor->core->qd->server));

    if (conn->delete_egress_connections) {
        //
        // The connector that this connection is associated with has been deleted.
        // Free the associated connections
        // It is ok to call qdr_connection_closed from this timer callback.
        //
        sys_mutex_unlock(qd_server_get_activation_lock(http2_adaptor->core->qd->server));
        qdr_connection_closed(conn->qdr_conn);
        free_qdr_http2_connection(conn, false);
        return;
    }

    //
    // If there is already a conn->pn_raw_conn, don't try to connect again.
    //
    if (conn->pn_raw_conn) {
        sys_mutex_unlock(qd_server_get_activation_lock(http2_adaptor->core->qd->server));
        return;
    }

    sys_mutex_unlock(qd_server_get_activation_lock(http2_adaptor->core->qd->server));

    if (conn->connection_established)
        return;

    if (!conn->ingress) {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] - Egress_conn_timer_handler - Trying to establish outbound connection", conn->conn_id);
        http_connector_establish(conn);
    }
}

static void create_dummy_link_on_egress_conn(qdr_http2_connection_t *egress_http_conn)
{
    //
    // Create a dummy link for connection activation. There needs to be at least one link on a connection
    // for the connection to be activated. If no link is present on the connection, you could call the
    // function to activate the connection but it will never be actually activated.
    // Hence the need for this dummy link. The link is a total dummy and does nothing.
    //
    qdr_terminus_t *source = qdr_terminus(0);
    qdr_terminus_set_address(source, "dummy_link");
    egress_http_conn->dummy_link = qdr_link_first_attach(egress_http_conn->qdr_conn,
                                                           QD_OUTGOING,
                                                           source,           //qdr_terminus_t   *source,
                                                           qdr_terminus(0),  //qdr_terminus_t   *target,
                                                           "dummy_link",     //const char       *name,
                                                           0,                //const char       *terminus_addr,
                                                           false,
                                                           0,
                                                           &(egress_http_conn->dummy_link_id));
}

static void create_stream_dispatcher_link(qdr_http2_connection_t *egress_http_conn)
{
    if (egress_http_conn->stream_dispatcher)
        return;

    qdr_terminus_t *source = qdr_terminus(0);
    qdr_terminus_set_address(source, egress_http_conn->config->adaptor_config->address);
    egress_http_conn->stream_dispatcher = qdr_link_first_attach(egress_http_conn->qdr_conn,
                                                           QD_OUTGOING,
                                                           source,           //qdr_terminus_t   *source,
                                                           qdr_terminus(0),  //qdr_terminus_t   *target,
                                                           "stream_dispatcher", //const char       *name,
                                                           0,                //const char       *terminus_addr,
                                                           false,
                                                           0,
                                                           &(egress_http_conn->stream_dispatcher_id));

    // Create a dummy stream_data object and set that as context.
    qdr_http2_stream_data_t *stream_data = new_qdr_http2_stream_data_t();

    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "] Created new stream_data for stream_dispatcher (%lx)", egress_http_conn->conn_id,
           (long) stream_data);

    ZERO(stream_data);
    stream_data->conn = egress_http_conn;
    qdr_link_set_context(egress_http_conn->stream_dispatcher, stream_data);

    // This is added specifically to deal with the shutdown leak of the dispatcher stream data.
    // The core frees all links before it calls adaptor final. so we cannot get the stream data from the qdr_link context.
    egress_http_conn->stream_dispatcher_stream_data = stream_data;
}


qdr_http2_connection_t *qdr_http_connection_egress(qd_http_connector_t *connector)
{
    qdr_http2_connection_t* egress_http_conn = new_qdr_http2_connection_t();
    ZERO(egress_http_conn);
    egress_http_conn->conn_id         = qd_server_allocate_connection_id(connector->server);
    egress_http_conn->activate_timer = qd_timer(http2_adaptor->core->qd, egress_conn_timer_handler, egress_http_conn);
    egress_http_conn->require_tls     = !!connector->tls_domain;
    egress_http_conn->ingress = false;
    egress_http_conn->context.context = egress_http_conn;
    egress_http_conn->context.handler = &handle_connection_event;
    egress_http_conn->connector = connector;

    // Incref the ref count on the connector since the qdr_http2_connection_t object is holding a ref to the connector
    sys_atomic_inc(&connector->ref_count);

    egress_http_conn->config = connector->config;
    egress_http_conn->server = connector->server;
    egress_http_conn->data_prd.read_callback = read_data_callback;
    DEQ_INIT(egress_http_conn->out_buffs);
    DEQ_INIT(egress_http_conn->streams);
    sys_atomic_init(&egress_http_conn->raw_closed_read, 0);
    sys_atomic_init(&egress_http_conn->raw_closed_write, 0);
    sys_atomic_init(&egress_http_conn->delay_buffer_write, 0);
    sys_atomic_init(&egress_http_conn->q2_restart, 0);

    sys_mutex_lock(&http2_adaptor->lock);
    DEQ_INSERT_TAIL(http2_adaptor->connections, egress_http_conn);
    sys_mutex_unlock(&http2_adaptor->lock);

    //
    // Start an egress connection level vanflow record. The parent of the connection level
    // vanflow record is the connector's vanflow record.
    //
    egress_http_conn->vflow = vflow_start_record(VFLOW_RECORD_FLOW, connector->vflow);
    vflow_set_uint64(egress_http_conn->vflow, VFLOW_ATTRIBUTE_OCTETS, 0);
    vflow_add_rate(egress_http_conn->vflow, VFLOW_ATTRIBUTE_OCTETS, VFLOW_ATTRIBUTE_OCTET_RATE);
    vflow_set_uint64(egress_http_conn->vflow, VFLOW_ATTRIBUTE_WINDOW_SIZE, WINDOW_SIZE);

    qdr_connection_info_t *info = qdr_connection_info(false, //bool             is_encrypted,
                                                      false, //bool             is_authenticated,
                                                      true,  //bool             opened,
                                                      "",   //char            *sasl_mechanisms,
                                                      QD_OUTGOING, //qd_direction_t   dir,
                                                      egress_http_conn->config->adaptor_config->host_port,    //const char      *host,
                                                      "",    //const char      *ssl_proto,
                                                      "",    //const char      *ssl_cipher,
                                                      "",    //const char      *user,
                                                      "httpAdaptor",    //const char      *container,
                                                      0,     //pn_data_t       *connection_properties,
                                                      0,     //int              ssl_ssf,
                                                      false, //bool             ssl,
                                                      "",                  // peer router version,
                                                      false,               // streaming links
                                                      false);              // connection trunking

    qdr_connection_t *conn     = qdr_connection_opened(http2_adaptor->core,
                                                       http2_adaptor->adaptor,
                                                       true,
                                                       QDR_ROLE_NORMAL,
                                                       1,
                                                       egress_http_conn->conn_id,
                                                       0,
                                                       0,
                                                       false,
                                                       false,
                                                       250,
                                                       0,
                                                       info,
                                                       0,
                                                       0);
    egress_http_conn->qdr_conn = conn;
    connector->ctx = conn;

    qdr_connection_set_context(conn, egress_http_conn);
    create_dummy_link_on_egress_conn(egress_http_conn);
    return egress_http_conn;
}

static void handle_raw_connected_event(qdr_http2_connection_t *conn)
{
    if (conn->ingress) {
        qdr_http_connection_ingress_accept(conn);
        if (conn->require_tls) {
            assert(!conn->tls);
            conn->tls = qd_tls(conn->listener->tls_domain, conn, conn->conn_id, on_tls_connection_secured);
            if (conn->tls) {
                // We were successfully able to gather the details from the associated sslProfile and start
                // a pn_tls_session. Grant read buffers so that we can now start reading the initial TLS handshake
                // bytes that the client is going to send us.
                grant_read_buffers(conn, "PN_RAW_CONNECTION_CONNECTED, ingress");
            } else {
                // There was some problem with starting up the proton tls session.
                // Check logs for detailed INFO level output to find out more about the failure
                qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR,
                       "[C%" PRIu64 "] PN_RAW_CONNECTION_CONNECTED ingress failed to start TLS, closing raw connection",
                       conn->conn_id);
                pn_raw_connection_close(conn->pn_raw_conn);
            }
        } else {
            send_settings_frame(conn);
        }
    } else {
        CLEAR_ATOMIC_FLAG(&conn->raw_closed_read);
        CLEAR_ATOMIC_FLAG(&conn->raw_closed_write);
        conn->connection_established = true;
        create_stream_dispatcher_link(conn);
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] Created stream dispatcher link in PN_RAW_CONNECTION_CONNECTED", conn->conn_id);
        if (!conn->session) {
            nghttp2_session_client_new(&conn->session, (nghttp2_session_callbacks *)http2_adaptor->callbacks, (void *)conn);
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] nghttp2_session_client_new",
                   conn->conn_id);
        }
        if (conn->require_tls && qd_tls_has_output(conn->tls)) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] Initiating TLS handshake on egress connection", conn->conn_id);
            encrypt_outgoing_tls(conn, 0, true);
            // Grant read buffers so we can read the tls handshake response sent to us by the other side
            grant_read_buffers(conn, "PN_RAW_CONNECTION_CONNECTED, egress");
        }
        while (qdr_connection_process(conn->qdr_conn)) {}
    }
}

static void encrypt_outgoing_tls(qdr_http2_connection_t *conn, qd_adaptor_buffer_t *unencrypted_buff,
                                 bool write_buffers)
{
    qd_adaptor_buffer_list_t encrypted_buffs;
    DEQ_INIT(encrypted_buffs);
    int bytes_out = qd_tls_encrypt(conn->tls, unencrypted_buff, &encrypted_buffs);
    if (bytes_out == QD_TLS_ERROR) {
        pn_raw_connection_close(conn->pn_raw_conn);
        return;
    }

    if (unencrypted_buff) {
        conn->bytes_out += bytes_out;
    }

    if (DEQ_SIZE(encrypted_buffs) > 0) {
        DEQ_APPEND(conn->out_buffs, encrypted_buffs);
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] encrypt_outgoing_tls() DEQ_SIZE(conn->out_buffs)=%zu\n", conn->conn_id,
               DEQ_SIZE(conn->out_buffs));
    }

    if (write_buffers) {
        int num_buffers_written = qd_raw_connection_write_buffers(conn->pn_raw_conn, &conn->out_buffs);
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] encrypt_outgoing_tls() num_buffers_written=%i\n", conn->conn_id, num_buffers_written);
    }
}

static void handle_connection_event(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    qdr_http2_connection_t *conn = (qdr_http2_connection_t *) context;
    CHECK_PROACTOR_RAW_CONNECTION(conn->pn_raw_conn);

    switch (pn_event_type(e)) {
    case PN_RAW_CONNECTION_CONNECTED: {
        qd_set_vflow_netaddr_string(conn->vflow, conn->pn_raw_conn, conn->ingress);
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] PN_RAW_CONNECTION_CONNECTED %s", conn->conn_id,
               conn->ingress ? "ingress" : "egress");
        handle_raw_connected_event(conn);
    } break;
    case PN_RAW_CONNECTION_CLOSED_READ: {
        if (conn->q2_blocked) {
            conn->q2_blocked = false;
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] q2 is unblocked on this connection (PN_RAW_CONNECTION_CLOSED_READ)", conn->conn_id);
        }
        SET_ATOMIC_FLAG(&conn->raw_closed_read);
        handle_incoming_http(conn);
        if (conn->pn_raw_conn)
            pn_raw_connection_close(conn->pn_raw_conn);
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] PN_RAW_CONNECTION_CLOSED_READ", conn->conn_id);
        break;
    }
    case PN_RAW_CONNECTION_CLOSED_WRITE: {
        SET_ATOMIC_FLAG(&conn->raw_closed_write);
        int num_drained_write_buffers = qd_raw_connection_drain_write_buffers(conn->pn_raw_conn);
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] PN_RAW_CONNECTION_CLOSED_WRITE, drained %i write buffers", conn->conn_id,
               num_drained_write_buffers);
        break;
    }
    case PN_RAW_CONNECTION_DISCONNECTED: {
        qd_set_condition_on_vflow(conn->pn_raw_conn, conn->vflow);
        if (!conn->ingress) {
            conn->initial_settings_frame_sent = false;
            if (conn->delete_egress_connections) {
                // The egress connection has been deleted, cancel any pending timer
                cancel_activation(conn);
            }
            else {
                if (schedule_activation(conn, 2000))
                        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                               "[C%" PRIu64 "] Scheduling 2 second timer to reconnect to egress connection",
                               conn->conn_id);
            }
        }
        conn->connection_established = false;
        // If somehow the PN_RAW_CONNECTION_CLOSED_WRITE and the PN_RAW_CONNECTION_CLOSED_READ events did not come by,
        // we will drain the buffers here just as a backup.
        int drained_buffers = qd_raw_connection_drain_read_write_buffers(conn->pn_raw_conn);
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] PN_RAW_CONNECTION_DISCONNECTED, ingress=%i, drained_buffers=%i", conn->conn_id,
               conn->ingress, drained_buffers);
        handle_disconnected(conn);
        break;
    }
    case PN_RAW_CONNECTION_NEED_WRITE_BUFFERS: {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] PN_RAW_CONNECTION_NEED_WRITE_BUFFERS Need write buffers", conn->conn_id);
        break;
    }
    case PN_RAW_CONNECTION_NEED_READ_BUFFERS: {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] PN_RAW_CONNECTION_NEED_READ_BUFFERS Need read buffers", conn->conn_id);
        if (!IS_ATOMIC_FLAG_SET(&conn->raw_closed_read)) {
            grant_read_buffers(conn, "PN_RAW_CONNECTION_NEED_READ_BUFFERS");
        }
        break;
    }
    case PN_RAW_CONNECTION_WAKE: {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] PN_RAW_CONNECTION_WAKE Wake-up",
               conn->conn_id);
        if (CLEAR_ATOMIC_FLAG(&conn->q2_restart)) {
            conn->q2_blocked = false;
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] q2 is unblocked on this connection",
                   conn->conn_id);
            handle_incoming_http(conn);
        }

        while (qdr_connection_process(conn->qdr_conn)) {}
        break;
    }
    case PN_RAW_CONNECTION_READ: {
        // We don't want to read when we are q2 blocked.
        if (conn->q2_blocked) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] PN_RAW_CONNECTION_READ conn->q2_blocked is true, returning", conn->conn_id);
            return;
        }
        int read = handle_incoming_http(conn);
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] PN_RAW_CONNECTION_READ Read %i bytes, Total %" PRIu64 " encrypted bytes", conn->conn_id,
               read, conn->encrypted_bytes_in);
        if (qd_tls_has_output(conn->tls)) {
            encrypt_outgoing_tls(conn, 0, true);
        }
        break;
    }
    case PN_RAW_CONNECTION_DRAIN_BUFFERS: {
        pn_raw_connection_t *pn_raw_conn     = pn_event_raw_connection(e);
        int                  drained_buffers = qd_raw_connection_drain_read_write_buffers(pn_raw_conn);
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] PN_RAW_CONNECTION_DRAIN_BUFFERS Drained a total of %i buffers", conn->conn_id,
               drained_buffers);
    } break;
    case PN_RAW_CONNECTION_WRITTEN: {
        pn_raw_buffer_t buffs[WRITE_BUFFERS];
        size_t n;
        size_t written = 0;

        if (conn->pn_raw_conn == 0) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] PN_RAW_CONNECTION_WRITTEN, No pn_raw_conn",
                   conn->conn_id);
            break;
        }
        while ( (n = pn_raw_connection_take_written_buffers(conn->pn_raw_conn, buffs, WRITE_BUFFERS)) ) {
            for (size_t i = 0; i < n; ++i) {
                written += buffs[i].size;
                qd_adaptor_buffer_t *qd_http2_buff = (qd_adaptor_buffer_t *) buffs[i].context;
                assert(qd_http2_buff);
                qd_adaptor_buffer_free(qd_http2_buff);
            }
        }
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] PN_RAW_CONNECTION_WRITTEN Wrote %zu bytes",
               conn->conn_id, written);
        restart_streams(conn);
        break;
    }
    default:
        break;
    }
}

// Handle the proactor listener accept event. This runs on the proactor listener thread.
//
static void handle_listener_accept(qd_adaptor_listener_t *adaptor_listener, pn_listener_t *pn_listener, void *context)
{
    CHECK_PROACTOR_LISTENER(pn_listener);

    qd_http_listener_t     *li   = (qd_http_listener_t *) context;
    qdr_http2_connection_t *conn = qdr_http_connection_ingress(li);
    pn_listener_raw_accept(pn_listener, conn->pn_raw_conn);
}

/**
 * Delete connector via Management request
 */
void qd_http2_delete_connector(qd_dispatch_t *qd, qd_http_connector_t *connector)
{
    if (connector) {
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_INFO, "Deleted HttpConnector for %s, %s:%s",
           connector->config->adaptor_config->address, connector->config->adaptor_config->host,
           connector->config->adaptor_config->port);

    sys_mutex_lock(&http2_adaptor->lock);
    DEQ_REMOVE(http2_adaptor->connectors, connector);
    sys_mutex_unlock(&http2_adaptor->lock);
    //
    // Deleting a connector must delete the corresponding qdr_connection_t and qdr_http2_connection_t objects also.
    //
    if (connector->ctx) {
        qdr_connection_t       *qdr_conn     = (qdr_connection_t *) connector->ctx;
        qdr_http2_connection_t *http_conn    = qdr_connection_get_context(qdr_conn);
        http_conn->delete_egress_connections = true;
        qdr_core_close_connection(qdr_conn);
        }
        qd_http_connector_decref(connector);
    }
}

/**
 * Delete listener via Management request
 */
void qd_http2_delete_listener(qd_dispatch_t *qd, qd_http_listener_t *li)
{
    if (li) {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_INFO, "Deleting HttpListener for %s, %s:%s",
               li->config->adaptor_config->address, li->config->adaptor_config->host, li->config->adaptor_config->port);

        qd_adaptor_listener_close(li->adaptor_listener);
        li->adaptor_listener = 0;

        sys_mutex_lock(&http2_adaptor->lock);
        DEQ_REMOVE(http2_adaptor->listeners, li);
        sys_mutex_unlock(&http2_adaptor->lock);

        qd_http_listener_decref(li);
    }
}

/**
 * Create listener via Management request
 */
qd_http_listener_t *qd_http2_configure_listener(qd_http_listener_t *li, qd_dispatch_t *qd, qd_entity_t *entity)
{
    if (li->config->adaptor_config->ssl_profile_name) {
        li->tls_domain = qd_tls_domain(li->config->adaptor_config, qd, LOG_HTTP_ADAPTOR, protocols,
                                       NUM_ALPN_PROTOCOLS, true);
        if (!li->tls_domain) {
            // note qd_tls_domain logged the error
            qd_http_listener_decref(li);
            return 0;
        }
    }

    li->adaptor_listener = qd_adaptor_listener(qd, li->config->adaptor_config, LOG_HTTP_ADAPTOR);

    //
    // This is the top level listener vanflow record. This vanflow has no parent record.
    // Reports the listener configuration to vflow
    //
    li->vflow = vflow_start_record(VFLOW_RECORD_LISTENER, 0);
    vflow_set_string(li->vflow, VFLOW_ATTRIBUTE_PROTOCOL, "http2");
    vflow_set_string(li->vflow, VFLOW_ATTRIBUTE_NAME, li->config->adaptor_config->name);
    vflow_set_string(li->vflow, VFLOW_ATTRIBUTE_DESTINATION_HOST, li->config->adaptor_config->host);
    vflow_set_string(li->vflow, VFLOW_ATTRIBUTE_DESTINATION_PORT, li->config->adaptor_config->port);
    vflow_set_string(li->vflow, VFLOW_ATTRIBUTE_VAN_ADDRESS, li->config->adaptor_config->address);

    sys_mutex_lock(&http2_adaptor->lock);
    DEQ_INSERT_TAIL(http2_adaptor->listeners, li);  // holds li refcount
    sys_mutex_unlock(&http2_adaptor->lock);

    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_INFO, "Configured http2_adaptor listener on %s",
           li->config->adaptor_config->host_port);
    // Note: the proactor may execute _handle_listener_accept on another thread during this call
    qd_adaptor_listener_listen(li->adaptor_listener, handle_listener_accept, (void *) li);

    return li;
}

qd_http_connector_t *qd_http2_configure_connector(qd_http_connector_t *connector, qd_dispatch_t *qd,
                                                  qd_entity_t *entity)
{
    if (connector->config->adaptor_config->ssl_profile_name) {
        connector->tls_domain = qd_tls_domain(connector->config->adaptor_config, qd, LOG_HTTP_ADAPTOR,
                                              protocols, NUM_ALPN_PROTOCOLS, false);
        if (!connector->tls_domain) {
            // note qd_tls_domain logged the error
            qd_http_connector_decref(connector);
            return 0;
        }
    }
    DEQ_INSERT_TAIL(http2_adaptor->connectors, connector);

    //
    // This is the top level connector vanflow record. This vanflow has no parent record.
    // Reports the connector configuration to vflow
    //
    connector->vflow = vflow_start_record(VFLOW_RECORD_CONNECTOR, 0);
    vflow_set_string(connector->vflow, VFLOW_ATTRIBUTE_PROTOCOL, "http2");
    vflow_set_string(connector->vflow, VFLOW_ATTRIBUTE_NAME, connector->config->adaptor_config->name);
    vflow_set_string(connector->vflow, VFLOW_ATTRIBUTE_DESTINATION_HOST, connector->config->adaptor_config->host);
    vflow_set_string(connector->vflow, VFLOW_ATTRIBUTE_DESTINATION_PORT, connector->config->adaptor_config->port);
    vflow_set_string(connector->vflow, VFLOW_ATTRIBUTE_VAN_ADDRESS, connector->config->adaptor_config->address);

    qdr_http_connection_egress(connector);
    return connector;
}

// avoid re-scheduling too rapidly after a connection drop - see ISSUE #582
static bool schedule_activation(qdr_http2_connection_t *conn, qd_duration_t msec)
{
    if (SET_ATOMIC_FLAG(&conn->activate_scheduled) == 0) {
        qd_timer_schedule(conn->activate_timer, msec);
        return true;
    }
    return false;
}

static void cancel_activation(qdr_http2_connection_t *conn)
{
    // order is important: clearing the flag after the cancel eliminates a race where the flag may be left set without
    // the timer being scheduled.
    qd_timer_cancel(conn->activate_timer);
    CLEAR_ATOMIC_FLAG(&conn->activate_scheduled);
}


/**
 * Called just before shutdown of the router. Frees listeners and connectors and any http2 buffers
 */
static void qdr_http2_adaptor_final(void *adaptor_context)
{
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "Shutting down HTTP2 Protocol adaptor");
    qdr_http2_adaptor_t *adaptor = (qdr_http2_adaptor_t*) adaptor_context;
    qdr_protocol_adaptor_free(adaptor->core, adaptor->adaptor);

    // Free all remaining connections.
    qdr_http2_connection_t *http_conn = DEQ_HEAD(adaptor->connections);
    while (http_conn) {
        if (http_conn->stream_dispatcher_stream_data) {
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] Freeing stream_data (stream_dispatcher, qdr_http2_adaptor_final) (%lx)",
                   http_conn->conn_id, (long) http_conn->stream_dispatcher_stream_data);
            free_qdr_http2_stream_data_t(http_conn->stream_dispatcher_stream_data);
            http_conn->stream_dispatcher_stream_data = 0;
        }
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] Freeing http2 connection (calling free_qdr_http2_connection)", http_conn->conn_id);
        qd_adaptor_buffer_list_free_buffers(&http_conn->out_buffs);
        free_qdr_http2_connection(http_conn, true);
        http_conn = DEQ_HEAD(adaptor->connections);
    }

    // Free all http listeners
    qd_http_listener_t *li = DEQ_HEAD(adaptor->listeners);
    while (li) {
        DEQ_REMOVE_HEAD(adaptor->listeners);
        assert(sys_atomic_get(&li->ref_count) == 1);  // leak check
        qd_http_listener_decref(li);
        li = DEQ_HEAD(adaptor->listeners);
    }

    // Free all http connectors
    qd_http_connector_t *ct = DEQ_HEAD(adaptor->connectors);
    while (ct) {
        DEQ_REMOVE_HEAD(adaptor->connectors);
        qd_http_connector_decref(ct);
        ct = DEQ_HEAD(adaptor->connectors);
    }

    sys_mutex_free(&adaptor->lock);
    nghttp2_session_callbacks_del(adaptor->callbacks);
    http2_adaptor =  NULL;
    free(adaptor);
}

/**
 * This initialization function will be invoked when the router core is ready for the protocol
 * adaptor to be created.  This function:
 *
 *   1) Registers the protocol adaptor with the router-core.
 *   2) Prepares the protocol adaptor to be configured.
 *   3) Registers nghttp2 callbacks
 */
static void qdr_http2_adaptor_init(qdr_core_t *core, void **adaptor_context)
{
    qdr_http2_adaptor_t *adaptor = NEW(qdr_http2_adaptor_t);
    adaptor->core    = core;
    adaptor->adaptor = qdr_protocol_adaptor(core,
                                            "http2",  // name
                                            adaptor,  // context
                                            qdr_http_activate,
                                            qdr_http_first_attach,
                                            qdr_http_second_attach,
                                            qdr_http_detach,
                                            qdr_http_flow,
                                            qdr_http_offer,
                                            qdr_http_drained,
                                            qdr_http_drain,
                                            qdr_http_push,
                                            qdr_http_deliver,
                                            qdr_http_get_credit,
                                            qdr_http_delivery_update,
                                            qdr_http_conn_close,
                                            qdr_http_conn_trace);
    sys_mutex_init(&adaptor->lock);
    *adaptor_context = adaptor;
    DEQ_INIT(adaptor->listeners);
    DEQ_INIT(adaptor->connectors);
    DEQ_INIT(adaptor->connections);

    //
    // Register all nghttp2 callbacks.
    //
    nghttp2_session_callbacks *callbacks;
    nghttp2_session_callbacks_new(&callbacks);

    //
    // These callbacks are called when we feed the incoming binary http2 data from the client or the server to nghttp2_session_mem_recv()
    // in handle_incoming_http
    //
    nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, on_frame_recv_callback);
    nghttp2_session_callbacks_set_on_begin_headers_callback(callbacks, on_begin_headers_callback);
    nghttp2_session_callbacks_set_on_header_callback(callbacks, on_header_callback);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks, on_data_chunk_recv_callback);
    nghttp2_session_callbacks_set_on_invalid_frame_recv_callback(callbacks, on_invalid_frame_recv_callback);
    nghttp2_session_callbacks_set_on_stream_close_callback(callbacks, on_stream_close_callback);
    nghttp2_session_callbacks_set_on_frame_send_callback(callbacks, on_frame_send_callback);
    nghttp2_session_callbacks_set_on_frame_not_send_callback(callbacks, on_frame_not_send_callback);

    // These callbacks are called when you try to push out amqp data to http2
    // More specifically, they are called from handle_outgoing_http()
    nghttp2_session_callbacks_set_send_data_callback(callbacks, send_data_callback);
    nghttp2_session_callbacks_set_send_callback(callbacks, send_callback);

    // This is a general callback
    nghttp2_session_callbacks_set_error_callback2(callbacks, on_error_callback);

    adaptor->callbacks = callbacks;
    http2_adaptor = adaptor;
}

/**
 * Declare the adaptor so that it will self-register on process startup.
 */
QDR_CORE_ADAPTOR_DECLARE("http2-adaptor", qdr_http2_adaptor_init, qdr_http2_adaptor_final)
