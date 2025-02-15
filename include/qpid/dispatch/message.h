#ifndef __dispatch_message_h__
#define __dispatch_message_h__ 1
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

#include "qpid/dispatch/buffer.h"
#include "qpid/dispatch/compose.h"
#include "qpid/dispatch/container.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/iterator.h"
#include "qpid/dispatch/log.h"
#include "qpid/dispatch/parse.h"

#include <proton/raw_connection.h>

typedef struct qdr_delivery_t qdr_delivery_t;

/**@file
 * Message representation. 
 *
 * @defgroup message message
 *
 * Message representation.
 * @{
 */

// DISPATCH-807 Queue depth limits
// upper and lower limits for bang bang hysteresis control
//
// Q2 defines the maximum number of buffers allowed in a message's buffer chain.  This limits the number of bytes that
// will be read from an incoming link for the current message. Once Q2 is enabled no further input data will be read
// from the link. Q2 remains in effect until enough bytes have been consumed by the outgoing link(s) to drop the number
// of buffered bytes below the lower threshold.

#define QD_QLIMIT_Q2_LOWER 32                        // Re-enable link receive
#define QD_QLIMIT_Q2_UPPER (QD_QLIMIT_Q2_LOWER * 2)  // Disable link receive

// Q3 limits the number of bytes allowed to be buffered in an AMQP session's outgoing buffer.  Once the Q3 upper limit
// is hit (read via pn_session_outgoing_bytes), pn_link_send will no longer be called for ALL outgoing links sharing the
// session.  When enough outgoing bytes have been drained below the lower limit pn_link_sends will resume. Note that Q3
// only applies to AMQP links. Non-AMQP (adaptor) link output is limited by the capacity of the raw connection buffer
// pool.

#define QD_QLIMIT_Q3_LOWER (QD_QLIMIT_Q2_UPPER * 2)  // in qd_buffer_t's
#define QD_QLIMIT_Q3_UPPER (QD_QLIMIT_Q3_LOWER * 2)

// Callback for status change (confirmed persistent, loaded-in-memory, etc.)

typedef struct qd_message_t             qd_message_t;
typedef struct qd_message_stream_data_t qd_message_stream_data_t;

/** Amount of message to be parsed.  */
typedef enum {
    QD_DEPTH_NONE,
    QD_DEPTH_ROUTER_ANNOTATIONS,
    QD_DEPTH_HEADER,
    QD_DEPTH_DELIVERY_ANNOTATIONS,
    QD_DEPTH_MESSAGE_ANNOTATIONS,
    QD_DEPTH_PROPERTIES,
    QD_DEPTH_APPLICATION_PROPERTIES,
    QD_DEPTH_BODY,
    QD_DEPTH_RAW_BODY,
    QD_DEPTH_ALL
} qd_message_depth_t;


/** Message fields */
typedef enum {
    QD_FIELD_NONE,   // reserved

    //
    // Message Sections
    //
    QD_FIELD_ROUTER_ANNOTATION,
    QD_FIELD_HEADER,
    QD_FIELD_DELIVERY_ANNOTATION,
    QD_FIELD_MESSAGE_ANNOTATION,
    QD_FIELD_PROPERTIES,
    QD_FIELD_APPLICATION_PROPERTIES,
    QD_FIELD_BODY,
    QD_FIELD_FOOTER,

    //
    // Fields of the Header Section
    // Ordered by list position
    //
    QD_FIELD_DURABLE,
    QD_FIELD_PRIORITY,
    QD_FIELD_TTL,
    QD_FIELD_FIRST_ACQUIRER,
    QD_FIELD_DELIVERY_COUNT,

    //
    // Fields of the Properties Section
    // Ordered by list position
    //
    QD_FIELD_MESSAGE_ID,
    QD_FIELD_USER_ID,
    QD_FIELD_TO,
    QD_FIELD_SUBJECT,
    QD_FIELD_REPLY_TO,
    QD_FIELD_CORRELATION_ID,
    QD_FIELD_CONTENT_TYPE,
    QD_FIELD_CONTENT_ENCODING,
    QD_FIELD_ABSOLUTE_EXPIRY_TIME,
    QD_FIELD_CREATION_TIME,
    QD_FIELD_GROUP_ID,
    QD_FIELD_GROUP_SEQUENCE,
    QD_FIELD_REPLY_TO_GROUP_ID
} qd_message_field_t;


/**
 * Allocate a new message.
 *
 * @return A pointer to a qd_message_t that is the sole reference to a newly allocated
 *         message.
 */
qd_message_t *qd_message(void);

/**
 * Free a message reference.  If this is the last reference to the message, free the
 * message as well.
 *
 * @param msg A pointer to a qd_message_t that is no longer needed.
 */
void qd_message_free(qd_message_t *msg);

/**
 * Make a new reference to an existing message.
 *
 * @param msg A pointer to a qd_message_t referencing a message.
 * @return A new pointer to the same referenced message.
 */
qd_message_t *qd_message_copy(qd_message_t *msg);

/**
 * Parse the router annotations section from a message and place them in
 * the qd_message_t/qd_message_content_t data structures.
 *
 * @param msg Pointer to a received message.
 * @return 0 on success, else an error message
 */
const char *qd_message_parse_router_annotations(qd_message_t *msg);

/**
 * Set the value for the QD_MA_TO field in the outgoing message annotations for
 * the message.
 *
 * @param msg Pointer to an outgoing message.
 * @param to_field Pointer to a c string holding the to override address that
 * will be used as the value for the outgoing QD_MA_TO annotations map entry.
 * If null, the message will not have a QA_MA_TO message annotation field.
 */
void qd_message_set_to_override_annotation(qd_message_t *msg, const char *to_field);

/**
 * Set the value for the ingress_mesh annotation for this message.
 *
 * @param msg Pointer to an outgoing message.
 * @param mesh_identifier Pointer to a character string holding ascii characters and of
 * a length equal to QD_DISCRIMINATOR_BYTES.
 */
void qd_message_set_ingress_mesh(qd_message_t *msg, const char *mesh_identifier);

/**
 * Classify the message as streaming.
 *
 * Marking a message as streaming will prevent downstream routers from manually
 * determining if this message should be sent on an inter-router streaming
 * link. Once a message is classified as streaming it retains the
 * classification until it is delivered to an endpoint
 *
 * @param msg Pointer to an outgoing message.
 *
 */
void qd_message_set_streaming_annotation(qd_message_t *msg);

/**
 * Test whether received message should be considered to be streaming.
 *
 * @param msg Pointer to an outgoing message.
 * @return true if the received message has the streaming annotation set, else false.
 *
 */
int qd_message_is_streaming(const qd_message_t *msg);

/**
 * Classify the message as resend-released.
 *
 * This classification is used when a message is to be re-routed in the event it is RELEASED.
 *
 * @param msg Pointer to an outgoing message.
 * @param value Boolean value to set or clear the resend-released state.
 */
void qd_message_set_resend_released_annotation(qd_message_t *msg, bool value);

/**
 * Test whether a received message is marked as resend-released.
 *
 * @param msg Pointer to an incoming message.
 * @return true if the received message has the resend-released flag set.
 */
bool qd_message_is_resend_released(const qd_message_t *msg);

/**
 * Prevent the router from doing any transformations to the message annotations
 * section of the message.
 *
 * Used by link-routing to completely skip all MA handling, including parsing
 * MA on receive and restoring/composing MA on send.
 */
void qd_message_disable_router_annotations(qd_message_t *in_msg);

/**
 * Receive message data frame by frame via a delivery.  This function may be called more than once on the same
 * delivery if the message spans multiple frames. Always returns a message. The message buffers are filled up to the point with the data that was been received so far.
 * The buffer keeps filling up on successive calls to this function.
 *
 * @param delivery An incoming delivery from a link
 * @return A pointer to the complete message or 0 if the message is not yet complete.
 */
qd_message_t *qd_message_receive(pn_delivery_t *delivery);

/**
 * Returns the PN_DELIVERY_CTX record from the attachments
 *
 * @param delivery An incoming delivery from a link
 * @return - pointer to qd_message_t object
 */
qd_message_t * qd_get_message_context(pn_delivery_t *delivery);

/**
 * Returns true if there is at least one non-empty buffer at the head of the content->buffers list
 * or if the content->pending buffer is non-empty.
 *
 * @param msg A pointer to a message.
 */
bool qd_message_has_data_in_content_or_pending_buffers(qd_message_t   *msg);

/**
 * Send the message outbound on an outgoing link.
 *
 * @param msg A pointer to a message to be sent.
 * @param link The outgoing link on which to send the message.
 * @param ra_flags [in] outbound router annotations control flag
 * @param q3_stalled [out] indicates that the link is stalled due to proton-buffer-full
 */
#define QD_MESSAGE_RA_STRIP_NONE    0x00  // send all router annotations
#define QD_MESSAGE_RA_STRIP_INGRESS 0x01
#define QD_MESSAGE_RA_STRIP_TRACE   0x02
#define QD_MESSAGE_RA_STRIP_ALL     0xFF  // no router annotations section sent
void qd_message_send(qd_message_t *msg, qd_link_t *link, unsigned int ra_flags, bool *q3_stalled);

/**
 * Check that the message is well-formed up to a certain depth.  Any part of the message that is
 * beyond the specified depth is not checked for validity.
 *
 * Note: some message sections are optional - QD_MESSAGE_OK is returned if the
 * optional section is not present, as that is valid.
 */
typedef enum {
    QD_MESSAGE_DEPTH_INVALID,     // corrupt or malformed message detected
    QD_MESSAGE_DEPTH_OK,          // valid up to depth, including 'depth' if not optional
    QD_MESSAGE_DEPTH_INCOMPLETE   // have not received up to 'depth', or partial depth
} qd_message_depth_status_t;

qd_message_depth_status_t qd_message_check_depth(const qd_message_t *msg, qd_message_depth_t depth);

/**
 * Return an iterator for the requested message field.  If the field is not in the message,
 * return NULL.
 *
 * @param msg A pointer to a message.
 * @param field The field to be returned via iterator.
 * @return A field iterator that spans the requested field.
 */
qd_iterator_t *qd_message_field_iterator_typed(qd_message_t *msg, qd_message_field_t field);
qd_iterator_t *qd_message_field_iterator(qd_message_t *msg, qd_message_field_t field);

ssize_t qd_message_field_length(qd_message_t *msg, qd_message_field_t field);
ssize_t qd_message_field_copy(qd_message_t *msg, qd_message_field_t field, char *buffer, size_t *hdr_length);

/**
 * Return the buffer and offset of the beginning of the raw body section.  Return NULL if there is no
 * raw body.
 *
 * Side effect: Atomically enable cut-through on this stream so there is no race-condition between
 *              the producer and consumer.
 *
 * @param msg A pointer to a stream
 * @param buf [out] pointer to the buffer containing the first octet of the raw body (or 0)
 * @param offset [out] The offset in the buffer to the first octet of the raw body
 */
void qd_message_raw_body_and_start_cutthrough(qd_message_t *msg, qd_buffer_t **buf, size_t *offset);

/**
 * This is called when the raw body has been completely consumed by a cut-through consumer.
 * In this function, the function _should_ free any buffers that purely contain body content.
 *
 * @param msg A pointer to a stream
 */
void qd_message_release_raw_body(qd_message_t *msg);

// Create a message using composed fields to supply content.
//
// This message constructor will create a new message using each fields buffers
// concatenated in order (f1 first, f2 second, etc). There is no need to
// provide all three fields: concatenation stops at the first null fx pointer.
//
// Note well that while this constructor can support up to three separate
// composed fields it is more efficent to chain as many message sections as
// possible into as few separate composed fields as possible.  This means that
// any passed composed field can contain several message sections.
//
// This constructor takes ownership of the composed fields - the caller must
// not reference them after the call.
//
qd_message_t *qd_message_compose(qd_composed_field_t *f1,
                                 qd_composed_field_t *f2,
                                 qd_composed_field_t *f3,
                                 bool receive_complete);

// The following qd_message_compose_X are deprecated: Please use the
// qd_message_compose() to create locally generated messages instead
void qd_message_compose_1(qd_message_t *msg, const char *to, qd_buffer_list_t *buffers);
void qd_message_compose_2(qd_message_t *msg, qd_composed_field_t *content, bool receive_complete);
void qd_message_compose_3(qd_message_t *msg, qd_composed_field_t *content1, qd_composed_field_t *content2, bool receive_complete);
void qd_message_compose_4(qd_message_t *msg, qd_composed_field_t *content1, qd_composed_field_t *content2, qd_composed_field_t *content3, bool receive_complete);
void qd_message_compose_5(qd_message_t *msg, qd_composed_field_t *field1, qd_composed_field_t *field2, qd_composed_field_t *field3, qd_composed_field_t *field4, bool receive_complete);

/**
 * qd_message_extend
 *
 * Extend the content of a streaming message with more buffers.
 *
 * @param msg Pointer to a message
 * @param field A composed field to be appended to the end of the message's stream
 * @param q2_blocked Set to true if this call caused Q2 to block
 * @return The number of buffers stored in the message's content
 */
int qd_message_extend(qd_message_t *msg, qd_composed_field_t *field, bool *q2_blocked);


/**
 * qd_message_stream_data_iterator
 *
 * Return an iterator that references the content (not the performative headers)
 * of the entire body-data section.
 *
 * The returned iterator must eventually be freed by the caller.
 *
 * @param stream_data Pointer to a stream_data object produced by qd_message_next_stream_data
 * @return Pointer to an iterator referencing the stream_data content
 */
qd_iterator_t *qd_message_stream_data_iterator(const qd_message_stream_data_t *stream_data);


/**
 * qd_message_stream_data_buffer_count
 *
 * Return the number of buffers that are needed to hold this body-data's content.
 *
 * @param stream_data Pointer to a stream_data object produced by qd_message_next_stream_data
 * @return Number of pn_raw_buffers needed to contain the entire content of this stream_data.
 */
int qd_message_stream_data_buffer_count(const qd_message_stream_data_t *stream_data);


/**
 * qd_message_stream_data_buffers
 *
 * Populate an array of pn_raw_buffer_t objects with references to the stream_data's content.
 *
 * @param stream_data Pointer to a stream_data object produced by qd_message_next_stream_data
 * @param buffers Pointer to an array of pn_raw_buffer_t objects
 * @param offset The offset (in the stream_data's buffer set) from which copying should begin
 * @param count The number of pn_raw_buffer_t objects in the buffers array
 * @return The number of pn_raw_buffer_t objects that were overwritten
 */
int qd_message_stream_data_buffers(qd_message_stream_data_t *stream_data, pn_raw_buffer_t *buffers, int offset, int count);

/**
 * qd_message_stream_data_payload_length
 *
 * Given a stream_data object, return the length of the payload.
 * This will equal the sum of the length of all qd_buffer_t objects contained in payload portion of the stream_data object
 *
 * @param stream_data Pointer to a stream_data object produced by qd_message_next_stream_data
 * @return The length of the payload of the passed in body data object.
 */
size_t qd_message_stream_data_payload_length(const qd_message_stream_data_t *stream_data);


/**
 * qd_message_stream_data_release
 *
 * Release buffers that were associated with a body-data section.  It is not required that body-data
 * objects be released in the same order in which they were offered.
 *
 * Once this function is called, the caller must drop its reference to the stream_data object
 * and not use it again.
 *
 * @param stream_data Pointer to a body data object returned by qd_message_next_stream_data
 */
void qd_message_stream_data_release(qd_message_stream_data_t *stream_data);


/**
 * qd_message_stream_data_release_up_to
 *
 * Release this stream data and all the previous ones also.
 *
 * @param stream_data Pointer to a body data object returned by qd_message_next_stream_data
 */
void qd_message_stream_data_release_up_to(qd_message_stream_data_t *stream_data);


typedef enum {
    QD_MESSAGE_STREAM_DATA_BODY_OK,      // A valid body data object has been returned
    QD_MESSAGE_STREAM_DATA_FOOTER_OK,    // A valid footer has been returned
    QD_MESSAGE_STREAM_DATA_INCOMPLETE,   // The next body data is incomplete, try again later
    QD_MESSAGE_STREAM_DATA_NO_MORE,      // There are no more body data objects in this stream
    QD_MESSAGE_STREAM_DATA_INVALID,      // The next body data is invalid, the stream is corrupted
    QD_MESSAGE_STREAM_DATA_ABORTED       // sender has terminated the transfer, message is incomplete
} qd_message_stream_data_result_t;


/**
 * qd_message_next_stream_data
 *
 * Get the next body-data section from this streaming message return the result and
 * possibly the valid, completed stream_data object.
 *
 * @param msg Pointer to a message
 * @param stream_data Output pointer to a stream_data object (or 0 if not OK)
 * @return The stream_data_result describing the result of this operation
 */
qd_message_stream_data_result_t qd_message_next_stream_data(qd_message_t *msg, qd_message_stream_data_t **stream_data);


/**
 * qd_message_stream_data_footer_append
 *
 * Constructs a footer field by calling the qd_compose(QD_PERFORMATIVE_FOOTER, field);
 * It then inserts the passed in buffer list to the composed field and proceeds to disable q2 before finally adding the footer
 * field to the message.
 *
 * Use this function if you have the complete footer data available in the passed in buffer list
 */
int qd_message_stream_data_footer_append(qd_message_t *message, qd_buffer_list_t *footer_props);


/**
 * qd_message_stream_data_append
 *
 * Append the buffers in data as a sequence of one or more BODY_DATA sections
 * to the given message.  The buffers in data are moved into the message
 * content by this function.
 *
 * @param msg Pointer to message under construction
 * @param data List of buffers containing body data.
 * @param qd_blocked Set to true if this call caused Q2 to block
 * @return The number of buffers stored in the message's content
 */
int qd_message_stream_data_append(qd_message_t *msg, qd_buffer_list_t *data, bool *q2_blocked);


/** Put string representation of a message suitable for logging in buffer.
 * @return buffer
 */
char* qd_message_repr(qd_message_t *msg, char* buffer, size_t len, qd_log_bits log_message);
/** Recommended buffer length for qd_message_repr */
int qd_message_repr_len(void);

qd_log_source_t *qd_message_log_source(void);

/**
 * Accessor for incoming messages ingress router annotation
 *
 * @param msg A pointer to the message
 * @return the parsed field or 0 if no ingress present in msg
 */
qd_parsed_field_t *qd_message_get_ingress_router(qd_message_t *msg);

/**
 * Accessor for message field to_override
 *
 * @param msg A pointer to the message
 * @return the parsed field or 0 if no to_override present
 */
qd_parsed_field_t *qd_message_get_to_override(qd_message_t *msg);

/**
 * Accessor for incoming messages trace annotation
 *
 * @param msg A pointer to the received message
 * @return the parsed field
 */
qd_parsed_field_t *qd_message_get_trace(qd_message_t *msg);

/**
 * Accessor for ingress edge-mesh annotation
 *
 * @param msg A pointer to the received message
 * @return the parsed field
 */
qd_parsed_field_t *qd_message_get_ingress_mesh(qd_message_t *msg);

/**
 * Should the message be discarded.
 * A message can be discarded if the disposition is released or rejected.
 *
 * @param msg A pointer to the message.
 **/
bool qd_message_is_discard(qd_message_t *msg);

/**
 *Set the discard field on the message to to the passed in boolean value.
 *
 * @param msg A pointer to the message.
 * @param discard - the boolean value of discard.
 */
void qd_message_set_discard(qd_message_t *msg, bool discard);

/**
 * Has the message been completely received?
 * Return true if the message is fully received
 * Returns false if only the partial message has been received, if there is more of the message to be received.
 *
 * @param msg A pointer to the message.
 */
bool qd_message_receive_complete(qd_message_t *msg);

/**
 * Returns true if the message has been completely received AND the message has been completely sent.
 */
bool qd_message_send_complete(qd_message_t *msg);

/**
 * Flag the message as being send-complete.
 */
void qd_message_set_send_complete(qd_message_t *msg);


/**
 * Flag the message as being receive-complete.
 */
void qd_message_set_receive_complete(qd_message_t *msg);


/**
 * Returns true if the delivery tag has already been sent.
 */
bool qd_message_tag_sent(qd_message_t *msg);


/**
 * Sets if the delivery tag has already been sent out or not.
 */
void qd_message_set_tag_sent(qd_message_t *msg, bool tag_sent);

/**
 * Increase the fanout of the message by 1.
 *
 * @param out_msg A pointer to the message to be sent outbound or to a local
 * subscriber.
 */
void qd_message_add_fanout(qd_message_t *out_msg);

/**
 * Disable the Q2-holdoff for this message.
 *
 * Note: this call may invoke the Q2 unblock handler routine associated with
 * this message.  See qd_message_set_q2_unblocked_handler().
 *
 * @param msg A pointer to the message
 */
void qd_message_Q2_holdoff_disable(qd_message_t *msg);

/**
 * Check if a message has hit its Q2 limit and is currently blocked.
 * When blocked no further message data will be read from the link.
 *
 * @param msg A pointer to the message
 */
bool qd_message_is_Q2_blocked(const qd_message_t *msg);


/**
 * Register a callback that will be invoked when the message has exited the Q2
 * blocking state. Note that the callback can be invoked on any I/O thread.
 * The callback must be thread safe.
 *
 * @param msg The message to monitor.
 * @param callback The method to invoke
 * @param context safe pointer holding the context
 */

typedef void (*qd_message_q2_unblocked_handler_t)(qd_alloc_safe_ptr_t context);
void qd_message_set_q2_unblocked_handler(qd_message_t *msg,
                                         qd_message_q2_unblocked_handler_t callback,
                                         qd_alloc_safe_ptr_t context);
void qd_message_clear_q2_unblocked_handler(qd_message_t *msg);

/**
 * Return message aborted state
 * @param msg A pointer to the message
 * @return true if the message has been aborted
 */
bool qd_message_aborted(const qd_message_t *msg);

/**
 * Set the aborted flag on the message.
 * @param msg A pointer to the message
 */
void qd_message_set_aborted(qd_message_t *msg);

/**
 * Return message priority
 * @param msg A pointer to the message
 * @return The message priority value. Default if not present.
 */
uint8_t qd_message_get_priority(qd_message_t *msg);

/**
 * True if message is larger that maxMessageSize
 * @param msg A pointer to the message
 * @return 
 */
bool qd_message_oversize(const qd_message_t *msg);

//=====================================================================================================
// Unicast/Cut-through API
//
// This is an optimization for the case where the message is streaming and is being delivered to
// exactly one destination.
//=====================================================================================================

#define UCT_SLOT_COUNT       8
#define UCT_RESUME_THRESHOLD 4

/**
 * Transition this message to unicast/cut-through operation.  This action cannot be reversed for a message.
 *
 * Once this mode is set for a message stream, the conventional methods for accessing the message body will
 * no longer work.
 *
 * @param stream Pointer to the message
 */
void qd_message_start_unicast_cutthrough(qd_message_t *stream);

/**
 * Indicate whether this message stream is in unicast/cut-through mode.
 *
 * @param stream Pointer to the message
 * @return true if the message is in unicast/cut-through mode
 * @return false if not
 */
bool qd_message_is_unicast_cutthrough(const qd_message_t *stream);

/**
 * Indicate whether there is capacity to produce buffers into the stream.
 *
 * @param stream Pointer to the message
 * @return true Yes, there is capacity to produce buffers
 * @return false No, do not attempt to produce buffers
 */
bool qd_message_can_produce_buffers(const qd_message_t *stream);

/**
 * Indicate whether there are buffers to consume from the stream.
 *
 * @param stream Pointer to the message
 * @return true Yes, there are buffers to consume
 * @return false No, there are no buffers to consumer
 */
bool qd_message_can_consume_buffers(const qd_message_t *stream);

/**
 * Return the number of cut-through slots that are filled
 * 
 * @param stream Pointer to the message
 * @return int The number of slots that contain produced content
 */
int qd_message_full_slot_count(const qd_message_t *stream);

/**
 * Produce a list of buffers into the message stream.  The pn_message_can_produce_buffers must be
 * called prior to calling this function to determine whether there is capacity to produce a list
 * of buffers into the stream.  If there is no capacity, this function must not be called.
 *
 * There is no scenario in which this function will partially consume the buffer list.
 *
 * @param stream Pointer to the message
 * @param buffers Pointer to a list of buffers to be appended to the message stream
 */
void qd_message_produce_buffers(qd_message_t *stream, qd_buffer_list_t *buffers);

/**
 * Consume buffers from a message stream.
 *
 * @param stream Pointer to the message
 * @param buffers Pointer to a list of buffers to fill.  Must be empty at the call.
 * @param limit The maximum number of buffers that should be consumed.
 * @return int The number of buffers actually consumed.
 */
int qd_message_consume_buffers(qd_message_t *stream, qd_buffer_list_t *buffers, int limit);


/**
 * Indicate whether this stream should be resumed from a stalled state.  This will be the case
 * if (a) the stream was stalled due to being full, and (b) the payload has shrunk down below
 * the resume threshold.
 *
 * If the result is true, there is a side effect of clearing the 'stalled' state.
 *
 * @param stream Pointer to the message
 * @return true Yes, the stream was stalled and buffer production may continue
 * @return false No, the stream was not stalled or it was stalled and is not yet ready to resume
 */
bool qd_message_resume_from_stalled(qd_message_t *stream);


typedef enum {
    QD_ACTIVATION_NONE = 0,
    QD_ACTIVATION_AMQP,
    QD_ACTIVATION_TCP
} qd_message_activation_type_t;

typedef struct {
    qd_message_activation_type_t  type;
    qd_alloc_safe_ptr_t           safeptr;
    qdr_delivery_t               *delivery;
} qd_message_activation_t;

/**
 * Tell the message stream which connection is consuming its buffers.
 *
 * @param stream Pointer to the message
 * @param connection Pointer to the qd_connection that is consuming this stream's buffers
 */
void qd_message_set_consumer_activation(qd_message_t *stream, qd_message_activation_t *activation);

/**
 * Return the connection that is consuming this message stream's buffers.
 *
 * @param stream Pointer to the message
 * @return qd_connection_t* Pointer to the connection that is consuming buffers from this stream
 */
void qd_message_get_consumer_activation(const qd_message_t *stream, qd_message_activation_t *activation);

/**
 * Tell the message stream which connection is producing its buffers.
 *
 * @param stream Pointer to the message
 * @param connection Pointer to the qd_connection that is consuming this stream's buffers
 */
void qd_message_set_producer_activation(qd_message_t *stream, qd_message_activation_t *activation);

/**
 * Return the connection that is producing this message stream's buffers.
 *
 * @param stream Pointer to the message
 * @return qd_connection_t* Pointer to the connection that is consuming buffers from this stream
 */
void qd_message_get_producer_activation(const qd_message_t *stream, qd_message_activation_t *activation);

///@}

#endif
