#ifndef __message_private_h__
#define __message_private_h__ 1
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

#include "qpid/dispatch/alloc.h"
#include "qpid/dispatch/atomic.h"
#include "qpid/dispatch/internal/thread_annotations.h"
#include "qpid/dispatch/message.h"
#include "qpid/dispatch/threading.h"

typedef struct qd_message_pvt_t qd_message_pvt_t;

/** @file
 * Message representation.
 * 
 * Architecture of the message module:
 *
 *     +--------------+            +----------------------+
 *     |              |            |                      |
 *     | qd_message_t |----------->| qd_message_content_t |
 *     |              |     +----->|                      |
 *     +--------------+     |      +----------------------+
 *                          |                |
 *     +--------------+     |                |    +-------------+   +-------------+   +-------------+
 *     |              |     |                +--->| qd_buffer_t |-->| qd_buffer_t |-->| qd_buffer_t |--/
 *     | qd_message_t |-----+                     +-------------+   +-------------+   +-------------+
 *     |              |
 *     +--------------+
 *
 * The message module provides chained-fixed-sized-buffer storage of message content with multiple
 * references.  If a message is received and is to be queued for multiple destinations, there is only
 * one copy of the message content in memory but multiple lightweight references to the content.
 *
 * @internal
 * @{ 
 */

typedef struct {
    qd_buffer_t *buffer;     // Buffer that contains the first octet of the field, null if the field is not present
    size_t       offset;     // Offset in the buffer to the first octet of the header
    size_t       length;     // Length of the field or zero if unneeded
    size_t       hdr_length; // Length of the field's header (not included in the length of the field)
    bool         parsed;     // True iff the buffer chain has been parsed to find this field
    uint8_t      tag;        // Type tag of the field
} qd_field_location_t;


typedef struct {
    qd_message_q2_unblocked_handler_t  handler;
    qd_alloc_safe_ptr_t                context;
} qd_message_q2_unblocker_t;


// TODO - consider using pointers to qd_field_location_t below to save memory
// TODO - provide a way to allocate a message without a lock for the link-routing case.
//        It's likely that link-routing will cause no contention for the message content.
//

typedef struct {
    sys_mutex_t          lock;
    sys_mutex_t          producer_activation_lock;        // These locks prevent either side from activating
    sys_mutex_t          consumer_activation_lock;        // the other during tear-down.
                                                          // Using these locks, rather than the content lock
                                                          // for this purpose, eliminates severe contention
                                                          // that was observed on the content lock.
    sys_atomic_t         ref_count;                       // The number of messages referencing this
    qd_buffer_list_t     buffers;                         // The buffer chain containing the message
    qd_buffer_t         *pending;                         // Buffer owned by and filled by qd_message_receive
    uint64_t             buffers_freed;                   // count of large msg buffers freed on send

    qd_field_location_t  section_router_annotation;       // Router annotations (internal use only)
    qd_field_location_t  section_message_header;          // The message header list
    qd_field_location_t  section_delivery_annotation;     // The delivery annotation map
    qd_field_location_t  section_message_annotation;      // The message annotation map
    qd_field_location_t  section_message_properties;      // The message properties list
    qd_field_location_t  section_application_properties;  // The application properties list
    qd_field_location_t  section_raw_body;                // Raw octets following the properties
    qd_field_location_t  section_body;                    // The message body: Data
    qd_field_location_t  section_footer;                  // The footer

    qd_field_location_t  field_message_id;                // The string value of the message-id
    qd_field_location_t  field_user_id;                   // The string value of the user-id
    qd_field_location_t  field_to;                        // The string value of the to field
    qd_field_location_t  field_subject;                   // The string value of the subject field
    qd_field_location_t  field_reply_to;                  // The string value of the reply_to field
    qd_field_location_t  field_correlation_id;            // The string value of the correlation_id field
    qd_field_location_t  field_content_type;
    qd_field_location_t  field_content_encoding;
    qd_field_location_t  field_absolute_expiry_time;
    qd_field_location_t  field_creation_time;
    qd_field_location_t  field_group_id;
    qd_field_location_t  field_group_sequence;
    qd_field_location_t  field_reply_to_group_id;

    qd_buffer_t         *parse_buffer;                    // Buffer where parsing should resume
    unsigned char       *parse_cursor;                    // Octet in parse_buffer where parsing should resume
    qd_message_depth_t   parse_depth;                     // Depth to which message content has been parsed

    // Per-message Router annotations.  These values are extracted from the
    // incoming messages router annotations section.  Refer to
    // docs/notes/router-annotations.adoc for more information.
    //
    qd_parsed_field_t   *ra_pf_ingress;                  // ingress router id
    qd_parsed_field_t   *ra_pf_to_override;              // optional dest address override
    qd_parsed_field_t   *ra_pf_trace;                    // the fields from the trace list
    qd_parsed_field_t   *ra_pf_flags;                    // misc control flags
    qd_parsed_field_t   *ra_pf_ingress_mesh;             // mesh_id of ingress edge router
    bool                 ra_disabled;                    // true: link routing - no router annotations involved.
    bool                 ra_parsed;

    uint64_t             max_message_size;               // Configured max; 0 if no max to enforce
    uint64_t             bytes_received;                 // Bytes returned by pn_link_recv()
                                                         //  when enforcing max_message_size
    size_t               protected_buffers;              // Count of permanent buffers that hold message headers
    uint32_t             fanout;                         // Number of receivers for this message
                                                         //  including in-process subscribers.
    qd_message_q2_unblocker_t q2_unblocker;              // Callback and context to signal Q2 unblocked to receiver

    bool                 q2_input_holdoff;               // Q2 state: hold off calling pn_link_recv
    bool                 disable_q2_holdoff;             // Disable Q2 flow control

    sys_atomic_t         discard;                        // Message is being discarded
    sys_atomic_t         receive_complete;               // Message has been completely received
    sys_atomic_t         priority_parsed;                // Message priority has been parsed
    sys_atomic_t         oversize;                       // Policy oversize-message handling in effect
    sys_atomic_t         no_body;                        // HTTP2 request has no body
    sys_atomic_t         priority;                       // Message AMQP priority
    sys_atomic_t         aborted;                        // Message has been aborted

    sys_atomic_t             uct_enabled;
    qd_buffer_list_t         uct_slots[UCT_SLOT_COUNT];
    sys_atomic_t             uct_produce_slot;
    sys_atomic_t             uct_consume_slot;
    qd_message_activation_t  uct_producer_activation;
    qd_message_activation_t  uct_consumer_activation;
} qd_message_content_t;

struct qd_message_pvt_t {
    struct {
        qd_buffer_t *buffer;
        unsigned char *cursor;
    }                              cursor;          // Pointer to current location of outgoing byte stream.
    qd_message_content_t          *content;         // Singleton content shared by reference between
                                                    //  incoming and all outgoing copies
    char                          *ra_to_override;  // new outgoing value for to-override annotation
    char                          *ra_ingress_mesh; // new outgoing value for ingress_mesh annotation
    uint32_t                       ra_flags;        // new outgoing value for flag annotation
    bool                           strip_annotations_in;
    bool                           ra_sent;         // false == router annotation section not yet sent
    bool                           tag_sent;        // Tags are sent
    bool                           is_fanout;       // Message is an outgoing fanout
    bool                           uct_started;     // Cut-through has been started for this message
    sys_atomic_t                   send_complete;   // Message has been been completely sent
};

ALLOC_DECLARE_SAFE(qd_message_t);
ALLOC_DECLARE(qd_message_content_t);

#define MSG_CONTENT(m)     (((qd_message_pvt_t*) m)->content)
#define MSG_FLAG_STREAMING       0x01u
#define MSG_FLAG_RESEND_RELEASED 0x02u
#define MSG_FLAG_DISABLE_Q2      0x04u

/** Initialize logging */
void qd_message_initialize(void);

//
// Internal API - exported for unit testing ONLY:
//

// These expect content->lock to be locked.
bool _Q2_holdoff_should_block_LH(const qd_message_content_t *content) TA_REQ(content->lock);
bool _Q2_holdoff_should_unblock_LH(const qd_message_content_t *content) TA_REQ(content->lock);

uint32_t _compose_router_annotations(qd_message_pvt_t *msg, unsigned int ra_flags, qd_buffer_list_t *ra_buffers);


///@}

#endif
