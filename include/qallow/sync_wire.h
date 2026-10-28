/* qallow/sync_wire.h
 * Wire format for Qallow mesh sync (Task 1).
 * Transport-agnostic: feed bytes in, get frames out. No sockets here.
 * Layout: hand-rolled, length-prefixed, little-endian, explicit
 * serialization (no struct casts; immune to padding/endianness).
 *
 * HARD INVARIANT: LIMEN credentials and quantum link traffic must
 * never enter these payloads. This layer carries semantic memory
 * envelopes only.
 *
 * NOTE: qsw_envelope is the merge input for qallow/persist_lmdb.h
 * (Task 0); the wire and on-disk contracts share this one struct.
 */
#ifndef QALLOW_SYNC_WIRE_H
#define QALLOW_SYNC_WIRE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define QSW_MAGIC        0x4E595351u /* "QSYN" LE */
#define QSW_PROTO_VER    1u
#define QSW_PROTO_VER_V2 2u
#define QSW_NODE_ID_LEN  16u
#define QSW_MAX_KEY_LEN  1024u
#define QSW_MAX_BLOB_LEN (16u * 1024u * 1024u) /* 16 MiB per envelope */
/* v2 only: scopes are a native wire field. C caps exist because the
 * decoded struct is fixed-size; DUCTEI emits at most a few short scopes
 * (closed Rust types). The Rust side allows more; the C decoder rejects
 * past these caps as QSW_E_MALFORMED. */
#define QSW_MAX_SCOPES    16u
#define QSW_MAX_SCOPE_LEN 256u

/* Frame types */
typedef enum {
    QSW_F_HELLO     = 1, /* handshake: magic, proto ver, node id, lamport */
    QSW_F_HELLO_ACK = 2, /* same body as HELLO, sent in reply */
    QSW_F_ENVELOPE  = 3, /* one sync envelope */
    QSW_F_BATCH_END = 4, /* sender finished a sync pass (carries lamport) */
    QSW_F_BYE       = 5  /* orderly close */
} qsw_frame_type;

/* Error codes (negative), success >= 0 */
typedef enum {
    QSW_OK          = 0,
    QSW_NEED_MORE   = 1,  /* decoder: partial frame, feed more bytes */
    QSW_E_ARG       = -1,
    QSW_E_SPACE     = -2, /* output buffer too small */
    QSW_E_MAGIC     = -3,
    QSW_E_VERSION   = -4,
    QSW_E_MALFORMED = -5,
    QSW_E_TOO_BIG   = -6
} qsw_status;

/* Handshake body (HELLO / HELLO_ACK / BATCH_END uses lamport only) */
typedef struct {
    uint32_t magic;
    uint16_t proto_ver;
    uint16_t caps;                        /* capability flags, 0 for now */
    uint8_t  node_id[QSW_NODE_ID_LEN];
    uint64_t lamport;                     /* sender clock at send time */
} qsw_hello;

/* Sync envelope. Mirrors the Task 0 versioned envelope:
 * origin node, Lamport clock, schema version, key, value blob. */
typedef struct {
    uint8_t     node_id[QSW_NODE_ID_LEN]; /* origin node */
    uint64_t    lamport;                  /* origin Lamport timestamp */
    uint16_t    schema_ver;               /* envelope schema version */
    uint16_t    flags;                    /* 0x1 = tombstone */
    uint32_t    key_len;
    uint32_t    blob_len;
    const void *key;                      /* borrowed, not owned */
    const void *blob;                     /* borrowed, not owned */
} qsw_envelope;

/* Decoded frame */
typedef struct {
    uint8_t type;                         /* qsw_frame_type */
    union {
        qsw_hello    hello;               /* HELLO / HELLO_ACK */
        qsw_envelope env;                 /* ENVELOPE */
        uint64_t     lamport;             /* BATCH_END */
    } u;
} qsw_frame;

/* --- QSW proto v2 (Task 5) -----------------------------------------
 * Scopes are a native length-prefixed wire field instead of the v1
 * key-prefix shim ("scope0,scope1|key"). Frame layout is unchanged
 * (u8 type | u32 body_len | body); only the ENVELOPE body differs and
 * HELLO carries proto_ver=2. The byte oracle is DUCTEI's
 * ductei-qallow/src/v2.rs; the conformance oracle in DUCTEI CI feeds
 * Rust-emitted v2 frames through qsw_decode_v2 below.
 *
 * v2 ENVELOPE body layout:
 *   node_id[16] | u64 lamport | u16 schema_ver | u16 flags |
 *   u16 scope_count | (u16 scope_len | scope bytes){scope_count} |
 *   u32 key_len | u32 blob_len | key | blob
 *
 * Merge-key parity: a v2 envelope is merged under the same LMDB key as
 * its v1 rendering - scopes sorted, joined with ",", then "|" + key.
 * Scopes are routing metadata; the on-disk merge contract stays
 * key -> blob (see qallow/persist_lmdb.h).
 */

/* One v2 scope: length-prefixed on the wire, borrowed here. */
typedef struct {
    uint16_t    len;                      /* byte length of data */
    const void *data;                     /* borrowed, points into buf */
} qsw_scope;

/* v2 body fixed part: node_id + lamport + schema + flags + scope_count */
#define QSW_ENV_V2_FIXED (QSW_NODE_ID_LEN + 8u + 2u + 2u + 2u)
/* Upper bound on a v2 ENVELOPE body (fixed + max scopes + key/blob lens
 * + key + blob); used by the decoder's too-big check, mirroring v1. */
#define QSW_ENV_V2_MAX_BODY \
    (QSW_ENV_V2_FIXED + \
     QSW_MAX_SCOPES * (2u + QSW_MAX_SCOPE_LEN) + \
     8u + QSW_MAX_KEY_LEN + QSW_MAX_BLOB_LEN)

/* v2 sync envelope. Mirrors DUCTEI's Envelope: the key is clean (no
 * scope shim on the wire); scopes ride in their own field. */
typedef struct {
    uint8_t     node_id[QSW_NODE_ID_LEN]; /* origin node */
    uint64_t    lamport;                  /* origin Lamport timestamp */
    uint16_t    schema_ver;               /* envelope schema version */
    uint16_t    flags;                    /* 0x1 = tombstone */
    uint16_t    scope_count;
    qsw_scope   scopes[QSW_MAX_SCOPES];
    uint32_t    key_len;
    uint32_t    blob_len;
    const void *key;                      /* borrowed, not owned */
    const void *blob;                     /* borrowed, not owned */
} qsw_envelope_v2;

/* Decoded v2 frame */
typedef struct {
    uint8_t type;                         /* qsw_frame_type */
    union {
        qsw_hello       hello;            /* HELLO / HELLO_ACK */
        qsw_envelope_v2 env;              /* ENVELOPE */
        uint64_t        lamport;          /* BATCH_END */
    } u;
} qsw_frame_v2;

/* --- Encoding ---------------------------------------------------
 * Each writes one complete frame into out. Returns bytes written,
 * or negative qsw_status. Frame layout on the wire:
 *   u8  type | u32 body_len | body
 */
int32_t qsw_encode_hello(uint8_t *out, size_t cap, uint8_t type,
                         const uint8_t node_id[QSW_NODE_ID_LEN],
                         uint64_t lamport);
int32_t qsw_encode_envelope(uint8_t *out, size_t cap,
                            const qsw_envelope *env);
int32_t qsw_encode_batch_end(uint8_t *out, size_t cap, uint64_t lamport);
int32_t qsw_encode_bye(uint8_t *out, size_t cap);

/* --- Decoding ----------------------------------------------------
 * Incremental: works with partial reads from any transport.
 * Feed a byte window; on QSW_OK, *frame is valid (env.key/env.blob
 * point INTO buf; copy before reusing buf) and *consumed is set.
 * On QSW_NEED_MORE, keep the bytes and feed a longer window.
 */
qsw_status qsw_decode(const uint8_t *buf, size_t len,
                      qsw_frame *frame, size_t *consumed);

/* Handshake validation: checks magic + proto version. */
qsw_status qsw_hello_validate(const qsw_hello *h);

/* --- v2 API (Task 5) ---------------------------------------------
 * Same conventions as v1: hand-rolled LE, length-prefixed, incremental
 * decode, borrowed pointers into the input buffer.
 */

/* HELLO / HELLO_ACK with proto_ver=2 on the wire. */
int32_t qsw_encode_hello_v2(uint8_t *out, size_t cap, uint8_t type,
                            const uint8_t node_id[QSW_NODE_ID_LEN],
                            uint64_t lamport);

/* Envelope -> QSW v2 ENVELOPE frame. Scopes are sorted bytewise on the
 * wire (matching DUCTEI's Rust encoder) and must fit
 * QSW_MAX_SCOPES x QSW_MAX_SCOPE_LEN. */
int32_t qsw_encode_envelope_v2(uint8_t *out, size_t cap,
                               const qsw_envelope_v2 *env);

/* Incremental v2 frame decode. Works with partial reads like qsw_decode.
 * A v1 stream fed here is rejected (QSW_E_MALFORMED): the versions are
 * distinct formats, never auto-detected. */
qsw_status qsw_decode_v2(const uint8_t *buf, size_t len,
                         qsw_frame_v2 *frame, size_t *consumed);

/* v2 ENVELOPE body only (no frame header) -> qsw_envelope_v2. Mirrors
 * ductei_qallow::v2::decode_envelope_body, including its exact-length
 * rule: body_len must equal consumed + key_len + blob_len. */
qsw_status qsw_decode_v2_envelope_body(const uint8_t *body, size_t body_len,
                                       qsw_envelope_v2 *env);

/* Handshake validation for v2: magic + proto_ver == QSW_PROTO_VER_V2. */
qsw_status qsw_hello_validate_v2(const qsw_hello *h);

/* Rebuild the v1 merge key from a decoded v2 envelope: scopes sorted
 * bytewise, joined with ",", then "|" + key. This is the string v1's
 * shim produces (DUCTEI encode_envelope), so a record lands under the
 * same LMDB key in either proto version. Returns bytes written (no
 * NUL), or a negative qsw_status. The result is never larger than
 * QSW_MAX_KEY_LEN (persist's key cap). */
int32_t qsw_v2_merge_key(const qsw_envelope_v2 *env,
                         uint8_t *out, size_t cap);

#ifdef __cplusplus
}
#endif
#endif /* QALLOW_SYNC_WIRE_H */
