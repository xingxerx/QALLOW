/* src/mind/persist_lmdb.c
 * Task 0: LMDB-backed store for sync envelopes. See
 * include/qallow/persist_lmdb.h for the merge contract.
 *
 * On-disk record layout (value blob for a key), explicit and
 * little-endian so a store is portable across hosts of either
 * endianness, matching the wire format's own convention:
 *   u64 lamport
 *   u8[QSW_NODE_ID_LEN] node_id
 *   u16 flags
 *   u16 scope
 *   u64 session_id
 *   u64 session_bound
 *   blob
 */
#include "qallow/persist_lmdb.h"
#include "lmdb.h"

#include <stdlib.h>
#include <string.h>

#define QLP_OFF_LAMPORT       0u
#define QLP_OFF_NODE_ID       (QLP_OFF_LAMPORT + 8u)
#define QLP_OFF_FLAGS         (QLP_OFF_NODE_ID + QSW_NODE_ID_LEN)
#define QLP_OFF_SCOPE         (QLP_OFF_FLAGS + 2u)
#define QLP_OFF_SESSION_ID    (QLP_OFF_SCOPE + 2u)
#define QLP_OFF_SESSION_BOUND (QLP_OFF_SESSION_ID + 8u)
#define QLP_REC_HDR_LEN       (QLP_OFF_SESSION_BOUND + 8u)
#define QLP_MAPSIZE      ((size_t)1 << 30) /* 1 GiB address space, lazily paged */

struct ql_persist_store {
    MDB_env *env;
};

static void put_u64le(uint8_t *p, uint64_t v) {
    for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (8 * i));
}
static uint64_t get_u64le(const uint8_t *p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= (uint64_t)p[i] << (8 * i);
    return v;
}
static void put_u16le(uint8_t *p, uint16_t v) {
    p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8);
}
static uint16_t get_u16le(const uint8_t *p) {
    return (uint16_t)(p[0] | (p[1] << 8));
}

/* Persist payload v2 header inside env->blob:
 *   u16 persist_ver (==2)
 *   u16 scope       (0 = broadcast/open → reject)
 *   u64 session_id  (non-zero)
 *   u64 session_bound (non-zero)
 *   u32 data_len
 *   u8[data_len] data
 */
static int has_reserved_prefix(const void *key, uint32_t key_len) {
    static const char *deny[] = { "env/", "cred/", "secret/", "secrets/", "token/", "password/", NULL };
    const char *k = (const char *)key;
    for (int i = 0; deny[i]; i++) {
        size_t n = strlen(deny[i]);
        if (key_len >= n && strncmp(k, deny[i], n) == 0) return 1;
    }
    return 0;
}
static uint32_t get_u32le(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static int parse_payload_v2(const uint8_t *blob, uint32_t blob_len,
                            uint16_t *out_scope,
                            uint64_t *out_sid, uint64_t *out_bound,
                            const uint8_t **out_data, uint32_t *out_data_len) {
    if (!blob || blob_len < 24) return 0;
    uint16_t ver = get_u16le(blob);
    if (ver != 2) return 0;
    uint16_t scope = get_u16le(blob + 2);
    uint64_t sid   = get_u64le(blob + 4);
    uint64_t bound = get_u64le(blob + 12);
    uint32_t data_len = get_u32le(blob + 20);
    if ((uint64_t)24 + data_len != blob_len) return 0;
    *out_scope = scope; *out_sid = sid; *out_bound = bound;
    *out_data = blob + 24; *out_data_len = data_len;
    return 1;
}

qlp_status ql_persist_open(const char *dir_path, ql_persist_store **out) {
    if (!dir_path || !out) return QLP_E_ARG;

    ql_persist_store *store = (ql_persist_store *)calloc(1, sizeof(*store));
    if (!store) return QLP_E_OPEN;

    if (mdb_env_create(&store->env) != 0) {
        free(store);
        return QLP_E_OPEN;
    }
    if (mdb_env_set_mapsize(store->env, QLP_MAPSIZE) != 0 ||
        mdb_env_open(store->env, dir_path, 0, 0664) != 0) {
        mdb_env_close(store->env);
        free(store);
        return QLP_E_OPEN;
    }

    *out = store;
    return QLP_OK;
}

void ql_persist_close(ql_persist_store *store) {
    if (!store) return;
    mdb_env_close(store->env);
    free(store);
}

qlp_status ql_persist_merge_blob(ql_persist_store *store,
                                 const qsw_envelope *env,
                                 bool *out_applied) {
    if (out_applied) *out_applied = false;
    if (!store || !env || (!env->key && env->key_len) ||
        (!env->blob && env->blob_len))
        return QLP_E_ARG;
    if (env->key_len > QSW_MAX_KEY_LEN || env->blob_len > QSW_MAX_BLOB_LEN)
        return QLP_E_TOO_BIG;

    /* Deny credentials and broadcast/open scope by name. */
    if (has_reserved_prefix(env->key, env->key_len)) return QLP_E_ARG;
    /* Enforce bounded session payload schema (v2); old unbounded fails closed. */
    if (env->schema_ver < 2) return QLP_E_ARG;
    uint16_t scope = 0; uint64_t sid = 0, bound = 0;
    const uint8_t *user_data = NULL; uint32_t user_len = 0;
    if (!parse_payload_v2((const uint8_t *)env->blob, env->blob_len,
                          &scope, &sid, &bound, &user_data, &user_len))
        return QLP_E_ARG;
    if (scope == 0 || sid == 0 || bound == 0) return QLP_E_ARG;

    bool tombstone = (env->flags & 0x1u) != 0;

    MDB_txn *txn = NULL;
    if (mdb_txn_begin(store->env, NULL, 0, &txn) != 0) return QLP_E_TXN;

    MDB_dbi dbi;
    if (mdb_dbi_open(txn, NULL, MDB_CREATE, &dbi) != 0) {
        mdb_txn_abort(txn);
        return QLP_E_OPEN;
    }

    MDB_val k, v;
    k.mv_size = env->key_len;
    k.mv_data = (void *)env->key;

    int get_rc = mdb_get(txn, dbi, &k, &v);
    if (get_rc == 0) {
        const uint8_t *rec = (const uint8_t *)v.mv_data;
        uint64_t cur_lamport = get_u64le(rec + QLP_OFF_LAMPORT);
        const uint8_t *cur_node = rec + QLP_OFF_NODE_ID;

        if (env->lamport < cur_lamport ||
            (env->lamport == cur_lamport &&
             memcmp(env->node_id, cur_node, QSW_NODE_ID_LEN) <= 0)) {
            mdb_txn_abort(txn);
            return QLP_OK; /* stale or tied-and-losing: silent no-op */
        }
    } else if (get_rc != MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        return QLP_E_IO;
    }

    /* A winning tombstone is kept as a header-only record (flags 0x1, no
     * data) instead of being deleted. Deleting it would forget the
     * (lamport, node_id) that won, and a delayed older write arriving
     * later from another peer would resurrect the key. ql_persist_get
     * already reports a tombstoned key as absent. */
    if (tombstone) user_len = 0;

    int put_rc;
    {
        size_t rec_len = QLP_REC_HDR_LEN + user_len;
        uint8_t *rec = (uint8_t *)malloc(rec_len);
        if (!rec) {
            mdb_txn_abort(txn);
            return QLP_E_IO;
        }
        put_u64le(rec + QLP_OFF_LAMPORT, env->lamport);
        memcpy(rec + QLP_OFF_NODE_ID, env->node_id, QSW_NODE_ID_LEN);
        put_u16le(rec + QLP_OFF_FLAGS, env->flags);
        put_u16le(rec + QLP_OFF_SCOPE, scope);
        put_u64le(rec + QLP_OFF_SESSION_ID, sid);
        put_u64le(rec + QLP_OFF_SESSION_BOUND, bound);
        if (user_len) memcpy(rec + QLP_REC_HDR_LEN, user_data, user_len);

        v.mv_size = rec_len;
        v.mv_data = rec;
        put_rc = mdb_put(txn, dbi, &k, &v, 0);
        free(rec);
        if (put_rc != 0) {
            mdb_txn_abort(txn);
            return QLP_E_IO;
        }
    }

    if (mdb_txn_commit(txn) != 0) return QLP_E_TXN;
    if (out_applied) *out_applied = true;
    return QLP_OK;
}

qlp_status ql_persist_get(ql_persist_store *store,
                          const void *key, uint32_t key_len,
                          void *out_val, uint32_t cap, uint32_t *out_len) {
    if (!store || !key || !out_len) return QLP_E_ARG;

    MDB_txn *txn = NULL;
    if (mdb_txn_begin(store->env, NULL, MDB_RDONLY, &txn) != 0) return QLP_E_TXN;

    MDB_dbi dbi;
    if (mdb_dbi_open(txn, NULL, 0, &dbi) != 0) {
        mdb_txn_abort(txn);
        return QLP_E_IO;
    }

    MDB_val k, v;
    k.mv_size = key_len;
    k.mv_data = (void *)key;

    int rc = mdb_get(txn, dbi, &k, &v);
    if (rc != 0) {
        mdb_txn_abort(txn);
        return QLP_E_IO;
    }
    if (v.mv_size < QLP_REC_HDR_LEN) {
        mdb_txn_abort(txn);
        return QLP_E_IO;
    }

    const uint8_t *rec = (const uint8_t *)v.mv_data;
    uint16_t flags = get_u16le(rec + QLP_OFF_FLAGS);
    if (flags & 0x1u) { /* tombstoned */
        mdb_txn_abort(txn);
        return QLP_E_IO;
    }

    uint32_t blob_len = (uint32_t)(v.mv_size - QLP_REC_HDR_LEN);
    *out_len = blob_len;
    if (out_val && cap) {
        uint32_t n = blob_len < cap ? blob_len : cap;
        memcpy(out_val, rec + QLP_REC_HDR_LEN, n);
    }

    mdb_txn_abort(txn); /* read-only: abort == cheap close */
    return QLP_OK;
}
