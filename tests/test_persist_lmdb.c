#include "qallow/persist_lmdb.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#ifdef _WIN32
#include <direct.h>
#include <windows.h> /* GetCurrentProcessId */
#define TEST_MKDIR(path) _mkdir(path)
#else
#include <sys/stat.h>
#define TEST_MKDIR(path) mkdir(path, 0755)
#endif

#ifndef _WIN32
#include <unistd.h>
#endif
static void put_u16le(uint8_t *p, uint16_t v){ p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); }
static void put_u32le(uint8_t *p, uint32_t v){ p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); p[2]=(uint8_t)(v>>16); p[3]=(uint8_t)(v>>24); }
static void put_u64le(uint8_t *p, uint64_t v){ for(int i=0;i<8;i++) p[i]=(uint8_t)(v>>(8*i)); }

static uint8_t* build_v2_payload(const char *data, uint16_t scope,
                                 uint64_t sid, uint64_t bound, uint32_t *out_len) {
    uint32_t dlen = (uint32_t)strlen(data);
    uint32_t total = 24u + dlen;
    uint8_t *buf = (uint8_t*)malloc(total);
    put_u16le(buf + 0, 2);       /* persist_ver */
    put_u16le(buf + 2, scope);   /* scope>0 */
    put_u64le(buf + 4, sid);     /* session id */
    put_u64le(buf + 12, bound);  /* session bound */
    put_u32le(buf + 20, dlen);   /* data_len */
    if (dlen) memcpy(buf + 24, data, dlen);
    *out_len = total;
    return buf;
}

static qsw_envelope mk2(const uint8_t node_id[QSW_NODE_ID_LEN], uint64_t lam,
                        const char *k, const char *v, uint16_t flags,
                        uint8_t **out_blob, uint32_t *out_blob_len) {
    qsw_envelope e = {0};
    memcpy(e.node_id, node_id, QSW_NODE_ID_LEN);
    e.lamport = lam; e.flags = flags; e.schema_ver = 2;
    e.key = k; e.key_len = (uint32_t)strlen(k);
    *out_blob = build_v2_payload(v, /*scope*/1, /*sid*/123, /*bound*/456, out_blob_len);
    e.blob = *out_blob; e.blob_len = *out_blob_len;
    return e;
}

int main(void) {
    char dir[256];
    TEST_MKDIR("build");
#ifdef _WIN32
    snprintf(dir, sizeof dir, "build/.test_persist_dir.%lu", (unsigned long)GetCurrentProcessId());
#else
    snprintf(dir, sizeof dir, "build/.test_persist_dir.%ld", (long)getpid());
#endif
    TEST_MKDIR(dir); /* ignore EEXIST: LMDB just needs the dir to be present */

    ql_persist_store *store = NULL;
    assert(ql_persist_open(dir, &store) == QLP_OK);

    uint8_t idA[QSW_NODE_ID_LEN], idB[QSW_NODE_ID_LEN];
    memset(idA, 0xAA, sizeof idA);
    memset(idB, 0xBB, sizeof idB);

    bool applied;
    uint8_t *b1=NULL,*b2=NULL,*b3=NULL,*b4=NULL,*b5=NULL,*b6=NULL; uint32_t bl1=0,bl2=0,bl3=0,bl4=0,bl5=0,bl6=0;
    qsw_envelope e1 = mk2(idA, 5, "k1", "v1-A", 0, &b1, &bl1);
    assert(ql_persist_merge_blob(store, &e1, &applied) == QLP_OK && applied);

    char buf[64]; uint32_t len;
    assert(ql_persist_get(store, "k1", 2, buf, sizeof buf, &len) == QLP_OK);
    assert(len == 4 && !memcmp(buf, "v1-A", 4));

    /* stale write must lose */
    qsw_envelope e2 = mk2(idB, 3, "k1", "stale", 0, &b2, &bl2);
    assert(ql_persist_merge_blob(store, &e2, &applied) == QLP_OK && !applied);
    assert(ql_persist_get(store, "k1", 2, buf, sizeof buf, &len) == QLP_OK);
    assert(len == 4 && !memcmp(buf, "v1-A", 4));

    /* tie, higher node id wins: B > A */
    qsw_envelope e3 = mk2(idB, 5, "k1", "v1-B", 0, &b3, &bl3);
    assert(ql_persist_merge_blob(store, &e3, &applied) == QLP_OK && applied);
    assert(ql_persist_get(store, "k1", 2, buf, sizeof buf, &len) == QLP_OK);
    assert(len == 4 && !memcmp(buf, "v1-B", 4));

    /* tie, lower node id loses: A < B */
    qsw_envelope e4 = mk2(idA, 5, "k1", "v1-A2", 0, &b4, &bl4);
    assert(ql_persist_merge_blob(store, &e4, &applied) == QLP_OK && !applied);

    /* tombstone wins and deletes */
    qsw_envelope e5 = mk2(idB, 6, "k1", "", 0x1, &b5, &bl5);
    assert(ql_persist_merge_blob(store, &e5, &applied) == QLP_OK && applied);
    assert(ql_persist_get(store, "k1", 2, buf, sizeof buf, &len) == QLP_E_IO);

    /* fresh key */
    qsw_envelope e6 = mk2(idA, 1, "k2", "hello", 0, &b6, &bl6);
    int rc_e6 = ql_persist_merge_blob(store, &e6, &applied);
    if (!(rc_e6 == QLP_OK && applied)) {
        fprintf(stderr, "merge k2 failed rc=%d applied=%d\n", rc_e6, (int)applied);
    }
    assert(rc_e6 == QLP_OK && applied);
    assert(ql_persist_get(store, "k2", 2, buf, sizeof buf, &len) == QLP_OK);
    assert(len == 5 && !memcmp(buf, "hello", 5));

    /* reject: old schema (v1) must fail closed */
    {
        qsw_envelope bad = {0};
        memcpy(bad.node_id, idA, QSW_NODE_ID_LEN);
        bad.lamport = 9; bad.flags = 0; bad.schema_ver = 1;
        bad.key = "k3"; bad.key_len = 2;
        const char *raw = "raw";
        bad.blob = raw; bad.blob_len = 3;
        assert(ql_persist_merge_blob(store, &bad, &applied) == QLP_E_ARG);
    }
    /* reject: reserved key prefix (env/) */
    {
        uint8_t *bx=NULL; uint32_t bl=0;
        qsw_envelope e = mk2(idA, 10, "env/SECRET", "nope", 0, &bx, &bl);
        assert(ql_persist_merge_blob(store, &e, &applied) == QLP_E_ARG);
        free(bx);
    }
    /* reject: open scope (0) */
    {
        uint32_t bl=0; uint8_t *b = build_v2_payload("x", 0, 1, 1, &bl);
        qsw_envelope e = {0};
        memcpy(e.node_id, idA, QSW_NODE_ID_LEN);
        e.lamport = 11; e.flags = 0; e.schema_ver = 2;
        e.key = "k4"; e.key_len = 2; e.blob = b; e.blob_len = bl;
        assert(ql_persist_merge_blob(store, &e, &applied) == QLP_E_ARG);
        free(b);
    }
    /* reject: zero bound */
    {
        uint32_t bl=0; uint8_t *b = build_v2_payload("x", 1, 1, 0, &bl);
        qsw_envelope e = {0};
        memcpy(e.node_id, idA, QSW_NODE_ID_LEN);
        e.lamport = 12; e.flags = 0; e.schema_ver = 2;
        e.key = "k5"; e.key_len = 2; e.blob = b; e.blob_len = bl;
        assert(ql_persist_merge_blob(store, &e, &applied) == QLP_E_ARG);
        free(b);
    }
    /* reject: malformed payload length mismatch */
    {
        uint32_t bl=0; uint8_t *b = build_v2_payload("ok", 1, 1, 1, &bl);
        /* corrupt data_len (at offset 20) */
        b[20] ^= 0x1;
        qsw_envelope e = {0};
        memcpy(e.node_id, idA, QSW_NODE_ID_LEN);
        e.lamport = 13; e.flags = 0; e.schema_ver = 2;
        e.key = "k6"; e.key_len = 2; e.blob = b; e.blob_len = bl;
        assert(ql_persist_merge_blob(store, &e, &applied) == QLP_E_ARG);
        free(b);
    }

    ql_persist_close(store);
    free(b1); free(b2); free(b3); free(b4); free(b5); free(b6);
    puts("persist_lmdb: all tests passed (LWW merge, tie-break, tombstone, get)");
    return 0;
}
