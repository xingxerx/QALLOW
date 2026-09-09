/* tests/test_sync_wire.c
 * Two in-process nodes, separate in-memory stores (stand-in for two
 * LMDB dirs until Task 0 lands on remote). Node A streams envelopes
 * to node B over a byte pipe; B merges last-writer-wins by Lamport,
 * origin node id as tiebreak — matching ql_persist_merge_blob semantics.
 * Also exercises partial-read decoding (1-byte drip feed).
 */
#include "qallow/sync_wire.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

#define STORE_MAX 16
typedef struct {
    char     key[64];
    char     val[128];
    uint64_t lamport;
    uint8_t  origin[QSW_NODE_ID_LEN];
    int      used;
} rec;
typedef struct { rec r[STORE_MAX]; uint64_t clock; } store;

static int node_cmp(const uint8_t *a, const uint8_t *b) {
    return memcmp(a, b, QSW_NODE_ID_LEN);
}
/* LWW merge: higher lamport wins; tie -> higher node id wins */
static void store_merge(store *s, const qsw_envelope *e) {
    if (e->lamport > s->clock) s->clock = e->lamport; /* Lamport recv rule */
    for (int i = 0; i < STORE_MAX; i++) {
        rec *r = &s->r[i];
        if (r->used && strlen(r->key) == e->key_len &&
            !memcmp(r->key, e->key, e->key_len)) {
            if (e->lamport < r->lamport) return;
            if (e->lamport == r->lamport &&
                node_cmp(e->node_id, r->origin) <= 0) return;
            memcpy(r->val, e->blob, e->blob_len); r->val[e->blob_len] = 0;
            r->lamport = e->lamport;
            memcpy(r->origin, e->node_id, QSW_NODE_ID_LEN);
            return;
        }
    }
    for (int i = 0; i < STORE_MAX; i++) {
        rec *r = &s->r[i];
        if (!r->used) {
            memcpy(r->key, e->key, e->key_len); r->key[e->key_len] = 0;
            memcpy(r->val, e->blob, e->blob_len); r->val[e->blob_len] = 0;
            r->lamport = e->lamport;
            memcpy(r->origin, e->node_id, QSW_NODE_ID_LEN);
            r->used = 1;
            return;
        }
    }
    assert(0 && "store full");
}
static const char *store_get(store *s, const char *k) {
    for (int i = 0; i < STORE_MAX; i++)
        if (s->r[i].used && !strcmp(s->r[i].key, k)) return s->r[i].val;
    return NULL;
}

static int32_t enc_env(uint8_t *out, size_t cap,
                       const uint8_t id[QSW_NODE_ID_LEN], uint64_t lam,
                       const char *k, const char *v) {
    qsw_envelope e = {0};
    memcpy(e.node_id, id, QSW_NODE_ID_LEN);
    e.lamport = lam; e.schema_ver = 1;
    e.key = k;  e.key_len  = (uint32_t)strlen(k);
    e.blob = v; e.blob_len = (uint32_t)strlen(v);
    return qsw_encode_envelope(out, cap, &e);
}

/* LE readers for wire-layout spot checks (mirror sync_wire.c) */
static uint16_t get16le(const uint8_t *p) {
    return (uint16_t)(p[0] | (uint16_t)p[1] << 8);
}
static uint32_t get32le(const uint8_t *p) {
    return (uint32_t)p[0]       | (uint32_t)p[1] << 8 |
           (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}

/* v2 negative + handshake tests (Task 5). buf2/w2 is a valid encoded
 * v2 ENVELOPE frame to mutate. */
static void v2_negative_tests(const uint8_t id[QSW_NODE_ID_LEN],
                              const uint8_t *buf2, size_t w2) {
    qsw_frame_v2 fbad; size_t ubad;

    /* HELLO v2: proto-version separation between validators. */
    uint8_t h2[64];
    int32_t hw = qsw_encode_hello_v2(h2, sizeof h2, QSW_F_HELLO, id, 4);
    assert(hw > 0);
    assert(qsw_decode_v2(h2, (size_t)hw, &fbad, &ubad) == QSW_OK);
    assert(fbad.u.hello.proto_ver == QSW_PROTO_VER_V2);
    assert(qsw_hello_validate_v2(&fbad.u.hello) == QSW_OK);
    assert(qsw_hello_validate(&fbad.u.hello) == QSW_E_VERSION);
    /* and the v1 decoder accepts the frame shape but rejects the version */
    {
        qsw_frame f1; size_t u1;
        assert(qsw_decode(h2, (size_t)hw, &f1, &u1) == QSW_OK);
        assert(qsw_hello_validate(&f1.u.hello) == QSW_E_VERSION);
    }

    /* scope_len past the body */
    {
        uint8_t m[512]; memcpy(m, buf2, w2);
        m[5 + 30] = 0xFF; m[5 + 31] = 0xFF;
        assert(qsw_decode_v2(m, w2, &fbad, &ubad) == QSW_E_MALFORMED);
    }
    /* scope_count past the C cap (0xFFFF on the wire) */
    {
        uint8_t m[512]; memcpy(m, buf2, w2);
        m[5 + 28] = 0xFF; m[5 + 29] = 0xFF;
        assert(qsw_decode_v2(m, w2, &fbad, &ubad) == QSW_E_MALFORMED);
    }
    /* key/blob lengths no longer sum to the body (exact-length rule) */
    {
        uint8_t m[512]; memcpy(m, buf2, w2);
        size_t blob_len_off = 5 + 30 + (2 + 20) + (2 + 6) + (2 + 15) + 4;
        m[blob_len_off] ^= 0x1;
        assert(qsw_decode_v2(m, w2, &fbad, &ubad) == QSW_E_MALFORMED);
    }
    /* a v1 stream fed to the v2 decoder is rejected, not guessed */
    {
        uint8_t v1buf[256];
        int32_t v1w = enc_env(v1buf, sizeof v1buf, id, 8,
                              "qallow.semantic.cert|k", "v");
        assert(v1w > 0);
        qsw_status vs = qsw_decode_v2(v1buf, (size_t)v1w, &fbad, &ubad);
        assert(vs != QSW_OK && vs != QSW_NEED_MORE);
    }
    /* encoder arg checks */
    {
        uint8_t out[512];
        qsw_envelope_v2 bad = {0};
        bad.scope_count = QSW_MAX_SCOPES + 1;
        assert(qsw_encode_envelope_v2(out, sizeof out, &bad) == QSW_E_ARG);
        bad.scope_count = 1;
        bad.scopes[0].len = QSW_MAX_SCOPE_LEN + 1;
        bad.scopes[0].data = "x";
        assert(qsw_encode_envelope_v2(out, sizeof out, &bad) == QSW_E_ARG);
    }

    /* full v2 stream: HELLO, ENVELOPE, BATCH_END, BYE, drip-fed */
    {
        uint8_t stream[1024]; size_t sn = 0; int32_t sw;
        sw = qsw_encode_hello_v2(stream + sn, sizeof stream - sn,
                                 QSW_F_HELLO, id, 8);
        assert(sw > 0); sn += (size_t)sw;
        memcpy(stream + sn, buf2, w2); sn += w2;
        sw = qsw_encode_batch_end(stream + sn, sizeof stream - sn, 9);
        assert(sw > 0); sn += (size_t)sw;
        sw = qsw_encode_bye(stream + sn, sizeof stream - sn);
        assert(sw > 0); sn += (size_t)sw;

        size_t off = 0; int frames = 0, got_env = 0, got_bye = 0;
        for (size_t i = 1; i <= sn; i++) {
            for (;;) {
                qsw_frame_v2 f; size_t used;
                qsw_status s = qsw_decode_v2(stream + off, i - off, &f, &used);
                if (s == QSW_NEED_MORE) break;
                assert(s == QSW_OK);
                frames++;
                if (f.type == QSW_F_ENVELOPE) got_env = 1;
                if (f.type == QSW_F_BYE) got_bye = 1;
                if (f.type == QSW_F_HELLO)
                    assert(qsw_hello_validate_v2(&f.u.hello) == QSW_OK);
                off += used;
            }
        }
        assert(frames == 4 && got_env && got_bye && off == sn);
    }
}

int main(void) {
    uint8_t idA[QSW_NODE_ID_LEN], idB[QSW_NODE_ID_LEN];
    memset(idA, 0xAA, sizeof idA);
    memset(idB, 0xBB, sizeof idB);
    store A = {0}, B = {0};
    A.clock = 7; B.clock = 3;

    /* --- pipe: A -> B ------------------------------------------ */
    uint8_t pipe[4096]; size_t n = 0; int32_t w;

    w = qsw_encode_hello(pipe + n, sizeof pipe - n, QSW_F_HELLO, idA, A.clock);
    assert(w > 0); n += (size_t)w;
    w = enc_env(pipe + n, sizeof pipe - n, idA, 5, "dream/41", "lucid:false");
    assert(w > 0); n += (size_t)w;
    w = enc_env(pipe + n, sizeof pipe - n, idA, 7, "phase12/target", "mesh");
    assert(w > 0); n += (size_t)w;
    /* conflict record: B already holds key "shared" at lamport 6 */
    {
        qsw_envelope pre = {0};
        memcpy(pre.node_id, idB, QSW_NODE_ID_LEN);
        pre.lamport = 6; pre.key = "shared"; pre.key_len = 6;
        pre.blob = "from-B"; pre.blob_len = 6;
        store_merge(&B, &pre);
    }
    w = enc_env(pipe + n, sizeof pipe - n, idA, 4, "shared", "stale-from-A");
    assert(w > 0); n += (size_t)w; /* must lose: lamport 4 < 6 */
    w = qsw_encode_batch_end(pipe + n, sizeof pipe - n, A.clock);
    assert(w > 0); n += (size_t)w;
    w = qsw_encode_bye(pipe + n, sizeof pipe - n);
    assert(w > 0); n += (size_t)w;
    /* also include an ACK from B to verify frame acceptance */
    w = qsw_encode_hello(pipe + n, sizeof pipe - n, QSW_F_HELLO_ACK, idB, B.clock);
    assert(w > 0); n += (size_t)w;

    /* --- B decodes with 1-byte drip feed (partial reads) ------- */
    uint8_t rx[4096]; size_t rxn = 0, off = 0;
    int frames = 0, got_hello = 0, got_bye = 0, got_ack = 0;
    for (size_t i = 0; i < n; i++) {
        rx[rxn++] = pipe[i]; /* transport delivers one byte at a time */
        for (;;) {
            qsw_frame f; size_t used;
            qsw_status st = qsw_decode(rx + off, rxn - off, &f, &used);
            if (st == QSW_NEED_MORE) break;
            assert(st == QSW_OK);
            frames++;
            switch (f.type) {
            case QSW_F_HELLO:
                assert(qsw_hello_validate(&f.u.hello) == QSW_OK);
                assert(f.u.hello.lamport == 7);
                got_hello = 1;
                break;
            case QSW_F_HELLO_ACK:
                assert(qsw_hello_validate(&f.u.hello) == QSW_OK);
                assert(f.u.hello.lamport == 6);
                got_ack = 1;
                break;
            case QSW_F_ENVELOPE:
                store_merge(&B, &f.u.env);
                break;
            case QSW_F_BATCH_END:
                assert(f.u.lamport == 7);
                break;
            case QSW_F_BYE:
                got_bye = 1;
                break;
            }
            off += used;
        }
    }
    assert(got_hello && got_bye && got_ack && frames == 7);

    /* --- verify merge results ----------------------------------- */
    assert(!strcmp(store_get(&B, "dream/41"), "lucid:false"));
    assert(!strcmp(store_get(&B, "phase12/target"), "mesh"));
    assert(!strcmp(store_get(&B, "shared"), "from-B")); /* LWW held */
    assert(B.clock >= 7); /* Lamport advanced on receive */

    /* --- malformed input rejected -------------------------------- */
    {
        uint8_t bad[13] = { 99, 8, 0, 0, 0 }; /* unknown type, hdr=5 */
        qsw_frame f; size_t used;
        assert(qsw_decode(bad, sizeof bad, &f, &used) == QSW_E_MALFORMED);
        uint8_t hello[64]; int32_t hl;
        hl = qsw_encode_hello(hello, sizeof hello, QSW_F_HELLO, idA, 1);
        hello[5] ^= 0xFF; /* corrupt magic (body starts after 5-byte hdr) */
        assert(qsw_decode(hello, (size_t)hl, &f, &used) == QSW_OK);
        assert(qsw_hello_validate(&f.u.hello) == QSW_E_MAGIC);
        /* corrupt an ENVELOPE body field so body size no longer matches */
        uint8_t envbuf[256]; int32_t el;
        el = enc_env(envbuf, sizeof envbuf, idA, 8, "k", "v");
        /* flip one bit in key_len (offset: 5 hdr + 16 id + 8 lam + 2 sch + 2 flg) */
        size_t key_len_off = 5 + QSW_NODE_ID_LEN + 8 + 2 + 2;
        envbuf[key_len_off] ^= 0x1;
        assert(qsw_decode(envbuf, (size_t)el, &f, &used) == QSW_E_MALFORMED);
    }

    /* --- v2 (Task 5): multi-scope roundtrip ---------------------- */
    {
        /* Scope order here is deliberately unsorted, and "veyn,x" is
         * the exact corruption class v1's comma shim cannot carry:
         * shim-joined it is indistinguishable from scopes "veyn","x". */
        const char *scopes[3] = { "veyn.sensor.eeg", "qallow.semantic.cert",
                                  "veyn,x" };
        qsw_envelope_v2 e2 = {0};
        memcpy(e2.node_id, idA, QSW_NODE_ID_LEN);
        e2.lamport = 9; e2.schema_ver = 2; e2.flags = 0;
        e2.scope_count = 3;
        for (int i = 0; i < 3; i++) {
            e2.scopes[i].len  = (uint16_t)strlen(scopes[i]);
            e2.scopes[i].data = scopes[i];
        }
        e2.key = "k";  e2.key_len  = 1;
        e2.blob = "v"; e2.blob_len = 1;

        uint8_t buf2[512];
        int32_t w2 = qsw_encode_envelope_v2(buf2, sizeof buf2, &e2);
        assert(w2 > 0);

        /* Wire-layout spot checks. Sorted bytewise, the scopes are
         * "qallow.semantic.cert" (20), "veyn,x" (6), "veyn.sensor.eeg"
         * (15): body = 30 fixed + (2+20)+(2+6)+(2+15) + 8 + key + blob
         *        = 30 + 47 + 8 + 1 + 1 = 87; frame = 92. */
        assert(w2 == 92);
        assert(buf2[0] == QSW_F_ENVELOPE);
        assert(get32le(buf2 + 1) == 87);
        assert(get16le(buf2 + 5 + 28) == 3);          /* scope_count  */
        assert(get16le(buf2 + 5 + 30) == 20);         /* scope[0] len */
        assert(!memcmp(buf2 + 5 + 32, "qallow.semantic.cert", 20));

        /* Drip-feed decode, one byte at a time (partial reads). */
        qsw_frame_v2 f2 = {0}; size_t used2 = 0;
        size_t off2 = 0; int got_env = 0;
        for (size_t i = 1; i <= (size_t)w2 && !got_env; i++) {
            for (;;) {
                qsw_status s = qsw_decode_v2(buf2 + off2, i - off2,
                                             &f2, &used2);
                if (s == QSW_NEED_MORE) break;
                assert(s == QSW_OK);
                got_env = 1;
                off2 += used2;
            }
        }
        assert(got_env && off2 == (size_t)w2);
        assert(f2.type == QSW_F_ENVELOPE);
        assert(f2.u.env.lamport == 9 && f2.u.env.schema_ver == 2);
        assert(f2.u.env.scope_count == 3); /* v1 shim would corrupt this */
        assert(f2.u.env.scopes[0].len == 20 &&
               !memcmp(f2.u.env.scopes[0].data, "qallow.semantic.cert", 20));
        assert(f2.u.env.scopes[1].len == 6 &&
               !memcmp(f2.u.env.scopes[1].data, "veyn,x", 6));
        assert(f2.u.env.scopes[2].len == 15 &&
               !memcmp(f2.u.env.scopes[2].data, "veyn.sensor.eeg", 15));
        assert(f2.u.env.key_len == 1 && !memcmp(f2.u.env.key, "k", 1));
        assert(f2.u.env.blob_len == 1 && !memcmp(f2.u.env.blob, "v", 1));

        /* Merge-key parity: v2 ingest rebuilds the v1 shim key
         * (scopes sorted, joined ",", then "|" + key) so the LMDB key
         * is identical regardless of proto version. */
        {
            char shim[QSW_MAX_SCOPES * QSW_MAX_SCOPE_LEN + 8];
            size_t o = 0;
            for (int i = 0; i < 3; i++) {
                if (i) shim[o++] = ',';
                memcpy(shim + o, f2.u.env.scopes[i].data,
                       f2.u.env.scopes[i].len);
                o += f2.u.env.scopes[i].len;
            }
            shim[o++] = '|';
            memcpy(shim + o, f2.u.env.key, f2.u.env.key_len);
            o += f2.u.env.key_len;
            shim[o] = 0;
            assert(!strcmp(shim,
                           "qallow.semantic.cert,veyn,x,veyn.sensor.eeg|k"));
        }

        /* --- v2 part 2: hello separation, malformed, full stream --- */
        v2_negative_tests(idA, buf2, (size_t)w2);
    }

    puts("sync_wire: all tests passed (handshake, drip-feed decode, "
         "LWW merge, malformed rejection, v2 multi-scope roundtrip)");
    return 0;
}
