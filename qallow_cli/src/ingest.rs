//! `qallow ingest <store_dir> <frame_file>`: the real half of DUCTEI's
//! Qallow ingestion seam (see ductei-qallow/src/ingest.rs's `QallowSink`
//! trait on the DUCTEI side). Reads a byte stream of QSW frames written
//! by ductei-qallow-relay, decodes each with the C wire decoder
//! (qsw_decode, sync_wire.c -- the same decoder the conformance job
//! checks byte-for-byte against DUCTEI's Rust encoder), and merges
//! ENVELOPE frames into the real LMDB-backed store via
//! ql_persist_merge_blob().
//!
//! Reject-before-write (Qallow governance): a malformed frame anywhere
//! in the stream aborts the whole ingest with a plain-English error and
//! a nonzero exit before any further merge is attempted. Frames already
//! merged earlier in the same call stay merged -- last-writer-wins is
//! defined per-key, not per-batch, so there is nothing to roll back.
use std::ffi::CString;
use std::fs;
use std::os::raw::{c_char, c_void};
use std::path::Path;

const QSW_F_ENVELOPE: u8 = 3;
const QSW_F_HELLO: u8 = 1;
const QSW_F_HELLO_ACK: u8 = 2;
const QSW_NEED_MORE: i32 = 1;
const QSW_OK: i32 = 0;
/// Mirrors QSW_MAX_SCOPES in qallow/sync_wire.h (v2, Task 5).
const QSW_MAX_SCOPES: usize = 16;

#[repr(C)]
#[derive(Clone, Copy)]
struct QswHello {
    magic: u32,
    proto_ver: u16,
    caps: u16,
    node_id: [u8; 16],
    lamport: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct QswEnvelope {
    node_id: [u8; 16],
    lamport: u64,
    schema_ver: u16,
    flags: u16,
    key_len: u32,
    blob_len: u32,
    key: *const c_void,
    blob: *const c_void,
}

#[repr(C)]
#[derive(Clone, Copy)]
union QswFrameUnion {
    hello: QswHello,
    env: QswEnvelope,
    lamport: u64,
}

#[repr(C)]
struct QswFrame {
    frame_type: u8,
    u: QswFrameUnion,
}

/* ---- QSW proto v2 (Task 5): mirrors qsw_envelope_v2 / qsw_frame_v2 in
 * include/qallow/sync_wire.h. repr(C) so the layouts match exactly,
 * including the alignment padding inside each borrowed qsw_scope. ---- */
#[repr(C)]
#[derive(Clone, Copy)]
struct QswScope {
    len: u16,
    _pad: [u8; 6],
    data: *const c_void,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct QswEnvelopeV2 {
    node_id: [u8; 16],
    lamport: u64,
    schema_ver: u16,
    flags: u16,
    scope_count: u16,
    scopes: [QswScope; QSW_MAX_SCOPES],
    key_len: u32,
    blob_len: u32,
    key: *const c_void,
    blob: *const c_void,
}

#[repr(C)]
#[derive(Clone, Copy)]
union QswFrameV2Union {
    hello: QswHello,
    env: QswEnvelopeV2,
    lamport: u64,
}

#[repr(C)]
struct QswFrameV2 {
    frame_type: u8,
    u: QswFrameV2Union,
}

#[repr(C)]
struct QlPersistStore {
    _private: [u8; 0],
}

extern "C" {
    fn qsw_decode(
        buf: *const u8,
        len: usize,
        frame: *mut QswFrame,
        consumed: *mut usize,
    ) -> i32;

    fn qsw_decode_v2(
        buf: *const u8,
        len: usize,
        frame: *mut QswFrameV2,
        consumed: *mut usize,
    ) -> i32;

    fn qsw_v2_merge_key(
        env: *const QswEnvelopeV2,
        out: *mut u8,
        cap: usize,
    ) -> i32;

    fn qsw_encode_envelope(out: *mut u8, cap: usize, env: *const QswEnvelope) -> i32;
    fn qsw_encode_envelope_v2(
        out: *mut u8,
        cap: usize,
        env: *const QswEnvelopeV2,
    ) -> i32;

    fn ql_persist_open(dir_path: *const c_char, out: *mut *mut QlPersistStore) -> i32;
    fn ql_persist_close(store: *mut QlPersistStore);
    fn ql_persist_merge_blob(
        store: *mut QlPersistStore,
        env: *const QswEnvelope,
        out_applied: *mut bool,
    ) -> i32;
    fn ql_persist_get(
        store: *mut QlPersistStore,
        key: *const c_void,
        key_len: u32,
        out_val: *mut c_void,
        cap: u32,
        out_len: *mut u32,
    ) -> i32;
}

const QLP_E_IO: i32 = -4;

/// Best-effort readable rendering of a borrowed key/blob pointer pair
/// for the witness log -- falls back to a byte count if not valid UTF-8.
unsafe fn describe(ptr: *const c_void, len: u32) -> String {
    let bytes = std::slice::from_raw_parts(ptr as *const u8, len as usize);
    match std::str::from_utf8(bytes) {
        Ok(s) => format!("\"{s}\""),
        Err(_) => format!("<{len} bytes>"),
    }
}

fn open_store(store_dir: &str) -> Result<*mut QlPersistStore, String> {
    fs::create_dir_all(store_dir)
        .map_err(|e| format!("could not create store dir {store_dir}: {e}"))?;
    let store_dir_c = CString::new(store_dir)
        .map_err(|_| "store_dir contains an interior NUL byte".to_string())?;
    let mut store: *mut QlPersistStore = std::ptr::null_mut();
    let rc = unsafe { ql_persist_open(store_dir_c.as_ptr(), &mut store) };
    if rc != 0 {
        return Err(format!("ql_persist_open({store_dir}) failed: qlp_status={rc}"));
    }
    Ok(store)
}

pub fn run(store_dir: &str, frame_file: &str) -> Result<(), String> {
    let store = open_store(store_dir)?;
    let result = ingest_frames(store, frame_file);
    unsafe { ql_persist_close(store) };
    result
}

/// `qallow get <store_dir> <key>`: reads a key back out of the real LMDB
/// store. Prints `FOUND:<value>` (value rendered as UTF-8 if valid, else
/// hex) or `NOT_FOUND` -- both are successful, informational outcomes;
/// only a store-open failure is an error.
pub fn run_get(store_dir: &str, key: &str) -> Result<(), String> {
    let store = open_store(store_dir)?;
    let key_bytes = key.as_bytes();
    let mut buf = vec![0u8; 1 << 20];
    let mut out_len: u32 = 0;
    let rc = unsafe {
        ql_persist_get(
            store,
            key_bytes.as_ptr() as *const c_void,
            key_bytes.len() as u32,
            buf.as_mut_ptr() as *mut c_void,
            buf.len() as u32,
            &mut out_len,
        )
    };
    unsafe { ql_persist_close(store) };

    if rc == QLP_E_IO {
        println!("NOT_FOUND");
        return Ok(());
    }
    if rc != 0 {
        return Err(format!("ql_persist_get failed: qlp_status={rc}"));
    }
    buf.truncate(out_len as usize);
    match std::str::from_utf8(&buf) {
        Ok(s) => println!("FOUND:{s}"),
        Err(_) => println!("FOUND:hex:{}", buf.iter().map(|b| format!("{b:02x}")).collect::<String>()),
    }
    Ok(())
}

/// Rebuilds the v1 merge key from a decoded v2 envelope via the C
/// oracle (`qsw_v2_merge_key`): scopes sorted bytewise, joined with
/// ",", then "|" + key -- the exact string v1's shim produces
/// (ductei-qallow/src/lib.rs `encode_envelope`). This keeps the LMDB
/// merge key identical regardless of which proto version carried the
/// record.
unsafe fn merge_key_v2(env: &QswEnvelopeV2) -> Result<Vec<u8>, String> {
    let mut out = vec![0u8; 1024]; /* QSW_MAX_KEY_LEN */
    let n = unsafe { qsw_v2_merge_key(env, out.as_mut_ptr(), out.len()) };
    if n < 0 {
        return Err(format!(
            "v2 merge-key rebuild failed: qsw_status={n} -- nothing merged"
        ));
    }
    out.truncate(n as usize);
    Ok(out)
}

/// Merge path for a v1 ENVELOPE frame (key already carries the shim).
unsafe fn merge_v1(
    store: *mut QlPersistStore,
    offset: usize,
    env: QswEnvelope,
) -> Result<(), String> {
    let mut applied = false;
    let rc = unsafe { ql_persist_merge_blob(store, &env, &mut applied) };
    if rc != 0 {
        return Err(format!(
            "ql_persist_merge_blob failed at byte offset {offset}: qlp_status={rc} -- nothing further merged"
        ));
    }
    let key_desc = unsafe { describe(env.key, env.key_len) };
    println!(
        "ingest: key={key_desc} lamport={} applied={applied}",
        env.lamport
    );
    Ok(())
}

/// Merge path for a v2 ENVELOPE frame: rebuild the v1 shim key, then
/// merge under it. Reject-before-write holds: a merge-key failure (or a
/// gate rejection inside ql_persist_merge_blob) aborts before any write.
unsafe fn merge_v2(
    store: *mut QlPersistStore,
    offset: usize,
    frame: &QswFrameV2,
) -> Result<(), String> {
    let env = frame.u.env;
    let merge_key = unsafe { merge_key_v2(&env)? };
    let scopes_desc: Vec<String> = env
        .scopes
        .iter()
        .take(env.scope_count as usize)
        .map(|s| {
            let bytes =
                std::slice::from_raw_parts(s.data as *const u8, s.len as usize);
            String::from_utf8_lossy(bytes).into_owned()
        })
        .collect();
    let shim = QswEnvelope {
        node_id: env.node_id,
        lamport: env.lamport,
        schema_ver: env.schema_ver,
        flags: env.flags,
        key_len: merge_key.len() as u32,
        blob_len: env.blob_len,
        key: merge_key.as_ptr() as *const c_void,
        blob: env.blob,
    };
    let mut applied = false;
    let rc = unsafe { ql_persist_merge_blob(store, &shim, &mut applied) };
    if rc != 0 {
        return Err(format!(
            "ql_persist_merge_blob failed at byte offset {offset}: qlp_status={rc} -- nothing further merged"
        ));
    }
    println!(
        "ingest: key=\"{}\" (v2 scopes: [{}]) lamport={} applied={applied}",
        String::from_utf8_lossy(&merge_key),
        scopes_desc.join(", "),
        env.lamport,
    );
    Ok(())
}

fn ingest_frames(store: *mut QlPersistStore, frame_file: &str) -> Result<(), String> {
    let path = Path::new(frame_file);
    let buf = fs::read(path).map_err(|e| format!("could not read {frame_file}: {e}"))?;

    let mut offset = 0usize;
    let mut merged = 0u32;
    let mut skipped = 0u32;

    // Proto negotiation (Task 5). The relay writes ENVELOPE-only frame
    // files (no HELLO), so the version is not always declared up front.
    // Try v1 first; on the first v1 MALFORMED, offer the same window to
    // the v2 decoder -- if it decodes, the whole stream is v2 (the two
    // formats are never mixed within a stream). A stream that starts
    // with HELLO is negotiated by its proto_ver field.
    let mut use_v2 = false;
    let mut v2_detected = false;

    while offset < buf.len() {
        if !use_v2 {
            let mut frame: QswFrame = unsafe { std::mem::zeroed() };
            let mut consumed: usize = 0;
            let status = unsafe {
                qsw_decode(
                    buf[offset..].as_ptr(),
                    buf.len() - offset,
                    &mut frame,
                    &mut consumed,
                )
            };

            if status == QSW_NEED_MORE {
                return Err(format!(
                    "truncated frame at byte offset {offset} ({} bytes remaining, not enough for a full frame) -- nothing further merged",
                    buf.len() - offset
                ));
            }
            if status < 0 {
                let mut frame2: QswFrameV2 = unsafe { std::mem::zeroed() };
                let mut consumed2: usize = 0;
                let st2 = unsafe {
                    qsw_decode_v2(
                        buf[offset..].as_ptr(),
                        buf.len() - offset,
                        &mut frame2,
                        &mut consumed2,
                    )
                };
                if st2 == QSW_OK {
                    if consumed2 == 0 {
                        return Err(format!(
                            "decoder made no progress at byte offset {offset}"
                        ));
                    }
                    use_v2 = true;
                    v2_detected = true;
                    if frame2.frame_type == QSW_F_ENVELOPE {
                        unsafe { merge_v2(store, offset, &frame2)? };
                        merged += 1;
                    } else {
                        println!(
                            "ingest: skipping non-envelope frame type {}",
                            frame2.frame_type
                        );
                        skipped += 1;
                    }
                    offset += consumed2;
                    continue;
                }
                return Err(format!(
                    "malformed frame at byte offset {offset}: qsw_status(v1)={status}, qsw_status(v2)={st2} -- nothing further merged"
                ));
            }
            if consumed == 0 {
                // Decoder made no progress on an OK status: refuse to spin.
                return Err(format!("decoder made no progress at byte offset {offset}"));
            }

            if frame.frame_type == QSW_F_HELLO || frame.frame_type == QSW_F_HELLO_ACK {
                if unsafe { frame.u.hello.proto_ver } == 2 {
                    // Declared v2 handshake: restart the stream under
                    // the v2 decoder. Nothing merges on HELLO frames,
                    // so restarting from 0 is safe.
                    use_v2 = true;
                    v2_detected = true;
                    offset = 0;
                    continue;
                }
            }

            if frame.frame_type == QSW_F_ENVELOPE {
                unsafe { merge_v1(store, offset, frame.u.env)? };
                merged += 1;
            } else {
                println!(
                    "ingest: skipping non-envelope frame type {}",
                    frame.frame_type
                );
                skipped += 1;
            }
            offset += consumed;
        } else {
            let mut frame2: QswFrameV2 = unsafe { std::mem::zeroed() };
            let mut consumed2: usize = 0;
            let status = unsafe {
                qsw_decode_v2(
                    buf[offset..].as_ptr(),
                    buf.len() - offset,
                    &mut frame2,
                    &mut consumed2,
                )
            };
            if status == QSW_NEED_MORE {
                return Err(format!(
                    "truncated v2 frame at byte offset {offset} ({} bytes remaining, not enough for a full frame) -- nothing further merged",
                    buf.len() - offset
                ));
            }
            if status < 0 {
                return Err(format!(
                    "malformed v2 frame at byte offset {offset}: qsw_status={status} -- nothing further merged"
                ));
            }
            if consumed2 == 0 {
                return Err(format!("decoder made no progress at byte offset {offset}"));
            }
            if frame2.frame_type == QSW_F_ENVELOPE {
                unsafe { merge_v2(store, offset, &frame2)? };
                merged += 1;
            } else {
                println!(
                    "ingest: skipping non-envelope frame type {}",
                    frame2.frame_type
                );
                skipped += 1;
            }
            offset += consumed2;
        }
    }

    let version_note = if v2_detected { " [proto v2]" } else { "" };
    println!(
        "ingest: {merged} envelope(s) merged, {skipped} frame(s) skipped, {offset} bytes read{version_note}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn persist_v2_blob(data: &[u8]) -> Vec<u8> {
        let mut b = Vec::with_capacity(24 + data.len());
        b.extend_from_slice(&2u16.to_le_bytes());
        b.extend_from_slice(&1u16.to_le_bytes());
        b.extend_from_slice(&123u64.to_le_bytes());
        b.extend_from_slice(&456u64.to_le_bytes());
        b.extend_from_slice(&(data.len() as u32).to_le_bytes());
        b.extend_from_slice(data);
        b
    }

    fn scratch(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "qallow-task5-{}-{}-{}",
            name,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).expect("scratch dir");
        p
    }

    fn get_raw(store_dir: &str, key: &[u8]) -> Option<Vec<u8>> {
        let store = open_store(store_dir).expect("open");
        let mut buf = vec![0u8; 1 << 16];
        let mut out_len: u32 = 0;
        let rc = unsafe {
            ql_persist_get(
                store,
                key.as_ptr() as *const c_void,
                key.len() as u32,
                buf.as_mut_ptr() as *mut c_void,
                buf.len() as u32,
                &mut out_len,
            )
        };
        unsafe { ql_persist_close(store) };
        if rc == QLP_E_IO {
            return None;
        }
        assert_eq!(rc, 0, "ql_persist_get qlp_status={rc}");
        buf.truncate(out_len as usize);
        Some(buf)
    }

    #[test]
    fn v2_merge_key_matches_v1_shim() {
        let key = b"limen.cert.j1";
        let s0 = b"veyn,x";
        let s1 = b"qallow.semantic.cert";
        let mut env: QswEnvelopeV2 = unsafe { std::mem::zeroed() };
        env.node_id = [0xAA; 16];
        env.scope_count = 2;
        env.scopes[0] = QswScope {
            len: s0.len() as u16,
            _pad: [0; 6],
            data: s0.as_ptr() as *const c_void,
        };
        env.scopes[1] = QswScope {
            len: s1.len() as u16,
            _pad: [0; 6],
            data: s1.as_ptr() as *const c_void,
        };
        env.key_len = key.len() as u32;
        env.key = key.as_ptr() as *const c_void;
        let shim = unsafe { merge_key_v2(&env) }.expect("shim");
        assert_eq!(shim, b"qallow.semantic.cert,veyn,x|limen.cert.j1");
    }

    #[test]
    fn v1_and_v2_ingest_share_lmdb_key() {
        let blob = persist_v2_blob(b"from-v1");
        let shim = b"qallow.semantic.cert,veyn,x|limen.cert.j1";
        let node = [0xAAu8; 16];

        let v1_env = QswEnvelope {
            node_id: node,
            lamport: 20,
            schema_ver: 2,
            flags: 0,
            key_len: shim.len() as u32,
            blob_len: blob.len() as u32,
            key: shim.as_ptr() as *const c_void,
            blob: blob.as_ptr() as *const c_void,
        };
        let mut v1_buf = vec![0u8; 4096];
        let v1_n = unsafe {
            qsw_encode_envelope(v1_buf.as_mut_ptr(), v1_buf.len(), &v1_env)
        };
        assert!(v1_n > 0);
        v1_buf.truncate(v1_n as usize);

        let blob2 = persist_v2_blob(b"from-v2");
        let key = b"limen.cert.j1";
        let s0 = b"veyn,x";
        let s1 = b"qallow.semantic.cert";
        let mut v2_env: QswEnvelopeV2 = unsafe { std::mem::zeroed() };
        v2_env.node_id = [0xBB; 16];
        v2_env.lamport = 21;
        v2_env.schema_ver = 2;
        v2_env.scope_count = 2;
        v2_env.scopes[0] = QswScope {
            len: s0.len() as u16,
            _pad: [0; 6],
            data: s0.as_ptr() as *const c_void,
        };
        v2_env.scopes[1] = QswScope {
            len: s1.len() as u16,
            _pad: [0; 6],
            data: s1.as_ptr() as *const c_void,
        };
        v2_env.key_len = key.len() as u32;
        v2_env.key = key.as_ptr() as *const c_void;
        v2_env.blob_len = blob2.len() as u32;
        v2_env.blob = blob2.as_ptr() as *const c_void;
        let mut v2_buf = vec![0u8; 4096];
        let v2_n = unsafe {
            qsw_encode_envelope_v2(v2_buf.as_mut_ptr(), v2_buf.len(), &v2_env)
        };
        assert!(v2_n > 0);
        v2_buf.truncate(v2_n as usize);

        let dir_v1 = scratch("v1");
        let frames_v1 = dir_v1.join("frames.qsw");
        fs::write(&frames_v1, &v1_buf).unwrap();
        run(dir_v1.to_str().unwrap(), frames_v1.to_str().unwrap()).unwrap();
        assert_eq!(
            get_raw(dir_v1.to_str().unwrap(), shim).as_deref(),
            Some(b"from-v1".as_ref())
        );

        let dir_v2 = scratch("v2");
        let frames_v2 = dir_v2.join("frames.qsw");
        fs::write(&frames_v2, &v2_buf).unwrap();
        run(dir_v2.to_str().unwrap(), frames_v2.to_str().unwrap()).unwrap();
        assert_eq!(
            get_raw(dir_v2.to_str().unwrap(), shim).as_deref(),
            Some(b"from-v2".as_ref())
        );

        /* malformed v2: corrupt scope_len, reject before any write. */
        let dir_bad = scratch("bad");
        let mut bad = v2_buf.clone();
        /* body starts at offset 5; scope[0] len is at 5+30 */
        bad[5 + 30] = 0xFF;
        bad[5 + 31] = 0xFF;
        let frames_bad = dir_bad.join("frames.qsw");
        fs::write(&frames_bad, &bad).unwrap();
        let err = run(dir_bad.to_str().unwrap(), frames_bad.to_str().unwrap())
            .expect_err("malformed v2 must fail closed");
        assert!(err.contains("malformed"), "{err}");
        assert!(get_raw(dir_bad.to_str().unwrap(), shim).is_none());

        let _ = fs::remove_dir_all(&dir_v1);
        let _ = fs::remove_dir_all(&dir_v2);
        let _ = fs::remove_dir_all(&dir_bad);
    }
}
