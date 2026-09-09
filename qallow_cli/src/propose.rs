//! `qallow propose <store_dir> <key> <spool_dir>`: the mind-to-hands arrow.
//!
//! Reads one record back out of the real LMDB store (the same
//! `ql_persist_get` that `qallow get` uses) and, if it is a REM cue that
//! VEYN delivered through DUCTEI (`veyn.rem_event` scope, value 1.0),
//! writes a LIMEN route request into limend's `spool/pending/`. That is
//! the only way Qallow ever asks LIMEN for compute: from durable state,
//! never from an in-flight event. Nothing persisted, nothing proposed.
//!
//! Governance (ATRIUM AGENTS.md):
//! - Reject-before-write: a missing key, a non-JSON value, or a value
//!   without a numeric `value` field writes nothing and exits non-zero
//!   with a plain-English reason. A well-formed record that is simply
//!   not a REM cue also writes nothing, but exits zero ("no proposal").
//! - Idempotent: the job id is derived from the LMDB key, and a request
//!   already present in pending/, done/, certs/ or failed/ is never
//!   re-proposed (no punitive retries; failed/ is an archive).
//! - Credentials never in payloads: the request is `offline: true` and
//!   carries no token; limend reads QPU credentials from its own
//!   environment only.
//!
//! The QUBO itself is deliberately the smallest real problem LIMEN's
//! offline simulator path certifies (two variables, one coupling). It is
//! the shape of the arrow, not the cognitive engine's objective; the
//! engine's own formulation plugs in here later without changing the
//! spool contract.
use std::ffi::CString;
use std::fs;
use std::os::raw::{c_char, c_void};
use std::path::{Path, PathBuf};

#[repr(C)]
struct QlPersistStore {
    _private: [u8; 0],
}

extern "C" {
    fn ql_persist_open(dir_path: *const c_char, out: *mut *mut QlPersistStore) -> i32;
    fn ql_persist_close(store: *mut QlPersistStore);
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
const REM_SCOPE: &str = "veyn.rem_event";
const SPOOL_DIRS: [&str; 4] = ["pending", "done", "certs", "failed"];

pub const DEFAULT_FIDELITY_TARGET: f64 = 0.9;
pub const DEFAULT_CREDIT_BUDGET: f64 = 10.0;

fn get_value(store_dir: &str, key: &str) -> Result<Option<Vec<u8>>, String> {
    let store_dir_c =
        CString::new(store_dir).map_err(|_| "store_dir contains an interior NUL byte".to_string())?;
    let mut store: *mut QlPersistStore = std::ptr::null_mut();
    let rc = unsafe { ql_persist_open(store_dir_c.as_ptr(), &mut store) };
    if rc != 0 {
        return Err(format!("ql_persist_open({store_dir}) failed: qlp_status={rc}"));
    }
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
        return Ok(None);
    }
    if rc != 0 {
        return Err(format!("ql_persist_get failed: qlp_status={rc}"));
    }
    buf.truncate(out_len as usize);
    Ok(Some(buf))
}

/// Job id derived from the LMDB key: stable, filesystem-safe, bounded.
pub fn job_id_for(key: &str) -> String {
    let body: String = key
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .take(72)
        .collect();
    format!("qallow-{body}")
}

/// The v1 key shim is `scope0,scope1|key`; a REM cue is one whose scope
/// set contains `veyn.rem_event`.
fn is_rem_scope(key: &str) -> bool {
    match key.split_once('|') {
        Some((scopes, _)) => scopes.split(',').any(|s| s == REM_SCOPE),
        None => false,
    }
}

/// Minimal JSON probe: finds a top-level `"value": <number>` without
/// pulling a JSON crate into a binary that otherwise has none.
fn value_field(json: &str) -> Option<f64> {
    let idx = json.find("\"value\"")?;
    let rest = &json[idx + "\"value\"".len()..];
    let rest = rest.trim_start();
    let rest = rest.strip_prefix(':')?.trim_start();
    let end = rest
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '-' || c == '+' || c == 'e' || c == 'E'))
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

fn already_present(spool: &Path, job_id: &str) -> Option<PathBuf> {
    for d in SPOOL_DIRS {
        let p = spool.join(d).join(format!("{job_id}.json"));
        if p.exists() {
            return Some(p);
        }
    }
    let sent = spool.join("certs").join("sent").join(format!("{job_id}.json"));
    if sent.exists() {
        return Some(sent);
    }
    None
}

fn request_json(job_id: &str, origin_key: &str, fidelity_target: f64, credit_budget: f64) -> String {
    let origin = origin_key.replace('\\', "\\\\").replace('"', "\\\"");
    format!(
        concat!(
            "{{\n",
            "  \"job_id\": \"{}\",\n",
            "  \"qubo\": [[[\"a\", \"a\"], -1.0], [[\"b\", \"b\"], -1.0], [[\"a\", \"b\"], 2.0]],\n",
            "  \"fidelity_target\": {},\n",
            "  \"credit_budget\": {},\n",
            "  \"offline\": true,\n",
            "  \"origin\": {{\"store_key\": \"{}\", \"cue\": \"rem_detected\"}}\n",
            "}}\n"
        ),
        job_id, fidelity_target, credit_budget, origin
    )
}

pub fn run(store_dir: &str, key: &str, spool_dir: &str, fidelity_target: f64, credit_budget: f64) -> Result<(), String> {
    let spool = Path::new(spool_dir);
    let pending = spool.join("pending");
    if !pending.is_dir() {
        return Err(format!(
            "spool dir {spool_dir} has no pending/ directory -- is this limend's spool root?"
        ));
    }

    let raw = match get_value(store_dir, key)? {
        Some(v) => v,
        None => {
            return Err(format!(
                "key {key:?} is not persisted in {store_dir} -- nothing proposed (persistence before proposal)"
            ))
        }
    };
    let text = std::str::from_utf8(&raw)
        .map_err(|_| format!("stored value for {key:?} is not UTF-8 JSON -- nothing proposed"))?;
    let value = value_field(text)
        .ok_or_else(|| format!("stored value for {key:?} has no numeric \"value\" field -- nothing proposed"))?;

    if !is_rem_scope(key) || value != 1.0 {
        println!(
            "no proposal: {key} is not a REM cue (scope has {REM_SCOPE}? {}, value={value})",
            is_rem_scope(key)
        );
        return Ok(());
    }

    let job_id = job_id_for(key);
    if let Some(existing) = already_present(spool, &job_id) {
        println!("already proposed: {job_id} present at {}", existing.display());
        return Ok(());
    }

    let body = request_json(&job_id, key, fidelity_target, credit_budget);
    let tmp = pending.join(format!(".{job_id}.json.tmp"));
    let dest = pending.join(format!("{job_id}.json"));
    fs::write(&tmp, body).map_err(|e| format!("write {}: {e}", tmp.display()))?;
    fs::rename(&tmp, &dest).map_err(|e| format!("rename into pending/: {e}"))?;
    println!("proposed: {job_id} -> {} (from REM cue {key})", dest.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rem_scope_detected_only_from_scope_part() {
        assert!(is_rem_scope("veyn.rem_event|veyn.watch.rem_detected.ab"));
        assert!(is_rem_scope("a.b,veyn.rem_event|k"));
        assert!(!is_rem_scope("veyn.hrv|veyn.rem_event"));
        assert!(!is_rem_scope("no-shim"));
    }

    #[test]
    fn value_field_parses_numbers() {
        assert_eq!(value_field(r#"{"value":1.0,"unit":"bool"}"#), Some(1.0));
        assert_eq!(value_field(r#"{"unit":"ms", "value": 55}"#), Some(55.0));
        assert_eq!(value_field(r#"{"unit":"ms"}"#), None);
    }

    #[test]
    fn job_id_is_safe_and_bounded() {
        let id = job_id_for("veyn.rem_event|veyn.watch.rem_detected.0123456789abcdef0123456789abcdef");
        assert!(id.starts_with("qallow-veyn_rem_event_veyn_watch_rem_detected_"));
        assert!(id.len() <= 7 + 72);
        assert!(id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    }

    #[test]
    fn request_is_offline_and_carries_no_credential_fields() {
        let r = request_json("j", "k", 0.9, 10.0);
        assert!(r.contains("\"offline\": true"));
        assert!(!r.contains("token"));
        assert!(!r.contains("api_"));
    }
}
