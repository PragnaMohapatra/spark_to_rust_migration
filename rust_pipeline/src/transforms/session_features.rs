use sha2::{Digest, Sha256};

pub struct SessionFeatures {
    pub device_short_id: String,
    pub session_prefix: String,
    pub memo_length: i32,
    pub has_memo: bool,
    pub row_id: String,
}

/// Spark parity: add_session_features
pub fn apply(
    device_fingerprint: &Option<String>,
    session_id: &Option<String>,
    memo: &Option<String>,
    transaction_id: &str,
) -> SessionFeatures {
    let device_short_id = device_fingerprint
        .as_deref()
        .map(|s| safe_substring(s, 8))
        .unwrap_or_default();

    let session_prefix = session_id
        .as_deref()
        .map(|s| safe_substring(s, 8))
        .unwrap_or_default();

    let memo_length = memo.as_deref().map(|m| m.len() as i32).unwrap_or(0);
    let has_memo = memo.as_deref().map(|m| !m.is_empty()).unwrap_or(false);

    let mut h = Sha256::new();
    h.update(transaction_id.as_bytes());
    let row_id = format!("{:x}", h.finalize());

    SessionFeatures {
        device_short_id,
        session_prefix,
        memo_length,
        has_memo,
        row_id,
    }
}

fn safe_substring(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}
