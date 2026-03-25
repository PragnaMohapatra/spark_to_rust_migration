use sha2::{Digest, Sha256};

pub struct AnonymizedColumns {
    pub sender_hash: String,
    pub receiver_hash: String,
    pub sender_account_masked: String,
    pub receiver_account_masked: String,
    pub ip_anonymized: String,
}

/// Spark parity: add_anonymized_columns
pub fn apply(
    sender_name: &Option<String>,
    receiver_name: &Option<String>,
    sender_account: &Option<String>,
    receiver_account: &Option<String>,
    ip_address: &Option<String>,
) -> AnonymizedColumns {
    AnonymizedColumns {
        sender_hash: sha256_hex(sender_name.as_deref().unwrap_or("")),
        receiver_hash: sha256_hex(receiver_name.as_deref().unwrap_or("")),
        sender_account_masked: mask_account(sender_account),
        receiver_account_masked: mask_account(receiver_account),
        ip_anonymized: anonymize_ip(ip_address),
    }
}

/// SHA-256 hex digest — same as Spark's sha2(col, 256)
fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Replicate Spark UDF mask_account:
/// null or < 4 chars → "****"
/// otherwise → "****...****XXXX" (mask all but last 4)
fn mask_account(acct: &Option<String>) -> String {
    match acct {
        None => "****".into(),
        Some(s) if s.len() < 4 => "****".into(),
        Some(s) => {
            let mask_len = s.len() - 4;
            format!("{}{}", "*".repeat(mask_len), &s[mask_len..])
        }
    }
}

/// Replicate Spark's regexp_replace(ip_address, r"\.\d+$", ".xxx")
fn anonymize_ip(ip: &Option<String>) -> String {
    match ip {
        None => String::new(),
        Some(addr) => {
            // Replace last octet: "192.168.1.100" → "192.168.1.xxx"
            if let Some(idx) = addr.rfind('.') {
                format!("{}.xxx", &addr[..idx])
            } else {
                addr.clone()
            }
        }
    }
}
