pub mod null_handling;
pub mod time_dimensions;
pub mod amount_features;
pub mod risk_features;
pub mod geo_features;
pub mod anonymized;
pub mod session_features;

use crate::schema::{KafkaMeta, TransactionInput, TransactionOutput};

/// Apply all 7 transforms in order, matching Spark's apply_all_transforms.
pub fn apply_all_transforms(input: &TransactionInput, meta: &KafkaMeta) -> TransactionOutput {
    // T1: Null handling
    let nh = null_handling::apply(input);
    // T2: Time dimensions
    let td = time_dimensions::apply(&input.timestamp);
    // T3: Amount features
    let af = amount_features::apply(input.amount, nh.fee, nh.exchange_rate);
    // T4: Risk features
    let rf = risk_features::apply(input.risk_score, input.is_flagged, input.amount);
    // T5: Geo features
    let gf = geo_features::apply(
        &nh.sender_country, &nh.receiver_country,
        &input.sender_bank_code, &input.receiver_bank_code,
    );
    // T6: Anonymized columns
    let anon = anonymized::apply(
        &input.sender_name, &input.receiver_name,
        &input.sender_account, &input.receiver_account,
        &input.ip_address,
    );
    // T7: Session features
    let sf = session_features::apply(
        &input.device_fingerprint, &input.session_id,
        &input.memo, &input.transaction_id,
    );

    TransactionOutput {
        kafka_key: meta.key.clone(),
        transaction_id: input.transaction_id.clone(),
        timestamp: input.timestamp.clone(),
        sender_account_id: input.sender_account_id.clone(),
        receiver_account_id: input.receiver_account_id.clone(),
        sender_account: input.sender_account.clone(),
        receiver_account: input.receiver_account.clone(),
        amount: input.amount,
        currency: input.currency.clone(),
        transaction_type: input.transaction_type.clone(),
        status: input.status.clone(),
        sender_name: input.sender_name.clone(),
        receiver_name: input.receiver_name.clone(),
        sender_bank_code: input.sender_bank_code.clone(),
        receiver_bank_code: input.receiver_bank_code.clone(),
        sender_country: nh.sender_country,
        receiver_country: nh.receiver_country,
        fee: nh.fee,
        exchange_rate: nh.exchange_rate,
        reference_id: input.reference_id.clone(),
        memo: input.memo.clone(),
        risk_score: input.risk_score,
        is_flagged: input.is_flagged,
        category: input.category.clone(),
        channel: input.channel.clone(),
        ip_address: input.ip_address.clone(),
        device_fingerprint: input.device_fingerprint.clone(),
        session_id: input.session_id.clone(),
        topic: meta.topic.clone(),
        kafka_partition: meta.partition,
        kafka_offset: meta.offset,
        kafka_timestamp: meta.timestamp_ms,
        // T1
        currency_safe: nh.currency_safe,
        risk_score_safe: nh.risk_score_safe,
        memo_clean: nh.memo_clean,
        // T2
        transaction_date: td.transaction_date,
        txn_year: td.txn_year,
        txn_month: td.txn_month,
        txn_day: td.txn_day,
        txn_hour: td.txn_hour,
        txn_dayofweek: td.txn_dayofweek,
        is_weekend: td.is_weekend,
        txn_quarter: td.txn_quarter,
        date_str: td.date_str,
        // T3
        amount_bucket: af.amount_bucket,
        net_amount: af.net_amount,
        fee_pct: af.fee_pct,
        amount_usd: af.amount_usd,
        log_amount: af.log_amount,
        // T4
        risk_tier: rf.risk_tier,
        risk_level: rf.risk_level,
        needs_review: rf.needs_review,
        // T5
        is_cross_border: gf.is_cross_border,
        corridor: gf.corridor,
        is_intra_bank: gf.is_intra_bank,
        // T6
        sender_hash: anon.sender_hash,
        receiver_hash: anon.receiver_hash,
        sender_account_masked: anon.sender_account_masked,
        receiver_account_masked: anon.receiver_account_masked,
        ip_anonymized: anon.ip_anonymized,
        // T7
        device_short_id: sf.device_short_id,
        session_prefix: sf.session_prefix,
        memo_length: sf.memo_length,
        has_memo: sf.has_memo,
        row_id: sf.row_id,
    }
}
