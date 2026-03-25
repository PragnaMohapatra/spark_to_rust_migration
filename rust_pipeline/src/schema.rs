use deltalake::arrow::array::*;
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use serde::Deserialize;
use std::sync::Arc;

// ── Input record (deserialized JSON from Kafka) ──────────────

#[derive(Debug, Deserialize, Clone)]
pub struct TransactionInput {
    pub transaction_id: String,
    pub timestamp: String,
    pub sender_account_id: Option<String>,
    pub receiver_account_id: Option<String>,
    pub sender_account: Option<String>,
    pub receiver_account: Option<String>,
    pub amount: Option<f64>,
    pub currency: Option<String>,
    pub transaction_type: Option<String>,
    pub status: Option<String>,
    pub sender_name: Option<String>,
    pub receiver_name: Option<String>,
    pub sender_bank_code: Option<String>,
    pub receiver_bank_code: Option<String>,
    pub sender_country: Option<String>,
    pub receiver_country: Option<String>,
    pub fee: Option<f64>,
    pub exchange_rate: Option<f64>,
    pub reference_id: Option<String>,
    pub memo: Option<String>,
    pub risk_score: Option<f64>,
    pub is_flagged: Option<bool>,
    pub category: Option<String>,
    pub channel: Option<String>,
    pub ip_address: Option<String>,
    pub device_fingerprint: Option<String>,
    pub session_id: Option<String>,
}

// ── Account input ────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct AccountInput {
    pub account_id: String,
    pub account_number: Option<String>,
    pub holder_name: Option<String>,
    pub bank_code: Option<String>,
    pub country: Option<String>,
    pub currency: Option<String>,
    pub account_type: Option<String>,
    pub account_status: Option<String>,
    pub balance: Option<f64>,
    pub total_sent: Option<f64>,
    pub total_received: Option<f64>,
    pub transaction_count: Option<i32>,
    pub risk_rating: Option<f64>,
    pub kyc_level: Option<String>,
    pub last_transaction_time: Option<String>,
    pub updated_at: Option<String>,
    pub created_at: Option<String>,
}

// ── Kafka metadata attached to each record ───────────────────

#[derive(Debug, Clone)]
pub struct KafkaMeta {
    pub key: Option<String>,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: i64,
}

// ── Fully-transformed output record ──────────────────────────

#[derive(Debug, Clone)]
pub struct TransactionOutput {
    // Original (with null-fills applied to fee/exchange_rate/sender_country/receiver_country)
    pub kafka_key: Option<String>,
    pub transaction_id: String,
    pub timestamp: String,
    pub sender_account_id: Option<String>,
    pub receiver_account_id: Option<String>,
    pub sender_account: Option<String>,
    pub receiver_account: Option<String>,
    pub amount: Option<f64>,
    pub currency: Option<String>,
    pub transaction_type: Option<String>,
    pub status: Option<String>,
    pub sender_name: Option<String>,
    pub receiver_name: Option<String>,
    pub sender_bank_code: Option<String>,
    pub receiver_bank_code: Option<String>,
    pub sender_country: String,
    pub receiver_country: String,
    pub fee: f64,
    pub exchange_rate: f64,
    pub reference_id: Option<String>,
    pub memo: Option<String>,
    pub risk_score: Option<f64>,
    pub is_flagged: Option<bool>,
    pub category: Option<String>,
    pub channel: Option<String>,
    pub ip_address: Option<String>,
    pub device_fingerprint: Option<String>,
    pub session_id: Option<String>,
    // Kafka metadata
    pub topic: String,
    pub kafka_partition: i32,
    pub kafka_offset: i64,
    pub kafka_timestamp: i64,
    // T1: null handling
    pub currency_safe: String,
    pub risk_score_safe: f64,
    pub memo_clean: String,
    // T2: time dimensions
    pub transaction_date: i32, // days-since-epoch for Arrow Date32
    pub txn_year: i32,
    pub txn_month: i32,
    pub txn_day: i32,
    pub txn_hour: i32,
    pub txn_dayofweek: i32,
    pub is_weekend: bool,
    pub txn_quarter: i32,
    pub date_str: String,
    // T3: amount features
    pub amount_bucket: String,
    pub net_amount: f64,
    pub fee_pct: Option<f64>,
    pub amount_usd: f64,
    pub log_amount: Option<f64>,
    // T4: risk features
    pub risk_tier: String,
    pub risk_level: i32,
    pub needs_review: bool,
    // T5: geo features
    pub is_cross_border: bool,
    pub corridor: String,
    pub is_intra_bank: bool,
    // T6: anonymized
    pub sender_hash: String,
    pub receiver_hash: String,
    pub sender_account_masked: String,
    pub receiver_account_masked: String,
    pub ip_anonymized: String,
    // T7: session features
    pub device_short_id: String,
    pub session_prefix: String,
    pub memo_length: i32,
    pub has_memo: bool,
    pub row_id: String,
}

// ── Arrow schema for the output Delta table ──────────────────

pub fn transaction_output_schema() -> Schema {
    Schema::new(vec![
        Field::new("kafka_key", DataType::Utf8, true),
        Field::new("transaction_id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Utf8, false),
        Field::new("sender_account_id", DataType::Utf8, true),
        Field::new("receiver_account_id", DataType::Utf8, true),
        Field::new("sender_account", DataType::Utf8, true),
        Field::new("receiver_account", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
        Field::new("currency", DataType::Utf8, true),
        Field::new("transaction_type", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("sender_name", DataType::Utf8, true),
        Field::new("receiver_name", DataType::Utf8, true),
        Field::new("sender_bank_code", DataType::Utf8, true),
        Field::new("receiver_bank_code", DataType::Utf8, true),
        Field::new("sender_country", DataType::Utf8, false),
        Field::new("receiver_country", DataType::Utf8, false),
        Field::new("fee", DataType::Float64, false),
        Field::new("exchange_rate", DataType::Float64, false),
        Field::new("reference_id", DataType::Utf8, true),
        Field::new("memo", DataType::Utf8, true),
        Field::new("risk_score", DataType::Float64, true),
        Field::new("is_flagged", DataType::Boolean, true),
        Field::new("category", DataType::Utf8, true),
        Field::new("channel", DataType::Utf8, true),
        Field::new("ip_address", DataType::Utf8, true),
        Field::new("device_fingerprint", DataType::Utf8, true),
        Field::new("session_id", DataType::Utf8, true),
        Field::new("topic", DataType::Utf8, false),
        Field::new("kafka_partition", DataType::Int32, false),
        Field::new("kafka_offset", DataType::Int64, false),
        Field::new("kafka_timestamp", DataType::Int64, false),
        // T1
        Field::new("currency_safe", DataType::Utf8, false),
        Field::new("risk_score_safe", DataType::Float64, false),
        Field::new("memo_clean", DataType::Utf8, false),
        // T2
        Field::new("transaction_date", DataType::Date32, false),
        Field::new("txn_year", DataType::Int32, false),
        Field::new("txn_month", DataType::Int32, false),
        Field::new("txn_day", DataType::Int32, false),
        Field::new("txn_hour", DataType::Int32, false),
        Field::new("txn_dayofweek", DataType::Int32, false),
        Field::new("is_weekend", DataType::Boolean, false),
        Field::new("txn_quarter", DataType::Int32, false),
        Field::new("date_str", DataType::Utf8, false),
        // T3
        Field::new("amount_bucket", DataType::Utf8, false),
        Field::new("net_amount", DataType::Float64, false),
        Field::new("fee_pct", DataType::Float64, true),
        Field::new("amount_usd", DataType::Float64, false),
        Field::new("log_amount", DataType::Float64, true),
        // T4
        Field::new("risk_tier", DataType::Utf8, false),
        Field::new("risk_level", DataType::Int32, false),
        Field::new("needs_review", DataType::Boolean, false),
        // T5
        Field::new("is_cross_border", DataType::Boolean, false),
        Field::new("corridor", DataType::Utf8, false),
        Field::new("is_intra_bank", DataType::Boolean, false),
        // T6
        Field::new("sender_hash", DataType::Utf8, false),
        Field::new("receiver_hash", DataType::Utf8, false),
        Field::new("sender_account_masked", DataType::Utf8, false),
        Field::new("receiver_account_masked", DataType::Utf8, false),
        Field::new("ip_anonymized", DataType::Utf8, false),
        // T7
        Field::new("device_short_id", DataType::Utf8, false),
        Field::new("session_prefix", DataType::Utf8, false),
        Field::new("memo_length", DataType::Int32, false),
        Field::new("has_memo", DataType::Boolean, false),
        Field::new("row_id", DataType::Utf8, false),
    ])
}

// ── Arrow schema for accounts Delta table ────────────────────

pub fn account_output_schema() -> Schema {
    Schema::new(vec![
        Field::new("account_id", DataType::Utf8, false),
        Field::new("account_number", DataType::Utf8, true),
        Field::new("holder_name", DataType::Utf8, true),
        Field::new("bank_code", DataType::Utf8, true),
        Field::new("country", DataType::Utf8, true),
        Field::new("currency", DataType::Utf8, true),
        Field::new("account_type", DataType::Utf8, true),
        Field::new("account_status", DataType::Utf8, true),
        Field::new("balance", DataType::Float64, true),
        Field::new("total_sent", DataType::Float64, true),
        Field::new("total_received", DataType::Float64, true),
        Field::new("transaction_count", DataType::Int32, true),
        Field::new("risk_rating", DataType::Float64, true),
        Field::new("kyc_level", DataType::Utf8, true),
        Field::new("last_transaction_time", DataType::Utf8, true),
        Field::new("updated_at", DataType::Utf8, true),
        Field::new("created_at", DataType::Utf8, true),
    ])
}

// ── Build Arrow RecordBatch from transformed records ─────────

macro_rules! col_utf8 {
    ($recs:expr, $f:ident) => {
        Arc::new(StringArray::from_iter_values($recs.iter().map(|r| r.$f.as_str())))
            as Arc<dyn Array>
    };
}

macro_rules! col_utf8_opt {
    ($recs:expr, $f:ident) => {
        Arc::new(StringArray::from(
            $recs.iter().map(|r| r.$f.as_deref()).collect::<Vec<_>>(),
        )) as Arc<dyn Array>
    };
}

macro_rules! col_f64 {
    ($recs:expr, $f:ident) => {
        Arc::new(Float64Array::from_iter_values($recs.iter().map(|r| r.$f)))
            as Arc<dyn Array>
    };
}

macro_rules! col_f64_opt {
    ($recs:expr, $f:ident) => {
        Arc::new(Float64Array::from(
            $recs.iter().map(|r| r.$f).collect::<Vec<_>>(),
        )) as Arc<dyn Array>
    };
}

macro_rules! col_i32 {
    ($recs:expr, $f:ident) => {
        Arc::new(Int32Array::from_iter_values($recs.iter().map(|r| r.$f)))
            as Arc<dyn Array>
    };
}

macro_rules! col_i64 {
    ($recs:expr, $f:ident) => {
        Arc::new(Int64Array::from_iter_values($recs.iter().map(|r| r.$f)))
            as Arc<dyn Array>
    };
}

macro_rules! col_bool {
    ($recs:expr, $f:ident) => {
        Arc::new(BooleanArray::from($recs.iter().map(|r| r.$f).collect::<Vec<_>>()))
            as Arc<dyn Array>
    };
}

macro_rules! col_bool_opt {
    ($recs:expr, $f:ident) => {
        Arc::new(BooleanArray::from(
            $recs.iter().map(|r| r.$f).collect::<Vec<Option<bool>>>(),
        )) as Arc<dyn Array>
    };
}

pub fn build_transaction_batch(records: &[TransactionOutput]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(transaction_output_schema());

    // Date32 array
    let date_arr = Arc::new(Date32Array::from_iter_values(
        records.iter().map(|r| r.transaction_date),
    )) as Arc<dyn Array>;

    let columns: Vec<Arc<dyn Array>> = vec![
        col_utf8_opt!(records, kafka_key),
        col_utf8!(records, transaction_id),
        col_utf8!(records, timestamp),
        col_utf8_opt!(records, sender_account_id),
        col_utf8_opt!(records, receiver_account_id),
        col_utf8_opt!(records, sender_account),
        col_utf8_opt!(records, receiver_account),
        col_f64_opt!(records, amount),
        col_utf8_opt!(records, currency),
        col_utf8_opt!(records, transaction_type),
        col_utf8_opt!(records, status),
        col_utf8_opt!(records, sender_name),
        col_utf8_opt!(records, receiver_name),
        col_utf8_opt!(records, sender_bank_code),
        col_utf8_opt!(records, receiver_bank_code),
        col_utf8!(records, sender_country),
        col_utf8!(records, receiver_country),
        col_f64!(records, fee),
        col_f64!(records, exchange_rate),
        col_utf8_opt!(records, reference_id),
        col_utf8_opt!(records, memo),
        col_f64_opt!(records, risk_score),
        col_bool_opt!(records, is_flagged),
        col_utf8_opt!(records, category),
        col_utf8_opt!(records, channel),
        col_utf8_opt!(records, ip_address),
        col_utf8_opt!(records, device_fingerprint),
        col_utf8_opt!(records, session_id),
        col_utf8!(records, topic),
        col_i32!(records, kafka_partition),
        col_i64!(records, kafka_offset),
        col_i64!(records, kafka_timestamp),
        // T1
        col_utf8!(records, currency_safe),
        col_f64!(records, risk_score_safe),
        col_utf8!(records, memo_clean),
        // T2
        date_arr,
        col_i32!(records, txn_year),
        col_i32!(records, txn_month),
        col_i32!(records, txn_day),
        col_i32!(records, txn_hour),
        col_i32!(records, txn_dayofweek),
        col_bool!(records, is_weekend),
        col_i32!(records, txn_quarter),
        col_utf8!(records, date_str),
        // T3
        col_utf8!(records, amount_bucket),
        col_f64!(records, net_amount),
        col_f64_opt!(records, fee_pct),
        col_f64!(records, amount_usd),
        col_f64_opt!(records, log_amount),
        // T4
        col_utf8!(records, risk_tier),
        col_i32!(records, risk_level),
        col_bool!(records, needs_review),
        // T5
        col_bool!(records, is_cross_border),
        col_utf8!(records, corridor),
        col_bool!(records, is_intra_bank),
        // T6
        col_utf8!(records, sender_hash),
        col_utf8!(records, receiver_hash),
        col_utf8!(records, sender_account_masked),
        col_utf8!(records, receiver_account_masked),
        col_utf8!(records, ip_anonymized),
        // T7
        col_utf8!(records, device_short_id),
        col_utf8!(records, session_prefix),
        col_i32!(records, memo_length),
        col_bool!(records, has_memo),
        col_utf8!(records, row_id),
    ];

    Ok(RecordBatch::try_new(schema, columns)?)
}

pub fn build_account_batch(records: &[AccountInput]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(account_output_schema());
    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(StringArray::from_iter_values(records.iter().map(|r| r.account_id.as_str()))),
        Arc::new(StringArray::from(records.iter().map(|r| r.account_number.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(records.iter().map(|r| r.holder_name.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(records.iter().map(|r| r.bank_code.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(records.iter().map(|r| r.country.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(records.iter().map(|r| r.currency.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(records.iter().map(|r| r.account_type.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(records.iter().map(|r| r.account_status.as_deref()).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(records.iter().map(|r| r.balance).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(records.iter().map(|r| r.total_sent).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(records.iter().map(|r| r.total_received).collect::<Vec<_>>())),
        Arc::new(Int32Array::from(records.iter().map(|r| r.transaction_count).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(records.iter().map(|r| r.risk_rating).collect::<Vec<_>>())),
        Arc::new(StringArray::from(records.iter().map(|r| r.kyc_level.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(records.iter().map(|r| r.last_transaction_time.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(records.iter().map(|r| r.updated_at.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(records.iter().map(|r| r.created_at.as_deref()).collect::<Vec<_>>())),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}
