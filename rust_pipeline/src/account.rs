use crate::checkpoint::Checkpoint;
use crate::config::AppConfig;
use crate::metrics::MetricsBridge;
use crate::schema::{self, AccountInput};
use crate::writer;

use anyhow::Result;
use deltalake::arrow::array::*;
use deltalake::arrow::datatypes::DataType;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::prelude::*;
use deltalake::DeltaOps;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::TopicPartitionList;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub async fn run_account_upsert_pipeline(cfg: &AppConfig) -> Result<()> {
    let topic = &cfg.kafka.account_topic;
    let num_partitions = cfg.kafka.partitions;
    let batch_size = cfg.rust.batch_size;
    let flush_timeout = Duration::from_secs(cfg.rust.flush_interval_seconds);
    let delta_path = cfg.rust_accounts_delta_path();
    let ckpt_path = cfg.rust_accounts_checkpoint_path();
    let metrics_path = cfg.rust_accounts_metrics_path();

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", cfg.kafka_bootstrap())
        .set("group.id", &cfg.rust.accounts_consumer_group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("fetch.min.bytes", "1")
        .set("fetch.wait.max.ms", "500")
        .create()?;

    let checkpoint = Checkpoint::open(&ckpt_path)?;

    let mut tpl = TopicPartitionList::new();
    for p in 0..num_partitions {
        let off = checkpoint.get_offset(topic, p)?;
        tpl.add_partition_offset(topic, p, off)?;
    }
    consumer.assign(&tpl)?;
    tracing::info!("Account upsert: assigned {} partitions on {}", num_partitions, topic);

    writer::ensure_parent_dir(&delta_path)?;
    let mut metrics = MetricsBridge::new(&metrics_path);
    metrics.update_status("running")?;
    let mut batch_id: u64 = 0;

    loop {
        let t_start = Instant::now();

        let messages = collect_account_batch(&consumer, batch_size, flush_timeout).await;
        if messages.is_empty() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }

        // Deserialize
        let mut records: Vec<AccountInput> = messages
            .iter()
            .filter_map(|m| serde_json::from_slice(&m.payload).ok())
            .collect();

        if records.is_empty() {
            continue;
        }

        // Deduplicate within batch: keep latest by updated_at per account_id
        records = deduplicate(records);
        let row_count = records.len();

        let t_transform = t_start.elapsed();

        // Write or merge
        let t_write_start = Instant::now();
        let batch = schema::build_account_batch(&records)?;

        // Try to open existing table for merge; if not exists, just write
        match deltalake::open_table(&delta_path).await {
            Ok(table) => {
                merge_accounts(table, batch).await?;
            }
            Err(_) => {
                // First write — creates the table
                writer::write_batch(
                    &delta_path,
                    batch,
                    &["country".to_string()],
                ).await?;
            }
        }
        let t_write = t_write_start.elapsed();

        // Checkpoint
        let offsets = compute_max_offsets(&messages);
        for ((top, part), off) in &offsets {
            checkpoint.commit_offset(top, *part, *off, batch_id)?;
        }

        let t_total = t_start.elapsed();
        metrics.record_batch(
            batch_id,
            row_count,
            t_total.as_millis() as f64,
            t_transform.as_millis() as f64,
            t_write.as_millis() as f64,
        )?;

        tracing::info!(
            "Account batch {}: {} rows (deduped) | merge {}ms | total {}ms",
            batch_id, row_count, t_write.as_millis(), t_total.as_millis(),
        );

        batch_id += 1;
    }
}

/// MERGE operation matching Spark's account_upsert_job.py:
/// WHEN MATCHED: accumulate total_sent/received/transaction_count; snapshot balance/status/risk/kyc
/// WHEN NOT MATCHED: INSERT ALL
async fn merge_accounts(
    table: deltalake::DeltaTable,
    source_batch: RecordBatch,
) -> Result<()> {
    let schema = source_batch.schema();
    let ctx = SessionContext::new();
    let mem_table = deltalake::datafusion::datasource::MemTable::try_new(
        schema, vec![vec![source_batch]],
    )?;
    let source_df = ctx.read_table(Arc::new(mem_table))?;

    let (table, _metrics) = DeltaOps(table)
        .merge(
            source_df,
            col("target.account_id").eq(col("source.account_id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_update(|update| {
            update
                .update("total_sent", col("target.total_sent") + col("source.total_sent"))
                .update("total_received", col("target.total_received") + col("source.total_received"))
                .update("transaction_count", col("target.transaction_count") + col("source.transaction_count"))
                .update("balance", col("source.balance"))
                .update("account_status", col("source.account_status"))
                .update("risk_rating", col("source.risk_rating"))
                .update("kyc_level", col("source.kyc_level"))
                .update("updated_at", col("source.updated_at"))
                .update(
                    "last_transaction_time",
                    when(
                        col("target.last_transaction_time").gt(col("source.last_transaction_time")),
                        col("target.last_transaction_time"),
                    )
                    .otherwise(col("source.last_transaction_time"))
                    .unwrap(),
                )
        })?
        .when_not_matched_insert(|insert| {
            insert
                .set("account_id", col("source.account_id"))
                .set("account_number", col("source.account_number"))
                .set("holder_name", col("source.holder_name"))
                .set("bank_code", col("source.bank_code"))
                .set("country", col("source.country"))
                .set("currency", col("source.currency"))
                .set("account_type", col("source.account_type"))
                .set("account_status", col("source.account_status"))
                .set("balance", col("source.balance"))
                .set("total_sent", col("source.total_sent"))
                .set("total_received", col("source.total_received"))
                .set("transaction_count", col("source.transaction_count"))
                .set("risk_rating", col("source.risk_rating"))
                .set("kyc_level", col("source.kyc_level"))
                .set("last_transaction_time", col("source.last_transaction_time"))
                .set("updated_at", col("source.updated_at"))
                .set("created_at", col("source.created_at"))
        })?
        .await?;

    Ok(())
}

/// Deduplicate accounts: keep the record with the latest updated_at per account_id
fn deduplicate(records: Vec<AccountInput>) -> Vec<AccountInput> {
    let mut latest: HashMap<String, AccountInput> = HashMap::new();
    for r in records {
        let key = r.account_id.clone();
        let dominated = if let Some(existing) = latest.get(&key) {
            existing.updated_at.as_deref().unwrap_or("") >= r.updated_at.as_deref().unwrap_or("")
        } else {
            false
        };
        if !dominated {
            latest.insert(key, r);
        }
    }
    latest.into_values().collect()
}

struct OwnedMsg {
    payload: Vec<u8>,
    topic: String,
    partition: i32,
    offset: i64,
}

async fn collect_account_batch(
    consumer: &StreamConsumer,
    max: usize,
    timeout: Duration,
) -> Vec<OwnedMsg> {
    let deadline = Instant::now() + timeout;
    let mut msgs = Vec::with_capacity(max.min(100_000));
    while msgs.len() < max {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, consumer.recv()).await {
            Ok(Ok(m)) => {
                if let Some(p) = m.payload() {
                    msgs.push(OwnedMsg {
                        payload: p.to_vec(),
                        topic: m.topic().to_string(),
                        partition: m.partition(),
                        offset: m.offset(),
                    });
                }
            }
            _ => break,
        }
    }
    msgs
}

fn compute_max_offsets(messages: &[OwnedMsg]) -> HashMap<(String, i32), i64> {
    let mut offsets: HashMap<(String, i32), i64> = HashMap::new();
    for m in messages {
        let key = (m.topic.clone(), m.partition);
        let entry = offsets.entry(key).or_insert(m.offset);
        if m.offset > *entry {
            *entry = m.offset;
        }
    }
    for v in offsets.values_mut() {
        *v += 1;
    }
    offsets
}
