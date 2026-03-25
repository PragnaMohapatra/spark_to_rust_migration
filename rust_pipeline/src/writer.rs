use anyhow::Result;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::protocol::SaveMode;
use deltalake::DeltaOps;
use std::path::Path;

/// Ensure the parent directory for a Delta table path exists.
pub fn ensure_parent_dir(path: &str) -> Result<()> {
    let p = Path::new(path);
    if !p.exists() {
        std::fs::create_dir_all(p)?;
    }
    Ok(())
}

/// Append an Arrow RecordBatch to a Delta table (creating it if needed).
pub async fn write_batch(
    table_path: &str,
    batch: RecordBatch,
    partition_columns: &[String],
) -> Result<()> {
    let ops = DeltaOps::try_from_uri(table_path).await?;
    ops.write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .with_partition_columns(partition_columns.to_vec())
        .await?;
    Ok(())
}

/// Overwrite a Delta table (used for account upsert when merge isn't available).
pub async fn write_batch_overwrite(
    table_path: &str,
    batch: RecordBatch,
    partition_columns: &[String],
) -> Result<()> {
    let ops = DeltaOps::try_from_uri(table_path).await?;
    ops.write(vec![batch])
        .with_save_mode(SaveMode::Overwrite)
        .with_partition_columns(partition_columns.to_vec())
        .await?;
    Ok(())
}
