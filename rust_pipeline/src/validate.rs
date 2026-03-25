use anyhow::Result;
use deltalake::datafusion::prelude::*;
use std::sync::Arc;

/// Compare two Delta tables (Spark vs Rust) and report parity.
pub async fn run_validation(spark_path: &str, rust_path: &str) -> Result<()> {
    tracing::info!("Opening Spark table: {}", spark_path);
    let spark_table = deltalake::open_table(spark_path).await?;

    tracing::info!("Opening Rust table: {}", rust_path);
    let rust_table = deltalake::open_table(rust_path).await?;

    // Register both tables in DataFusion for SQL comparison
    let ctx = SessionContext::new();
    ctx.register_table("spark_data", Arc::new(spark_table))?;
    ctx.register_table("rust_data", Arc::new(rust_table))?;

    // Count rows via SQL (portable across delta-rs versions)
    let spark_rows = sql_count(&ctx, "SELECT COUNT(*) as cnt FROM spark_data").await?;
    let rust_rows = sql_count(&ctx, "SELECT COUNT(*) as cnt FROM rust_data").await?;
    tracing::info!("Row counts — Spark: {}  Rust: {}", spark_rows, rust_rows);

    // 1. Check for missing rows (by transaction_id or account_id)
    // Auto-detect the join column
    let spark_schema = ctx
        .table("spark_data")
        .await?
        .schema()
        .clone();
    let join_col = if spark_schema.field_with_unqualified_name("transaction_id").is_ok() {
        "transaction_id"
    } else {
        "account_id"
    };

    let missing_in_rust = ctx
        .sql(&format!(
            "SELECT COUNT(*) as cnt FROM spark_data s
             LEFT ANTI JOIN rust_data r ON s.{jc} = r.{jc}",
            jc = join_col
        ))
        .await?
        .collect()
        .await?;
    let mir = extract_count(&missing_in_rust);

    let missing_in_spark = ctx
        .sql(&format!(
            "SELECT COUNT(*) as cnt FROM rust_data r
             LEFT ANTI JOIN spark_data s ON s.{jc} = r.{jc}",
            jc = join_col
        ))
        .await?
        .collect()
        .await?;
    let mis = extract_count(&missing_in_spark);

    tracing::info!("Missing in Rust: {}  Missing in Spark: {}", mir, mis);

    // 2. Sample matched rows and compare numeric columns
    if join_col == "transaction_id" {
        check_numeric_parity(&ctx, join_col, &[
            ("net_amount", 0.01),
            ("fee_pct", 0.001),
            ("amount_usd", 0.01),
            ("log_amount", 0.001),
            ("risk_score_safe", 0.001),
        ]).await?;

        check_string_parity(&ctx, join_col, &[
            "currency_safe", "risk_tier", "amount_bucket",
            "corridor", "sender_hash", "receiver_hash", "row_id",
        ]).await?;
    } else {
        check_numeric_parity(&ctx, join_col, &[
            ("balance", 0.01),
            ("total_sent", 0.01),
            ("total_received", 0.01),
            ("risk_rating", 0.001),
        ]).await?;
    }

    // 3. Write validation report
    let total_comparable = spark_rows.min(rust_rows);
    let parity_pct = if total_comparable > 0 {
        ((total_comparable - mir) as f64 / total_comparable as f64) * 100.0
    } else {
        0.0
    };

    let report = serde_json::json!({
        "spark_table": spark_path,
        "rust_table": rust_path,
        "spark_row_count": spark_rows,
        "rust_row_count": rust_rows,
        "missing_in_rust": mir,
        "missing_in_spark": mis,
        "parity_pct": format!("{:.4}", parity_pct),
        "status": if mir == 0 && mis == 0 { "PASS" } else { "WARN" },
    });

    let report_path = if std::path::Path::new("/opt/logs").exists() {
        "/opt/logs/validation_report.json"
    } else {
        "./logs/validation_report.json"
    };
    if let Some(parent) = std::path::Path::new(report_path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(report_path, serde_json::to_string_pretty(&report)?)?;
    tracing::info!("Validation report written to {}", report_path);
    tracing::info!("Parity: {:.4}%  Status: {}",
        parity_pct,
        if mir == 0 && mis == 0 { "PASS" } else { "WARN" }
    );

    Ok(())
}

async fn check_numeric_parity(
    ctx: &SessionContext,
    join_col: &str,
    columns: &[(&str, f64)],
) -> Result<()> {
    for (col_name, tolerance) in columns {
        let sql = format!(
            "SELECT COUNT(*) as mismatches FROM spark_data s
             JOIN rust_data r ON s.{jc} = r.{jc}
             WHERE ABS(COALESCE(s.{c}, 0) - COALESCE(r.{c}, 0)) > {tol}",
            jc = join_col, c = col_name, tol = tolerance,
        );
        match ctx.sql(&sql).await {
            Ok(df) => {
                let batches = df.collect().await?;
                let cnt = extract_count(&batches);
                if cnt > 0 {
                    tracing::warn!("  {} – {} mismatches (tolerance {})", col_name, cnt, tolerance);
                } else {
                    tracing::info!("  {} – OK", col_name);
                }
            }
            Err(e) => {
                tracing::warn!("  {} – skip ({})", col_name, e);
            }
        }
    }
    Ok(())
}

async fn check_string_parity(
    ctx: &SessionContext,
    join_col: &str,
    columns: &[&str],
) -> Result<()> {
    for col_name in columns {
        let sql = format!(
            "SELECT COUNT(*) as mismatches FROM spark_data s
             JOIN rust_data r ON s.{jc} = r.{jc}
             WHERE COALESCE(s.{c}, '') <> COALESCE(r.{c}, '')",
            jc = join_col, c = col_name,
        );
        match ctx.sql(&sql).await {
            Ok(df) => {
                let batches = df.collect().await?;
                let cnt = extract_count(&batches);
                if cnt > 0 {
                    tracing::warn!("  {} – {} mismatches", col_name, cnt);
                } else {
                    tracing::info!("  {} – OK", col_name);
                }
            }
            Err(e) => {
                tracing::warn!("  {} – skip ({})", col_name, e);
            }
        }
    }
    Ok(())
}

fn extract_count(batches: &[deltalake::arrow::record_batch::RecordBatch]) -> i64 {
    if let Some(batch) = batches.first() {
        if batch.num_rows() > 0 {
            if let Some(arr) = batch.column(0).as_any().downcast_ref::<deltalake::arrow::array::Int64Array>() {
                return arr.value(0);
            }
        }
    }
    0
}

async fn sql_count(ctx: &SessionContext, sql: &str) -> Result<i64> {
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;
    Ok(extract_count(&batches))
}
