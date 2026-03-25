mod account;
mod checkpoint;
mod config;
mod consumer;
mod metrics;
mod schema;
mod transforms;
mod validate;
mod writer;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "rust-pipeline", about = "Rust Kafka→Delta pipeline for migration evaluation")]
struct Cli {
    /// Path to app_config.yaml (shared with Spark)
    #[arg(short, long)]
    config: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run financial-transactions streaming pipeline
    Stream,
    /// Run account-updates upsert pipeline
    Upsert,
    /// Validate Rust output against Spark output
    Validate {
        #[arg(long)]
        spark_path: Option<String>,
        #[arg(long)]
        rust_path: Option<String>,
        /// Also validate account tables
        #[arg(long, default_value_t = false)]
        accounts: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("rust_pipeline=info".parse()?))
        .init();

    let cli = Cli::parse();
    let cfg = config::load_config(&cli.config)?;

    match cli.command {
        Command::Stream => {
            tracing::info!("Starting financial-transactions streaming pipeline");
            consumer::run_streaming_pipeline(&cfg).await?;
        }
        Command::Upsert => {
            tracing::info!("Starting account-upsert pipeline");
            account::run_account_upsert_pipeline(&cfg).await?;
        }
        Command::Validate { spark_path, rust_path, accounts } => {
            let sp = spark_path.unwrap_or_else(|| cfg.delta_table_path());
            let rp = rust_path.unwrap_or_else(|| cfg.rust_delta_table_path());
            tracing::info!("Validating: spark={} rust={}", sp, rp);
            validate::run_validation(&sp, &rp).await?;
            if accounts {
                let sa = cfg.accounts_delta_path();
                let ra = cfg.rust_accounts_delta_path();
                tracing::info!("Validating accounts: spark={} rust={}", sa, ra);
                validate::run_validation(&sa, &ra).await?;
            }
        }
    }
    Ok(())
}
