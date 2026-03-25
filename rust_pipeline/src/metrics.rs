use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_TIMINGS: usize = 200;

/// JSON metrics file — same schema as Spark's metrics_bridge.py
#[derive(Debug, Serialize, Deserialize)]
pub struct MetricsData {
    pub status: String,
    pub micro_batches_completed: u64,
    pub total_rows_processed: u64,
    pub last_batch_rows: u64,
    pub first_update: Option<f64>,
    pub last_update: Option<f64>,
    pub batch_durations_ms: Vec<f64>,
    pub transform_times_ms: Vec<f64>,
    pub delta_write_times_ms: Vec<f64>,
    pub batch_details: Vec<serde_json::Value>,
    pub executor: serde_json::Value,
}

impl Default for MetricsData {
    fn default() -> Self {
        Self {
            status: "idle".into(),
            micro_batches_completed: 0,
            total_rows_processed: 0,
            last_batch_rows: 0,
            first_update: None,
            last_update: None,
            batch_durations_ms: vec![],
            transform_times_ms: vec![],
            delta_write_times_ms: vec![],
            batch_details: vec![],
            executor: serde_json::json!({
                "total_tasks": 0,
                "active_cores": 0,
                "peak_memory_mb": 0,
                "total_gc_ms": 0,
                "gc_pct": 0.0
            }),
        }
    }
}

pub struct MetricsBridge {
    path: String,
}

impl MetricsBridge {
    pub fn new(path: &str) -> Self {
        Self { path: path.into() }
    }

    fn load(&self) -> MetricsData {
        if let Ok(text) = fs::read_to_string(&self.path) {
            serde_json::from_str(&text).unwrap_or_default()
        } else {
            MetricsData::default()
        }
    }

    fn save(&self, data: &MetricsData) -> Result<()> {
        if let Some(parent) = Path::new(&self.path).parent() {
            fs::create_dir_all(parent)?;
        }
        // Atomic write: temp file then rename
        let tmp = format!("{}.tmp", self.path);
        let mut f = fs::File::create(&tmp)?;
        serde_json::to_writer(&mut f, data)?;
        f.flush()?;
        drop(f);
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    fn now_epoch() -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64()
    }

    pub fn update_status(&mut self, status: &str) -> Result<()> {
        let mut d = self.load();
        d.status = status.into();
        d.last_update = Some(Self::now_epoch());
        if d.first_update.is_none() {
            d.first_update = d.last_update;
        }
        self.save(&d)
    }

    pub fn record_batch(
        &mut self,
        batch_id: u64,
        row_count: usize,
        batch_ms: f64,
        transform_ms: f64,
        write_ms: f64,
    ) -> Result<()> {
        let mut d = self.load();
        let now = Self::now_epoch();

        d.status = "running".into();
        if d.first_update.is_none() {
            d.first_update = Some(now);
        }
        d.last_update = Some(now);

        d.micro_batches_completed += 1;
        d.total_rows_processed += row_count as u64;
        d.last_batch_rows = row_count as u64;

        d.batch_durations_ms.push(batch_ms);
        if d.batch_durations_ms.len() > MAX_TIMINGS {
            let start = d.batch_durations_ms.len() - MAX_TIMINGS;
            d.batch_durations_ms = d.batch_durations_ms[start..].to_vec();
        }

        d.transform_times_ms.push(transform_ms);
        if d.transform_times_ms.len() > MAX_TIMINGS {
            let start = d.transform_times_ms.len() - MAX_TIMINGS;
            d.transform_times_ms = d.transform_times_ms[start..].to_vec();
        }

        d.delta_write_times_ms.push(write_ms);
        if d.delta_write_times_ms.len() > MAX_TIMINGS {
            let start = d.delta_write_times_ms.len() - MAX_TIMINGS;
            d.delta_write_times_ms = d.delta_write_times_ms[start..].to_vec();
        }

        // Per-batch detail — match Spark batch_detail schema
        // Memory from /proc/self/status (Linux)
        let peak_mem_mb = read_peak_rss_mb();
        d.batch_details.push(serde_json::json!({
            "batch_id": batch_id,
            "rows": row_count,
            "wall_ms": batch_ms,
            "transform_ms": transform_ms,
            "write_ms": write_ms,
            "cpu_ms": transform_ms,  // In Rust, transform is CPU-bound
            "gc_ms": 0,              // No GC in Rust
            "cpu_util_pct": if batch_ms > 0.0 { transform_ms / batch_ms * 100.0 } else { 0.0 },
            "gc_pct": 0.0,
            "peak_memory_mb": peak_mem_mb,
            "tasks": rayon::current_num_threads(),
        }));
        if d.batch_details.len() > MAX_TIMINGS {
            let start = d.batch_details.len() - MAX_TIMINGS;
            d.batch_details = d.batch_details[start..].to_vec();
        }

        // Executor snapshot
        d.executor = serde_json::json!({
            "total_tasks": d.micro_batches_completed * rayon::current_num_threads() as u64,
            "active_cores": rayon::current_num_threads(),
            "peak_memory_mb": peak_mem_mb,
            "total_gc_ms": 0,
            "gc_pct": 0.0,
        });

        self.save(&d)
    }
}

/// Read peak RSS from /proc/self/status (Linux only, returns 0 on other OS)
fn read_peak_rss_mb() -> f64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(text) = fs::read_to_string("/proc/self/status") {
            for line in text.lines() {
                if line.starts_with("VmHWM:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<f64>() {
                            return kb / 1024.0;
                        }
                    }
                }
            }
        }
        0.0
    }
    #[cfg(not(target_os = "linux"))]
    {
        0.0
    }
}
