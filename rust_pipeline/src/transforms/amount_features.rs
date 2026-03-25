pub struct AmountFeatures {
    pub amount_bucket: String,
    pub net_amount: f64,
    pub fee_pct: Option<f64>,
    pub amount_usd: f64,
    pub log_amount: Option<f64>,
}

fn round_n(v: f64, n: u32) -> f64 {
    let factor = 10f64.powi(n as i32);
    (v * factor).round() / factor
}

/// Spark parity: add_amount_features
pub fn apply(amount: Option<f64>, fee: f64, exchange_rate: f64) -> AmountFeatures {
    let amt = amount.unwrap_or(0.0);

    let amount_bucket = match amt {
        a if a < 100.0 => "MICRO",
        a if a < 1_000.0 => "SMALL",
        a if a < 10_000.0 => "MEDIUM",
        a if a < 100_000.0 => "LARGE",
        _ => "WHALE",
    }
    .to_string();

    let net_amount = round_n(amt - fee, 2);

    let fee_pct = if amt > 0.0 {
        Some(round_n(fee / amt * 100.0, 4))
    } else {
        None
    };

    let amount_usd = round_n(amt * exchange_rate, 2);

    let log_amount = if amt > 0.0 {
        Some(round_n(amt.ln(), 4))
    } else {
        None
    };

    AmountFeatures {
        amount_bucket,
        net_amount,
        fee_pct,
        amount_usd,
        log_amount,
    }
}
