pub struct RiskFeatures {
    pub risk_tier: String,
    pub risk_level: i32,
    pub needs_review: bool,
}

/// Spark parity: add_risk_features
/// - risk_tier via classify_risk UDF
/// - risk_level: numeric 0-5
/// - needs_review: risk_score >= 0.7 OR is_flagged OR amount >= 100_000
pub fn apply(risk_score: Option<f64>, is_flagged: Option<bool>, amount: Option<f64>) -> RiskFeatures {
    let (tier, level) = classify_risk(risk_score);
    let needs_review = risk_score.unwrap_or(0.0) >= 0.7
        || is_flagged.unwrap_or(false)
        || amount.unwrap_or(0.0) >= 100_000.0;

    RiskFeatures {
        risk_tier: tier.to_string(),
        risk_level: level,
        needs_review,
    }
}

/// Replicates Spark UDF classify_risk(risk_score)
fn classify_risk(score: Option<f64>) -> (&'static str, i32) {
    match score {
        None => ("UNKNOWN", 0),
        Some(s) if s >= 0.8 => ("CRITICAL", 5),
        Some(s) if s >= 0.6 => ("HIGH", 4),
        Some(s) if s >= 0.4 => ("MEDIUM", 3),
        Some(s) if s >= 0.2 => ("LOW", 2),
        Some(_) => ("NEGLIGIBLE", 1),
    }
}
