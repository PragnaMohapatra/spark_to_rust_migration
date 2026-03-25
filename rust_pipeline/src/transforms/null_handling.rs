use crate::schema::TransactionInput;

pub struct NullHandled {
    pub currency_safe: String,
    pub risk_score_safe: f64,
    pub memo_clean: String,
    pub fee: f64,
    pub exchange_rate: f64,
    pub sender_country: String,
    pub receiver_country: String,
}

/// Spark parity: add_null_handling
/// - currency_safe ← coalesce(currency, "USD")
/// - risk_score_safe ← coalesce(risk_score, 0.0)
/// - memo_clean ← isNull(memo) ? "(no memo)" : trim(memo)
/// - fee ← coalesce(fee, 0.0)
/// - exchange_rate ← coalesce(exchange_rate, 1.0)
/// - sender_country ← coalesce(sender_country, "XX")
/// - receiver_country ← coalesce(receiver_country, "XX")
pub fn apply(input: &TransactionInput) -> NullHandled {
    NullHandled {
        currency_safe: input.currency.clone().unwrap_or_else(|| "USD".into()),
        risk_score_safe: input.risk_score.unwrap_or(0.0),
        memo_clean: match &input.memo {
            Some(m) if !m.trim().is_empty() => m.trim().to_string(),
            _ => "(no memo)".into(),
        },
        fee: input.fee.unwrap_or(0.0),
        exchange_rate: input.exchange_rate.unwrap_or(1.0),
        sender_country: input.sender_country.clone().unwrap_or_else(|| "XX".into()),
        receiver_country: input.receiver_country.clone().unwrap_or_else(|| "XX".into()),
    }
}
