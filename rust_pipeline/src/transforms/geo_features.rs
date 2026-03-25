pub struct GeoFeatures {
    pub is_cross_border: bool,
    pub corridor: String,
    pub is_intra_bank: bool,
}

/// Spark parity: add_geo_features
pub fn apply(
    sender_country: &str,
    receiver_country: &str,
    sender_bank_code: &Option<String>,
    receiver_bank_code: &Option<String>,
) -> GeoFeatures {
    let is_cross_border = sender_country != receiver_country;

    let corridor = format!(
        "{}→{}",
        sender_country.to_uppercase(),
        receiver_country.to_uppercase()
    );

    let is_intra_bank = match (sender_bank_code, receiver_bank_code) {
        (Some(s), Some(r)) => s == r,
        _ => false,
    };

    GeoFeatures {
        is_cross_border,
        corridor,
        is_intra_bank,
    }
}
