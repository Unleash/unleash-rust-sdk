// Copyright 2020 Cognite AS
//! <https://docs.getunleash.io/api/client/features>
use std::collections::HashMap;
use std::default::Default;

const RUSTC_VERSION: &str = env!("RUSTC_VERSION");
use serde::{Deserialize, Serialize};

pub fn features_endpoint(api_url: &str) -> String {
    format!("{}/client/features", api_url.trim_end_matches('/'))
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MetricsMetadata {
    pub(crate) platform_name: String,
    pub(crate) platform_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) sdk_flavour: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) sdk_flavour_version: Option<String>,
}

impl Default for MetricsMetadata {
    fn default() -> Self {
        Self {
            platform_name: "rust".into(),
            platform_version: RUSTC_VERSION.into(),
            sdk_flavour: None,
            sdk_flavour_version: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Registration {
    pub app_name: String,
    pub instance_id: String,
    pub connection_id: String,
    pub sdk_version: String,
    pub strategies: Vec<String>,
    pub started: chrono::DateTime<chrono::Utc>,
    pub interval: u64,
    #[serde(flatten)]
    pub(crate) metadata: MetricsMetadata,
}

impl Registration {
    pub fn endpoint(api_url: &str) -> String {
        format!("{}/client/register", api_url.trim_end_matches('/'))
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Metrics {
    pub app_name: String,
    pub instance_id: String,
    pub connection_id: String,
    pub bucket: MetricsBucket,
    #[serde(flatten)]
    pub(crate) metadata: MetricsMetadata,
}

impl Metrics {
    pub fn endpoint(api_url: &str) -> String {
        format!("{}/client/metrics", api_url.trim_end_matches('/'))
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ToggleMetrics {
    pub yes: u64,
    pub no: u64,
    pub variants: HashMap<String, u64>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MetricsBucket {
    pub start: chrono::DateTime<chrono::Utc>,
    pub stop: chrono::DateTime<chrono::Utc>,
    pub toggles: HashMap<String, ToggleMetrics>,
}

#[cfg(test)]
mod tests {

    use super::{features_endpoint, Metrics, Registration};

    #[test]
    fn test_endpoints_handle_trailing_slashes() {
        assert_eq!(
            Registration::endpoint("https://localhost:4242/api"),
            "https://localhost:4242/api/client/register"
        );
        assert_eq!(
            Registration::endpoint("https://localhost:4242/api/"),
            "https://localhost:4242/api/client/register"
        );

        assert_eq!(
            features_endpoint("https://localhost:4242/api"),
            "https://localhost:4242/api/client/features"
        );
        assert_eq!(
            features_endpoint("https://localhost:4242/api/"),
            "https://localhost:4242/api/client/features"
        );

        assert_eq!(
            Metrics::endpoint("https://localhost:4242/api"),
            "https://localhost:4242/api/client/metrics"
        );
        assert_eq!(
            Metrics::endpoint("https://localhost:4242/api/"),
            "https://localhost:4242/api/client/metrics"
        );
    }
}
