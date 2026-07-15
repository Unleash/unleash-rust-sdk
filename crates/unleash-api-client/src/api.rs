// Copyright 2020 Cognite AS
//! <https://docs.getunleash.io/api/client/features>
use std::collections::HashMap;
use std::default::Default;

use crate::version::get_sdk_version;
use chrono::Utc;
use serde::{Deserialize, Serialize};

pub fn features_endpoint(api_url: &str) -> String {
    format!("{}/client/features", api_url.trim_end_matches('/'))
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Registration {
    #[serde(rename = "appName")]
    pub app_name: String,
    #[serde(rename = "instanceId")]
    pub instance_id: String,
    #[serde(rename = "connectionId")]
    pub connection_id: String,
    #[serde(rename = "sdkVersion")]
    pub sdk_version: String,
    pub strategies: Vec<String>,
    pub started: chrono::DateTime<chrono::Utc>,
    pub interval: u64,
}

impl Registration {
    pub fn endpoint(api_url: &str) -> String {
        format!("{}/client/register", api_url.trim_end_matches('/'))
    }
}

impl Default for Registration {
    fn default() -> Self {
        Self {
            app_name: "".into(),
            instance_id: "".into(),
            connection_id: "".into(),
            sdk_version: get_sdk_version().into(),
            strategies: vec![],
            started: Utc::now(),
            interval: 15 * 1000,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Metrics {
    #[serde(rename = "appName")]
    pub app_name: String,
    #[serde(rename = "instanceId")]
    pub instance_id: String,
    #[serde(rename = "connectionId")]
    pub connection_id: String,
    pub bucket: MetricsBucket,
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
    fn test_registration_customisation() {
        Registration {
            app_name: "test-suite".into(),
            instance_id: "test".into(),
            connection_id: "test".into(),
            strategies: vec!["default".into()],
            interval: 5000,
            ..Default::default()
        };
    }

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
