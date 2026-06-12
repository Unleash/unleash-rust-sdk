//! SDK-owned HTTP transport abstraction.
//!
//! Callers hand the transport a complete request and get back a status/body.
//! Header parsing, request builder types, and JSON convenience APIs stay out of
//! the public trait.

use std::error::Error;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use crate::version::get_sdk_version;

pub type BoxError = Box<dyn Error + Send + Sync + 'static>;
pub type TransportRef = Arc<dyn Transport>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Method {
    Get,
    Post,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Request {
    pub method: Method,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}

#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn execute(&self, request: Request) -> Result<Response, BoxError>;
}

#[async_trait]
impl<T> Transport for Arc<T>
where
    T: Transport + ?Sized,
{
    async fn execute(&self, request: Request) -> Result<Response, BoxError> {
        (**self).execute(request).await
    }
}

pub struct Http<T: Transport> {
    app_name: String,
    sdk_version: &'static str,
    instance_id: String,
    connection_id: String,
    authorization: Option<String>,
    transport: T,
}

impl<T: Transport> Http<T> {
    pub fn new(
        transport: T,
        app_name: String,
        instance_id: String,
        connection_id: String,
        authorization: Option<String>,
    ) -> Self {
        Self {
            transport,
            app_name,
            sdk_version: get_sdk_version(),
            connection_id,
            instance_id,
            authorization,
        }
    }

    pub async fn get_json<R: DeserializeOwned>(
        &self,
        endpoint: &str,
        interval: Option<u64>,
    ) -> Result<R, BoxError> {
        let request = self.request(Method::Get, endpoint, interval, None);
        let response = self.transport.execute(request).await?;
        Ok(serde_json::from_slice(&response.body)?)
    }

    pub async fn post_json<B: Serialize + Sync>(
        &self,
        endpoint: &str,
        content: B,
        interval: Option<u64>,
    ) -> Result<bool, BoxError> {
        let body = serde_json::to_vec(&content)?;
        let request = self.request(Method::Post, endpoint, interval, Some(body));
        let response = self.transport.execute(request).await?;
        Ok((200..300).contains(&response.status))
    }

    fn request(
        &self,
        method: Method,
        endpoint: &str,
        interval: Option<u64>,
        body: Option<Vec<u8>>,
    ) -> Request {
        let mut headers = vec![
            ("appname".to_string(), self.app_name.clone()),
            ("unleash-appname".to_string(), self.app_name.clone()),
            ("unleash-sdk".to_string(), self.sdk_version.to_string()),
            (
                "unleash-connection-id".to_string(),
                self.connection_id.clone(),
            ),
            ("unleash-instanceid".to_string(), self.instance_id.clone()),
        ];
        if let Some(authorization) = &self.authorization {
            headers.push(("authorization".to_string(), authorization.clone()));
        }
        if let Some(interval) = interval {
            headers.push(("unleash-interval".to_string(), interval.to_string()));
        }

        Request {
            method,
            url: endpoint.to_string(),
            headers,
            body,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MockTransport {
        requests: Mutex<Vec<Request>>,
    }

    #[async_trait]
    impl Transport for MockTransport {
        async fn execute(&self, request: Request) -> Result<Response, BoxError> {
            self.requests.lock().unwrap().push(request);
            Ok(Response {
                status: 202,
                body: serde_json::to_vec(&json!({"ok": true}))?,
            })
        }
    }

    #[tokio::test]
    async fn http_builds_owned_requests() {
        let http = Http::new(
            MockTransport::default(),
            "my_app".to_string(),
            "my_instance".to_string(),
            "connection-id".to_string(),
            Some("token".to_string()),
        );

        assert!(http
            .post_json(
                "https://example.test/client/register",
                &json!({"x": 1}),
                Some(15)
            )
            .await
            .unwrap());

        let requests = http.transport.requests.lock().unwrap();
        let request = requests.first().unwrap();
        assert_eq!(request.method, Method::Post);
        assert_eq!(request.url, "https://example.test/client/register");
        assert_eq!(request.headers.len(), 7);
        assert!(request
            .headers
            .contains(&("unleash-appname".to_string(), "my_app".to_string())));
        assert!(request
            .headers
            .contains(&("unleash-instanceid".to_string(), "my_instance".to_string())));
        assert!(request
            .headers
            .contains(&("authorization".to_string(), "token".to_string())));
        assert!(request
            .headers
            .contains(&("unleash-interval".to_string(), "15".to_string())));
    }
}
