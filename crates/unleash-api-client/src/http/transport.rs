use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use crate::version::get_sdk_version;

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
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonResponse<R> {
    pub value: R,
    pub headers: Vec<(String, String)>,
}

/// Error returned by [`Http::get_json`].
///
/// The status code is carried as a first-class variant so callers can react to
/// it directly (matching `UnexpectedStatus`/`NotModified`) instead of
/// downcasting an opaque error and losing the type information.
#[derive(Debug)]
#[non_exhaustive]
pub enum GetError {
    /// The server returned 304 Not Modified: the previously fetched data is
    /// still current, so there is nothing new to deserialize.
    NotModified,
    /// The server returned a status that is neither a success (2xx) nor 304.
    UnexpectedStatus(u16),
    /// The request could not be executed by the transport.
    Transport(anyhow::Error),
    /// The response body could not be deserialized into the expected type.
    Deserialize(serde_json::Error),
}

impl fmt::Display for GetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GetError::NotModified => write!(f, "HTTP 304 Not Modified"),
            GetError::UnexpectedStatus(status) => write!(f, "unexpected HTTP status {status}"),
            GetError::Transport(err) => write!(f, "transport error: {err}"),
            GetError::Deserialize(err) => write!(f, "failed to deserialize response body: {err}"),
        }
    }
}

impl std::error::Error for GetError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            GetError::Deserialize(err) => Some(err),
            _ => None,
        }
    }
}

#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn execute(&self, request: Request) -> Result<Response, anyhow::Error>;
}

#[async_trait]
impl<T> Transport for Arc<T>
where
    T: Transport + ?Sized,
{
    async fn execute(&self, request: Request) -> Result<Response, anyhow::Error> {
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
        headers: Vec<(String, String)>,
    ) -> Result<JsonResponse<R>, GetError> {
        let request = self.request(Method::Get, endpoint, interval, headers, None);
        let response = self
            .transport
            .execute(request)
            .await
            .map_err(GetError::Transport)?;
        match response.status {
            200..=299 => {
                let value =
                    serde_json::from_slice(&response.body).map_err(GetError::Deserialize)?;
                Ok(JsonResponse {
                    value,
                    headers: response.headers,
                })
            }
            304 => Err(GetError::NotModified),
            status => Err(GetError::UnexpectedStatus(status)),
        }
    }

    pub async fn post_json<B: Serialize + Sync>(
        &self,
        endpoint: &str,
        content: B,
        interval: Option<u64>,
    ) -> Result<bool, anyhow::Error> {
        let body = serde_json::to_vec(&content)?;
        let request = self.request(Method::Post, endpoint, interval, Vec::new(), Some(body));
        let response = self.transport.execute(request).await?;
        Ok((200..300).contains(&response.status))
    }

    fn request(
        &self,
        method: Method,
        endpoint: &str,
        interval: Option<u64>,
        extra_headers: Vec<(String, String)>,
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
        headers.extend(extra_headers);

        Request {
            method,
            url: endpoint.to_string(),
            headers,
            body,
        }
    }
}
