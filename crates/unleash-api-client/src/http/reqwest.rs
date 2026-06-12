use std::sync::Arc;

use async_trait::async_trait;

use super::{Method, Request, Response, Transport, TransportRef};

#[cfg(feature = "reqwest")]
type ReqwestClient = ::reqwest::Client;
#[cfg(all(not(feature = "reqwest"), feature = "reqwest-11"))]
type ReqwestClient = reqwest_11::Client;
#[cfg(all(
    not(any(feature = "reqwest", feature = "reqwest-11")),
    feature = "reqwest-13"
))]
type ReqwestClient = reqwest_13::Client;

pub struct ReqwestTransport {
    client: ReqwestClient,
}

impl ReqwestTransport {
    pub fn new(client: ReqwestClient) -> Self {
        Self { client }
    }
}

impl Default for ReqwestTransport {
    fn default() -> Self {
        Self::new(ReqwestClient::default())
    }
}

#[async_trait]
impl Transport for ReqwestTransport {
    async fn execute(&self, request: Request) -> Result<Response, anyhow::Error> {
        let mut builder = match request.method {
            Method::Get => self.client.get(&request.url),
            Method::Post => self.client.post(&request.url),
        };
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        if let Some(body) = request.body {
            builder = builder
                .header("content-type", "application/json")
                .body(body);
        }

        let response = builder.send().await?;
        let status = response.status().as_u16();
        let body = response.bytes().await?.to_vec();

        Ok(Response { status, body })
    }
}

pub fn default_transport() -> TransportRef {
    Arc::new(ReqwestTransport::default())
}
