// Copyright 2020, 2022 Cognite AS
//! The HTTP layer.

#[cfg(any(feature = "reqwest", feature = "reqwest-11", feature = "reqwest-13"))]
pub mod reqwest;
pub mod transport;

#[cfg(any(feature = "reqwest", feature = "reqwest-11", feature = "reqwest-13"))]
pub use reqwest::default_transport;
pub use transport::{Http, Method, Request, Response, Transport, TransportRef};
