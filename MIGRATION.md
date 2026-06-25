# Migrating from 0.15.0 to 0.16.0

This guide covers the breaking API between the `0.15.0` and `0.16.0` releases.

## Feature enums now derive `FeatureKey`

`unleash-api-client` no longer uses `enum-map` to address typed feature flags.
Feature enums now implement `FeatureKey`, usually by deriving it.

Before:

```rust
use enum_map::Enum;
use serde::{Deserialize, Serialize};

#[allow(non_camel_case_types)]
#[derive(Debug, Deserialize, Serialize, Enum, Clone)]
enum UserFeatures {
    CheckoutRedesign,
}
```

After:

```rust
use unleash_api_client::client::FeatureKey;

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, FeatureKey)]
enum UserFeatures {
    CheckoutRedesign,
}
```

By default, the derived implementation uses the enum variant name as the
feature name. If the Unleash feature name differs from the Rust variant name,
use `#[feature_name("...")]`.

```rust
use unleash_api_client::client::FeatureKey;

#[derive(Debug, Clone, Copy, FeatureKey)]
enum UserFeatures {
    #[feature_name("checkout-redesign")]
    CheckoutRedesign,
}
```

The derive macro supports unit enum variants only. If you need a more complex
mapping, you can implement `FeatureKey` manually. Generally avoid doing so if you can help it.

```rust
use unleash_api_client::client::FeatureKey;

#[derive(Debug, Clone, Copy)]
enum UserFeatures {
    CheckoutRedesign,
}

impl FeatureKey for UserFeatures {
    fn name(self) -> &'static str {
        match self {
            UserFeatures::CheckoutRedesign => "checkout-redesign",
        }
    }
}
```

### Remove old feature enum dependencies

Feature enums no longer need these derives for SDK lookup:

```rust
use enum_map::Enum;
use serde::{Deserialize, Serialize};
```

You can remove `Enum`, `Deserialize`, and `Serialize` from your feature enum
derive list unless your own application code still needs them.

## Client no longer carries an HTTP client type parameter

In `0.15.0`, the client type and builder carried the concrete HTTP client as a
generic parameter.

Before:

```rust
use unleash_api_client::prelude::DefaultClient;

let client = ClientBuilder::default()
    .into_client::<UserFeatures, DefaultClient>(
        &config.api_url,
        &config.app_name,
        &config.instance_id,
        config.secret,
    )?;
```

After:

```rust
let client = ClientBuilder::default()
    .into_client::<UserFeatures>(
        &config.api_url,
        &config.app_name,
        &config.instance_id,
        config.secret,
    )?;
```

Any bounds on `HttpClient` in application code can usually be removed. The
client stores a transport internally, so most users only need the feature enum
type parameter.

## Custom HTTP implementations now use `Transport`

The old `HttpClient` abstraction has been replaced by a request/response
transport abstraction. This should only require a change if you previously
had a custom implementation for an HTTP client we don't support. Feel free
to open an issue requesting official support instead of maintaining a custom
implementation.

To update your custom handler implement `Transport` instead of `HttpClient`
and pass it with `into_client_with_transport`.

```rust
use std::sync::Arc;

use async_trait::async_trait;
use unleash_api_client::http::{Request, Response, Transport};

struct MyTransport;

#[async_trait]
impl Transport for MyTransport {
    async fn execute(&self, request: Request) -> Result<Response, anyhow::Error> {
        // Send the request with your HTTP stack.
        todo!("send {request:?}")
    }
}

let client = ClientBuilder::default()
    .into_client_with_transport::<UserFeatures>(
        &config.api_url,
        &config.app_name,
        &config.instance_id,
        config.secret,
        Arc::new(MyTransport),
    )?;
```

`Request` contains the HTTP method, full URL, headers, and optional body.
`Response` contains the numeric status and response body bytes. JSON encoding,
JSON decoding, and SDK headers are handled by the SDK.

## Cargo changes

The `FeatureKey` derive macro is provided by the companion
`unleash-api-client-macros` crate and is re-exported from
`unleash_api_client::client::FeatureKey`, so typical users do not need to add a
direct dependency on the macro crate.

If your application only used `enum-map`, `serde`, or `serde_derive` for SDK
feature enum support, those dependencies can be removed.
