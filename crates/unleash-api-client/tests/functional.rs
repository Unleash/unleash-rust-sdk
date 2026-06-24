// Copyright 2020 Cognite AS

//! Functional test against an unleashed API server running locally.
//! Set environment variables as per config.rs to exercise this.
//!
//! Currently expects a feature called default with one strategy default
//! Additional features are ignored.

#[cfg(feature = "functional")]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;
    use std::{future::Future, pin::Pin, task};

    use async_trait::async_trait;
    use futures_timer::Delay;

    use unleash_api_client::{
        client::{self, FeatureKey},
        config::EnvironmentConfig,
        http::Transport,
    };

    #[allow(non_camel_case_types)]
    #[derive(Debug, Copy, Clone, FeatureKey)]
    enum UserFeatures {
        default,
    }

    #[async_trait]
    trait AsyncImpl {
        type JoinHandle: Future<Output = ()>;
        fn spawn<F>(f: F) -> Self::JoinHandle
        where
            F: Future<Output = ()> + Send + 'static;

        async fn sleep(d: Duration);
    }

    #[cfg(any(feature = "reqwest", feature = "reqwest-11", feature = "reqwest-13"))]
    struct TokioJoinHandle {
        inner: tokio::task::JoinHandle<()>,
    }

    impl Unpin for TokioJoinHandle {}

    impl Future for TokioJoinHandle {
        type Output = ();

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut task::Context<'_>,
        ) -> core::task::Poll<Self::Output> {
            let inner = Pin::new(&mut self.inner);
            match inner.poll(cx) {
                core::task::Poll::Pending => core::task::Poll::Pending,
                core::task::Poll::Ready(r) => core::task::Poll::Ready(r.unwrap()),
            }
        }
    }

    #[cfg(any(feature = "reqwest", feature = "reqwest-11", feature = "reqwest-13"))]
    struct TokioAsync;
    #[cfg(any(feature = "reqwest", feature = "reqwest-11", feature = "reqwest-13"))]
    #[async_trait]
    impl AsyncImpl for TokioAsync {
        type JoinHandle = TokioJoinHandle;
        fn spawn<F>(f: F) -> Self::JoinHandle
        where
            F: Future<Output = ()> + Send + 'static,
        {
            TokioJoinHandle {
                inner: tokio::spawn(f),
            }
        }

        async fn sleep(d: Duration) {
            tokio::time::sleep(d).await
        }
    }

    async fn test_smoke_async<T>() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>
    where
        T: Transport + Default + 'static,
    {
        let _ = simple_logger::init();

        let config = EnvironmentConfig::from_env()?;
        let client = client::ClientBuilder::default()
            .interval(500)
            .into_client_with_transport::<UserFeatures>(
                &config.api_url,
                &config.app_name,
                &config.instance_id,
                config.secret,
                Arc::new(T::default()),
            )?;
        client.register().await?;
        futures::future::join(client.poll_for_updates(), async {
            // Ensure we have features
            Delay::new(Duration::from_millis(500)).await;
            assert!(client.is_enabled(UserFeatures::default, None, false));
            // Ensure the metrics get up-loaded
            Delay::new(Duration::from_millis(500)).await;
            client.stop_poll().await;
        })
        .await;
        println!("got metrics");
        Ok(())
    }

    #[cfg(feature = "reqwest")]
    #[tokio::test]
    async fn test_smoke_async_reqwest() {
        test_smoke_async::<unleash_api_client::http::reqwest::ReqwestTransport>()
            .await
            .unwrap();
    }
    #[cfg(all(feature = "reqwest-11", not(feature = "reqwest")))]
    #[tokio::test]
    async fn test_smoke_async_reqwest() {
        test_smoke_async::<unleash_api_client::http::reqwest::ReqwestTransport>()
            .await
            .unwrap();
    }
    #[cfg(all(
        feature = "reqwest-13",
        not(any(feature = "reqwest", feature = "reqwest-11"))
    ))]
    #[tokio::test]
    async fn test_smoke_async_reqwest() {
        test_smoke_async::<unleash_api_client::http::reqwest::ReqwestTransport>()
            .await
            .unwrap();
    }

    async fn test_smoke_threaded<T, A>(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>
    where
        T: Transport + Default + 'static,
        A: AsyncImpl,
    {
        let _ = simple_logger::init();
        let config = EnvironmentConfig::from_env()?;
        let client = Arc::new(
            client::ClientBuilder::default()
                .interval(500)
                .into_client_with_transport::<_>(
                    &config.api_url,
                    &config.app_name,
                    &config.instance_id,
                    config.secret,
                    Arc::new(T::default()),
                )?,
        );

        if let Err(e) = client.register().await {
            Err(e)
        } else {
            Ok(())
        }?;
        // Spin off a polling thread
        let poll_handle = client.clone();
        let handler = A::spawn(async move {
            // thread code
            poll_handle.poll_for_updates().await;
        });

        // Ensure we have features
        A::sleep(Duration::from_millis(500)).await;
        assert!(client.is_enabled(UserFeatures::default, None, false));
        // Ensure the metrics get up-loaded
        A::sleep(Duration::from_millis(500)).await;
        client.stop_poll().await;

        handler.await;
        println!("got metrics");
        Ok(())
    }

    #[cfg(feature = "reqwest")]
    #[tokio::test]
    async fn test_smoke_threaded_reqwest() {
        test_smoke_threaded::<unleash_api_client::http::reqwest::ReqwestTransport, TokioAsync>()
            .await
            .unwrap();
    }
    #[cfg(all(feature = "reqwest-11", not(feature = "reqwest")))]
    #[tokio::test]
    async fn test_smoke_threaded_reqwest() {
        test_smoke_threaded::<unleash_api_client::http::reqwest::ReqwestTransport, TokioAsync>()
            .await
            .unwrap();
    }
    #[cfg(all(
        feature = "reqwest-13",
        not(any(feature = "reqwest", feature = "reqwest-11"))
    ))]
    #[tokio::test]
    async fn test_smoke_threaded_reqwest() {
        test_smoke_threaded::<unleash_api_client::http::reqwest::ReqwestTransport, TokioAsync>()
            .await
            .unwrap();
    }
}
