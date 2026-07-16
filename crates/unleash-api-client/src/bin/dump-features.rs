// Copyright 2020 Cognite AS
use async_std::task;
use uuid::Uuid;

use unleash_api_client::config::EnvironmentConfig;
use unleash_api_client::http;
use unleash_yggdrasil::UpdateMessage;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    task::block_on(async {
        let config = EnvironmentConfig::from_env()?;
        let endpoint = unleash_api_client::api::features_endpoint(&config.api_url);
        let client = http::Http::new(
            http::default_transport(),
            config.app_name,
            config.instance_id,
            Uuid::new_v4().to_string(),
            config.secret,
        );
        let res: UpdateMessage = client.get_json(&endpoint, None).await?;
        dbg!(res);
        Ok(())
    })
}
