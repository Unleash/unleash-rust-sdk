// Copyright 2020 Cognite AS
//! The primary interface for users of the library.
use std::collections::hash_map::HashMap;
use std::default::Default;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use chrono::Utc;
use futures_timer::Delay;
use log::{debug, trace, warn};
use unleash_types::client_features::ClientFeatures as YggdrasilClientFeatures;

use unleash_yggdrasil::state::{EnrichedContext, ExternalResultsRef, PropertiesRef};
use unleash_yggdrasil::{EngineState, UpdateMessage};
use uuid::Uuid;

use crate::api::{Features, Metrics, MetricsBucket, Registration, ToggleMetrics};
use crate::context::Context;
use crate::http::{HttpClient, HTTP};
use crate::strategy;

// ----------------- Variant

/// Variant is returned from `Client.get_variant` and is a cut down and
/// ergonomic version of `api.get_variant`
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Variant {
    pub name: String,
    pub payload: HashMap<String, String>,
    pub enabled: bool,
}

impl Variant {
    fn disabled() -> Self {
        Self {
            name: "disabled".into(),
            ..Default::default()
        }
    }
}

// ----------------- ClientBuilder

pub struct ClientBuilder {
    disable_metric_submission: bool,
    enable_str_features: bool,
    interval: u64,
    strategies: HashMap<String, strategy::Strategy>,
}

impl ClientBuilder {
    pub fn into_client<F, C>(
        self,
        api_url: &str,
        app_name: &str,
        instance_id: &str,
        authorization: Option<String>,
    ) -> Result<Client<F, C>, C::Error>
    where
        F: FeatureKey,
        C: HttpClient + Default,
    {
        let connection_id = Uuid::new_v4().to_string();
        Ok(Client {
            api_url: api_url.into(),
            app_name: app_name.into(),
            disable_metric_submission: self.disable_metric_submission,
            enable_str_features: self.enable_str_features,
            instance_id: instance_id.into(),
            connection_id: connection_id.clone(),
            interval: self.interval,
            polling: AtomicBool::new(false),
            http: HTTP::new(
                app_name.into(),
                instance_id.into(),
                connection_id,
                authorization,
            )?,
            cached_state: ArcSwapOption::from(None),
            strategies: Mutex::new(self.strategies),
        })
    }

    pub fn disable_metric_submission(mut self) -> Self {
        self.disable_metric_submission = true;
        self
    }

    pub fn enable_string_features(mut self) -> Self {
        self.enable_str_features = true;
        self
    }

    pub fn interval(mut self, interval: u64) -> Self {
        self.interval = interval;
        self
    }

    pub fn strategy(mut self, name: &str, strategy: strategy::Strategy) -> Self {
        self.strategies.insert(name.into(), strategy);
        self
    }
}

impl Default for ClientBuilder {
    fn default() -> ClientBuilder {
        ClientBuilder {
            disable_metric_submission: false,
            enable_str_features: false,
            interval: 15000,
            strategies: Default::default(),
        }
    }
}

fn compute_custom_strategy_results<F: FeatureKey>(
    cache: &CachedState<F>,
    toggle_name: &str,
    ctx: &Context,
) -> Option<HashMap<String, bool>> {
    let evals = cache.memoized_custom_strategies.get(toggle_name)?;
    if evals.is_empty() {
        return None;
    }

    let mut out = HashMap::with_capacity(evals.len());
    for (i, ev) in evals.iter().enumerate() {
        out.insert(format!("customStrategy{}", i + 1), ev(ctx));
    }
    Some(out)
}

#[inline]
fn lock_engine<'a>(m: &'a Mutex<EngineState>) -> MutexGuard<'a, EngineState> {
    match m.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

pub trait FeatureKey: Copy + Debug + 'static {
    fn name(self) -> &'static str;
}

pub struct CachedState<F>
where
    F: FeatureKey,
{
    engine_state: Arc<Mutex<EngineState>>,
    memoized_custom_strategies: HashMap<String, Vec<strategy::Evaluate>>,
    // Use a phantom marker to tie this struct to the feature enum type - gives us nice compiler ergonomics
    _feature_type: PhantomData<fn() -> F>,
}

pub struct Client<F, C>
where
    F: FeatureKey,
    C: HttpClient,
{
    api_url: String,
    app_name: String,
    disable_metric_submission: bool,
    enable_str_features: bool,
    instance_id: String,
    connection_id: String,
    interval: u64,
    polling: AtomicBool,
    // Permits making extension calls to the Unleash API not yet modelled in the Rust SDK.
    pub http: HTTP<C>,
    // known strategies: strategy_name : memoiser
    strategies: Mutex<HashMap<String, strategy::Strategy>>,
    // memoised state: feature_name: [callback, callback, ...]
    cached_state: ArcSwapOption<CachedState<F>>,
}

impl<F, C> Client<F, C>
where
    F: FeatureKey,
    C: HttpClient + Default,
{
    /// The cached state can be accessed. It may be uninitialised, and
    /// represents a point in time snapshot: subsequent calls may have wound the
    /// metrics back, entirely lost string features etc.
    pub fn cached_state(&self) -> arc_swap::Guard<Option<Arc<CachedState<F>>>> {
        let cache = self.cached_state.load();
        if cache.is_none() {
            // No API state loaded
            trace!("is_enabled: No API state");
        }
        cache
    }

    /// Determine what variant (if any) of the feature the given context is
    /// selected for. With default stickiness the selection is consistent
    /// against user_id, session_id or randomly selected, in that order.
    /// Custom stickiness is respected but if the stickiness property is not
    /// present on the passed context, the variant will always be considered disabled.
    pub fn get_variant(&self, feature: F, ctx: &Context) -> Variant {
        self.get_variant_impl(feature.name(), ctx)
    }

    /// Determine what variant (if any) of the feature the given context is
    /// selected for. With default stickiness the selection is consistent
    /// against user_id, session_id or randomly selected, in that order.
    /// Custom stickiness is respected but if the stickiness property is not
    /// present on the passed context, the variant will always be considered disabled.
    pub fn get_variant_str(&self, feature_name: &str, ctx: &Context) -> Variant {
        assert!(
            self.enable_str_features,
            "String feature lookup not enabled"
        );
        self.get_variant_impl(feature_name, ctx)
    }

    fn get_variant_impl(&self, feature_name: &str, ctx: &Context) -> Variant {
        trace!("get_variant: feature {feature_name} context {ctx:?}");
        let cache = self.cached_state();
        let cache = match cache.as_ref() {
            None => {
                trace!("get_variant: feature {feature_name} no cached state");
                return Variant::disabled();
            }
            Some(cache) => cache,
        };

        // start mapping goop, this is fine for a spike but it's a bit noisy. Needs a small patch in Ygg to productionize cleanly
        let current_time_s;
        let remote_address_s;

        let current_time_ref: Option<&str> = match ctx.current_time.as_ref() {
            Some(dt) => {
                current_time_s = dt.to_rfc3339();
                Some(current_time_s.as_str())
            }
            None => None,
        };

        let remote_address_ref: Option<&str> = match ctx.remote_address.as_ref() {
            Some(ip) => {
                remote_address_s = ip.0.to_string();
                Some(remote_address_s.as_str())
            }
            None => None,
        };

        let ygg_context = EnrichedContext {
            user_id: ctx.user_id.as_deref(),
            session_id: ctx.session_id.as_deref(),
            environment: Some(ctx.environment.as_str()),
            app_name: Some(ctx.app_name.as_str()),
            current_time: current_time_ref,
            remote_address: remote_address_ref,
            properties: Some(PropertiesRef::Strings(&ctx.properties)),
            external_results: None,
            toggle_name: feature_name,
            runtime_hostname: None,
        };
        // end mapping goop

        let engine = lock_engine(&cache.engine_state);
        let variant = engine.check_variant(&ygg_context);

        if let Some(variant) = &variant {
            let feature_enabled = engine.check_enabled(&ygg_context).unwrap_or(false);
            engine.count_toggle(feature_name, feature_enabled);
            engine.count_variant(feature_name, variant.name.as_str());
            Variant {
                enabled: variant.enabled,
                name: variant.name.clone(),
                payload: variant
                    .payload
                    .as_ref()
                    .map(|payload| {
                        HashMap::from([
                            ("type".to_string(), payload.payload_type.clone()),
                            ("value".to_string(), payload.value.clone()),
                        ])
                    })
                    .unwrap_or_default(),
            }
        } else {
            // This branch executes only if the feature itself is missing
            engine.count_variant(feature_name, "disabled");
            Variant::disabled()
        }
    }

    /// Determine if the feature is enabled for the given context. With default
    /// stickiness the selection is consistent against user_id, session_id or
    /// randomly selected, in that order. Custom stickiness is respected but if the
    /// stickiness property is not present on the passed context, the variant will
    /// always be considered disabled.
    pub fn is_enabled(&self, feature_enum: F, context: Option<&Context>, default: bool) -> bool {
        self.is_enabled_impl(feature_enum.name(), context, default)
    }

    /// Determine if the feature is enabled for the given context. With default
    /// stickiness the selection is consistent against user_id, session_id or
    /// randomly selected, in that order. Custom stickiness is respected but if the
    /// stickiness property is not present on the passed context, the variant will
    /// always be considered disabled.
    pub fn is_enabled_str(
        &self,
        feature_name: &str,
        context: Option<&Context>,
        default: bool,
    ) -> bool {
        assert!(
            self.enable_str_features,
            "String feature lookup not enabled"
        );
        self.is_enabled_impl(feature_name, context, default)
    }

    fn is_enabled_impl(
        &self,
        feature_name: &str,
        context: Option<&Context>,
        default: bool,
    ) -> bool {
        trace!("is_enabled: feature {feature_name:?} default {default}, context {context:?}");
        let cache = self.cached_state();
        let cache = match cache.as_ref() {
            None => {
                trace!("is_enabled: feature {feature_name:?} no cached state");
                return false;
            }
            Some(cache) => cache,
        };

        let default_context = Context::default();
        let ctx = context.unwrap_or(&default_context);

        // start mapping goop, this is fine for a spike but it's a bit noisy. Needs a small patch in Ygg to productionize cleanly
        let current_time_s;
        let remote_address_s;

        let current_time_ref: Option<&str> = match ctx.current_time.as_ref() {
            Some(dt) => {
                current_time_s = dt.to_rfc3339();
                Some(current_time_s.as_str())
            }
            None => None,
        };

        let remote_address_ref: Option<&str> = match ctx.remote_address.as_ref() {
            Some(ip) => {
                remote_address_s = ip.0.to_string();
                Some(remote_address_s.as_str())
            }
            None => None,
        };

        let custom_results = compute_custom_strategy_results(cache, feature_name, ctx);
        let external_results = custom_results.as_ref().map(ExternalResultsRef::Strings);

        let ygg_context = EnrichedContext {
            user_id: ctx.user_id.as_deref(),
            session_id: ctx.session_id.as_deref(),
            environment: Some(ctx.environment.as_str()),
            app_name: Some(ctx.app_name.as_str()),
            current_time: current_time_ref,
            remote_address: remote_address_ref,
            properties: Some(PropertiesRef::Strings(&ctx.properties)),
            external_results: external_results,
            toggle_name: feature_name,
            runtime_hostname: None,
        };
        // end mapping goop

        let engine = lock_engine(&cache.engine_state);
        let enabled = engine.check_enabled(&ygg_context);
        if let Some(enabled) = enabled {
            engine.count_toggle(feature_name, enabled);
        }

        enabled.unwrap_or(default)
    }

    /// Memoize new features into the cached state
    ///
    /// Interior mutability is used, via the arc-swap crate.
    ///
    /// Note that this is primarily public to facilitate benchmarking;
    /// poll_for_updates is the usual way in which memoize will be called.
    pub fn memoize(
        &self,
        client_features: YggdrasilClientFeatures,
    ) -> Result<Option<Metrics>, Box<dyn std::error::Error + Send + Sync>> {
        self.memoize_update_message(UpdateMessage::FullResponse(client_features))
    }

    pub fn memoize_update_message(
        &self,
        update_message: UpdateMessage,
    ) -> Result<Option<Metrics>, Box<dyn std::error::Error + Send + Sync>> {
        let now = Utc::now();

        let prior_state = self
            .cached_state
            .load()
            .as_ref()
            .map(|cached_state| lock_engine(&cached_state.engine_state).get_state());

        let memoized_custom_strategies = strategy::compile_custom_strategies_for_state(
            &update_message,
            &self.strategies.lock().unwrap(),
        );

        let mut engine_state = EngineState::default();
        if let Some(state) = prior_state {
            let _ = engine_state.take_state(UpdateMessage::FullResponse(state));
        }
        if let Some(warnings) = engine_state.take_state(update_message) {
            for warning in warnings {
                warn!(
                    "Failed to compile toggle '{}' in Yggdrasil: {}",
                    warning.toggle_name, warning.message
                );
            }
        }

        let current_state = engine_state.get_state();
        trace!(
            "memoize: start with {} features",
            current_state.features.len()
        );
        let new_cache = CachedState {
            engine_state: Arc::new(Mutex::new(engine_state)),
            _feature_type: PhantomData,
            memoized_custom_strategies: memoized_custom_strategies,
        };
        // Now we have the new cache compiled, swap it in.
        let old = self.cached_state.swap(Some(Arc::new(new_cache)));
        trace!("memoize: swapped memoized state in");
        if let Some(old) = old {
            let mut engine_state = lock_engine(&old.engine_state);
            let Some(yggdrasil_metrics) = engine_state.get_metrics(now) else {
                return Ok(None);
            };
            let bucket = MetricsBucket {
                start: yggdrasil_metrics.start,
                stop: yggdrasil_metrics.stop,
                toggles: yggdrasil_metrics
                    .toggles
                    .into_iter()
                    .map(|(name, toggle)| {
                        (
                            name,
                            ToggleMetrics {
                                yes: toggle.yes as u64,
                                no: toggle.no as u64,
                                variants: toggle
                                    .variants
                                    .into_iter()
                                    .map(|(variant, count)| (variant, count as u64))
                                    .collect(),
                            },
                        )
                    })
                    .collect(),
            };
            let metrics = Metrics {
                app_name: self.app_name.clone(),
                instance_id: self.instance_id.clone(),
                connection_id: self.connection_id.clone(),
                bucket,
            };
            Ok(Some(metrics))
        } else {
            Ok(None)
        }
    }

    /// Query the API endpoint for features and push metrics
    ///
    /// Immediately and then every self.interval milliseconds the API server is
    /// queryed for features and the previous cycles metrics are uploaded.
    ///
    /// May be dropped, or will terminate at the next polling cycle after
    /// stop_poll is called().
    pub async fn poll_for_updates(&self) {
        // TODO: add an event / pipe to permit immediate exit.
        let endpoint = Features::endpoint(&self.api_url);
        let metrics_endpoint = Metrics::endpoint(&self.api_url);
        self.polling.store(true, Ordering::Relaxed);
        loop {
            debug!("poll: retrieving features");
            match self
                .http
                .get_json::<UpdateMessage>(&endpoint, Some(self.interval))
                .await
            {
                Ok(update_message) => match self.memoize_update_message(update_message) {
                    Ok(None) => {}
                    Ok(Some(metrics)) => {
                        if !self.disable_metric_submission {
                            let mut metrics_uploaded = false;
                            let res = self
                                .http
                                .post_json(&metrics_endpoint, metrics, Some(self.interval))
                                .await;
                            if let Ok(successful) = res {
                                if successful {
                                    metrics_uploaded = true;
                                    debug!("poll: uploaded feature metrics")
                                }
                            }
                            if !metrics_uploaded {
                                warn!("poll: error uploading feature metrics");
                            }
                        }
                    }
                    Err(err) => {
                        warn!("poll: failed to memoize features: {err:?}");
                    }
                },
                Err(err) => {
                    warn!("poll: failed to retrieve features: {err:?}");
                }
            }

            let duration = Duration::from_millis(self.interval);
            debug!("poll: waiting {duration:?}");
            Delay::new(duration).await;

            if !self.polling.load(Ordering::Relaxed) {
                return;
            }
        }
    }

    /// Register this client with the API endpoint.
    pub async fn register(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let registration = Registration {
            app_name: self.app_name.clone(),
            instance_id: self.instance_id.clone(),
            connection_id: self.connection_id.clone(),
            interval: self.interval,
            strategies: self
                .strategies
                .lock()
                .unwrap()
                .keys()
                .map(|s| s.to_owned())
                .collect(),
            ..Default::default()
        };
        let success = self
            .http
            .post_json(&Registration::endpoint(&self.api_url), &registration, None)
            .await
            .map_err(|err| anyhow::anyhow!(err))?;
        if !success {
            return Err(anyhow::anyhow!("Failed to register with unleash API server").into());
        }
        Ok(())
    }

    /// stop the poll_for_updates() function.
    ///
    /// If poll is not running, will wait-loop until poll_for_updates is
    /// running, then signal it to stop, then return. Will wait for ever if
    /// poll_for_updates never starts running.
    pub async fn stop_poll(&self) {
        loop {
            match self
                .polling
                .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {
                    return;
                }
                Err(_) => {
                    Delay::new(Duration::from_millis(50)).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use enum_map::Enum;
    use maplit::hashmap;
    use std::collections::hash_map::HashMap;
    use std::collections::hash_set::HashSet;
    use std::default::Default;
    use std::hash::BuildHasher;
    use unleash_yggdrasil::{EngineState, UpdateMessage};

    use super::{ClientBuilder, Variant};
    use crate::api::{self, Feature, Features, Strategy};
    use crate::client::FeatureKey;
    use crate::context::{Context, IPAddress};
    use crate::strategy;

    use crate::api::ConstraintExpression;

    use unleash_types::client_features::{
        ClientFeature, Constraint as YggdrasilConstraint, Operator as YggdrasilOperator,
        Override as YggdrasilOverride, Payload as YggdrasilPayload, Strategy as YggdrasilStrategy,
        Variant as YggdrasilVariant,
    };

    use unleash_types::client_features::ClientFeatures as YggdrasilClientFeatures;

    cfg_if::cfg_if! {
        if #[cfg(feature = "reqwest")] {
            use reqwest::Client as HttpClient;
        } else if #[cfg(feature = "reqwest-11")] {
            use reqwest_11::Client as HttpClient;
        } else {
            compile_error!("Cannot run test suite without a client enabled");
        }
    }

    fn features() -> Features {
        Features {
            version: 1,
            features: vec![
                Feature {
                    description: Some("default".to_string()),
                    enabled: true,
                    created_at: None,
                    variants: None,
                    name: "default".into(),
                    strategies: vec![Strategy {
                        name: "default".into(),
                        ..Default::default()
                    }],
                },
                Feature {
                    description: Some("userWithId".to_string()),
                    enabled: true,
                    created_at: None,
                    variants: None,
                    name: "userWithId".into(),
                    strategies: vec![Strategy {
                        name: "userWithId".into(),
                        parameters: Some(hashmap!["userIds".into()=>"present".into()]),
                        ..Default::default()
                    }],
                },
                Feature {
                    description: Some("userWithId+default".to_string()),
                    enabled: true,
                    created_at: None,
                    variants: None,
                    name: "userWithId+default".into(),
                    strategies: vec![
                        Strategy {
                            name: "userWithId".into(),
                            parameters: Some(hashmap!["userIds".into()=>"present".into()]),
                            ..Default::default()
                        },
                        Strategy {
                            name: "default".into(),
                            ..Default::default()
                        },
                    ],
                },
                Feature {
                    description: Some("disabled".to_string()),
                    enabled: false,
                    created_at: None,
                    variants: None,
                    name: "disabled".into(),
                    strategies: vec![Strategy {
                        name: "default".into(),
                        ..Default::default()
                    }],
                },
                Feature {
                    description: Some("nostrategies".to_string()),
                    enabled: true,
                    created_at: None,
                    variants: None,
                    name: "nostrategies".into(),
                    strategies: vec![],
                },
            ],
        }
    }

    #[test]
    fn test_memoization_enum() {
        let _ = simple_logger::SimpleLogger::new()
            .with_utc_timestamps()
            .with_module_level("isahc::agent", log::LevelFilter::Off)
            .with_module_level("tracing::span", log::LevelFilter::Off)
            .with_module_level("tracing::span::active", log::LevelFilter::Off)
            .init();
        let f = features();

        #[allow(non_camel_case_types)]
        #[derive(Debug, Enum, Clone, Copy)]
        enum UserFeatures {
            unknown,
            default,
            userWithId,
            userWithId_Default,
            disabled,
            nostrategies,
        }

        impl FeatureKey for UserFeatures {
            fn name(self) -> &'static str {
                match self {
                    UserFeatures::unknown => "unknown",
                    UserFeatures::default => "default",
                    UserFeatures::userWithId => "userWithId",
                    UserFeatures::userWithId_Default => "userWithId+default",
                    UserFeatures::disabled => "disabled",
                    UserFeatures::nostrategies => "nostrategies",
                }
            }
        }
        let c = ClientBuilder::default()
            .into_client::<UserFeatures, HttpClient>("http://127.0.0.1:1234/", "foo", "test", None)
            .unwrap();

        c.memoize(api_features_to_yggdrasil(f)).unwrap();
        let present: Context = Context {
            user_id: Some("present".into()),
            ..Default::default()
        };
        let missing: Context = Context {
            user_id: Some("missing".into()),
            ..Default::default()
        };
        // features unknown on the server should honour the default
        assert!(!c.is_enabled(UserFeatures::unknown, None, false));
        assert!(c.is_enabled(UserFeatures::unknown, None, true));
        // default should be enabled, no context needed
        assert!(c.is_enabled(UserFeatures::default, None, false));
        // user present should be present on userWithId
        assert!(c.is_enabled(UserFeatures::userWithId, Some(&present), false));
        // user missing should not
        assert!(!c.is_enabled(UserFeatures::userWithId, Some(&missing), false));
        // user missing should be present on userWithId+default
        assert!(c.is_enabled(UserFeatures::userWithId_Default, Some(&missing), false));
        // disabled should be disabled
        assert!(!c.is_enabled(UserFeatures::disabled, None, true));
        // no strategies should result in enabled features.
        assert!(c.is_enabled(UserFeatures::nostrategies, None, false));
    }

    #[test]
    fn test_memoization_strs() {
        let _ = simple_logger::SimpleLogger::new()
            .with_utc_timestamps()
            .with_module_level("isahc::agent", log::LevelFilter::Off)
            .with_module_level("tracing::span", log::LevelFilter::Off)
            .with_module_level("tracing::span::active", log::LevelFilter::Off)
            .init();
        let f = features();
        // And with plain old strings
        #[derive(Debug, Enum, Clone, Copy)]
        enum NoFeatures {}

        impl FeatureKey for NoFeatures {
            fn name(self) -> &'static str {
                unreachable!()
            }
        }

        let c = ClientBuilder::default()
            .enable_string_features()
            .into_client::<NoFeatures, HttpClient>("http://127.0.0.1:1234/", "foo", "test", None)
            .unwrap();

        c.memoize(api_features_to_yggdrasil(f)).unwrap();
        let present: Context = Context {
            user_id: Some("present".into()),
            ..Default::default()
        };
        let missing: Context = Context {
            user_id: Some("missing".into()),
            ..Default::default()
        };
        // features unknown on the server should honour the default
        assert!(!c.is_enabled_str("unknown", None, false));
        assert!(c.is_enabled_str("unknown", None, true));
        // default should be enabled, no context needed
        assert!(c.is_enabled_str("default", None, false));
        // user present should be present on userWithId
        assert!(c.is_enabled_str("userWithId", Some(&present), false));
        // user missing should not
        assert!(!c.is_enabled_str("userWithId", Some(&missing), false));
        // user missing should be present on userWithId+default
        assert!(c.is_enabled_str("userWithId+default", Some(&missing), false));
        // disabled should be disabled
        assert!(!c.is_enabled_str("disabled", None, true));
        // no strategies should result in enabled features.
        assert!(c.is_enabled_str("nostrategies", None, false));
    }

    fn _reversed_uids<S: BuildHasher>(
        parameters: Option<HashMap<String, String, S>>,
    ) -> strategy::Evaluate {
        let mut uids: HashSet<String> = HashSet::new();
        if let Some(parameters) = parameters {
            if let Some(uids_list) = parameters.get("userIds") {
                for uid in uids_list.split(',') {
                    uids.insert(uid.chars().rev().collect());
                }
            }
        }
        Box::new(move |context: &Context| -> bool {
            context
                .user_id
                .as_ref()
                .map(|uid| uids.contains(uid))
                .unwrap_or(false)
        })
    }

    #[test]
    fn test_custom_strategy() {
        let _ = simple_logger::SimpleLogger::new()
            .with_utc_timestamps()
            .with_module_level("isahc::agent", log::LevelFilter::Off)
            .with_module_level("tracing::span", log::LevelFilter::Off)
            .with_module_level("tracing::span::active", log::LevelFilter::Off)
            .init();
        #[allow(non_camel_case_types)]
        #[derive(Debug, Copy, Clone)]
        enum UserFeatures {
            default,
            reversed,
        }

        impl FeatureKey for UserFeatures {
            fn name(self) -> &'static str {
                match self {
                    UserFeatures::default => "default",
                    UserFeatures::reversed => "reversed",
                }
            }
        }

        let client = ClientBuilder::default()
            .strategy("reversed", Box::new(&_reversed_uids))
            .into_client::<UserFeatures, HttpClient>("http://127.0.0.1:1234/", "foo", "test", None)
            .unwrap();

        let f = Features {
            version: 1,
            features: vec![
                Feature {
                    description: Some("default".to_string()),
                    enabled: true,
                    created_at: None,
                    variants: None,
                    name: "default".into(),
                    strategies: vec![Strategy {
                        name: "default".into(),
                        ..Default::default()
                    }],
                },
                Feature {
                    description: Some("reversed".to_string()),
                    enabled: true,
                    created_at: None,
                    variants: None,
                    name: "reversed".into(),
                    strategies: vec![Strategy {
                        name: "reversed".into(),
                        parameters: Some(hashmap!["userIds".into()=>"abc".into()]),
                        ..Default::default()
                    }],
                },
            ],
        };
        client.memoize(api_features_to_yggdrasil(f)).unwrap();
        let present: Context = Context {
            user_id: Some("cba".into()),
            ..Default::default()
        };
        let missing: Context = Context {
            user_id: Some("abc".into()),
            ..Default::default()
        };
        // user cba should be present on reversed
        assert!(client.is_enabled(UserFeatures::reversed, Some(&present), false));
        // user abc should not
        assert!(!client.is_enabled(UserFeatures::reversed, Some(&missing), false));
        // adding custom strategies shouldn't disable built-in ones
        // default should be enabled, no context needed
        assert!(client.is_enabled(UserFeatures::default, None, false));
    }

    fn variant_features() -> Features {
        Features {
            version: 1,
            features: vec![
                Feature {
                    description: Some("disabled".to_string()),
                    enabled: false,
                    created_at: None,
                    variants: None,
                    name: "disabled".into(),
                    strategies: vec![],
                },
                Feature {
                    description: Some("novariants".to_string()),
                    enabled: true,
                    created_at: None,
                    variants: None,
                    name: "novariants".into(),
                    strategies: vec![Strategy {
                        name: "default".into(),
                        ..Default::default()
                    }],
                },
                Feature {
                    description: Some("one".to_string()),
                    enabled: true,
                    created_at: None,
                    variants: Some(vec![api::Variant {
                        name: "variantone".into(),
                        weight: 100,
                        payload: Some(hashmap![
                            "type".into() => "string".into(),
                            "value".into() => "val1".into()]),
                        overrides: None,
                    }]),
                    name: "one".into(),
                    strategies: vec![],
                },
                Feature {
                    description: Some("two".to_string()),
                    enabled: true,
                    created_at: None,
                    variants: Some(vec![
                        api::Variant {
                            name: "variantone".into(),
                            weight: 50,
                            payload: Some(hashmap![
                            "type".into() => "string".into(),
                            "value".into() => "val1".into()]),
                            overrides: None,
                        },
                        api::Variant {
                            name: "varianttwo".into(),
                            weight: 50,
                            payload: Some(hashmap![
                            "type".into() => "string".into(),
                            "value".into() => "val2".into()]),
                            overrides: None,
                        },
                    ]),
                    name: "two".into(),
                    strategies: vec![],
                },
                Feature {
                    description: Some("nostrategies".to_string()),
                    enabled: true,
                    created_at: None,
                    variants: None,
                    name: "nostrategies".into(),
                    strategies: vec![],
                },
            ],
        }
    }

    #[test]
    fn variants_enum() {
        let _ = simple_logger::SimpleLogger::new()
            .with_utc_timestamps()
            .with_module_level("isahc::agent", log::LevelFilter::Off)
            .with_module_level("tracing::span", log::LevelFilter::Off)
            .with_module_level("tracing::span::active", log::LevelFilter::Off)
            .init();
        let f = variant_features();
        // with an enum
        #[allow(non_camel_case_types)]
        #[derive(Debug, Enum, Clone, Copy)]
        enum UserFeatures {
            disabled,
            novariants,
            one,
            two,
        }

        impl FeatureKey for UserFeatures {
            fn name(self) -> &'static str {
                match self {
                    UserFeatures::disabled => "disabled",
                    UserFeatures::novariants => "novariants",
                    UserFeatures::one => "one",
                    UserFeatures::two => "two",
                }
            }
        }

        let c = ClientBuilder::default()
            .into_client::<UserFeatures, HttpClient>("http://127.0.0.1:1234/", "foo", "test", None)
            .unwrap();

        c.memoize(api_features_to_yggdrasil(f)).unwrap();

        // disabled should be disabled
        let variant = Variant::disabled();
        assert_eq!(
            variant,
            c.get_variant(UserFeatures::disabled, &Context::default())
        );

        // enabled no variants should get the disabled variant
        let variant = Variant::disabled();
        assert_eq!(
            variant,
            c.get_variant(UserFeatures::novariants, &Context::default())
        );

        // One variant
        let variant = Variant {
            name: "variantone".to_string(),
            payload: hashmap![
                "type".into()=>"string".into(),
                "value".into()=>"val1".into()
            ],
            enabled: true,
        };
        assert_eq!(
            variant,
            c.get_variant(UserFeatures::one, &Context::default())
        );

        // Two variants
        let uid1: Context = Context {
            user_id: Some("user1".into()),
            ..Default::default()
        };
        let session1: Context = Context {
            session_id: Some("session1".into()),
            ..Default::default()
        };
        let host1: Context = Context {
            remote_address: Some(IPAddress("10.10.10.10".parse().unwrap())),
            ..Default::default()
        };
        let uid_variant = c.get_variant(UserFeatures::two, &uid1);
        let session_variant = c.get_variant(UserFeatures::two, &session1);
        let host_variant = c.get_variant(UserFeatures::two, &host1);
        assert!(uid_variant.enabled);
        assert!(session_variant.enabled);
        assert!(host_variant.enabled);
        assert!(uid_variant.name == "variantone" || uid_variant.name == "varianttwo");
        assert!(session_variant.name == "variantone" || session_variant.name == "varianttwo");
        assert!(host_variant.name == "variantone" || host_variant.name == "varianttwo");
        assert_eq!(uid_variant, c.get_variant(UserFeatures::two, &uid1));
        assert_eq!(session_variant, c.get_variant(UserFeatures::two, &session1));
    }

    #[test]
    fn variants_str() {
        let _ = simple_logger::SimpleLogger::new()
            .with_utc_timestamps()
            .with_module_level("isahc::agent", log::LevelFilter::Off)
            .with_module_level("tracing::span", log::LevelFilter::Off)
            .with_module_level("tracing::span::active", log::LevelFilter::Off)
            .init();
        let f = variant_features();
        // without the enum API
        #[derive(Debug, Enum, Clone, Copy)]
        enum NoFeatures {}

        impl FeatureKey for NoFeatures {
            fn name(self) -> &'static str {
                unreachable!()
            }
        }

        let c = ClientBuilder::default()
            .enable_string_features()
            .into_client::<NoFeatures, HttpClient>("http://127.0.0.1:1234/", "foo", "test", None)
            .unwrap();

        c.memoize(api_features_to_yggdrasil(f)).unwrap();

        // disabled should be disabled
        let variant = Variant::disabled();
        assert_eq!(variant, c.get_variant_str("disabled", &Context::default()));

        // enabled no variants should get the disabled variant
        let variant = Variant::disabled();
        assert_eq!(
            variant,
            c.get_variant_str("novariants", &Context::default())
        );

        // One variant
        let variant = Variant {
            name: "variantone".to_string(),
            payload: hashmap![
                "type".into()=>"string".into(),
                "value".into()=>"val1".into()
            ],
            enabled: true,
        };
        assert_eq!(variant, c.get_variant_str("one", &Context::default()));

        // Two variants
        let uid1: Context = Context {
            user_id: Some("user1".into()),
            ..Default::default()
        };
        let session1: Context = Context {
            session_id: Some("session1".into()),
            ..Default::default()
        };
        let host1: Context = Context {
            remote_address: Some(IPAddress("10.10.10.10".parse().unwrap())),
            ..Default::default()
        };
        let uid_variant = c.get_variant_str("two", &uid1);
        let session_variant = c.get_variant_str("two", &session1);
        let host_variant = c.get_variant_str("two", &host1);
        assert!(uid_variant.enabled);
        assert!(session_variant.enabled);
        assert!(host_variant.enabled);
        assert!(uid_variant.name == "variantone" || uid_variant.name == "varianttwo");
        assert!(session_variant.name == "variantone" || session_variant.name == "varianttwo");
        assert!(host_variant.name == "variantone" || host_variant.name == "varianttwo");
        assert_eq!(uid_variant, c.get_variant_str("two", &uid1));
        assert_eq!(session_variant, c.get_variant_str("two", &session1));
    }

    #[test]
    fn variant_metrics() {
        let _ = simple_logger::SimpleLogger::new()
            .with_utc_timestamps()
            .with_module_level("isahc::agent", log::LevelFilter::Off)
            .with_module_level("tracing::span", log::LevelFilter::Off)
            .with_module_level("tracing::span::active", log::LevelFilter::Off)
            .init();
        let f = variant_features();
        // with an enum
        #[allow(non_camel_case_types)]
        #[derive(Debug, Enum, Clone, Copy)]
        enum UserFeatures {
            disabled,
            novariants,
            one,
            two,
        }

        impl FeatureKey for UserFeatures {
            fn name(self) -> &'static str {
                match self {
                    UserFeatures::disabled => "disabled",
                    UserFeatures::novariants => "novariants",
                    UserFeatures::one => "one",
                    UserFeatures::two => "two",
                }
            }
        }

        let c = ClientBuilder::default()
            .into_client::<UserFeatures, HttpClient>("http://127.0.0.1:1234/", "foo", "test", None)
            .unwrap();

        c.memoize(api_features_to_yggdrasil(f)).unwrap();

        c.get_variant(UserFeatures::disabled, &Context::default());
        c.get_variant(UserFeatures::novariants, &Context::default());

        let session1: Context = Context {
            session_id: Some("session1".into()),
            ..Default::default()
        };

        let host1: Context = Context {
            remote_address: Some(IPAddress("10.10.10.10".parse().unwrap())),
            ..Default::default()
        };
        c.get_variant(UserFeatures::two, &session1);
        c.get_variant(UserFeatures::two, &host1);

        let metrics = c
            .memoize(api_features_to_yggdrasil(variant_features()))
            .unwrap()
            .unwrap();
        let variant_count = |feature_name, variant_name| -> u64 {
            metrics
                .bucket
                .toggles
                .get(feature_name)
                .unwrap()
                .variants
                .get(variant_name)
                .copied()
                .unwrap_or(0)
        };
        let disabled_variant_count =
            |feature_name| -> u64 { variant_count(feature_name, "disabled") };

        assert_eq!(disabled_variant_count("disabled"), 1);
        assert_eq!(disabled_variant_count("novariants"), 1);
        assert_eq!(
            variant_count("two", "variantone") + variant_count("two", "varianttwo"),
            2
        );
    }

    #[test]
    fn variant_metrics_str() {
        let _ = simple_logger::SimpleLogger::new()
            .with_utc_timestamps()
            .with_module_level("isahc::agent", log::LevelFilter::Off)
            .with_module_level("tracing::span", log::LevelFilter::Off)
            .with_module_level("tracing::span::active", log::LevelFilter::Off)
            .init();
        let f = variant_features();
        // with an enum
        #[allow(non_camel_case_types)]
        #[derive(Debug, Enum, Clone, Copy)]
        enum NoFeatures {}
        impl FeatureKey for NoFeatures {
            fn name(self) -> &'static str {
                unreachable!()
            }
        }
        let c = ClientBuilder::default()
            .enable_string_features()
            .into_client::<NoFeatures, HttpClient>("http://127.0.0.1:1234/", "foo", "test", None)
            .unwrap();

        c.memoize(api_features_to_yggdrasil(f)).unwrap();

        c.get_variant_str("disabled", &Context::default());
        c.get_variant_str("novariants", &Context::default());

        let session1: Context = Context {
            session_id: Some("session1".into()),
            ..Default::default()
        };

        let host1: Context = Context {
            remote_address: Some(IPAddress("10.10.10.10".parse().unwrap())),
            ..Default::default()
        };
        c.get_variant_str("two", &session1);
        c.get_variant_str("two", &host1);

        // Metrics should also be tracked for features that don't exist.
        c.get_variant_str("nonexistent-feature", &Context::default());
        c.get_variant_str("nonexistent-feature", &Context::default());

        let metrics = c
            .memoize(api_features_to_yggdrasil(variant_features()))
            .unwrap()
            .unwrap();
        let variant_count = |feature_name, variant_name| -> u64 {
            metrics
                .bucket
                .toggles
                .get(feature_name)
                .unwrap()
                .variants
                .get(variant_name)
                .copied()
                .unwrap_or(0)
        };
        let disabled_variant_count =
            |feature_name| -> u64 { variant_count(feature_name, "disabled") };

        assert_eq!(disabled_variant_count("disabled"), 1);
        assert_eq!(disabled_variant_count("novariants"), 1);
        assert_eq!(
            variant_count("two", "variantone") + variant_count("two", "varianttwo"),
            2
        );
        assert_eq!(variant_count("nonexistent-feature", "disabled"), 2);
    }

    #[test]
    fn yggdrasil_usage() {
        let client_features = YggdrasilClientFeatures {
            ..Default::default()
        };

        let update_message = UpdateMessage::FullResponse(client_features);

        let mut engine = EngineState::default();
        engine.take_state(update_message);

        let context = unleash_yggdrasil::Context {
            user_id: Some("user-id".to_string()),
            session_id: Some("session-id".to_string()),
            remote_address: Some("10.0.0.1".to_string()),
            properties: Some(HashMap::new()),
            app_name: Some("app".to_string()),
            environment: Some("default".to_string()),
            current_time: None,
        };

        engine.is_enabled("test", &context, &None);
        engine.get_variant("test", &context, &None);

        engine.count_toggle("test", true);
        engine.count_variant("test", "variantone");
    }

    fn api_constraint_to_yggdrasil(constraint: api::Constraint) -> Option<YggdrasilConstraint> {
        let (operator, values, value) = match constraint.expression {
            ConstraintExpression::DateAfter { value } => {
                (YggdrasilOperator::DateAfter, None, Some(value.to_rfc3339()))
            }
            ConstraintExpression::DateBefore { value } => (
                YggdrasilOperator::DateBefore,
                None,
                Some(value.to_rfc3339()),
            ),
            ConstraintExpression::In { values } => (YggdrasilOperator::In, Some(values), None),
            ConstraintExpression::NotIn { values } => {
                (YggdrasilOperator::NotIn, Some(values), None)
            }
            ConstraintExpression::NumEq { value } => {
                (YggdrasilOperator::NumEq, None, Some(value.to_string()))
            }
            ConstraintExpression::NumGT { value } => {
                (YggdrasilOperator::NumGt, None, Some(value.to_string()))
            }
            ConstraintExpression::NumGTE { value } => {
                (YggdrasilOperator::NumGte, None, Some(value.to_string()))
            }
            ConstraintExpression::NumLT { value } => {
                (YggdrasilOperator::NumLt, None, Some(value.to_string()))
            }
            ConstraintExpression::NumLTE { value } => {
                (YggdrasilOperator::NumLte, None, Some(value.to_string()))
            }
            ConstraintExpression::SemverEq { value } => {
                (YggdrasilOperator::SemverEq, None, Some(value.to_string()))
            }
            ConstraintExpression::SemverGT { value } => {
                (YggdrasilOperator::SemverGt, None, Some(value.to_string()))
            }
            ConstraintExpression::SemverLT { value } => {
                (YggdrasilOperator::SemverLt, None, Some(value.to_string()))
            }
            ConstraintExpression::StrContains { values } => {
                (YggdrasilOperator::StrContains, Some(values), None)
            }
            ConstraintExpression::StrStartsWith { values } => {
                (YggdrasilOperator::StrStartsWith, Some(values), None)
            }
            ConstraintExpression::StrEndsWith { values } => {
                (YggdrasilOperator::StrEndsWith, Some(values), None)
            }
            ConstraintExpression::Unknown(_) => (YggdrasilOperator::In, Some(Vec::new()), None),
        };
        Some(YggdrasilConstraint {
            context_name: constraint.context_name,
            operator,
            case_insensitive: constraint.case_insensitive,
            inverted: constraint.inverted,
            values,
            value,
        })
    }

    fn api_strategy_to_yggdrasil(strategy: api::Strategy) -> YggdrasilStrategy {
        YggdrasilStrategy {
            name: strategy.name,
            sort_order: None,
            segments: None,
            constraints: strategy
                .constraints
                .map(|constraints| {
                    constraints
                        .into_iter()
                        .filter_map(api_constraint_to_yggdrasil)
                        .collect::<Vec<_>>()
                })
                .filter(|constraints| !constraints.is_empty()),
            parameters: strategy.parameters,
            variants: None,
        }
    }

    fn api_variant_to_yggdrasil(variant: api::Variant) -> YggdrasilVariant {
        let payload = variant.payload.and_then(|payload| {
            let payload_type = payload.get("type").cloned();
            let value = payload.get("value").cloned();
            match (payload_type, value) {
                (Some(payload_type), Some(value)) => Some(YggdrasilPayload {
                    payload_type,
                    value,
                }),
                _ => None,
            }
        });

        YggdrasilVariant {
            name: variant.name,
            weight: i32::from(variant.weight),
            weight_type: None,
            stickiness: None,
            payload,
            overrides: variant.overrides.map(|overrides| {
                overrides
                    .into_iter()
                    .map(|override_value| YggdrasilOverride {
                        context_name: override_value.context_name,
                        values: override_value.values,
                    })
                    .collect()
            }),
        }
    }

    fn client_feature_to_yggdrasil(feature: Feature) -> ClientFeature {
        ClientFeature {
            name: feature.name,
            feature_type: None,
            description: feature.description,
            created_at: feature.created_at,
            last_seen_at: None,
            enabled: feature.enabled,
            stale: None,
            impression_data: None,
            project: None,
            strategies: Some(
                feature
                    .strategies
                    .into_iter()
                    .map(api_strategy_to_yggdrasil)
                    .collect(),
            ),
            variants: feature
                .variants
                .map(|variants| variants.into_iter().map(api_variant_to_yggdrasil).collect()),
            dependencies: None,
        }
    }

    fn api_features_to_yggdrasil(features: Features) -> YggdrasilClientFeatures {
        YggdrasilClientFeatures {
            version: features.version.into(),
            features: features
                .features
                .into_iter()
                .map(client_feature_to_yggdrasil)
                .collect(),
            segments: None,
            query: None,
            meta: None,
        }
    }
}
