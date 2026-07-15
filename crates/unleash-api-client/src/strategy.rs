// Copyright 2020 Cognite AS
//! <https://docs.getunleash.io/user_guide/activation_strategy>
use std::collections::hash_map::HashMap;
use std::hash::BuildHasher;

use log::warn;
use unleash_types::client_features::ClientFeature;
use unleash_yggdrasil::{UpdateMessage, KNOWN_STRATEGIES};

use crate::context::Context;

/// Memoise feature state for a strategy.
pub type Strategy =
    Box<dyn Fn(Option<HashMap<String, String>>) -> Evaluate + Sync + Send + 'static>;
/// Apply memoised state to a context.
pub trait Evaluator: Fn(&Context) -> bool {
    fn clone_boxed(&self) -> Box<dyn Evaluator + Send + Sync + 'static>;
}
pub type Evaluate = Box<dyn Evaluator + Send + Sync + 'static>;

impl<T> Evaluator for T
where
    T: 'static + Clone + Sync + Send + Fn(&Context) -> bool,
{
    fn clone_boxed(&self) -> Box<dyn Evaluator + Send + Sync + 'static> {
        Box::new(T::clone(self))
    }
}

impl Clone for Box<dyn Evaluator + Send + Sync + 'static> {
    fn clone(&self) -> Self {
        self.as_ref().clone_boxed()
    }
}

/// <https://docs.getunleash.io/user_guide/activation_strategy#standard>
pub fn default<S: BuildHasher>(_: Option<HashMap<String, String, S>>) -> Evaluate {
    Box::new(|_: &Context| -> bool { true })
}

fn extract_features(update: &UpdateMessage) -> Vec<&ClientFeature> {
    match update {
        UpdateMessage::FullResponse(features) => features.features.iter().collect(),
        UpdateMessage::PartialUpdate(delta) => {
            let mut all_features = vec![];
            for event in &delta.events {
                match event {
                    unleash_types::client_features::DeltaEvent::Hydration { features, .. } => {
                        all_features.extend(features.iter());
                    }
                    unleash_types::client_features::DeltaEvent::FeatureUpdated {
                        feature, ..
                    } => all_features.push(feature),
                    unleash_types::client_features::DeltaEvent::FeatureRemoved {
                        feature_name,
                        ..
                    } => {
                        all_features.retain(|f| f.name != *feature_name);
                    }
                    _ => {}
                }
            }
            all_features
        }
    }
}

pub(crate) fn compile_custom_strategies_for_state(
    update: &UpdateMessage,
    registry: &HashMap<String, Strategy>,
) -> HashMap<String, Vec<Evaluate>> {
    let mut all_custom_strategies = HashMap::new();

    for feature in extract_features(update) {
        let mut feature_strategies = Vec::new();

        let Some(strategies) = feature.strategies.as_ref() else {
            continue;
        };

        for strategy in strategies {
            if KNOWN_STRATEGIES.contains(&strategy.name.as_str()) {
                continue;
            }

            if let Some(strategy_factory) = registry.get(&strategy.name) {
                let eval = strategy_factory(strategy.parameters.clone());
                feature_strategies.push(eval);
            } else {
                warn!(
                    "Strategy '{}' required by feature '{}' is not registered, this strategy will be ignored for evaluation",
                    strategy.name, feature.name
                );
            }
        }

        if !feature_strategies.is_empty() {
            all_custom_strategies.insert(feature.name.clone(), feature_strategies);
        }
    }

    all_custom_strategies
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use unleash_types::client_features::{
        ClientFeature, ClientFeatures, ClientFeaturesDelta, DeltaEvent, Strategy as FeatureStrategy,
    };
    use unleash_yggdrasil::UpdateMessage;

    use super::{compile_custom_strategies_for_state, Strategy};
    use crate::context::Context;

    fn feature(name: &str, strategies: Option<Vec<FeatureStrategy>>) -> ClientFeature {
        ClientFeature {
            name: name.into(),
            enabled: true,
            strategies,
            ..Default::default()
        }
    }

    fn strategy(name: &str, parameters: Option<HashMap<String, String>>) -> FeatureStrategy {
        FeatureStrategy {
            name: name.into(),
            sort_order: None,
            segments: None,
            constraints: None,
            parameters,
            variants: None,
        }
    }

    fn matching_user_id(parameters: Option<HashMap<String, String>>) -> super::Evaluate {
        let allowed_user = parameters
            .and_then(|parameters| parameters.get("userId").cloned())
            .unwrap_or_default();

        Box::new(move |context: &Context| context.user_id.as_deref() == Some(allowed_user.as_str()))
    }

    #[test]
    fn compiles_registered_custom_strategies_from_full_response() {
        let mut registry: HashMap<String, Strategy> = HashMap::new();
        registry.insert("custom".into(), Box::new(matching_user_id));

        let update = UpdateMessage::FullResponse(ClientFeatures {
            version: 1,
            features: vec![
                feature(
                    "with-custom",
                    Some(vec![
                        strategy("default", None),
                        strategy(
                            "custom",
                            Some(HashMap::from([("userId".into(), "alice".into())])),
                        ),
                        strategy("unregistered", None),
                    ]),
                ),
                feature("known-only", Some(vec![strategy("flexibleRollout", None)])),
                feature("without-strategies", None),
            ],
            segments: None,
            query: None,
            meta: None,
        });

        let compiled = compile_custom_strategies_for_state(&update, &registry);

        assert_eq!(compiled.len(), 1);
        let evals = compiled.get("with-custom").unwrap();
        assert_eq!(evals.len(), 1);
        assert!(evals[0](&Context {
            user_id: Some("alice".into()),
            ..Default::default()
        }));
        assert!(!evals[0](&Context {
            user_id: Some("bob".into()),
            ..Default::default()
        }));
    }

    #[test]
    fn compiles_custom_strategies_from_partial_updates_after_applying_event_order() {
        let mut registry: HashMap<String, Strategy> = HashMap::new();
        registry.insert("custom".into(), Box::new(matching_user_id));

        let update = UpdateMessage::PartialUpdate(ClientFeaturesDelta {
            events: vec![
                DeltaEvent::Hydration {
                    event_id: 1,
                    features: vec![feature(
                        "removed-feature",
                        Some(vec![strategy(
                            "custom",
                            Some(HashMap::from([("userId".into(), "removed".into())])),
                        )]),
                    )],
                    segments: vec![],
                },
                DeltaEvent::FeatureRemoved {
                    event_id: 2,
                    feature_name: "removed-feature".into(),
                    project: "default".into(),
                },
                DeltaEvent::FeatureUpdated {
                    event_id: 3,
                    feature: feature(
                        "updated-feature",
                        Some(vec![strategy(
                            "custom",
                            Some(HashMap::from([("userId".into(), "updated".into())])),
                        )]),
                    ),
                },
            ],
        });

        let compiled = compile_custom_strategies_for_state(&update, &registry);

        assert!(!compiled.contains_key("removed-feature"));
        let evals = compiled.get("updated-feature").unwrap();
        assert_eq!(evals.len(), 1);
        assert!(evals[0](&Context {
            user_id: Some("updated".into()),
            ..Default::default()
        }));
    }
}
