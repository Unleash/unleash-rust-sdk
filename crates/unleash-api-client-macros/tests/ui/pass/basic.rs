use unleash_api_client_macros::FeatureKey;

trait FeatureKey: Copy + core::fmt::Debug + 'static {
    fn name(self) -> &'static str;
}

#[derive(Copy, Clone, Debug, FeatureKey)]
enum Features {
    #[feature_name("feature-a")]
    FeatureA,
    FeatureB,
}

fn main() {
    assert_eq!(Features::FeatureA.name(), "feature-a");
    assert_eq!(Features::FeatureB.name(), "FeatureB");
}
