use unleash_api_client_macros::FeatureKey;

trait FeatureKey: Copy + core::fmt::Debug + 'static {
    fn name(self) -> &'static str;
}

#[derive(Copy, Clone, Debug, FeatureKey)]
enum Features {
    #[feature_name = "a"]
    FeatureA,
}

fn main() {}