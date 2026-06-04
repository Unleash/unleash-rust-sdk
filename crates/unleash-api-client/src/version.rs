//! Managing SDK versions
//!
//! This module includes utilities to handle versioning aspects used internally
//! by the crate.
use std::env;

/// Returns the version of the `unleash-rust-sdk` SDK compiled into the binary.
///
/// The version number is included at compile time using the cargo package version
/// and is formatted as "unleash-rust-sdk:X.Y.Z", where X.Y.Z is the semantic
/// versioning format. This ensures a consistent versioning approach that aligns
/// with other Unleash SDKs.
pub(crate) fn get_sdk_version() -> &'static str {
    concat!("unleash-rust-sdk:", env!("CARGO_PKG_VERSION"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use semver::Version;

    #[test]
    fn test_get_sdk_version_with_version_set() {
        let version_output = get_sdk_version();
        let sdk_version = version_output.strip_prefix("unleash-rust-sdk:").unwrap();
        Version::parse(sdk_version).unwrap_or_else(|_| {
            panic!("Version output did not match expected format: {sdk_version}")
        });
    }
}
