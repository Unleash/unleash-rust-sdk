use proc_macro::TokenStream;

/// Passthrough helper macro used as a foundation for future proc-macro APIs.
#[proc_macro]
pub fn passthrough(input: TokenStream) -> TokenStream {
    input
}
