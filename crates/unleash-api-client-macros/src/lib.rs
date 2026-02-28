use proc_macro::TokenStream;
use quote::quote;
use syn::parse::Parser;
use syn::LitStr;
use syn::{
    parse_macro_input, spanned::Spanned, Attribute, Data, DeriveInput, Fields, Lit, Meta, Token,
};

/// Derive `FeatureKey` for a unit enum.
///
/// Rules:
/// - Default name = variant identifier as &'static str (e.g. FeatureA -> "FeatureA")
/// - Override with #[feature_name("feature-a")] if present
#[proc_macro_derive(FeatureKey, attributes(feature_name))]
pub fn derive_feature_key(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let enum_ident = &input.ident;

    let Data::Enum(data_enum) = &input.data else {
        return syn::Error::new(input.span(), "FeatureKey can only be derived for enums")
            .to_compile_error()
            .into();
    };

    let mut arms = Vec::with_capacity(data_enum.variants.len());

    for variant in &data_enum.variants {
        // just narrowing down this down to unit variants to reduce complexity more than anything else
        // don't really want users going ham with this and doing weird stuff. If people want to do
        // this they can derive the trait themselves and we can deal with that in a GH issue
        if !matches!(variant.fields, Fields::Unit) {
            return syn::Error::new(
                variant.span(),
                "FeatureKey only supports unit enum variants (no fields)",
            )
            .to_compile_error()
            .into();
        }

        let v_ident = &variant.ident;

        // grab the the user defined attribute or, if that doesn't exist, we'll
        // just use the variant name directly as the feature name
        let name = match parse_feature_name(&variant.attrs) {
            Ok(Some(lit)) => lit,
            Ok(None) => syn::LitStr::new(&v_ident.to_string(), v_ident.span()),
            Err(e) => return e.to_compile_error().into(),
        };

        arms.push(quote! { #enum_ident::#v_ident => #name });
    }

    let expanded = quote! {
        impl FeatureKey for #enum_ident {
            #[inline]
            fn name(self) -> &'static str {
                match self {
                    #(#arms, )*
                }
            }
        }
    };

    expanded.into()
}

// There's maybe more going on here than we need but I'm trying to be super explicit and narrow
// about errors that happen here - these are all compile time things so the clearer the nicer the UX
// Nice to haves that this is doing
// - reject duplicate #[feature_name(...)] attributes on the same variant
// - reject #[feature_name(...)] attributes that aren't in the form of #[feature_name("…")]
// - reject #[feature_name(...)] attributes that don't have exactly one argument
fn parse_feature_name(attrs: &[Attribute]) -> Result<Option<LitStr>, syn::Error> {
    let mut found: Option<LitStr> = None;

    for attr in attrs {
        if !attr.path().is_ident("feature_name") {
            continue;
        }

        if found.is_some() {
            return Err(syn::Error::new(
                attr.span(),
                r#"duplicate #[feature_name(...)] attribute"#,
            ));
        }

        let Meta::List(meta_list) = &attr.meta else {
            return Err(syn::Error::new(
                attr.span(),
                r#"expected #[feature_name("…")]"#,
            ));
        };

        let parser = syn::punctuated::Punctuated::<Lit, Token![,]>::parse_terminated;
        let args = parser.parse2(meta_list.tokens.clone())?;

        if args.len() != 1 {
            return Err(syn::Error::new(
                meta_list.tokens.span(),
                r#"expected exactly one string literal: #[feature_name("…")]"#,
            ));
        }

        let lit = match args.first().unwrap() {
            Lit::Str(s) => s.clone(),
            other => {
                return Err(syn::Error::new(
                    other.span(),
                    r#"expected string literal: #[feature_name("…")]"#,
                ))
            }
        };

        found = Some(lit);
    }

    Ok(found)
}
