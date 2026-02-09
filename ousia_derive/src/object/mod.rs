pub mod generate;
pub mod unique;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

pub fn derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    // Generate the main OusiaObject impl
    let object_impl = match generate::generate_object_impl(&input) {
        Ok(tokens) => tokens,
        Err(e) => return e.to_compile_error().into(),
    };

    // Generate uniqueness impl
    let uniqueness_impl = match unique::generate::generate_uniqueness_impl(&input) {
        Ok(tokens) => tokens,
        Err(e) => return e.to_compile_error().into(),
    };

    // Combine both implementations
    let expanded = quote::quote! {
        #object_impl
        #uniqueness_impl
    };

    TokenStream::from(expanded)
}
