mod edge;
mod object;
mod shared;

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

use crate::shared::{import_ousia, is_meta_field};

#[proc_macro_derive(OusiaObject, attributes(ousia, ousia_meta))]
pub fn derive_ousia_object(input: TokenStream) -> TokenStream {
    object::derive(input)
}

#[proc_macro_derive(OusiaEdge, attributes(ousia))]
pub fn derive_ousia_edge(input: TokenStream) -> TokenStream {
    edge::derive(input)
}

#[proc_macro_derive(OusiaDefault)]
pub fn derive_ousia_default(input: TokenStream) -> TokenStream {
    let ousia = import_ousia();
    let input = parse_macro_input!(input as DeriveInput);
    let ident = &input.ident;

    let fields = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(f) => &f.named,
            _ => panic!("OusiaDefault only supports named structs"),
        },
        _ => panic!("OusiaDefault only supports structs"),
    };

    // Find meta field
    let meta_fields: Vec<_> = fields.iter().filter(|f| is_meta_field(f)).collect();

    let meta_field_ident = if meta_fields.is_empty() {
        fields
            .iter()
            .find(|f| f.ident.as_ref().unwrap() == "_meta")
            .map(|f| f.ident.as_ref().unwrap())
            .unwrap_or_else(|| {
                panic!(
                    "No meta field found. Either mark a field with #[ousia(meta)] or use '_meta' as field name"
                )
            })
    } else {
        meta_fields[0].ident.as_ref().unwrap()
    };

    // Determine if this is an edge or object based on meta field type
    let meta_field = fields
        .iter()
        .find(|f| f.ident.as_ref().unwrap() == meta_field_ident)
        .unwrap();

    let meta_type = &meta_field.ty;
    let meta_type_str = quote!(#meta_type).to_string();
    let is_edge = meta_type_str.contains("EdgeMeta");

    let default_fields = fields.iter().map(|f| {
        let name = f.ident.as_ref().unwrap();
        if name == meta_field_ident {
            if is_edge {
                quote! { #name: #ousia::edge::meta::EdgeMeta::new(uuid::Uuid::now_v7(), uuid::Uuid::now_v7()) }
            } else {
                quote! { #name: #ousia::object::meta::Meta::default() }
            }
        } else {
            quote! { #name: Default::default() }
        }
    });

    let expanded = quote! {
        impl Default for #ident {
            fn default() -> Self {
                Self {
                    #(#default_fields,)*
                }
            }
        }
    };

    TokenStream::from(expanded)
}
