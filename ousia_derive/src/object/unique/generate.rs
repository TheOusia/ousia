use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Result};

use crate::import_ousia;

use super::parse::{UniqueConfig, UniqueConstraint};

pub fn generate_uniqueness_impl(input: &DeriveInput) -> Result<TokenStream> {
    let ousia = import_ousia();
    // Parse uniqueness config
    let config = match UniqueConfig::from_attributes(&input.attrs) {
        Ok(config) => config,
        Err(e) => return Err(e),
    };

    let name = &input.ident;
    let type_name_str = name.to_string();

    let has_unique_fields = config.has_constraints();

    if !has_unique_fields {
        // No unique constraints
        return Ok(quote! {
            impl #ousia::Unique for #name {
                const HAS_UNIQUE_FIELDS: bool = false;

                fn derive_unique_hashes(&self) -> ::std::vec::Vec<(::std::string::String, &'static str)> {
                    ::std::vec::Vec::new()
                }
            }
        });
    }

    // Generate hash derivation logic
    let hash_generations = config
        .constraints
        .iter()
        .map(|constraint| match constraint {
            UniqueConstraint::Single(field) => {
                let field_ident = syn::Ident::new(field, proc_macro2::Span::call_site());
                quote! {
                    {
                        let value = ::std::format!("{}", &self.#field_ident);
                        let hash = #ousia::derive_unique_hash(
                            #type_name_str,
                            #field,
                            &value
                        );
                        hashes.push((hash, #field));
                    }
                }
            }
            UniqueConstraint::Composite(fields) => {
                let field_idents: Vec<_> = fields
                    .iter()
                    .map(|f| syn::Ident::new(f, proc_macro2::Span::call_site()))
                    .collect();

                let composite_key = fields.join("+");

                // Build format string properly: "field1:{}:field2:{}"
                let format_parts: Vec<_> = fields.iter().map(|f| format!("{}:{{}}", f)).collect();
                let format_str = format_parts.join(":");

                quote! {
                    {
                        let value = ::std::format!(
                            #format_str,
                            #(&self.#field_idents),*
                        );
                        let hash = ::ousia::derive_unique_hash(
                            #type_name_str,
                            #composite_key,
                            &value
                        );
                        hashes.push((hash, #composite_key));
                    }
                }
            }
        });

    Ok(quote! {
        impl #ousia::Unique for #name {
            const HAS_UNIQUE_FIELDS: bool = true;

            fn derive_unique_hashes(&self) -> ::std::vec::Vec<(::std::string::String, &'static str)> {
                let mut hashes = ::std::vec::Vec::new();
                #(#hash_generations)*
                hashes
            }
        }
    })
}
