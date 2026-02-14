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
                if field == "owner" {
                    // Handle owner from meta field
                    quote! {
                        {
                            let value = ::std::format!("{}", &self._meta.owner);
                            let hash = #ousia::derive_unique_hash(
                                #type_name_str,
                                "owner",
                                &value
                            );
                            hashes.push((hash, "owner"));
                        }
                    }
                } else {
                    // Handle regular data field
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
            }
            UniqueConstraint::Composite(fields) => {
                // Separate owner field from data fields
                let (owner_refs, field_refs): (Vec<_>, Vec<_>) =
                    fields.iter().partition(|f| *f == "owner");

                let composite_key = fields.join("+");

                // Build format string properly: "field1:{}:field2:{}"
                let format_parts: Vec<_> = fields.iter().map(|f| format!("{}:{{}}", f)).collect();
                let format_str = format_parts.join(":");

                // Build value references
                let value_refs = fields.iter().map(|f| {
                    if f == "owner" {
                        quote! { &self._meta.owner }
                    } else {
                        let field_ident = syn::Ident::new(f, proc_macro2::Span::call_site());
                        quote! { &self.#field_ident }
                    }
                });

                quote! {
                    {
                        let value = ::std::format!(
                            #format_str,
                            #(#value_refs),*
                        );
                        let hash = #ousia::derive_unique_hash(
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
