use proc_macro_crate::{FoundCrate, crate_name};
use quote::quote;
use syn::{Attribute, Expr, ExprLit, Field, Lit, Meta};

pub fn import_ousia() -> proc_macro2::TokenStream {
    // This finds the ousia crate in the user's dependencies
    let found_crate = crate_name("ousia").unwrap_or(FoundCrate::Itself);

    match found_crate {
        FoundCrate::Itself => quote! { ::ousia },
        FoundCrate::Name(name) => {
            let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote! { ::#ident }
        }
    }
}

/// Extract #[ousia(...)] attributes from struct
pub fn get_ousia_attr(attrs: &[Attribute]) -> Option<&Attribute> {
    attrs.iter().find(|attr| attr.path().is_ident("ousia"))
}

/// Check if a field has #[ousia(meta)] attribute
pub fn is_meta_field(field: &Field) -> bool {
    field.attrs.iter().any(|attr| {
        if !attr.path().is_ident("ousia") {
            return false;
        }

        if let Meta::List(meta_list) = &attr.meta {
            let result = meta_list.parse_args_with(
                syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
            );

            if let Ok(nested) = result {
                return nested.iter().any(|meta| {
                    if let Meta::Path(path) = meta {
                        path.is_ident("meta")
                    } else {
                        false
                    }
                });
            }
        }
        false
    })
}

/// Extract default value from #[ousia(default = "value")] attribute
pub fn get_field_default_value(field: &Field) -> Option<String> {
    for attr in &field.attrs {
        if !attr.path().is_ident("ousia") {
            continue;
        }

        if let Meta::List(meta_list) = &attr.meta {
            let result = meta_list.parse_args_with(
                syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
            );

            if let Ok(nested) = result {
                for meta in nested {
                    if let Meta::NameValue(nv) = meta {
                        if nv.path.is_ident("default") {
                            if let Expr::Lit(ExprLit {
                                lit: Lit::Str(s), ..
                            }) = &nv.value
                            {
                                return Some(s.value());
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

/// Parse type and index list from `#[ousia(...)]` using updated syn API
pub fn parse_ousia_attr(attr: Option<&Attribute>) -> (Option<String>, Vec<(String, String)>) {
    let mut type_name = None;
    let mut indexes = vec![];

    if let Some(attr) = attr {
        let meta = &attr.meta;

        if let Meta::List(meta_list) = meta {
            let nested = meta_list
                .parse_args_with(
                    syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
                )
                .expect("Failed to parse ousia attribute arguments");

            for meta in nested {
                match meta {
                    Meta::NameValue(nv) if nv.path.is_ident("type_name") => {
                        if let Expr::Lit(ExprLit {
                            lit: Lit::Str(s), ..
                        }) = &nv.value
                        {
                            type_name = Some(s.value());
                        } else {
                            panic!("type_name must be a string literal");
                        }
                    }
                    Meta::NameValue(nv) if nv.path.is_ident("index") => {
                        if let Expr::Lit(ExprLit {
                            lit: Lit::Str(s), ..
                        }) = &nv.value
                        {
                            let index_str = s.value();
                            let parts: Vec<&str> = index_str.split(':').collect();

                            if parts.len() != 2 {
                                panic!("Index must be in format 'field:kind', got: {}", index_str);
                            }

                            indexes.push((parts[0].to_string(), parts[1].to_string()));
                        } else {
                            panic!("index must be a string literal");
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    (type_name, indexes)
}

/// Check if a field has #[ousia(private)] attribute
pub fn is_private_field(field: &Field) -> bool {
    field.attrs.iter().any(|attr| {
        if !attr.path().is_ident("ousia") {
            return false;
        }

        if let Meta::List(meta_list) = &attr.meta {
            let result = meta_list.parse_args_with(
                syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
            );

            if let Ok(nested) = result {
                return nested.iter().any(|meta| {
                    if let Meta::Path(path) = meta {
                        path.is_ident("private")
                    } else {
                        false
                    }
                });
            }
        }
        false
    })
}

/// Helper to parse kind strings into index kind tokens
pub fn parse_index_kinds(kind_str: &str) -> Vec<proc_macro2::TokenStream> {
    let ousia = import_ousia();
    kind_str
        .split('+')
        .map(|k| k.trim())
        .map(|k| match k {
            "search" => quote!(#ousia::query::IndexKind::Search),
            "sort" => quote!(#ousia::query::IndexKind::Sort),
            _ => panic!("Invalid index kind `{}`. Valid kinds: search, sort", k),
        })
        .collect()
}
