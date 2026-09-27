use proc_macro_crate::{FoundCrate, crate_name};
use quote::quote;
use syn::{Attribute, Expr, ExprArray, ExprLit, Field, Lit, Meta};

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

/// Extract default value from `#[ousia(default = <expr>)]`.
///
/// Supports: string literals (→ `String::from(...)`), bool/int/float literals, and array
/// expressions (→ `vec![...]`).
pub fn get_field_default_value(field: &Field) -> Option<proc_macro2::TokenStream> {
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
                            return Some(default_expr_to_tokens(&nv.value));
                        }
                    }
                }
            }
        }
    }
    None
}

/// `SortAs` for an indexed field: its Rust type's `ToIndexValue::SORT_AS`, or
/// `Json` when `name` isn't a struct field (a geo field's virtual name).
pub fn sort_as_tokens(
    ousia: &proc_macro2::TokenStream,
    fields: &[&Field],
    name: &str,
) -> proc_macro2::TokenStream {
    match fields.iter().find(|f| f.ident.as_ref().is_some_and(|i| i == name)) {
        Some(f) => {
            let ty = &f.ty;
            quote! { <#ty as #ousia::query::ToIndexValue>::SORT_AS }
        }
        None => quote! { #ousia::query::SortAs::Json },
    }
}

/// `#[ousia(rename = "old_name")]`: previous name of a field, still accepted when decoding.
pub fn get_rename_value(field: &Field) -> Option<String> {
    for attr in &field.attrs {
        if !attr.path().is_ident("ousia") {
            continue;
        }
        let Meta::List(meta_list) = &attr.meta else {
            continue;
        };
        let Ok(nested) = meta_list.parse_args_with(
            syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
        ) else {
            continue;
        };
        for meta in nested {
            if let Meta::NameValue(nv) = meta {
                if nv.path.is_ident("rename") {
                    if let Expr::Lit(ExprLit { lit: Lit::Str(s), .. }) = &nv.value {
                        return Some(s.value());
                    }
                }
            }
        }
    }
    None
}

fn default_expr_to_tokens(expr: &Expr) -> proc_macro2::TokenStream {
    match expr {
        Expr::Lit(ExprLit { lit: Lit::Str(s), .. }) => {
            let val = s.value();
            quote! { String::from(#val) }
        }
        Expr::Lit(ExprLit { lit: Lit::Bool(b), .. }) => {
            let val = b.value;
            quote! { #val }
        }
        Expr::Lit(ExprLit { lit: Lit::Int(i), .. }) => quote! { #i },
        Expr::Lit(ExprLit { lit: Lit::Float(f), .. }) => quote! { #f },
        Expr::Array(ExprArray { elems, .. }) => {
            let items: Vec<proc_macro2::TokenStream> =
                elems.iter().map(default_expr_to_tokens).collect();
            quote! { vec![#(#items),*] }
        }
        other => quote! { #other },
    }
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

/// Raw `#[ousia(composite_index = "a, b desc")]` values, in declaration order.
pub fn parse_composite_index_attrs(attr: Option<&Attribute>) -> Vec<String> {
    let Some(Meta::List(meta_list)) = attr.map(|a| &a.meta) else {
        return vec![];
    };
    let nested = meta_list
        .parse_args_with(syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated)
        .expect("Failed to parse ousia attribute arguments");
    nested
        .iter()
        .filter_map(|meta| match meta {
            Meta::NameValue(nv) if nv.path.is_ident("composite_index") => match &nv.value {
                Expr::Lit(ExprLit { lit: Lit::Str(s), .. }) => Some(s.value()),
                _ => panic!("composite_index must be a string literal"),
            },
            _ => None,
        })
        .collect()
}

/// Split one `composite_index` value into `(element, descending)` pairs.
/// Elements are comma-separated, each an optional `asc` / `desc` after the
/// name (default `asc`).
pub fn parse_composite_index_elements(decl: &str) -> Vec<(String, bool)> {
    let elements: Vec<(String, bool)> = decl
        .split(',')
        .map(|part| {
            let words: Vec<&str> = part.split_whitespace().collect();
            match words.as_slice() {
                [name] => (name.to_string(), false),
                [name, dir] if dir.eq_ignore_ascii_case("asc") => (name.to_string(), false),
                [name, dir] if dir.eq_ignore_ascii_case("desc") => (name.to_string(), true),
                _ => panic!(
                    "composite_index element must be `field`, `field asc` or `field desc`, \
                     got `{}` in \"{}\"",
                    part.trim(),
                    decl
                ),
            }
        })
        .collect();
    // Postgres' INDEX_MAX_KEYS.
    if elements.len() > 32 {
        panic!("composite_index \"{}\" has {} elements; Postgres allows 32", decl, elements.len());
    }
    let mut seen = std::collections::HashSet::new();
    for (name, _) in &elements {
        if !seen.insert(name) {
            panic!("composite_index \"{}\" lists `{}` twice", decl, name);
        }
    }
    elements
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

/// Helper to parse kind strings into index kind tokens.
///
/// Supports:
/// - `search`, `sort` (combinable with `+`)
/// - `geo(lat_field, lon_field)` — cannot be combined with anything else
pub fn parse_index_kinds(kind_str: &str) -> Vec<proc_macro2::TokenStream> {
    let ousia = import_ousia();
    let trimmed = kind_str.trim();
    if let Some(rest) = trimmed.strip_prefix("geo(") {
        let inner = rest
            .strip_suffix(')')
            .unwrap_or_else(|| panic!("geo(...) index must end with ')', got: {}", kind_str));
        let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
        if parts.len() != 2 || parts.iter().any(|p| p.is_empty()) {
            panic!(
                "geo(...) index must reference exactly two fields, got: {}",
                kind_str
            );
        }
        let lat = parts[0];
        let lon = parts[1];
        return vec![quote!(#ousia::query::IndexKind::Geo {
            lat_field: #lat,
            lon_field: #lon,
        })];
    }
    trimmed
        .split('+')
        .map(|k| k.trim())
        .map(|k| match k {
            "search" => quote!(#ousia::query::IndexKind::Search),
            "sort" => quote!(#ousia::query::IndexKind::Sort),
            _ => panic!(
                "Invalid index kind `{}`. Valid kinds: search, sort, geo(lat_field, lon_field)",
                k
            ),
        })
        .collect()
}

/// If `kind_str` is a `geo(lat_field, lon_field)` expression, return
/// `Some((lat_field, lon_field))`. Used by the macro codegen to:
///   - skip struct-field-existence validation for the geo's virtual field name
///   - generate `geo_points()` impls that read the source lat/lon fields
pub fn parse_geo_source_fields(kind_str: &str) -> Option<(String, String)> {
    let trimmed = kind_str.trim();
    let rest = trimmed.strip_prefix("geo(")?;
    let inner = rest.strip_suffix(')')?;
    let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
    if parts.len() != 2 || parts.iter().any(|p| p.is_empty()) {
        return None;
    }
    Some((parts[0].to_string(), parts[1].to_string()))
}
