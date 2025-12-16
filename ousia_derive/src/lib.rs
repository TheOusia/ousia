use proc_macro::TokenStream;
use quote::quote;
use syn::{Attribute, Data, DeriveInput, Expr, ExprLit, Fields, Lit, Meta, parse_macro_input};

const RESERVED_META_FIELDS: &[&str] = &["id", "owner", "type", "created_at", "updated_at"];

/// Extract #[ousia(...)] attributes
fn get_ousia_attr(attrs: &[Attribute]) -> Option<&Attribute> {
    attrs.iter().find(|attr| attr.path().is_ident("ousia"))
}

/// Parse type and index list from `#[ousia(...)]` using updated syn API
fn parse_ousia_attr(attr: Option<&Attribute>) -> (Option<String>, Vec<(String, String)>) {
    let mut type_name = None;
    let mut indexes = vec![];

    if let Some(attr) = attr {
        // Parse the attribute as a Meta
        let meta = &attr.meta;

        // Handle #[ousia(...)]
        if let Meta::List(meta_list) = meta {
            // Parse the tokens inside the parentheses
            let nested = meta_list
                .parse_args_with(
                    syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
                )
                .expect("Failed to parse ousia attribute arguments");

            for meta in nested {
                match meta {
                    // Handle `type_name = "User"`
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
                    // Handle `index = "name:search"`
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
                    _ => {
                        // Ignore unknown attributes or handle them as needed
                    }
                }
            }
        }
    }

    (type_name, indexes)
}

#[proc_macro_derive(OusiaObject, attributes(ousia))]
pub fn derive_ousia_object(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = &input.ident;

    // --- get ousia attribute ---
    let attr = get_ousia_attr(&input.attrs);
    let (type_name, indexes) = parse_ousia_attr(attr);
    let type_name = type_name.unwrap_or_else(|| ident.to_string());

    // --- ensure no reserved meta fields are defined in struct ---
    let fields = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(f) => f
                .named
                .iter()
                .map(|f| f.ident.as_ref().unwrap())
                .collect::<Vec<_>>(),
            _ => panic!("OusiaObject only supports named structs"),
        },
        _ => panic!("OusiaObject only supports structs"),
    };

    for f in &fields {
        let f_str = f.to_string();
        if RESERVED_META_FIELDS.contains(&f_str.as_str()) {
            panic!(
                "Field `{}` is reserved for meta and cannot be declared in struct {}",
                f_str, ident
            );
        }
    }

    // --- generate IndexField list, ensure no meta fields are indexed ---
    let index_fields = indexes.iter().map(|(name, kind)| {
        if RESERVED_META_FIELDS.contains(&name.as_str()) {
            panic!(
                "Index field `{}` is reserved for meta and cannot be indexed",
                name
            );
        }
        if !fields.iter().any(|f| &f.to_string() == name) {
            panic!("Indexed field `{}` does not exist on {}", name, ident);
        }

        // Split by '+' and parse each kind
        let kinds: Vec<_> = kind
            .split('+')
            .map(|k| k.trim())
            .map(|k| match k {
                "search" => quote!(crate::object::query::IndexKind::Search),
                "sort" => quote!(crate::object::query::IndexKind::Sort),
                _ => panic!("Invalid index kind `{}`. Valid kinds: search, sort", k),
            })
            .collect();

        quote! {
            crate::object::query::IndexField {
                name: #name,
                kinds: &[#(#kinds),*],
            }
        }
    });

    // --- generate index_meta insertions ---
    let index_meta_insertions = indexes.iter().map(|(name, _kind)| {
        let field_name = syn::Ident::new(name, proc_macro2::Span::call_site());
        let name_str = name.as_str();

        quote! {
            values.insert(
                #name_str.to_string(),
                crate::object::query::ToIndexValue::to_index_value(&self.#field_name)
            );
        }
    });

    // --- generate Indexes struct fields and initialization ---
    let indexes_struct_name =
        syn::Ident::new(&format!("{}Indexes", ident), proc_macro2::Span::call_site());

    // Deduplicate index field names (in case a field has multiple index kinds)
    let unique_index_names: std::collections::HashSet<_> =
        indexes.iter().map(|(name, _)| name.clone()).collect();
    let mut unique_index_names: Vec<_> = unique_index_names.into_iter().collect();
    unique_index_names.sort(); // For deterministic output

    let indexes_struct_fields = unique_index_names.iter().map(|name| {
        let field_ident = syn::Ident::new(name, proc_macro2::Span::call_site());
        quote! {
            pub #field_ident: crate::engine::adapters::Field
        }
    });

    let indexes_const_fields = unique_index_names.iter().map(|name| {
        let field_ident = syn::Ident::new(name, proc_macro2::Span::call_site());
        let name_str = name.as_str();
        quote! {
            #field_ident: crate::engine::adapters::Field { name: #name_str }
        }
    });

    // --- generate impl ---
    let expanded = quote! {
        impl crate::object::traits::Object for #ident {
            const TYPE: &'static str = #type_name;

            fn meta(&self) -> &crate::object::meta::Meta {
                &self._meta
            }

            fn meta_mut(&mut self) -> &mut crate::object::meta::Meta {
                &mut self._meta
            }

            fn index_meta(&self) -> crate::object::query::IndexMeta {
                let mut values = std::collections::BTreeMap::new();
                #(#index_meta_insertions)*
                crate::object::query::IndexMeta { values }
            }
        }

        impl crate::object::query::ObjectQuery for #ident {
            fn indexed_fields() -> &'static [crate::object::query::IndexField] {
                &[ #(#index_fields),* ]
            }
        }

        pub struct #indexes_struct_name {
            #(#indexes_struct_fields),*
        }

        impl #ident {
            pub const INDEXES: #indexes_struct_name = #indexes_struct_name {
                #(#indexes_const_fields),*
            };
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(OusiaDefault)]
pub fn derive_ousia_default(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = &input.ident;

    let default_fields = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields_named) => {
                fields_named
                    .named
                    .iter()
                    .map(|f| {
                        let name = &f.ident;
                        if let Some(ident) = name {
                            if ident == "_meta" {
                                // Always use Meta::default() for _meta
                                quote! { #ident: crate::object::meta::Meta::default() }
                            } else {
                                // Other fields: Default::default()
                                quote! { #ident: Default::default() }
                            }
                        } else {
                            quote! {}
                        }
                    })
                    .collect::<Vec<_>>()
            }
            Fields::Unnamed(_) | Fields::Unit => {
                panic!("OusiaDefault only supports named struct fields")
            }
        },
        _ => panic!("OusiaDefault only supports structs"),
    };

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
