use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::ExprPath;
use syn::{
    Attribute, Data, DeriveInput, Expr, ExprLit, Field, Fields, Lit, Meta, Type, parse_macro_input,
};

const RESERVED_META_FIELDS: &[&str] = &["id", "owner", "type", "created_at", "updated_at"];

/// Extract #[ousia(...)] attributes from struct
fn get_ousia_attr(attrs: &[Attribute]) -> Option<&Attribute> {
    attrs.iter().find(|attr| attr.path().is_ident("ousia"))
}

/// Check if a field has #[ousia(meta)] attribute
fn is_meta_field(field: &Field) -> bool {
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

/// Parse type and index list from `#[ousia(...)]` using updated syn API
fn parse_ousia_attr(attr: Option<&Attribute>) -> (Option<String>, Vec<(String, String)>) {
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

#[proc_macro_derive(OusiaObject, attributes(ousia))]
pub fn derive_ousia_object(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = &input.ident;

    // --- get ousia attribute ---
    let attr = get_ousia_attr(&input.attrs);
    let (type_name, indexes) = parse_ousia_attr(attr);
    let type_name = type_name.unwrap_or_else(|| ident.to_string());

    // --- extract fields and identify meta field ---
    let fields = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(f) => &f.named,
            _ => panic!("OusiaObject only supports named structs"),
        },
        _ => panic!("OusiaObject only supports structs"),
    };

    // Find meta field
    let meta_fields: Vec<_> = fields.iter().filter(|f| is_meta_field(f)).collect();

    let meta_field_ident = if meta_fields.is_empty() {
        // Default to _meta if no field is marked
        fields
            .iter()
            .find(|f| f.ident.as_ref().unwrap() == "_meta")
            .map(|f| f.ident.as_ref().unwrap())
            .unwrap_or_else(|| {
                panic!(
                    "No meta field found. Either mark a field with #[ousia(meta)] or use '_meta' as field name"
                )
            })
    } else if meta_fields.len() > 1 {
        panic!("Only one field can be marked with #[ousia(meta)]");
    } else {
        meta_fields[0].ident.as_ref().unwrap()
    };

    // Get all non-meta fields
    let non_meta_fields: Vec<_> = fields
        .iter()
        .filter(|f| f.ident.as_ref().unwrap() != meta_field_ident)
        .collect();

    // Validate no reserved meta field names
    for field in &non_meta_fields {
        let f_str = field.ident.as_ref().unwrap().to_string();
        if RESERVED_META_FIELDS.contains(&f_str.as_str()) {
            panic!(
                "Field `{}` is reserved for meta and cannot be declared in struct {}",
                f_str, ident
            );
        }
    }

    // --- generate IndexField list ---
    let index_fields = indexes.iter().map(|(name, kind)| {
        if RESERVED_META_FIELDS.contains(&name.as_str()) {
            panic!(
                "Index field `{}` is reserved for meta and cannot be indexed",
                name
            );
        }
        if !non_meta_fields
            .iter()
            .any(|f| &f.ident.as_ref().unwrap().to_string() == name)
        {
            panic!("Indexed field `{}` does not exist on {}", name, ident);
        }

        let kinds: Vec<_> = kind
            .split('+')
            .map(|k| k.trim())
            .map(|k| match k {
                "search" => quote!(crate::query::IndexKind::Search),
                "sort" => quote!(crate::query::IndexKind::Sort),
                _ => panic!("Invalid index kind `{}`. Valid kinds: search, sort", k),
            })
            .collect();

        quote! {
            crate::query::IndexField {
                name: #name,
                kinds: &[#(#kinds),*],
            }
        }
    });

    // --- generate index_meta insertions ---
    let index_meta_insertions = indexes.iter().map(|(name, _kind)| {
        let field_name = format_ident!("{}", name);
        let name_str = name.as_str();

        quote! {
            values.insert(
                #name_str.to_string(),
                crate::query::ToIndexValue::to_index_value(&self.#field_name)
            );
        }
    });

    // --- generate Indexes struct ---
    let indexes_struct_name = format_ident!("{}Indexes", ident);

    let unique_index_names: std::collections::HashSet<_> =
        indexes.iter().map(|(name, _)| name.clone()).collect();
    let mut unique_index_names: Vec<_> = unique_index_names.into_iter().collect();
    unique_index_names.sort();

    let indexes_struct_fields = unique_index_names.iter().map(|name| {
        let field_ident = format_ident!("{}", name);
        quote! {
            pub #field_ident: crate::query::IndexField
        }
    });

    let indexes_const_fields = unique_index_names.iter().map(|name| {
        let field_ident = format_ident!("{}", name);
        let name_str = name.as_str();
        quote! {
            #field_ident: crate::query::IndexField {
                name: #name_str,
                kinds: &[crate::query::IndexKind::Search],
            }// fix this to construct the struct properly
        }
    });

    // --- generate Serialize implementation (skip meta field) ---
    let serialize_fields = non_meta_fields.iter().map(|f| {
        let field_name = f.ident.as_ref().unwrap();
        let field_name_str = field_name.to_string();
        quote! {
            state.serialize_field(#field_name_str, &self.#field_name)?;
        }
    });

    let field_count = non_meta_fields.len();

    // --- generate Deserialize implementation ---
    let deserialize_field_names: Vec<_> = non_meta_fields
        .iter()
        .map(|f| f.ident.as_ref().unwrap().to_string())
        .collect();

    let deserialize_field_idents: Vec<_> = non_meta_fields
        .iter()
        .map(|f| f.ident.as_ref().unwrap())
        .collect();

    // Create UpperCamelCase enum variants from snake_case field names
    // e.g., "username" -> Username, "display_name" -> DisplayName
    let deserialize_field_variants: Vec<_> = deserialize_field_names
        .iter()
        .map(|name| {
            let camel = name
                .split('_')
                .map(|word| {
                    let mut chars = word.chars();
                    match chars.next() {
                        None => String::new(),
                        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                    }
                })
                .collect::<String>();
            format_ident!("{}", camel)
        })
        .collect();

    let deserialize_field_types: Vec<_> = non_meta_fields.iter().map(|f| &f.ty).collect();

    let visitor_name = format_ident!("{}Visitor", ident);

    // --- generate impl ---
    let expanded = quote! {
        impl crate::object::traits::Object for #ident {
            const TYPE: &'static str = #type_name;

            fn meta(&self) -> &crate::object::meta::Meta {
                &self.#meta_field_ident
            }

            fn meta_mut(&mut self) -> &mut crate::object::meta::Meta {
                &mut self.#meta_field_ident
            }

            fn index_meta(&self) -> crate::query::IndexMeta {
                let mut values = std::collections::BTreeMap::new();
                #(#index_meta_insertions)*
                crate::query::IndexMeta(values)
            }
        }

        impl #ident {
            pub fn set_owner(&mut self, owner: ulid::Ulid) {
                   self.#meta_field_ident.owner = owner;
            }
        }

        impl crate::query::ObjectQuery for #ident {
            fn indexed_fields() -> &'static [crate::query::IndexField] {
                &[ #(#index_fields),* ]
            }
        }

        pub struct #indexes_struct_name {
            #(#indexes_struct_fields),*
        }

        impl #ident {
            pub const FIELDS: #indexes_struct_name = #indexes_struct_name {
                #(#indexes_const_fields),*
            };
        }

        // Custom Serialize implementation (excludes meta field)
        impl serde::Serialize for #ident {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                use serde::ser::SerializeStruct;
                let mut state = serializer.serialize_struct(stringify!(#ident), #field_count)?;
                #(#serialize_fields)*
                state.end()
            }
        }

        // Custom Deserialize implementation (excludes meta field, uses default)
        impl<'de> serde::Deserialize<'de> for #ident {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                #[derive(serde::Deserialize)]
                #[serde(field_identifier, rename_all = "snake_case")]
                enum Field {
                    #(#deserialize_field_variants,)*
                }

                struct #visitor_name;

                impl<'de> serde::de::Visitor<'de> for #visitor_name {
                    type Value = #ident;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                        formatter.write_str(concat!("struct ", stringify!(#ident)))
                    }

                    fn visit_map<V>(self, mut map: V) -> Result<#ident, V::Error>
                    where
                        V: serde::de::MapAccess<'de>,
                    {
                        #(
                            let mut #deserialize_field_idents: Option<#deserialize_field_types> = None;
                        )*

                        while let Some(key) = map.next_key()? {
                            match key {
                                #(
                                    Field::#deserialize_field_variants => {
                                        if #deserialize_field_idents.is_some() {
                                            return Err(serde::de::Error::duplicate_field(#deserialize_field_names));
                                        }
                                        #deserialize_field_idents = Some(map.next_value()?);
                                    }
                                )*
                            }
                        }

                        Ok(#ident {
                            #meta_field_ident: crate::object::meta::Meta::default(),
                            #(
                                #deserialize_field_idents: #deserialize_field_idents
                                    .ok_or_else(|| serde::de::Error::missing_field(#deserialize_field_names))?,
                            )*
                        })
                    }
                }

                const FIELDS: &[&str] = &[#(#deserialize_field_names),*];
                deserializer.deserialize_struct(stringify!(#ident), FIELDS, #visitor_name)
            }
        }
    };

    TokenStream::from(expanded)
}

const RESERVED_EDGE_FIELDS: &[&str] = &["from", "to", "type"];

/// Parse edge-specific attributes: type_name, from, to, and indexes
fn parse_edge_attr(
    attr: Option<&Attribute>,
    struct_name: &syn::Ident,
) -> (String, Type, Type, Vec<(String, String)>) {
    let mut type_name = None;
    let mut from_type = None;
    let mut to_type = None;
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
                    Meta::NameValue(nv) if nv.path.is_ident("from") => {
                        if let Expr::Path(ExprPath { path, .. }) = &nv.value {
                            from_type = Some(Type::Path(syn::TypePath {
                                qself: None,
                                path: path.clone(),
                            }));
                        } else {
                            panic!("from must be a type path");
                        }
                    }
                    Meta::NameValue(nv) if nv.path.is_ident("to") => {
                        if let Expr::Path(ExprPath { path, .. }) = &nv.value {
                            to_type = Some(Type::Path(syn::TypePath {
                                qself: None,
                                path: path.clone(),
                            }));
                        } else {
                            panic!("to must be a type path");
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

    let type_name = type_name.unwrap_or_else(|| struct_name.to_string());
    let from_type = from_type.expect("Edge must specify 'from' type in #[ousia(from = Type)]");
    let to_type = to_type.expect("Edge must specify 'to' type in #[ousia(to = Type)]");

    (type_name, from_type, to_type, indexes)
}

#[proc_macro_derive(OusiaEdge, attributes(ousia))]
pub fn derive_ousia_edge(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = &input.ident;

    // --- get ousia attribute ---
    let attr = get_ousia_attr(&input.attrs);
    let (type_name, from_type, to_type, indexes) = parse_edge_attr(attr, ident);

    // --- extract fields and identify meta field ---
    let fields = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(f) => &f.named,
            _ => panic!("OusiaEdge only supports named structs"),
        },
        _ => panic!("OusiaEdge only supports structs"),
    };

    // Find meta field
    let meta_fields: Vec<_> = fields.iter().filter(|f| is_meta_field(f)).collect();

    let meta_field_ident = if meta_fields.is_empty() {
        // Default to _meta if no field is marked
        fields
            .iter()
            .find(|f| f.ident.as_ref().unwrap() == "_meta")
            .map(|f| f.ident.as_ref().unwrap())
            .unwrap_or_else(|| {
                panic!(
                    "No meta field found. Either mark a field with #[ousia(meta)] or use '_meta' as field name"
                )
            })
    } else if meta_fields.len() > 1 {
        panic!("Only one field can be marked with #[ousia(meta)]");
    } else {
        meta_fields[0].ident.as_ref().unwrap()
    };

    // Get all non-meta fields
    let non_meta_fields: Vec<_> = fields
        .iter()
        .filter(|f| f.ident.as_ref().unwrap() != meta_field_ident)
        .collect();

    // Validate no reserved edge field names
    for field in &non_meta_fields {
        let f_str = field.ident.as_ref().unwrap().to_string();
        if RESERVED_EDGE_FIELDS.contains(&f_str.as_str()) {
            panic!(
                "Field `{}` is reserved for edge meta and cannot be declared in struct {}",
                f_str, ident
            );
        }
    }

    // --- generate IndexField list ---
    let index_fields = indexes.iter().map(|(name, kind)| {
        if RESERVED_EDGE_FIELDS.contains(&name.as_str()) {
            panic!(
                "Index field `{}` is reserved for edge meta and cannot be indexed",
                name
            );
        }
        if !non_meta_fields
            .iter()
            .any(|f| &f.ident.as_ref().unwrap().to_string() == name)
        {
            panic!("Indexed field `{}` does not exist on {}", name, ident);
        }

        let kinds: Vec<_> = kind
            .split('+')
            .map(|k| k.trim())
            .map(|k| match k {
                "search" => quote!(crate::query::IndexKind::Search),
                "sort" => quote!(crate::query::IndexKind::Sort),
                _ => panic!("Invalid index kind `{}`. Valid kinds: search, sort", k),
            })
            .collect();

        quote! {
            crate::query::IndexField {
                name: #name,
                kinds: &[#(#kinds),*],
            }
        }
    });

    // --- generate index_meta insertions ---
    let index_meta_insertions = indexes.iter().map(|(name, _kind)| {
        let field_name = format_ident!("{}", name);
        let name_str = name.as_str();

        quote! {
            values.insert(
                #name_str.to_string(),
                crate::query::ToIndexValue::to_index_value(&self.#field_name)
            );
        }
    });

    // --- generate Indexes struct ---
    let indexes_struct_name = format_ident!("{}Indexes", ident);

    let unique_index_names: std::collections::HashSet<_> =
        indexes.iter().map(|(name, _)| name.clone()).collect();
    let mut unique_index_names: Vec<_> = unique_index_names.into_iter().collect();
    unique_index_names.sort();

    let indexes_struct_fields = unique_index_names.iter().map(|name| {
        let field_ident = format_ident!("{}", name);
        quote! {
            pub #field_ident: crate::query::IndexField //fix this to construct the struct properly
        }
    });

    let indexes_const_fields = unique_index_names.iter().map(|name| {
        let field_ident = format_ident!("{}", name);
        let name_str = name.as_str();
        quote! {
            #field_ident: crate::query::IndexField { name: #name_str } // fix this to construct the struct properly
        }
    });

    // --- generate Serialize implementation (skip meta field) ---
    let serialize_fields = non_meta_fields.iter().map(|f| {
        let field_name = f.ident.as_ref().unwrap();
        let field_name_str = field_name.to_string();
        quote! {
            state.serialize_field(#field_name_str, &self.#field_name)?;
        }
    });

    let field_count = non_meta_fields.len();

    // --- generate Deserialize implementation ---
    let deserialize_field_names: Vec<_> = non_meta_fields
        .iter()
        .map(|f| f.ident.as_ref().unwrap().to_string())
        .collect();

    let deserialize_field_idents: Vec<_> = non_meta_fields
        .iter()
        .map(|f| f.ident.as_ref().unwrap())
        .collect();

    // Create UpperCamelCase enum variants from snake_case field names
    let deserialize_field_variants: Vec<_> = deserialize_field_names
        .iter()
        .map(|name| {
            let camel = name
                .split('_')
                .map(|word| {
                    let mut chars = word.chars();
                    match chars.next() {
                        None => String::new(),
                        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                    }
                })
                .collect::<String>();
            format_ident!("{}", camel)
        })
        .collect();

    let deserialize_field_types: Vec<_> = non_meta_fields.iter().map(|f| &f.ty).collect();

    let visitor_name = format_ident!("{}Visitor", ident);

    // --- generate impl ---
    let expanded = quote! {
        impl crate::edge::Edge for #ident {
            type From = #from_type;
            type To = #to_type;
            const TYPE: &'static str = #type_name;

            fn meta(&self) -> &crate::edge::EdgeMeta {
                &self.#meta_field_ident
            }

            fn meta_mut(&mut self) -> &mut crate::edge::EdgeMeta {
                &mut self.#meta_field_ident
            }

            fn index_meta(&self) -> crate::query::IndexMeta {
                let mut values = std::collections::BTreeMap::new();
                #(#index_meta_insertions)*
                crate::query::IndexMeta(values)
            }
        }

        impl #ident {
            pub fn new(from: ulid::Ulid, to: ulid::Ulid) -> Self {
                Self {
                    #meta_field_ident: crate::edge::EdgeMeta::new(from, to),
                    ..Default::default()
                }
            }

            pub fn set_from(&mut self, from: ulid::Ulid) {
                self.#meta_field_ident.from = from;
            }

            pub fn set_to(&mut self, to: ulid::Ulid) {
                self.#meta_field_ident.to = to;
            }
        }

        impl crate::edge::query::EdgeQuery for #ident {
            fn indexed_fields() -> &'static [crate::query::IndexField] {
                &[ #(#index_fields),* ]
            }
        }

        pub struct #indexes_struct_name {
            #(#indexes_struct_fields),*
        }

        impl #ident {
            pub const FIELDS: #indexes_struct_name = #indexes_struct_name {
                #(#indexes_const_fields),*
            };
        }

        // Custom Serialize implementation (excludes meta field)
        impl serde::Serialize for #ident {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                use serde::ser::SerializeStruct;
                let mut state = serializer.serialize_struct(stringify!(#ident), #field_count)?;
                #(#serialize_fields)*
                state.end()
            }
        }

        // Custom Deserialize implementation (excludes meta field, uses default)
        impl<'de> serde::Deserialize<'de> for #ident {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                #[derive(serde::Deserialize)]
                #[serde(field_identifier, rename_all = "snake_case")]
                enum Field {
                    #(#deserialize_field_variants,)*
                }

                struct #visitor_name;

                impl<'de> serde::de::Visitor<'de> for #visitor_name {
                    type Value = #ident;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                        formatter.write_str(concat!("struct ", stringify!(#ident)))
                    }

                    fn visit_map<V>(self, mut map: V) -> Result<#ident, V::Error>
                    where
                        V: serde::de::MapAccess<'de>,
                    {
                        #(
                            let mut #deserialize_field_idents: Option<#deserialize_field_types> = None;
                        )*

                        while let Some(key) = map.next_key()? {
                            match key {
                                #(
                                    Field::#deserialize_field_variants => {
                                        if #deserialize_field_idents.is_some() {
                                            return Err(serde::de::Error::duplicate_field(#deserialize_field_names));
                                        }
                                        #deserialize_field_idents = Some(map.next_value()?);
                                    }
                                )*
                            }
                        }

                        Ok(#ident {
                            #meta_field_ident: crate::edge::EdgeMeta::default(),
                            #(
                                #deserialize_field_idents: #deserialize_field_idents
                                    .ok_or_else(|| serde::de::Error::missing_field(#deserialize_field_names))?,
                            )*
                        })
                    }
                }

                const FIELDS: &[&str] = &[#(#deserialize_field_names),*];
                deserializer.deserialize_struct(stringify!(#ident), FIELDS, #visitor_name)
            }
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(OusiaDefault)]
pub fn derive_ousia_default(input: TokenStream) -> TokenStream {
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

    let default_fields = fields.iter().map(|f| {
        let name = f.ident.as_ref().unwrap();
        if name == meta_field_ident {
            quote! { #name: crate::object::meta::Meta::default() }
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
