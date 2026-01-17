use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use quote::{format_ident, quote};
use std::collections::{BTreeMap, HashSet};
use syn::{
    Attribute, Data, DeriveInput, Expr, ExprLit, Field, Fields, Lit, Meta, Type, parse_macro_input,
};

const RESERVED_FIELDS: &[&str] = &["id", "owner", "type", "created_at", "updated_at"];

fn import_ousia() -> proc_macro2::TokenStream {
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

/// Check if a field has #[ousia(private)] attribute
fn is_private_field(field: &Field) -> bool {
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

/// Extract view names from #[ousia(view(name1), view(name2))] attributes
fn extract_field_views(field: &Field) -> Vec<String> {
    let mut views = Vec::new();

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
                    if let Meta::List(list) = meta {
                        if list.path.is_ident("view") {
                            // Parse view(name)
                            let tokens = list.tokens.to_string();
                            views.push(tokens);
                        }
                    }
                }
            }
        }
    }

    views
}

/// Parse #[ousia_meta(view(name="field1, field2"))] attributes
fn parse_meta_views(field: &Field) -> BTreeMap<String, Vec<String>> {
    let mut meta_views = BTreeMap::new();

    for attr in &field.attrs {
        if !attr.path().is_ident("ousia_meta") {
            continue;
        }

        if let Meta::List(meta_list) = &attr.meta {
            let result = meta_list.parse_args_with(
                syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
            );

            if let Ok(nested) = result {
                for meta in nested {
                    if let Meta::List(list) = meta {
                        if list.path.is_ident("view") {
                            // Parse view(name="field1, field2")
                            if let Ok(nv) = syn::parse2::<syn::MetaNameValue>(list.tokens.clone()) {
                                if let Expr::Lit(ExprLit {
                                    lit: Lit::Str(s), ..
                                }) = &nv.value
                                {
                                    let view_name = nv.path.get_ident().unwrap().to_string();
                                    let fields: Vec<String> = s
                                        .value()
                                        .split(',')
                                        .map(|f| f.trim().to_string())
                                        .filter(|f| !f.is_empty())
                                        .collect();
                                    meta_views.insert(view_name, fields);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    meta_views
}

/// Validate that a view name is a valid Rust identifier
fn is_valid_rust_identifier(name: &str) -> bool {
    const RUST_KEYWORDS: &[&str] = &[
        "as", "break", "const", "continue", "crate", "else", "enum", "extern", "false", "fn",
        "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref",
        "return", "self", "Self", "static", "struct", "super", "trait", "true", "type", "unsafe",
        "use", "where", "while", "async", "await", "dyn", "abstract", "become", "box", "do",
        "final", "macro", "override", "priv", "typeof", "unsized", "virtual", "yield", "try",
    ];

    if name.is_empty() || RUST_KEYWORDS.contains(&name) {
        return false;
    }

    let mut chars = name.chars();
    let first = chars.next().unwrap();

    if !first.is_alphabetic() && first != '_' {
        return false;
    }

    chars.all(|c| c.is_alphanumeric() || c == '_')
}

/// Default meta fields when no explicit view(default="...") is specified
const DEFAULT_META_FIELDS: &[&str] = &["id", "created_at", "updated_at"];

/// Generate view struct and conversion method
fn generate_view_code(
    struct_name: &syn::Ident,
    view_name: &str,
    meta_fields: &[String],
    data_fields: &[(syn::Ident, Type)],
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    // Create PascalCase view name
    let view_pascal = view_name
        .split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect::<String>();

    let view_struct_name = format_ident!("{}{}View", struct_name, view_pascal);
    let method_name = format_ident!("_{}", view_name);

    // Generate struct fields
    let mut struct_fields = Vec::new();
    let mut field_assignments = Vec::new();

    // Add meta fields
    for meta_field in meta_fields {
        match meta_field.as_str() {
            "id" => {
                struct_fields.push(quote! { pub id: uuid::Uuid });
                field_assignments.push(quote! { id: self._meta.id });
            }
            "owner" => {
                struct_fields.push(quote! { pub owner: uuid::Uuid });
                field_assignments.push(quote! { owner: self._meta.owner });
            }
            "created_at" => {
                struct_fields.push(quote! { pub created_at: chrono::DateTime<chrono::Utc> });
                field_assignments.push(quote! { created_at: self._meta.created_at });
            }
            "updated_at" => {
                struct_fields.push(quote! { pub updated_at: chrono::DateTime<chrono::Utc> });
                field_assignments.push(quote! { updated_at: self._meta.updated_at });
            }
            _ => panic!(
                "Invalid meta field: {}. Valid fields are: id, owner, created_at, updated_at",
                meta_field
            ),
        }
    }

    // Add data fields
    for (field_name, field_type) in data_fields {
        struct_fields.push(quote! { pub #field_name: #field_type });
        field_assignments.push(quote! { #field_name: self.#field_name.clone() });
    }

    let view_struct = quote! {
        #[derive(serde::Serialize, Clone, Debug)]
        pub struct #view_struct_name {
            #(#struct_fields),*
        }
    };

    let view_method = quote! {
        pub fn #method_name(&self) -> #view_struct_name {
            #view_struct_name {
                #(#field_assignments),*
            }
        }
    };

    (view_struct, view_method)
}

/// Generate the internal serialization implementation
fn generate_internal_serialize(non_meta_fields: &[&Field]) -> proc_macro2::TokenStream {
    let field_serializations = non_meta_fields.iter().map(|f| {
        let field_name = f.ident.as_ref().unwrap();
        let field_name_str = field_name.to_string();
        quote! { #field_name_str: self.#field_name }
    });

    quote! {
        serde_json::json!({
            #(#field_serializations),*
        })
    }
}

/// Helper to parse kind strings into index kind tokens
fn parse_index_kinds(kind_str: &str) -> Vec<proc_macro2::TokenStream> {
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

#[proc_macro_derive(OusiaObject, attributes(ousia, ousia_meta))]
pub fn derive_ousia_object(input: TokenStream) -> TokenStream {
    let ousia = import_ousia();
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
        if RESERVED_FIELDS.contains(&f_str.as_str()) {
            panic!(
                "Field `{}` is reserved for meta and cannot be declared in struct {}",
                f_str, ident
            );
        }
    }

    // --- Collect view information ---
    let mut all_view_names = HashSet::new();
    let mut field_view_map: BTreeMap<String, Vec<String>> = BTreeMap::new();

    // Get meta field views
    let meta_field = fields
        .iter()
        .find(|f| f.ident.as_ref().unwrap() == meta_field_ident)
        .unwrap();
    let meta_views = parse_meta_views(meta_field);

    // Collect all view names from meta
    for view_name in meta_views.keys() {
        if view_name != "default" {
            all_view_names.insert(view_name.clone());
        }
    }

    // Collect view names from fields
    for field in &non_meta_fields {
        let field_name = field.ident.as_ref().unwrap().to_string();
        let views = extract_field_views(field);

        for view in views {
            if !is_valid_rust_identifier(&view) {
                panic!(
                    "Invalid view name '{}' on field '{}'. View names must be valid Rust identifiers.",
                    view, field_name
                );
            }
            if view != "default" {
                all_view_names.insert(view.clone());
            }
            field_view_map
                .entry(field_name.clone())
                .or_insert_with(Vec::new)
                .push(view);
        }
    }

    // --- Generate view structs and methods ---
    let mut view_structs = Vec::new();
    let mut view_methods = Vec::new();

    for view_name in &all_view_names {
        // Get meta fields for this view
        let meta_fields = meta_views.get(view_name).cloned().unwrap_or_else(Vec::new);

        // Get data fields for this view
        let data_fields: Vec<_> = non_meta_fields
            .iter()
            .filter(|f| {
                let fname = f.ident.as_ref().unwrap().to_string();
                field_view_map
                    .get(&fname)
                    .map(|views| views.contains(view_name))
                    .unwrap_or(false)
                    && !is_private_field(f)
            })
            .map(|f| (f.ident.as_ref().unwrap().clone(), f.ty.clone()))
            .collect();

        let (view_struct, view_method) =
            generate_view_code(ident, view_name, &meta_fields, &data_fields);
        view_structs.push(view_struct);
        view_methods.push(view_method);
    }

    // --- generate IndexField list ---
    let index_fields = indexes.iter().map(|(name, kind)| {
        if RESERVED_FIELDS.contains(&name.as_str()) {
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

        let kinds = parse_index_kinds(kind);

        quote! {
            #ousia::query::IndexField {
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
                #ousia::query::ToIndexValue::to_index_value(&self.#field_name)
            );
        }
    });

    // --- generate Indexes struct ---
    let indexes_struct_name = format_ident!("{}Fields", ident);

    // Build a map of field names to their kinds (merge multiple declarations)
    let mut field_kinds_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, kind) in &indexes {
        field_kinds_map
            .entry(name.clone())
            .or_insert_with(Vec::new)
            .push(kind.clone());
    }

    let indexes_struct_fields = field_kinds_map.keys().map(|name| {
        let field_ident = format_ident!("{}", name);
        quote! {
            pub #field_ident: #ousia::query::IndexField
        }
    });

    let indexes_const_fields = field_kinds_map.iter().map(|(name, kinds)| {
        let field_ident = format_ident!("{}", name);
        let name_str = name.as_str();

        // Collect all unique kinds for this field
        let mut all_kinds = Vec::new();
        for kind_str in kinds {
            all_kinds.extend(parse_index_kinds(kind_str));
        }

        // Remove duplicates by converting to a set-like structure
        let unique_kinds = {
            let mut seen = std::collections::HashSet::new();
            all_kinds
                .into_iter()
                .filter(move |k| {
                    let key = k.to_string();
                    seen.insert(key)
                })
                .collect::<Vec<_>>()
        };

        quote! {
            #field_ident: #ousia::query::IndexField {
                name: #name_str,
                kinds: &[#(#unique_kinds),*],
            }
        }
    });

    // --- generate Serialize implementation (default view) ---
    // Get default meta fields
    let default_meta_fields = meta_views
        .get("default")
        .cloned()
        .unwrap_or_else(|| DEFAULT_META_FIELDS.iter().map(|s| s.to_string()).collect());

    let serialize_meta_fields = default_meta_fields.iter().map(|field_name| {
        let meta_field = format_ident!("{}", field_name);
        quote! {
            state.serialize_field(#field_name, &self._meta.#meta_field)?;
        }
    });

    let serialize_fields = non_meta_fields.iter().filter_map(|f| {
        let field_name = f.ident.as_ref().unwrap();
        let field_name_str = field_name.to_string();

        // Skip private fields in default view
        if is_private_field(f) {
            return None;
        }

        Some(quote! {
            state.serialize_field(#field_name_str, &self.#field_name)?;
        })
    });

    let non_private_count = non_meta_fields
        .iter()
        .filter(|f| !is_private_field(f))
        .count();
    let field_count = non_private_count + default_meta_fields.len();

    // --- generate internal serialization ---
    let internal_serialize_body = generate_internal_serialize(&non_meta_fields);

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

    // Handle the case where there are no data fields (only meta)
    let deserialize_impl = if non_meta_fields.is_empty() {
        // Simple case: no data fields, just create with default meta
        quote! {
            impl<'de> serde::Deserialize<'de> for #ident {
                fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
                where
                    D: serde::Deserializer<'de>,
                {
                    // For empty structs, we just need to consume the empty map
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
                            // Consume any fields in the map (ignore them)
                            while map.next_entry::<String, serde_json::Value>()?.is_some() {}

                            Ok(#ident {
                                #meta_field_ident: #ousia::object::meta::Meta::default(),
                            })
                        }

                        fn visit_unit<E>(self) -> Result<#ident, E>
                        where
                            E: serde::de::Error,
                        {
                            Ok(#ident {
                                #meta_field_ident: #ousia::object::meta::Meta::default(),
                            })
                        }
                    }

                    deserializer.deserialize_struct(stringify!(#ident), &[], #visitor_name)
                }
            }
        }
    } else {
        // Normal case: struct has data fields
        fn is_option_type(ty: &Type) -> bool {
            if let Type::Path(type_path) = ty {
                if let Some(segment) = type_path.path.segments.last() {
                    return segment.ident == "Option";
                }
            }
            false
        }

        // Check which fields are Option types
        let field_is_optional: Vec<bool> = non_meta_fields
            .iter()
            .map(|f| is_option_type(&f.ty))
            .collect();

        // Generate match arms - handle Option<T> fields differently
        let match_arms = deserialize_field_variants
            .iter()
            .zip(deserialize_field_idents.iter())
            .zip(deserialize_field_names.iter())
            .zip(field_is_optional.iter())
            .map(|(((variant, ident), name), is_opt)| {
                if *is_opt {
                    // For Option<T>: don't wrap in Some, just assign directly
                    // map.next_value()? returns Option<T>, store as Some(Option<T>)
                    quote! {
                        Field::#variant => {
                            if #ident.is_some() {
                                return Err(serde::de::Error::duplicate_field(#name));
                            }
                            #ident = Some(map.next_value()?);
                        }
                    }
                } else {
                    // For T: wrap in Some as before
                    quote! {
                        Field::#variant => {
                            if #ident.is_some() {
                                return Err(serde::de::Error::duplicate_field(#name));
                            }
                            #ident = Some(map.next_value()?);
                        }
                    }
                }
            });

        // Generate field initialization
        let field_inits = deserialize_field_idents
            .iter()
            .zip(deserialize_field_names.iter())
            .zip(field_is_optional.iter())
            .map(|((ident, name), is_opt)| {
                if *is_opt {
                    // For Option<T>: unwrap outer Option, inner Option becomes the field value
                    // Variable is Option<Option<T>>, we want Option<T>
                    quote! {
                        #ident: #ident.unwrap_or(None)
                    }
                } else {
                    // For T: error if missing
                    quote! {
                        #ident: #ident.ok_or_else(|| serde::de::Error::missing_field(#name))?
                    }
                }
            });

        quote! {
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
                                    #(#match_arms)*
                                }
                            }

                            Ok(#ident {
                                #meta_field_ident: #ousia::object::meta::Meta::default(),
                                #(#field_inits,)*
                            })
                        }
                    }

                    const FIELDS: &[&str] = &[#(#deserialize_field_names),*];
                    deserializer.deserialize_struct(stringify!(#ident), FIELDS, #visitor_name)
                }
            }
        }
    };

    // --- generate impl ---
    let expanded = quote! {
        const _: () = {
            use #ousia::object::traits::Object;
            use #ousia::object::traits::ObjectOwnership;
        };

        impl #ousia::object::traits::Object for #ident {
            const TYPE: &'static str = #type_name;

            fn meta(&self) -> &#ousia::object::meta::Meta {
                &self.#meta_field_ident
            }

            fn meta_mut(&mut self) -> &mut #ousia::object::meta::Meta {
                &mut self.#meta_field_ident
            }

            fn index_meta(&self) -> #ousia::query::IndexMeta {
                let mut values = std::collections::BTreeMap::new();
                values.insert("created_at".to_string(), #ousia::query::ToIndexValue::to_index_value(&self.#meta_field_ident.created_at));
                values.insert("updated_at".to_string(), #ousia::query::ToIndexValue::to_index_value(&self.#meta_field_ident.updated_at));

                #(#index_meta_insertions)*
                #ousia::query::IndexMeta(values)
            }
        }

        impl #ousia::object::ObjectInternal for #ident {
            fn __serialize_internal(&self) -> serde_json::Value {
                #internal_serialize_body
            }
        }

        impl #ident {
            #(#view_methods)*
        }

        impl #ousia::query::IndexQuery for #ident {
            fn indexed_fields() -> &'static [#ousia::query::IndexField] {
                &[ #(#index_fields),* ]
            }
        }

        pub struct #indexes_struct_name {
            pub created_at: #ousia::query::IndexField,
            pub updated_at: #ousia::query::IndexField,
            #(#indexes_struct_fields),*
        }

        impl #ident {
            pub const FIELDS: #indexes_struct_name = #indexes_struct_name {
                created_at: #ousia::query::IndexField {
                    name: "created_at",
                    kinds: &[#ousia::query::IndexKind::Search, #ousia::query::IndexKind::Sort],
                },
                updated_at: #ousia::query::IndexField {
                    name: "updated_at",
                    kinds: &[#ousia::query::IndexKind::Search, #ousia::query::IndexKind::Sort],
                },
                #(#indexes_const_fields),*
            };
        }

        #(#view_structs)*

        // Custom Serialize implementation (default view)
        impl serde::Serialize for #ident {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                use serde::ser::SerializeStruct;
                let mut state = serializer.serialize_struct(stringify!(#ident), #field_count)?;
                #(#serialize_meta_fields)*
                #(#serialize_fields)*
                state.end()
            }
        }

        #deserialize_impl
    };

    TokenStream::from(expanded)
}

const RESERVED_EDGE_FIELDS: &[&str] = &["from", "to", "type"];

/// Parse edge-specific attributes: type_name and indexes (from/to removed)
fn parse_edge_attr(
    attr: Option<&Attribute>,
    struct_name: &syn::Ident,
) -> (String, Vec<(String, String)>) {
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

    let type_name = type_name.unwrap_or_else(|| struct_name.to_string());

    (type_name, indexes)
}

#[proc_macro_derive(OusiaEdge, attributes(ousia))]
pub fn derive_ousia_edge(input: TokenStream) -> TokenStream {
    let ousia = import_ousia();
    let input = parse_macro_input!(input as DeriveInput);
    let ident = &input.ident;

    // --- get ousia attribute ---
    let attr = get_ousia_attr(&input.attrs);
    let (type_name, indexes) = parse_edge_attr(attr, ident);

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

        let kinds = parse_index_kinds(kind);

        quote! {
            #ousia::query::IndexField {
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
                #ousia::query::ToIndexValue::to_index_value(&self.#field_name)
            );
        }
    });

    // --- generate Indexes struct ---
    let indexes_struct_name = format_ident!("{}Indexes", ident);

    // Build a map of field names to their kinds (merge multiple declarations)
    let mut field_kinds_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, kind) in &indexes {
        field_kinds_map
            .entry(name.clone())
            .or_insert_with(Vec::new)
            .push(kind.clone());
    }

    let indexes_struct_fields = field_kinds_map.keys().map(|name| {
        let field_ident = format_ident!("{}", name);
        quote! {
            pub #field_ident: #ousia::query::IndexField
        }
    });

    let indexes_const_fields = field_kinds_map.iter().map(|(name, kinds)| {
        let field_ident = format_ident!("{}", name);
        let name_str = name.as_str();

        // Collect all unique kinds for this field
        let mut all_kinds = Vec::new();
        for kind_str in kinds {
            all_kinds.extend(parse_index_kinds(kind_str));
        }

        // Remove duplicates
        let unique_kinds = {
            let mut seen = std::collections::HashSet::new();
            all_kinds
                .into_iter()
                .filter(move |k| {
                    let key = k.to_string();
                    seen.insert(key)
                })
                .collect::<Vec<_>>()
        };

        quote! {
            #field_ident: #ousia::query::IndexField {
                name: #name_str,
                kinds: &[#(#unique_kinds),*],
            }
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

    // Handle the case where there are no data fields (only meta)
    let deserialize_impl = if non_meta_fields.is_empty() {
        // Simple case: no data fields, just create with default meta
        quote! {
            impl<'de> serde::Deserialize<'de> for #ident {
                fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
                where
                    D: serde::Deserializer<'de>,
                {
                    // For empty structs, we just need to consume the empty map
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
                            // Consume any fields in the map (ignore them)
                            while map.next_entry::<String, serde_json::Value>()?.is_some() {}

                            Ok(#ident {
                                #meta_field_ident: #ousia::edge::EdgeMeta::new(uuid::Uuid::now_v7(), uuid::Uuid::now_v7()),
                            })
                        }

                        fn visit_unit<E>(self) -> Result<#ident, E>
                        where
                            E: serde::de::Error,
                        {
                            Ok(#ident {
                                #meta_field_ident: #ousia::edge::EdgeMeta::new(uuid::Uuid::now_v7(), uuid::Uuid::now_v7()),
                            })
                        }
                    }

                    deserializer.deserialize_struct(stringify!(#ident), &[], #visitor_name)
                }
            }
        }
    } else {
        // Normal case: struct has data fields
        quote! {
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
                                #meta_field_ident: #ousia::edge::EdgeMeta::new(uuid::Uuid::now_v7(), uuid::Uuid::now_v7()),
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
        }
    };

    // --- generate impl ---
    let expanded = quote! {
        impl #ousia::edge::Edge for #ident {
            const TYPE: &'static str = #type_name;

            fn meta(&self) -> &#ousia::edge::EdgeMeta {
                &self.#meta_field_ident
            }

            fn meta_mut(&mut self) -> &mut #ousia::edge::EdgeMeta {
                &mut self.#meta_field_ident
            }

            fn index_meta(&self) -> #ousia::query::IndexMeta {
                let mut values = std::collections::BTreeMap::new();
                values.insert("from".to_string(), #ousia::query::ToIndexValue::to_index_value(&self.#meta_field_ident.from));
                values.insert("to".to_string(), #ousia::query::ToIndexValue::to_index_value(&self.#meta_field_ident.to));

                #(#index_meta_insertions)*
                #ousia::query::IndexMeta(values)
            }
        }

        impl #ousia::query::IndexQuery for #ident {
            fn indexed_fields() -> &'static [#ousia::query::IndexField] {
                &[ #(#index_fields),* ]
            }
        }

        pub struct #indexes_struct_name {
            pub from: #ousia::query::IndexField,
            pub to: #ousia::query::IndexField,
            #(#indexes_struct_fields),*
        }

        impl #ident {
            pub const FIELDS: #indexes_struct_name = #indexes_struct_name {
                from: #ousia::query::IndexField {
                    name: "from",
                    kinds: &[#ousia::query::IndexKind::Search],
                },
                to: #ousia::query::IndexField {
                    name: "to",
                    kinds: &[#ousia::query::IndexKind::Search],
                },
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

        #deserialize_impl
    };

    TokenStream::from(expanded)
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
