use std::collections::BTreeMap;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Attribute, Data, DeriveInput, Expr, ExprLit, Fields, Lit, Meta, Type, parse_macro_input,
};

use crate::shared::{
    get_field_default_value, get_ousia_attr, import_ousia, is_meta_field, parse_index_kinds,
};

const RESERVED_EDGE_FIELDS: &[&str] = &["from", "to", "type"];

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

pub fn derive(input: TokenStream) -> TokenStream {
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
        fn is_option_type(ty: &Type) -> bool {
            if let Type::Path(type_path) = ty {
                if let Some(segment) = type_path.path.segments.last() {
                    return segment.ident == "Option";
                }
            }
            false
        }

        fn should_use_default(ty: &Type) -> bool {
            // Check if type is Option (always uses default)
            if is_option_type(ty) {
                return true;
            }

            // Check for common types that implement Default
            if let Type::Path(type_path) = ty {
                if let Some(segment) = type_path.path.segments.last() {
                    let type_name = segment.ident.to_string();

                    // Common stdlib types that implement Default
                    match type_name.as_str() {
                           // Collections
                           "Vec" | "HashMap" | "HashSet" | "BTreeMap" | "BTreeSet" |
                           "VecDeque" | "LinkedList" | "BinaryHeap" |
                           // Strings
                           "String" |
                           // Numbers (all primitive number types implement Default)
                           "i8" | "i16" | "i32" | "i64" | "i128" | "isize" |
                           "u8" | "u16" | "u32" | "u64" | "u128" | "usize" |
                           "f32" | "f64" |
                           // Other common types
                           "bool" | "PathBuf" | "Duration" => return true,
                           _ => {}
                       }
                }
            }

            // For other types (including custom enums), assume they implement Default
            // The compiler will verify at compile time
            true
        }

        // Check which fields are Option types (special handling)
        let field_is_optional: Vec<bool> = non_meta_fields
            .iter()
            .map(|f| is_option_type(&f.ty))
            .collect();

        // Check which fields should use Default::default()
        let field_uses_default: Vec<bool> = non_meta_fields
            .iter()
            .map(|f| should_use_default(&f.ty))
            .collect();

        // Extract explicit default values from #[ousia(default = "value")]
        let field_default_values: Vec<Option<String>> = non_meta_fields
            .iter()
            .map(|f| get_field_default_value(f))
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

        // Generate field initialization - now handles four cases:
        // 1. Option<T> fields
        // 2. Fields with explicit #[ousia(default = "value")]
        // 3. Fields that implement Default
        // 4. Required fields
        let field_inits = deserialize_field_idents
            .iter()
            .zip(deserialize_field_names.iter())
            .zip(field_is_optional.iter())
            .zip(field_uses_default.iter())
            .zip(field_default_values.iter())
            .map(|((((ident, name), is_opt), uses_default), default_value)| {
                if *is_opt {
                    // For Option<T>: unwrap outer Option, inner Option becomes the field value
                    // Variable is Option<Option<T>>, we want Option<T>
                    quote! {
                        #ident: #ident.unwrap_or(None)
                    }
                } else if let Some(default_expr) = default_value {
                    // For fields with explicit default value: parse and use the expression
                    let default_tokens: proc_macro2::TokenStream = default_expr
                        .parse()
                        .expect("Failed to parse default value expression");
                    quote! {
                        #ident: #ident.unwrap_or_else(|| #default_tokens)
                    }
                } else if *uses_default {
                    // For types that implement Default: use Default::default() if missing
                    quote! {
                        #ident: #ident.unwrap_or_else(|| Default::default())
                    }
                } else {
                    // For required fields: error if missing
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
                                #meta_field_ident: #ousia::edge::EdgeMeta::new(uuid::Uuid::now_v7(), uuid::Uuid::now_v7()),
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
