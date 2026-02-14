use syn::{Attribute, Error, Expr, ExprLit, Lit, Meta, Result};

#[derive(Debug, Clone)]
pub enum UniqueConstraint {
    Single(String),         // #[ousia(unique = "phone")] or #[ousia(unique = "owner")]
    Composite(Vec<String>), // #[ousia(unique = "owner+type")]
}

#[derive(Debug, Default)]
pub struct UniqueConfig {
    pub constraints: Vec<UniqueConstraint>,
}

impl UniqueConfig {
    pub fn from_attributes(attrs: &[Attribute]) -> Result<Self> {
        let mut config = UniqueConfig::default();

        for attr in attrs {
            if !attr.path().is_ident("ousia") {
                continue;
            }

            // Parse the Meta::List manually to avoid consuming other attributes
            if let Meta::List(meta_list) = &attr.meta {
                let nested = meta_list
                    .parse_args_with(
                        syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
                    )
                    .map_err(|e| {
                        Error::new_spanned(attr, format!("Failed to parse ousia attributes: {}", e))
                    })?;

                for meta in nested {
                    // Only process Meta::NameValue where path is "unique"
                    if let Meta::NameValue(nv) = meta {
                        if nv.path.is_ident("unique") {
                            if let Expr::Lit(ExprLit {
                                lit: Lit::Str(lit_str),
                                ..
                            }) = &nv.value
                            {
                                let unique_str = lit_str.value();

                                // Check if it's composite (contains '+')
                                if unique_str.contains('+') {
                                    let fields: Vec<String> = unique_str
                                        .split('+')
                                        .map(|s| s.trim().to_string())
                                        .collect();

                                    if fields.len() < 2 {
                                        return Err(Error::new_spanned(
                                            lit_str,
                                            "Composite unique constraint must have at least 2 fields",
                                        ));
                                    }

                                    // Validate fields
                                    Self::validate_unique_fields(&fields, lit_str)?;

                                    config.constraints.push(UniqueConstraint::Composite(fields));
                                } else {
                                    // Validate single field
                                    Self::validate_unique_fields(&[unique_str.clone()], lit_str)?;

                                    config
                                        .constraints
                                        .push(UniqueConstraint::Single(unique_str));
                                }
                            } else {
                                return Err(Error::new_spanned(
                                    &nv.value,
                                    "unique value must be a string literal",
                                ));
                            }
                        }
                    }
                }
            }
        }

        Ok(config)
    }

    fn validate_unique_fields(fields: &[String], span: &syn::LitStr) -> Result<()> {
        for field in fields {
            let field = field.trim();
            // Check if it's a reserved meta field that's not allowed
            if ["id", "created_at", "updated_at", "type"].contains(&field) {
                return Err(Error::new_spanned(
                    span,
                    format!(
                        "Field '{}' cannot be used in unique constraints (reserved meta field)",
                        field
                    ),
                ));
            }
            // 'owner' and data fields are allowed (no need to validate further here)
        }
        Ok(())
    }

    pub fn has_constraints(&self) -> bool {
        !self.constraints.is_empty()
    }
}
