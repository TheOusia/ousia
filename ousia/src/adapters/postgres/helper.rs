use super::PostgresAdapter;
use sqlx::{
    Postgres, Row,
    postgres::{PgArguments, PgRow},
    query::{Query as PgQuery, QueryScalar},
};
use uuid::Uuid;

use crate::{
    adapters::{EdgeQuery, EdgeRecord, Error, ObjectRecord, TraversalDirection},
    query::{Cursor, IndexValue, IndexValueInner, QueryFilter},
};

impl PostgresAdapter {
    /// Slim mapper — for all read paths. Skips index_meta (not in SELECT, not needed by to_object()).
    pub(super) fn map_row_to_object_record_slim(row: PgRow) -> Result<ObjectRecord, Error> {
        let type_name = row
            .try_get::<String, _>("type")
            .map_err(|e| Error::Deserialize(e.to_string()))?;
        let id = row
            .try_get::<Uuid, _>("id")
            .map_err(|e| Error::Deserialize(e.to_string()))?;
        let owner = row
            .try_get::<Uuid, _>("owner")
            .map_err(|e| Error::Deserialize(e.to_string()))?;
        let created_at = row
            .try_get("created_at")
            .map_err(|e| Error::Deserialize(e.to_string()))?;
        let updated_at = row
            .try_get("updated_at")
            .map_err(|e| Error::Deserialize(e.to_string()))?;
        let data: serde_json::Value = row
            .try_get("data")
            .map_err(|e| Error::Deserialize(e.to_string()))?;
        Ok(ObjectRecord {
            id,
            type_name: std::borrow::Cow::Owned(type_name),
            owner,
            created_at,
            updated_at,
            data,
            index_meta: serde_json::Value::Null,
        })
    }

    pub(super) fn map_row_to_edge_record(row: PgRow) -> Result<EdgeRecord, Error> {
        let type_name = row
            .try_get::<String, _>("type")
            .map_err(|e| Error::Deserialize(e.to_string()))?;
        let from = row
            .try_get::<Uuid, _>("from")
            .map_err(|e| Error::Deserialize(e.to_string()))?;
        let to = row
            .try_get::<Uuid, _>("to")
            .map_err(|e| Error::Deserialize(e.to_string()))?;
        let data: serde_json::Value = row
            .try_get("data")
            .map_err(|e| Error::Deserialize(e.to_string()))?;
        let index_meta: serde_json::Value = row
            .try_get("index_meta")
            .map_err(|e| Error::Deserialize(e.to_string()))?;
        Ok(EdgeRecord {
            type_name: std::borrow::Cow::Owned(type_name),
            from,
            to,
            data,
            index_meta,
        })
    }

    pub(super) fn map_row_to_edge_and_object(
        row: PgRow,
    ) -> Result<(EdgeRecord, ObjectRecord), Error> {
        let de = |e: sqlx::Error| Error::Deserialize(e.to_string());
        let edge = EdgeRecord {
            type_name: std::borrow::Cow::Owned(row.try_get::<String, _>("edge_type").map_err(de)?),
            from: row.try_get::<Uuid, _>("edge_from").map_err(de)?,
            to: row.try_get::<Uuid, _>("edge_to").map_err(de)?,
            data: row
                .try_get::<serde_json::Value, _>("edge_data")
                .map_err(de)?,
            index_meta: row
                .try_get::<serde_json::Value, _>("edge_index_meta")
                .map_err(de)?,
        };
        let obj = ObjectRecord {
            id: row.try_get::<Uuid, _>("obj_id").map_err(de)?,
            type_name: std::borrow::Cow::Owned(row.try_get::<String, _>("obj_type").map_err(de)?),
            owner: row.try_get::<Uuid, _>("obj_owner").map_err(de)?,
            created_at: row.try_get("obj_created_at").map_err(de)?,
            updated_at: row.try_get("obj_updated_at").map_err(de)?,
            data: row
                .try_get::<serde_json::Value, _>("obj_data")
                .map_err(de)?,
            index_meta: serde_json::Value::Null,
        };
        Ok((edge, obj))
    }

    pub(super) async fn query_edges_with_objects_inner(
        &self,
        edge_type_name: &str,
        type_name: &str,
        owner: Uuid,
        obj_filters: &[QueryFilter],
        plan: EdgeQuery,
        direction: TraversalDirection,
    ) -> Result<Vec<(EdgeRecord, ObjectRecord)>, Error> {
        let where_clause = Self::build_object_traversal_query_conditions(
            direction.clone(),
            obj_filters,
            &plan.filters,
            plan.cursor,
        );
        let order_clause = Self::build_edge_order_clause(&plan.filters);
        let join_col = match direction {
            TraversalDirection::Forward => "to",
            TraversalDirection::Reverse => "from",
        };
        let mut sql = format!(
            r#"
            SELECT
                e."from" AS edge_from, e."to" AS edge_to, e.type AS edge_type,
                e.data AS edge_data, e.index_meta AS edge_index_meta,
                o.id AS obj_id, o.type AS obj_type, o.owner AS obj_owner,
                o.created_at AS obj_created_at, o.updated_at AS obj_updated_at,
                o.data AS obj_data
            FROM edges e
            JOIN objects o ON e."{join_col}" = o.id
            {where_clause}
            {order_clause}
            "#,
        );
        if let Some(limit) = plan.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }
        let mut query = sqlx::query(&sql)
            .bind(type_name)
            .bind(edge_type_name)
            .bind(owner);
        if let Some(cursor) = plan.cursor {
            query = query.bind(cursor.last_id);
        }
        query = Self::query_bind_filters(query, obj_filters);
        query = Self::query_bind_filters(query, &plan.filters);
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(rows
            .into_iter()
            .filter_map(|row| Self::map_row_to_edge_and_object(row).ok())
            .collect())
    }

    /// Wraps a value as `{"field": value}` for use with the `@>` GIN operator.
    pub(super) fn make_eq_json(field: &str, val: serde_json::Value) -> serde_json::Value {
        let mut map = serde_json::Map::with_capacity(1);
        map.insert(field.to_string(), val);
        serde_json::Value::Object(map)
    }

    pub(super) fn inner_to_json(elem: &IndexValueInner) -> serde_json::Value {
        match elem {
            IndexValueInner::String(s) => serde_json::Value::String(s.clone()),
            IndexValueInner::Int(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
            IndexValueInner::Float(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
        }
    }

    pub(super) fn index_value_to_json(value: &IndexValue) -> serde_json::Value {
        match value {
            IndexValue::String(s) => serde_json::Value::String(s.clone()),
            IndexValue::Int(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
            IndexValue::Float(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            IndexValue::Bool(b) => serde_json::Value::Bool(*b),
            _ => unreachable!("UUID/Timestamp/Array handled in extraction path"),
        }
    }

    pub(super) fn build_filter_condition(
        alias: &str,
        filter: &QueryFilter,
        param_idx: &mut usize,
    ) -> Option<(String, &'static str)> {
        let crate::query::QueryMode::Search(ref qs) = filter.mode else {
            return None;
        };

        let operator = match qs.operator {
            crate::query::Operator::And => "AND",
            _ => "OR",
        };

        use crate::query::Comparison::*;

        // GIN jsonb_path_ops @> path: hits the index for equality and array containment
        match (&qs.comparison, &filter.value) {
            // Scalar equality for types with safe JSON value semantics
            (
                Equal,
                IndexValue::String(_)
                | IndexValue::Int(_)
                | IndexValue::Float(_)
                | IndexValue::Bool(_),
            ) => {
                let cond = format!("{}.index_meta @> ${}", alias, param_idx);
                *param_idx += 1;
                return Some((cond, operator));
            }
            // ContainsAll array: single @> with the full array
            (ContainsAll, IndexValue::Array(arr)) if !arr.is_empty() => {
                let cond = format!("{}.index_meta @> ${}", alias, param_idx);
                *param_idx += 1;
                return Some((cond, operator));
            }
            // Empty array filters: skip (vacuously true/false — no useful predicate)
            (Contains | ContainsAll, IndexValue::Array(arr)) if arr.is_empty() => {
                return None;
            }
            // Contains array: one @> per element, joined with OR
            (Contains, IndexValue::Array(arr)) => {
                let conds: Vec<String> = (0..arr.len())
                    .map(|i| format!("{}.index_meta @> ${}", alias, *param_idx + i))
                    .collect();
                *param_idx += arr.len();
                let combined = if conds.len() == 1 {
                    conds.into_iter().next().unwrap()
                } else {
                    format!("({})", conds.join(" OR "))
                };
                return Some((combined, operator));
            }
            _ => {}
        }

        // Extraction path: range ops, ILIKE, UUID/timestamp equality
        let index_type = Self::index_type_str(&filter.value);
        let comparison = match qs.comparison {
            Equal => "=",
            NotEqual => "<>",
            GreaterThan => ">",
            LessThan => "<",
            GreaterThanOrEqual => ">=",
            LessThanOrEqual => "<=",
            BeginsWith => "ILIKE",
            Contains => "ILIKE",
            ContainsAll => "ILIKE",
        };

        let condition = format!(
            "({}.index_meta->>'{}')::{} {} ${}",
            alias, filter.field.name, index_type, comparison, param_idx
        );
        *param_idx += 1;
        Some((condition, operator))
    }

    pub(super) fn join_conditions(conditions: &[(String, &str)]) -> String {
        let mut out = String::new();
        for (i, (cond, op)) in conditions.iter().enumerate() {
            out.push_str(cond);
            if i < conditions.len() - 1 {
                out.push(' ');
                out.push_str(op);
                out.push(' ');
            }
        }
        out
    }

    /// Maps an `IndexValue` to its Postgres cast type string.
    pub(super) fn index_type_str(value: &IndexValue) -> &'static str {
        match value {
            IndexValue::String(_) => "text",
            IndexValue::Int(_) => "bigint",
            IndexValue::Float(_) => "double precision",
            IndexValue::Bool(_) => "boolean",
            IndexValue::Timestamp(_) => "timestamptz",
            IndexValue::Uuid(_) => "uuid",
            IndexValue::Array(arr) => match arr.first() {
                Some(IndexValueInner::String(_)) => "text[]",
                Some(IndexValueInner::Int(_)) => "bigint[]",
                Some(IndexValueInner::Float(_)) => "double precision[]",
                None => "text[]",
            },
        }
    }

    pub(super) fn build_object_query_conditions(
        filters: &[QueryFilter],
        cursor: Option<Cursor>,
    ) -> String {
        // $1 = type, $2 = owner, $3 = cursor (optional), $4+ = filter values
        let mut conditions: Vec<(String, &str)> = vec![
            ("o.type = $1".to_string(), "AND"),
            ("o.owner = $2".to_string(), "AND"),
        ];
        let mut param_idx = 3;

        if cursor.is_some() {
            conditions.push((format!("o.id < ${}", param_idx), "AND"));
            param_idx += 1;
        }

        for filter in filters {
            if let Some((cond, op)) = Self::build_filter_condition("o", filter, &mut param_idx) {
                conditions.push((cond, op));
            }
        }

        format!("WHERE {}", Self::join_conditions(&conditions))
    }

    pub(super) fn build_edge_query_conditions(
        filters: &[QueryFilter],
        cursor: Option<Cursor>,
        direction: TraversalDirection,
    ) -> String {
        // $1 = type, $2 = from/to owner, $3 = cursor (optional), $4+ = filter values
        let anchor_col = match direction {
            TraversalDirection::Forward => r#"e."from""#,
            TraversalDirection::Reverse => r#"e."to""#,
        };
        let cursor_col = match direction {
            TraversalDirection::Forward => r#"e."to""#,
            TraversalDirection::Reverse => r#"e."from""#,
        };

        let mut conditions: Vec<(String, &str)> = vec![
            ("e.type = $1".to_string(), "AND"),
            (format!("{} = $2", anchor_col), "AND"),
        ];
        let mut param_idx = 3;

        if cursor.is_some() {
            conditions.push((format!("{} < ${}", cursor_col, param_idx), "AND"));
            param_idx += 1;
        }

        for filter in filters {
            if let Some((cond, op)) = Self::build_filter_condition("e", filter, &mut param_idx) {
                conditions.push((cond, op));
            }
        }

        format!("WHERE {}", Self::join_conditions(&conditions))
    }

    pub(super) fn build_order_clause(filters: &[QueryFilter], is_edge: bool) -> String {
        Self::build_order_clause_aliased(filters, "", is_edge)
    }

    pub(super) fn build_edge_order_clause(filters: &[QueryFilter]) -> String {
        Self::build_order_clause_aliased(filters, "e", true)
    }

    pub(super) fn build_order_clause_aliased(
        filters: &[QueryFilter],
        alias: &str,
        is_edge: bool,
    ) -> String {
        let prefix = if alias.is_empty() {
            String::new()
        } else {
            format!("{}.", alias)
        };

        let sort: Vec<&QueryFilter> = filters
            .iter()
            .filter(|f| f.mode.as_sort().is_some())
            .collect();

        if sort.is_empty() {
            if is_edge {
                return "".to_string();
            }
            return format!("ORDER BY {}id DESC", prefix);
        }

        let order_terms: Vec<String> = sort
            .iter()
            .filter(|s| s.value.as_array().is_none())
            .map(|s| {
                let direction = if s.mode.as_sort().unwrap().ascending {
                    "ASC"
                } else {
                    "DESC"
                };
                // Native columns: use direct column reference so composite indexes are hit
                if matches!(s.field.name, "created_at" | "updated_at") {
                    return format!("{}{} {}", prefix, s.field.name, direction);
                }
                let index_type = match &s.value {
                    IndexValue::String(_) => "text",
                    IndexValue::Int(_) => "bigint",
                    IndexValue::Float(_) => "double precision",
                    IndexValue::Bool(_) => "boolean",
                    IndexValue::Timestamp(_) => "timestamptz",
                    _ => "text",
                };
                format!(
                    "({}index_meta->>'{}')::{} {}",
                    prefix, s.field.name, index_type, direction,
                )
            })
            .collect();

        format!("ORDER BY {}", order_terms.join(", "))
    }

    pub(super) fn build_object_traversal_query_conditions(
        direction: TraversalDirection,
        obj_filters: &[QueryFilter],
        edge_filters: &Vec<QueryFilter>,
        cursor: Option<Cursor>,
    ) -> String {
        // $1 = object type_name
        // $2 = edge type_name
        // $3 = owner
        let mut param_idx: usize = 4; // next free slot

        // ── Object conditions ────────────────────────────────────────────────────
        let mut obj_conditions: Vec<(String, &str)> = vec![("o.type = $1".to_string(), "AND")];

        if cursor.is_some() {
            obj_conditions.push((format!("o.id < ${}", param_idx), "AND"));
            param_idx += 1;
        }

        for filter in obj_filters {
            if let Some((cond, op)) = Self::build_filter_condition("o", filter, &mut param_idx) {
                obj_conditions.push((cond, op));
            }
        }

        // ── Edge conditions ──────────────────────────────────────────────────────
        let owner_col = match direction {
            TraversalDirection::Forward => r#"e."from""#,
            TraversalDirection::Reverse => r#"e."to""#,
        };

        let mut edge_conditions: Vec<(String, &str)> = vec![
            ("e.type = $2".to_string(), "AND"),
            (format!("{} = $3", owner_col), "AND"),
        ];

        for filter in edge_filters {
            if let Some((cond, op)) = Self::build_filter_condition("e", filter, &mut param_idx) {
                edge_conditions.push((cond, op));
            }
        }

        // ── Combine: obj AND edge ────────────────────────────────────────────────
        let obj_clause = Self::join_conditions(&obj_conditions);
        let edge_clause = Self::join_conditions(&edge_conditions);

        format!("WHERE {} AND ({})", obj_clause, edge_clause)
    }

    pub(super) fn query_bind_filters<'a>(
        mut query: PgQuery<'a, Postgres, PgArguments>,
        filters: &'a [QueryFilter],
    ) -> PgQuery<'a, Postgres, PgArguments> {
        use crate::query::Comparison::*;
        for filter in filters.iter().filter(|f| f.mode.as_search().is_some()) {
            let search = filter.mode.as_search().unwrap();
            match (&search.comparison, &filter.value) {
                // GIN @> binds: {"field": value}
                (
                    Equal,
                    IndexValue::String(_)
                    | IndexValue::Int(_)
                    | IndexValue::Float(_)
                    | IndexValue::Bool(_),
                ) => {
                    query = query.bind(Self::make_eq_json(
                        filter.field.name,
                        Self::index_value_to_json(&filter.value),
                    ));
                }
                (ContainsAll, IndexValue::Array(arr)) if !arr.is_empty() => {
                    let elements: Vec<serde_json::Value> =
                        arr.iter().map(Self::inner_to_json).collect();
                    query = query.bind(Self::make_eq_json(
                        filter.field.name,
                        serde_json::Value::Array(elements),
                    ));
                }
                (Contains, IndexValue::Array(arr)) if !arr.is_empty() => {
                    for elem in arr.iter() {
                        let val = Self::inner_to_json(elem);
                        query = query.bind(Self::make_eq_json(
                            filter.field.name,
                            serde_json::Value::Array(vec![val]),
                        ));
                    }
                }
                // Extraction-based binds: range ops, ILIKE, UUID, timestamp
                (_, IndexValue::String(s)) => {
                    query = match search.comparison {
                        BeginsWith => query.bind(format!("{}%", s)),
                        Contains => query.bind(format!("%{}%", s)),
                        _ => query.bind(s),
                    };
                }
                (_, IndexValue::Int(i)) => {
                    query = query.bind(i);
                }
                (_, IndexValue::Float(f)) => {
                    query = query.bind(f);
                }
                (_, IndexValue::Bool(b)) => {
                    query = query.bind(b);
                }
                (_, IndexValue::Timestamp(t)) => {
                    query = query.bind(t);
                }
                (_, IndexValue::Uuid(uid)) => {
                    query = query.bind(uid);
                }
                // Empty arrays and remaining array cases: condition was skipped, no bind
                (_, IndexValue::Array(_)) => {}
            }
        }
        query
    }

    pub(super) fn query_scalar_bind_filters<'a, O>(
        mut query: QueryScalar<'a, Postgres, O, PgArguments>,
        filters: &'a [QueryFilter],
    ) -> QueryScalar<'a, Postgres, O, PgArguments> {
        use crate::query::Comparison::*;
        for filter in filters.iter().filter(|f| f.mode.as_search().is_some()) {
            let search = filter.mode.as_search().unwrap();
            match (&search.comparison, &filter.value) {
                // GIN @> binds: {"field": value}
                (
                    Equal,
                    IndexValue::String(_)
                    | IndexValue::Int(_)
                    | IndexValue::Float(_)
                    | IndexValue::Bool(_),
                ) => {
                    query = query.bind(Self::make_eq_json(
                        filter.field.name,
                        Self::index_value_to_json(&filter.value),
                    ));
                }
                (ContainsAll, IndexValue::Array(arr)) if !arr.is_empty() => {
                    let elements: Vec<serde_json::Value> =
                        arr.iter().map(Self::inner_to_json).collect();
                    query = query.bind(Self::make_eq_json(
                        filter.field.name,
                        serde_json::Value::Array(elements),
                    ));
                }
                (Contains, IndexValue::Array(arr)) if !arr.is_empty() => {
                    for elem in arr.iter() {
                        let val = Self::inner_to_json(elem);
                        query = query.bind(Self::make_eq_json(
                            filter.field.name,
                            serde_json::Value::Array(vec![val]),
                        ));
                    }
                }
                // Extraction-based binds: range ops, ILIKE, UUID, timestamp
                (_, IndexValue::String(s)) => {
                    query = match search.comparison {
                        BeginsWith => query.bind(format!("{}%", s)),
                        Contains => query.bind(format!("%{}%", s)),
                        _ => query.bind(s),
                    };
                }
                (_, IndexValue::Int(i)) => {
                    query = query.bind(i);
                }
                (_, IndexValue::Float(f)) => {
                    query = query.bind(f);
                }
                (_, IndexValue::Bool(b)) => {
                    query = query.bind(b);
                }
                (_, IndexValue::Timestamp(t)) => {
                    query = query.bind(t);
                }
                (_, IndexValue::Uuid(uid)) => {
                    query = query.bind(uid);
                }
                (_, IndexValue::Array(_)) => {}
            }
        }
        query
    }
}

impl PostgresAdapter {
    pub(super) async fn edge_traversal_inner(
        &self,
        edge_type_name: &str,
        type_name: &str,
        owner: Uuid,
        filters: &[QueryFilter],
        plan: EdgeQuery,
        direction: TraversalDirection,
    ) -> Result<Vec<ObjectRecord>, Error> {
        let where_clause = Self::build_object_traversal_query_conditions(
            direction.clone(),
            filters,
            &plan.filters,
            plan.cursor,
        );
        let order_clause = Self::build_edge_order_clause(&plan.filters);

        let mut sql = format!(
            r#"
            SELECT o.id, o.type, o.owner, o.created_at, o.updated_at, o.data
            FROM edges e
            LEFT JOIN objects o ON e."{join_col}" = o.id
            {where_clause}
            {order_clause}
            "#,
            join_col = match direction {
                TraversalDirection::Forward => "to",
                TraversalDirection::Reverse => "from",
            },
            where_clause = where_clause,
            order_clause = order_clause,
        );

        if let Some(limit) = plan.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        // Bind in the fixed order that build_object_traversal_query_conditions expects:
        //   $1 = object type_name
        //   $2 = edge type_name
        //   $3 = owner
        //   $4 = cursor (optional)
        //   $5+ = filter values (object then edge, matching the WHERE clause order)
        let mut query = sqlx::query(&sql)
            .bind(type_name)
            .bind(edge_type_name)
            .bind(owner);

        if let Some(cursor) = plan.cursor {
            query = query.bind(cursor.last_id);
        }

        // Bind object filters then edge filters in the same order as the WHERE clause.
        query = Self::query_bind_filters(query, filters);
        query = Self::query_bind_filters(query, &plan.filters);

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;

        Ok(rows
            .into_iter()
            .filter_map(|row| Self::map_row_to_object_record_slim(row).ok())
            .collect())
    }

    /// Build WHERE clause for batch traversal queries (multiple pivot IDs).
    /// Bindings: $1=obj_type, $2=edge_type, $3=ids (Vec<Uuid>), $4+=filters.
    pub(super) fn build_batch_traversal_conditions(
        direction: TraversalDirection,
        obj_filters: &[QueryFilter],
        edge_filters: &[QueryFilter],
    ) -> String {
        let mut param_idx: usize = 4;

        let mut obj_conditions: Vec<(String, &str)> = vec![("o.type = $1".to_string(), "AND")];
        for f in obj_filters {
            if let Some((c, op)) = Self::build_filter_condition("o", f, &mut param_idx) {
                obj_conditions.push((c, op));
            }
        }

        let anchor = match direction {
            TraversalDirection::Forward => r#"e."from""#,
            TraversalDirection::Reverse => r#"e."to""#,
        };
        let mut edge_conditions: Vec<(String, &str)> = vec![
            ("e.type = $2".to_string(), "AND"),
            (format!("{} = ANY($3)", anchor), "AND"),
        ];
        for f in edge_filters {
            if let Some((c, op)) = Self::build_filter_condition("e", f, &mut param_idx) {
                edge_conditions.push((c, op));
            }
        }

        format!(
            "WHERE {} AND ({})",
            Self::join_conditions(&obj_conditions),
            Self::join_conditions(&edge_conditions)
        )
    }

    /// Build WHERE clause for batch edge-only queries (no object JOIN).
    /// Bindings: $1=edge_type, $2=ids (Vec<Uuid>), $3+=filters.
    pub(super) fn build_batch_edge_only_conditions(
        direction: TraversalDirection,
        edge_filters: &[QueryFilter],
    ) -> String {
        let anchor = match direction {
            TraversalDirection::Forward => r#"e."from""#,
            TraversalDirection::Reverse => r#"e."to""#,
        };
        let mut conditions: Vec<(String, &str)> = vec![
            ("e.type = $1".to_string(), "AND"),
            (format!("{} = ANY($2)", anchor), "AND"),
        ];
        let mut param_idx = 3;
        for f in edge_filters {
            if let Some((c, op)) = Self::build_filter_condition("e", f, &mut param_idx) {
                conditions.push((c, op));
            }
        }
        format!("WHERE {}", Self::join_conditions(&conditions))
    }

    /// Build WHERE clause for one branch of a UNION both-directions query (object JOIN).
    /// Both branches share the same param slots:
    ///   $1=obj_type, $2=edge_type, $3=pivot, $4+=filters.
    pub(super) fn build_union_branch_with_obj_conditions(
        direction: TraversalDirection,
        obj_filters: &[QueryFilter],
        edge_filters: &[QueryFilter],
    ) -> String {
        let mut param_idx: usize = 4;

        let mut obj_conditions: Vec<(String, &str)> = vec![("o.type = $1".to_string(), "AND")];
        for f in obj_filters {
            if let Some((c, op)) = Self::build_filter_condition("o", f, &mut param_idx) {
                obj_conditions.push((c, op));
            }
        }

        let anchor = match direction {
            TraversalDirection::Forward => r#"e."from""#,
            TraversalDirection::Reverse => r#"e."to""#,
        };
        let mut edge_conditions: Vec<(String, &str)> = vec![
            ("e.type = $2".to_string(), "AND"),
            (format!("{} = $3", anchor), "AND"),
        ];
        for f in edge_filters {
            if let Some((c, op)) = Self::build_filter_condition("e", f, &mut param_idx) {
                edge_conditions.push((c, op));
            }
        }

        format!(
            "WHERE {} AND ({})",
            Self::join_conditions(&obj_conditions),
            Self::join_conditions(&edge_conditions)
        )
    }

    /// Build WHERE clause for one branch of a UNION both-directions edge-only query.
    /// Both branches share: $1=edge_type, $2=pivot, $3+=filters.
    pub(super) fn build_union_branch_edge_only_conditions(
        direction: TraversalDirection,
        edge_filters: &[QueryFilter],
    ) -> String {
        let anchor = match direction {
            TraversalDirection::Forward => r#"e."from""#,
            TraversalDirection::Reverse => r#"e."to""#,
        };
        let mut conditions: Vec<(String, &str)> = vec![
            ("e.type = $1".to_string(), "AND"),
            (format!("{} = $2", anchor), "AND"),
        ];
        let mut param_idx = 3;
        for f in edge_filters {
            if let Some((c, op)) = Self::build_filter_condition("e", f, &mut param_idx) {
                conditions.push((c, op));
            }
        }
        format!("WHERE {}", Self::join_conditions(&conditions))
    }

    pub(super) async fn query_edges_internal(
        &self,
        type_name: &'static str,
        owner: Uuid,
        plan: EdgeQuery,
        direction: TraversalDirection,
    ) -> Result<Vec<EdgeRecord>, Error> {
        let where_clause = Self::build_edge_query_conditions(&plan.filters, plan.cursor, direction);
        let order_clause = Self::build_edge_order_clause(&plan.filters);

        let mut sql = format!(
            r#"
            SELECT e."from", e."to", e.type, e.data, e.index_meta
            FROM edges e
            {}
            {}
            "#,
            where_clause, order_clause
        );

        if let Some(limit) = plan.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        let mut query = sqlx::query(&sql).bind(type_name).bind(owner);
        if let Some(cursor) = plan.cursor {
            query = query.bind(cursor.last_id);
        }

        query = Self::query_bind_filters(query, &plan.filters);

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;

        Ok(rows
            .into_iter()
            .filter_map(|row| Self::map_row_to_edge_record(row).ok())
            .collect())
    }
}
