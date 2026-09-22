use super::PostgresAdapter;
use sqlx::{
    Postgres, Row,
    postgres::{PgArguments, PgRow},
    query::{Query as PgQuery, QueryScalar},
};
use uuid::Uuid;

use crate::{
    adapters::{EdgeQuery, EdgeRecord, Error, ObjectRecord, TraversalDirection},
    query::{Cursor, GeoFilter, GeoOrder, IndexValue, IndexValueInner, QueryFilter},
};

/// Resolved JOIN plan for the geo filters + order on a single query.
/// Each entry maps a geo-field name to its emitted alias.
#[derive(Debug, Default)]
pub(super) struct GeoJoinPlan {
    /// JOIN block to splice into the FROM clause (may be empty).
    pub joins: String,
    /// Map: geo-field name -> alias (e.g. `g0`, `g1`, `g_ord`).
    pub aliases: Vec<(String, String)>,
    /// Alias for the order field (if `geo_order` is set).
    pub order_alias: Option<String>,
}


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
        let de = |e: sqlx::Error| Error::Deserialize(e.to_string());
        Ok(EdgeRecord {
            type_name: std::borrow::Cow::Owned(row.try_get::<String, _>("type").map_err(de)?),
            from: row.try_get::<Uuid, _>("from").map_err(de)?,
            to: row.try_get::<Uuid, _>("to").map_err(de)?,
            data: row.try_get::<serde_json::Value, _>("data").map_err(de)?,
            index_meta: serde_json::Value::Null,
            created_at: row.try_get("created_at").map_err(de)?,
            updated_at: row.try_get("updated_at").map_err(de)?,
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
            index_meta: serde_json::Value::Null,
            created_at: row.try_get("edge_created_at").map_err(de)?,
            updated_at: row.try_get("edge_updated_at").map_err(de)?,
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
        let order_clause =
            Self::build_traversal_order_clause(direction.clone(), obj_filters, &plan.filters);
        let join_col = match direction {
            TraversalDirection::Forward => "to",
            TraversalDirection::Reverse => "from",
        };
        let mut sql = format!(
            r#"
            SELECT
                e."from" AS edge_from, e."to" AS edge_to, e.type AS edge_type,
                e.data AS edge_data,
                e.created_at AS edge_created_at, e.updated_at AS edge_updated_at,
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
            IndexValue::Uuid(u) => serde_json::to_value(u).unwrap_or(serde_json::Value::Null),
            IndexValue::Timestamp(ts) => {
                serde_json::to_value(ts).unwrap_or(serde_json::Value::Null)
            }
            IndexValue::Array(_) => unreachable!("Array handled separately"),
        }
    }

    /// `created_at` / `updated_at` compared with a timestamp use the real
    /// column: it has btree indexes, it's the value `transfer_object` updates
    /// (the `index_meta` copy isn't touched), and edges don't copy it into
    /// `index_meta` at all. Returns the SQL operator when that applies.
    fn native_timestamp_op(filter: &QueryFilter) -> Option<&'static str> {
        use crate::query::Comparison::*;
        if !matches!(filter.field.name, "created_at" | "updated_at")
            || !matches!(filter.value, IndexValue::Timestamp(_))
        {
            return None;
        }
        match filter.mode.as_search()?.comparison {
            Equal => Some("="),
            NotEqual => Some("<>"),
            GreaterThan => Some(">"),
            LessThan => Some("<"),
            GreaterThanOrEqual => Some(">="),
            LessThanOrEqual => Some("<="),
            _ => None,
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

        if let Some(op) = Self::native_timestamp_op(filter) {
            let cond = format!("{}.{} {} ${}", alias, filter.field.name, op, param_idx);
            *param_idx += 1;
            return Some((cond, operator));
        }

        use crate::query::Comparison::*;

        // GIN jsonb_path_ops @> path: hits the index for equality and array containment
        match (&qs.comparison, &filter.value) {
            // ── Equality: scalar types safe for @> ───────────────────────────────────
            (
                Equal,
                IndexValue::String(_)
                | IndexValue::Int(_)
                | IndexValue::Float(_)
                | IndexValue::Bool(_)
                | IndexValue::Uuid(_)
                | IndexValue::Timestamp(_),
            ) => {
                let cond = format!("{}.index_meta @> ${}", alias, param_idx);
                *param_idx += 1;
                return Some((cond, operator));
            }

            // ── NotEqual: scalar — key must exist AND value must differ ─────────────
            // Without the `?` existence check, a row missing the field entirely
            // would satisfy `NOT @>` (vacuously true), making `where_ne(field, true)`
            // and `where_eq(field, false)` return different sets for boolean fields
            // — see test_query_ne_requires_key_existence.
            (
                NotEqual,
                IndexValue::String(_)
                | IndexValue::Int(_)
                | IndexValue::Float(_)
                | IndexValue::Bool(_)
                | IndexValue::Uuid(_)
                | IndexValue::Timestamp(_),
            ) => {
                let cond = format!(
                    "({alias}.index_meta ? '{field}' AND NOT ({alias}.index_meta @> ${idx}))",
                    alias = alias,
                    field = filter.field.name,
                    idx = param_idx
                );
                *param_idx += 1;
                return Some((cond, operator));
            }

            // ── ContainsAll: full array must be present in one @> ────────────────────
            (ContainsAll, IndexValue::Array(arr)) if !arr.is_empty() => {
                let cond = format!("{}.index_meta @> ${}", alias, param_idx);
                *param_idx += 1;
                return Some((cond, operator));
            }

            // ── Contains / NotContains / ContainsAll on a single string ──────────────
            (Contains | NotContains | BeginsWith, IndexValue::String(_)) => {
                // Falls through to the ILIKE extraction path below.
            }

            // ── Contains: each element tested independently, joined with OR ──────────
            (Contains, IndexValue::Array(arr)) if !arr.is_empty() => {
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

            // ── NotContains: none of the array elements may be present ───────────────
            (NotContains, IndexValue::Array(arr)) if !arr.is_empty() => {
                let conds: Vec<String> = (0..arr.len())
                    .map(|i| format!("NOT ({}.index_meta @> ${})", alias, *param_idx + i))
                    .collect();
                *param_idx += arr.len();
                let combined = if conds.len() == 1 {
                    conds.into_iter().next().unwrap()
                } else {
                    format!("({})", conds.join(" AND "))
                };
                return Some((combined, operator));
            }

            // ── Empty array: no useful predicate ─────────────────────────────────────
            (Contains | ContainsAll | NotContains, IndexValue::Array(arr)) if arr.is_empty() => {
                return None;
            }

            // ── Everything else falls through to the extraction path ─────────────────
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
            NotContains => "NOT ILIKE",
        };

        let condition = format!(
            "({}.index_meta->>'{}')::{} {} ${}",
            alias, filter.field.name, index_type, comparison, param_idx
        );
        *param_idx += 1;
        Some((condition, operator))
    }

    pub(super) fn join_conditions(conditions: &[(String, &str)]) -> String {
        if conditions.is_empty() {
            return String::new();
        }

        // Each condition's operator means "how to connect THIS to the PREVIOUS condition".
        // Consecutive conditions joined by OR are wrapped in parentheses so that
        // SQL operator precedence (AND > OR) does not break the intended semantics.
        //
        // Example: [type=$1/AND, owner=$2/AND, meta@>$3/AND, meta@>$4/OR]
        //   → "type=$1 AND owner=$2 AND (meta@>$3 OR meta@>$4)"
        let mut segments: Vec<String> = Vec::new();
        let mut or_group: Vec<String> = vec![conditions[0].0.clone()];

        for (cond, op) in &conditions[1..] {
            if *op == "OR" {
                or_group.push(cond.clone());
            } else {
                // Flush the current OR group into a segment.
                if or_group.len() == 1 {
                    segments.push(or_group.remove(0));
                } else {
                    segments.push(format!("({})", or_group.join(" OR ")));
                    or_group.clear();
                }
                or_group.push(cond.clone());
            }
        }
        // Flush the final group.
        if or_group.len() == 1 {
            segments.push(or_group.remove(0));
        } else {
            segments.push(format!("({})", or_group.join(" OR ")));
        }

        segments.join(" AND ")
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
        Self::build_object_query_conditions_with_geo(filters, cursor, &[], None, &mut 3).0
    }

    /// Build the `JOIN ... object_geo ...` block needed by geo filters + order.
    ///
    /// Aliasing:
    /// - One alias (`g0`, `g1`, ...) per filter in `geo_filters` order.
    /// - If two filters target the same field, each still gets its own alias
    ///   (so two AND'd predicates on the same field work).
    /// - For `geo_order`: reuse the first matching filter's alias if any;
    ///   otherwise emit `g_ord`.
    pub(super) fn plan_geo_joins(
        geo_filters: &[GeoFilter],
        geo_order: Option<&GeoOrder>,
    ) -> GeoJoinPlan {
        if geo_filters.is_empty() && geo_order.is_none() {
            return GeoJoinPlan::default();
        }
        let mut plan = GeoJoinPlan::default();
        let mut joins = String::new();

        for (i, gf) in geo_filters.iter().enumerate() {
            let alias = format!("g{}", i);
            joins.push_str(&format!(
                "JOIN public.object_geo {alias} ON {alias}.object_id = o.id\n",
                alias = alias
            ));
            plan.aliases.push((gf.field().to_string(), alias));
        }

        if let Some(go) = geo_order {
            // Reuse an existing alias only if the field matches one of the filters.
            let reused = plan
                .aliases
                .iter()
                .find(|(f, _)| f == &go.field)
                .map(|(_, a)| a.clone());
            match reused {
                Some(a) => plan.order_alias = Some(a),
                None => {
                    let a = "g_ord".to_string();
                    joins.push_str(&format!(
                        "JOIN public.object_geo {alias} ON {alias}.object_id = o.id\n",
                        alias = a
                    ));
                    plan.order_alias = Some(a);
                }
            }
        }

        plan.joins = joins;
        plan
    }

    /// Build WHERE clause and advance `param_idx` past every geo-related
    /// parameter. Returns `(where_clause, geo_plan)` so the caller can:
    /// 1. splice `geo_plan.joins` into the FROM block, and
    /// 2. forward `geo_plan` into `build_geo_order_suffix` when assembling ORDER BY.
    pub(super) fn build_object_query_conditions_with_geo(
        filters: &[QueryFilter],
        cursor: Option<Cursor>,
        geo_filters: &[GeoFilter],
        geo_order: Option<&GeoOrder>,
        param_idx: &mut usize,
    ) -> (String, GeoJoinPlan) {
        // $1 = type, $2 = owner, $3 = cursor (optional), then filter values,
        // then geo filter params (per filter), then geo order field param (if not reused).
        let mut conditions: Vec<(String, &str)> = vec![
            ("o.type = $1".to_string(), "AND"),
            ("o.owner = $2".to_string(), "AND"),
        ];

        // The cursor takes this slot; its condition depends on the ORDER BY and
        // is added by the caller via `build_object_cursor_condition`.
        if cursor.is_some() {
            *param_idx += 1;
        }

        for filter in filters {
            if let Some((cond, op)) = Self::build_filter_condition("o", filter, param_idx) {
                conditions.push((cond, op));
            }
        }

        let plan = Self::plan_geo_joins(geo_filters, geo_order);

        // Geo filter predicates — bind in declared order.
        for (gf, (_, alias)) in geo_filters.iter().zip(plan.aliases.iter()) {
            match gf {
                GeoFilter::Within { .. } => {
                    let field_p = *param_idx;
                    let lon_p = *param_idx + 1;
                    let lat_p = *param_idx + 2;
                    let rad_p = *param_idx + 3;
                    conditions.push((format!("{}.field = ${}", alias, field_p), "AND"));
                    conditions.push((
                        format!(
                            "ST_DWithin({}.location, ST_SetSRID(ST_MakePoint(${}, ${}), 4326)::geography, ${})",
                            alias, lon_p, lat_p, rad_p
                        ),
                        "AND",
                    ));
                    *param_idx += 4;
                }
                GeoFilter::InBbox { .. } => {
                    let field_p = *param_idx;
                    let min_lon_p = *param_idx + 1;
                    let min_lat_p = *param_idx + 2;
                    let max_lon_p = *param_idx + 3;
                    let max_lat_p = *param_idx + 4;
                    conditions.push((format!("{}.field = ${}", alias, field_p), "AND"));
                    conditions.push((
                        format!(
                            "ST_Within({}.location::geometry, ST_MakeEnvelope(${}, ${}, ${}, ${}, 4326))",
                            alias, min_lon_p, min_lat_p, max_lon_p, max_lat_p
                        ),
                        "AND",
                    ));
                    *param_idx += 5;
                }
            }
        }

        // Geo order: if its alias wasn't reused from filters, bind its field name + add field constraint.
        if let (Some(go), Some(order_alias)) = (geo_order, plan.order_alias.as_ref()) {
            let reused = plan.aliases.iter().any(|(_, a)| a == order_alias);
            if !reused {
                let field_p = *param_idx;
                conditions.push((format!("{}.field = ${}", order_alias, field_p), "AND"));
                *param_idx += 1;
            }
            // Bind lon/lat happens in bind_geo_filters (always — they're used in ORDER BY too).
            let _ = go;
        }

        (
            format!("WHERE {}", Self::join_conditions(&conditions)),
            plan,
        )
    }

    /// Compute the `$N` placeholder indices that the binders will use for the
    /// geo_order point (lon, lat). `start_idx` must be the `param_idx` returned
    /// AFTER `build_object_query_conditions_with_geo` has finished — at that
    /// point every WHERE-side slot (filter values + order field if not reused)
    /// is already accounted for, so the order lon/lat bindings come immediately
    /// at `start_idx` and `start_idx + 1`.
    ///
    /// Returns `(0, 0)` when no geo_order is set — callers must check first.
    pub(super) fn compute_geo_order_param_slots(
        start_idx: usize,
        _geo_filters: &[GeoFilter],
        plan: &GeoJoinPlan,
    ) -> (usize, usize) {
        if plan.order_alias.is_some() {
            (start_idx, start_idx + 1)
        } else {
            (0, 0)
        }
    }

    /// ORDER BY snippet for distance ordering. Returns `None` if no geo_order.
    pub(super) fn build_geo_order_suffix(
        geo_order: Option<&GeoOrder>,
        plan: &GeoJoinPlan,
        lon_param: usize,
        lat_param: usize,
    ) -> Option<String> {
        let go = geo_order?;
        let alias = plan.order_alias.as_deref()?;
        let dir = if go.ascending { "ASC" } else { "DESC" };
        Some(format!(
            "{}.location <-> ST_SetSRID(ST_MakePoint(${}, ${}), 4326)::geography {}",
            alias, lon_param, lat_param, dir
        ))
    }

    /// Bind values in the same order the WHERE planner allocated their `$N`
    /// placeholders: each geo filter first (field, then lon/lat/etc.), then
    /// the geo_order field (only if its alias isn't reused), then geo_order
    /// lon/lat. Use `compute_geo_order_param_slots` to find the slot numbers
    /// for the order lon/lat — that's what the ORDER BY suffix needs.
    pub(super) fn bind_geo_filters<'a>(
        mut query: PgQuery<'a, Postgres, PgArguments>,
        geo_filters: &'a [GeoFilter],
        geo_order: Option<&'a GeoOrder>,
        plan: &GeoJoinPlan,
    ) -> PgQuery<'a, Postgres, PgArguments> {
        for gf in geo_filters {
            match gf {
                GeoFilter::Within {
                    field,
                    lon,
                    lat,
                    radius_m,
                } => {
                    query = query
                        .bind(field.as_str())
                        .bind(*lon)
                        .bind(*lat)
                        .bind(*radius_m);
                }
                GeoFilter::InBbox {
                    field,
                    min_lon,
                    min_lat,
                    max_lon,
                    max_lat,
                } => {
                    query = query
                        .bind(field.as_str())
                        .bind(*min_lon)
                        .bind(*min_lat)
                        .bind(*max_lon)
                        .bind(*max_lat);
                }
            }
        }
        if let Some(go) = geo_order {
            let reused = plan
                .order_alias
                .as_deref()
                .map(|a| plan.aliases.iter().any(|(_, x)| x == a))
                .unwrap_or(false);
            if !reused {
                query = query.bind(go.field.as_str());
            }
            query = query.bind(go.lon).bind(go.lat);
        }
        query
    }

    pub(super) fn bind_geo_filters_scalar<'a, O>(
        mut query: QueryScalar<'a, Postgres, O, PgArguments>,
        geo_filters: &'a [GeoFilter],
        geo_order: Option<&'a GeoOrder>,
        plan: &GeoJoinPlan,
    ) -> QueryScalar<'a, Postgres, O, PgArguments> {
        for gf in geo_filters {
            match gf {
                GeoFilter::Within {
                    field,
                    lon,
                    lat,
                    radius_m,
                } => {
                    query = query
                        .bind(field.as_str())
                        .bind(*lon)
                        .bind(*lat)
                        .bind(*radius_m);
                }
                GeoFilter::InBbox {
                    field,
                    min_lon,
                    min_lat,
                    max_lon,
                    max_lat,
                } => {
                    query = query
                        .bind(field.as_str())
                        .bind(*min_lon)
                        .bind(*min_lat)
                        .bind(*max_lon)
                        .bind(*max_lat);
                }
            }
        }
        if let Some(go) = geo_order {
            let reused = plan
                .order_alias
                .as_deref()
                .map(|a| plan.aliases.iter().any(|(_, x)| x == a))
                .unwrap_or(false);
            if !reused {
                query = query.bind(go.field.as_str());
            }
            query = query.bind(go.lon).bind(go.lat);
        }
        query
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
        let (cursor_col, cursor_col_bare, anchor_col_bare) = match direction {
            TraversalDirection::Forward => (r#"e."to""#, r#""to""#, r#""from""#),
            TraversalDirection::Reverse => (r#"e."from""#, r#""from""#, r#""to""#),
        };

        let mut conditions: Vec<(String, &str)> = vec![
            ("e.type = $1".to_string(), "AND"),
            (format!("{} = $2", anchor_col), "AND"),
        ];
        let mut param_idx = 3;

        if cursor.is_some() {
            // Keyset over the same keys as `query_edges_internal`'s ORDER BY: the
            // sort column, then the from/to id. `Cursor` only carries that id, so
            // the cursor edge's sort value is looked up (unique on from, to, type).
            let (outer, bare, ascending) = Self::resolve_edge_sort(filters);
            let nullable = !matches!(bare.as_str(), "created_at" | "updated_at");
            let keys = [
                KeysetKey {
                    outer,
                    at_cursor: format!(
                        "(SELECT {bare} FROM edges WHERE type = $1 AND {anchor_col_bare} = $2 AND {cursor_col_bare} = ${param_idx})"
                    ),
                    ascending,
                    nullable,
                },
                KeysetKey {
                    outer: cursor_col.to_string(),
                    at_cursor: format!("${param_idx}"),
                    ascending,
                    nullable: false,
                },
            ];
            conditions.push((keyset_condition(&keys), "AND"));
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

    /// Keys an edge→object traversal is ordered by: target-object sorts, then
    /// the edge sort (default `created_at` newest first), then the target id.
    /// `cursor_p` is the cursor's `$N` (a target id) used by the lookups in
    /// `at_cursor`; params `$1` = object type, `$2` = edge type, `$3` = pivot.
    fn traversal_keys(
        direction: TraversalDirection,
        obj_filters: &[QueryFilter],
        edge_filters: &[QueryFilter],
        cursor_p: usize,
    ) -> Vec<KeysetKey> {
        let (join_col, join_bare, anchor_bare) = match direction {
            TraversalDirection::Forward => (r#"e."to""#, r#""to""#, r#""from""#),
            TraversalDirection::Reverse => (r#"e."from""#, r#""from""#, r#""to""#),
        };
        let native = |bare: &str| matches!(bare, "created_at" | "updated_at");
        let mut keys: Vec<KeysetKey> = Self::sort_keys(obj_filters, "o.")
            .into_iter()
            .zip(Self::sort_keys(obj_filters, ""))
            .map(|((outer, ascending), (bare, _))| KeysetKey {
                outer,
                at_cursor: format!(
                    "(SELECT {bare} FROM objects WHERE type = $1 AND id = ${cursor_p})"
                ),
                ascending,
                nullable: !native(&bare),
            })
            .collect();
        let (outer, bare, ascending) = Self::resolve_edge_sort(edge_filters);
        keys.push(KeysetKey {
            outer,
            at_cursor: format!(
                "(SELECT {bare} FROM edges WHERE type = $2 AND {anchor_bare} = $3 AND {join_bare} = ${cursor_p})"
            ),
            ascending,
            nullable: !native(&bare),
        });
        keys.push(KeysetKey {
            outer: join_col.to_string(),
            at_cursor: format!("${cursor_p}"),
            ascending,
            nullable: false,
        });
        keys
    }

    /// ORDER BY for edge→object traversals; see `traversal_keys`.
    pub(super) fn build_traversal_order_clause(
        direction: TraversalDirection,
        obj_filters: &[QueryFilter],
        edge_filters: &[QueryFilter],
    ) -> String {
        let terms: Vec<String> = Self::traversal_keys(direction, obj_filters, edge_filters, 0)
            .into_iter()
            .map(|k| format!("{} {}", k.outer, if k.ascending { "ASC" } else { "DESC" }))
            .collect();
        format!("ORDER BY {}", terms.join(", "))
    }

    pub(super) fn build_edge_order_clause(filters: &[QueryFilter]) -> String {
        let (outer, _, ascending) = Self::resolve_edge_sort(filters);
        format!("ORDER BY {outer} {}", if ascending { "ASC" } else { "DESC" })
    }

    /// Edge sort: the first sort filter, or `created_at` newest first. Returns
    /// (expression on alias `e`, bare expression, ascending).
    fn resolve_edge_sort(filters: &[QueryFilter]) -> (String, String, bool) {
        match filters.iter().find(|f| f.mode.as_sort().is_some()) {
            Some(f) => (
                Self::sort_expr("e.", f.field.name),
                Self::sort_expr("", f.field.name),
                f.mode.as_sort().unwrap().ascending,
            ),
            None => ("e.created_at".to_string(), "created_at".to_string(), false),
        }
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

        let keys = Self::sort_keys(filters, &prefix);
        if keys.is_empty() {
            if is_edge {
                return "".to_string();
            }
            return format!("ORDER BY {}id DESC", prefix);
        }

        let mut order_terms: Vec<String> = keys
            .iter()
            .map(|(expr, asc)| format!("{} {}", expr, if *asc { "ASC" } else { "DESC" }))
            .collect();
        // Objects: id breaks ties so the order is total, which cursor paging needs.
        if !is_edge {
            order_terms.push(format!("{}id DESC", prefix));
        }
        format!("ORDER BY {}", order_terms.join(", "))
    }

    /// `(expression, ascending)` for each sort filter, columns qualified by `prefix`.
    fn sort_keys(filters: &[QueryFilter], prefix: &str) -> Vec<(String, bool)> {
        filters
            .iter()
            .filter(|f| f.value.as_array().is_none())
            .filter_map(|f| Some((Self::sort_expr(prefix, f.field.name), f.mode.as_sort()?.ascending)))
            .collect()
    }

    /// What a sort on `name` orders by. The sort filter's value is only a
    /// placeholder (a string or bool), so it can't pick a SQL cast; ordering by
    /// the stored jsonb value sorts numbers numerically and strings as text.
    /// A JSON null becomes SQL NULL so it sorts like a missing field.
    fn sort_expr(prefix: &str, name: &str) -> String {
        // Native columns: direct reference so composite indexes are hit
        if matches!(name, "created_at" | "updated_at") {
            return format!("{prefix}{name}");
        }
        format!("NULLIF({prefix}index_meta->'{name}', 'null'::jsonb)")
    }

    /// Keyset condition selecting the rows that come strictly after the
    /// cursor object in the query's ORDER BY: optional distance, then the
    /// scalar sort keys, then `id DESC`. `cursor_p` is the cursor's `$N`.
    pub(super) fn build_object_cursor_condition(
        filters: &[QueryFilter],
        geo: Option<(&str, &GeoOrder, usize, usize)>,
        cursor_p: usize,
    ) -> String {
        let mut keys: Vec<KeysetKey> = Vec::new();
        if let Some((alias, go, lon_p, lat_p)) = geo {
            let point = format!("ST_SetSRID(ST_MakePoint(${lon_p}, ${lat_p}), 4326)::geography");
            keys.push(KeysetKey {
                outer: format!("{alias}.location <-> {point}"),
                at_cursor: format!(
                    "(SELECT gc.location <-> {point} FROM object_geo gc \
                     WHERE gc.type = $1 AND gc.object_id = ${cursor_p} AND gc.field = '{}')",
                    go.field.replace('\'', "''")
                ),
                ascending: go.ascending,
                nullable: true,
            });
        }
        let outer = Self::sort_keys(filters, "o.");
        for ((outer, ascending), (bare, _)) in outer.into_iter().zip(Self::sort_keys(filters, "")) {
            let nullable = !matches!(bare.as_str(), "created_at" | "updated_at");
            keys.push(KeysetKey {
                outer,
                at_cursor: format!(
                    "(SELECT {bare} FROM objects WHERE type = $1 AND id = ${cursor_p})"
                ),
                ascending,
                nullable,
            });
        }
        if keys.is_empty() {
            return format!("o.id < ${cursor_p}");
        }
        keys.push(KeysetKey {
            outer: "o.id".to_string(),
            at_cursor: format!("${cursor_p}"),
            ascending: false,
            nullable: false,
        });
        keyset_condition(&keys)
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
            // The cursor is the last target id; page over the same keys the
            // ORDER BY uses (`build_traversal_order_clause`).
            let keys = Self::traversal_keys(direction.clone(), obj_filters, edge_filters, param_idx);
            obj_conditions.push((keyset_condition(&keys), "AND"));
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
            if let (Some(_), IndexValue::Timestamp(t)) = (Self::native_timestamp_op(filter), &filter.value) {
                query = query.bind(*t);
                continue;
            }
            let search = filter.mode.as_search().unwrap();
            match (&search.comparison, &filter.value) {
                // GIN @> binds: {"field": value}
                (
                    Equal | NotEqual,
                    IndexValue::String(_)
                    | IndexValue::Int(_)
                    | IndexValue::Float(_)
                    | IndexValue::Bool(_)
                    | IndexValue::Uuid(_)
                    | IndexValue::Timestamp(_),
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
                (Contains | NotContains, IndexValue::Array(arr)) if !arr.is_empty() => {
                    for elem in arr.iter() {
                        let val = Self::inner_to_json(elem);
                        query = query.bind(Self::make_eq_json(
                            filter.field.name,
                            serde_json::Value::Array(vec![val]),
                        ));
                    }
                }
                // Extraction-based binds: range ops, ILIKE
                (_, IndexValue::String(s)) => {
                    query = match search.comparison {
                        BeginsWith => query.bind(format!("{}%", s)),
                        Contains | NotContains => query.bind(format!("%{}%", s)),
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
            if let (Some(_), IndexValue::Timestamp(t)) = (Self::native_timestamp_op(filter), &filter.value) {
                query = query.bind(*t);
                continue;
            }
            let search = filter.mode.as_search().unwrap();
            match (&search.comparison, &filter.value) {
                // GIN @> binds: {"field": value}
                (
                    Equal | NotEqual,
                    IndexValue::String(_)
                    | IndexValue::Int(_)
                    | IndexValue::Float(_)
                    | IndexValue::Bool(_)
                    | IndexValue::Uuid(_)
                    | IndexValue::Timestamp(_),
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
                (Contains | NotContains, IndexValue::Array(arr)) if !arr.is_empty() => {
                    for elem in arr.iter() {
                        let val = Self::inner_to_json(elem);
                        query = query.bind(Self::make_eq_json(
                            filter.field.name,
                            serde_json::Value::Array(vec![val]),
                        ));
                    }
                }
                // Extraction-based binds: range ops, ILIKE
                (_, IndexValue::String(s)) => {
                    query = match search.comparison {
                        BeginsWith => query.bind(format!("{}%", s)),
                        Contains | NotContains => query.bind(format!("%{}%", s)),
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
        let order_clause =
            Self::build_traversal_order_clause(direction.clone(), filters, &plan.filters);

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
        let where_clause =
            Self::build_edge_query_conditions(&plan.filters, plan.cursor, direction.clone());
        // Same keys as the cursor condition: sort column, then the from/to id.
        let (outer, _, ascending) = Self::resolve_edge_sort(&plan.filters);
        let cursor_col = match direction {
            TraversalDirection::Forward => r#"e."to""#,
            TraversalDirection::Reverse => r#"e."from""#,
        };
        let dir = if ascending { "ASC" } else { "DESC" };
        let order_clause = format!("ORDER BY {outer} {dir}, {cursor_col} {dir}");

        let mut sql = format!(
            r#"
            SELECT e."from", e."to", e.type, e.data, e.index_meta, e.created_at, e.updated_at
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

struct KeysetKey {
    outer: String,
    at_cursor: String,
    ascending: bool,
    nullable: bool,
}

/// Rows strictly after the cursor row for `ORDER BY k1, k2, ...` with
/// Postgres' default NULL placement (ASC NULLS LAST, DESC NULLS FIRST).
fn keyset_condition(keys: &[KeysetKey]) -> String {
    let same_dir = keys.iter().all(|k| k.ascending == keys[0].ascending);
    if same_dir && keys.iter().all(|k| !k.nullable) {
        // Row comparison is equivalent here and lets Postgres use a btree index.
        let outer: Vec<&str> = keys.iter().map(|k| k.outer.as_str()).collect();
        let cursor: Vec<&str> = keys.iter().map(|k| k.at_cursor.as_str()).collect();
        let cmp = if keys[0].ascending { ">" } else { "<" };
        return format!("(({}) {} ({}))", outer.join(", "), cmp, cursor.join(", "));
    }

    let after = |k: &KeysetKey| {
        let (o, c) = (&k.outer, &k.at_cursor);
        match (k.ascending, k.nullable) {
            (true, false) => format!("{o} > {c}"),
            (false, false) => format!("{o} < {c}"),
            // values ascending, then NULLs
            (true, true) => format!("({c} IS NOT NULL AND ({o} > {c} OR {o} IS NULL))"),
            // NULLs, then values descending
            (false, true) => format!("(({c} IS NULL AND {o} IS NOT NULL) OR {o} < {c})"),
        }
    };
    let equal = |k: &KeysetKey| {
        if k.nullable {
            format!("{} IS NOT DISTINCT FROM {}", k.outer, k.at_cursor)
        } else {
            format!("{} = {}", k.outer, k.at_cursor)
        }
    };
    let branches: Vec<String> = (0..keys.len())
        .map(|i| {
            let mut parts: Vec<String> = keys[..i].iter().map(equal).collect();
            parts.push(after(&keys[i]));
            format!("({})", parts.join(" AND "))
        })
        .collect();
    format!("({})", branches.join(" OR "))
}

#[cfg(test)]
mod timestamp_filter_tests {
    use super::PostgresAdapter;
    use crate::query::{IndexField, IndexKind};
    use crate::Query;

    static CREATED_AT: IndexField = IndexField {
        name: "created_at",
        kinds: &[IndexKind::Search, IndexKind::Sort],
    };

    #[test]
    fn range_filters_on_meta_timestamps_use_the_indexed_column() {
        let q = Query::default()
            .where_gt(&CREATED_AT, chrono::Utc::now())
            .where_lte(&CREATED_AT, chrono::Utc::now());
        let mut idx = 3;
        let conds: Vec<String> = q
            .filters
            .iter()
            .filter_map(|f| PostgresAdapter::build_filter_condition("o", f, &mut idx))
            .map(|(c, _)| c)
            .collect();
        assert_eq!(conds, ["o.created_at > $3", "o.created_at <= $4"]);
    }
}
