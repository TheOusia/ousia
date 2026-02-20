use chrono::Utc;
use sqlx::{
    PgPool, Postgres, Row,
    postgres::{PgArguments, PgRow},
    query::{Query as PgQuery, QueryScalar},
};
use uuid::Uuid;

use crate::{
    adapters::{
        Adapter, EdgeQuery, EdgeRecord, EdgeTraversal, Error, ObjectRecord, Query,
        TraversalDirection, UniqueAdapter,
    },
    query::{Cursor, IndexValue, IndexValueInner, QueryFilter},
};

/// CockroachDB adapter using a unified JSON storage model
///
/// Schema:
/// ```sql
/// CREATE TABLE public.objects (
///     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
///     type TEXT NOT NULL,
///     owner UUID NOT NULL,
///     created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
///     updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
///     data JSONB NOT NULL,
///     index_meta JSONB NOT NULL,
///     INDEX idx_objects_type_owner (type, owner),
///     INDEX idx_objects_owner (owner),
///     INDEX idx_objects_created_at (created_at),
///     INDEX idx_objects_updated_at (updated_at),
///     INVERTED INDEX idx_objects_index_meta (index_meta)
/// );
///
/// CREATE TABLE public.edges (
///     "from" UUID NOT NULL,
///     "to" UUID NOT NULL,
///     type TEXT NOT NULL,
///     data JSONB NOT NULL,
///     index_meta JSONB NOT NULL,
///     PRIMARY KEY ("from", "to", type),
///     INDEX idx_edges_from_type ("from", type),
///     INDEX idx_edges_to_type ("to", type),
///     INVERTED INDEX idx_edges_index_meta (index_meta)
/// );
/// ```

pub struct CockroachAdapter {
    pub(crate) pool: PgPool,
}

impl CockroachAdapter {
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Initialize the database schema
    pub async fn init_schema(&self) -> Result<(), Error> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS public.objects (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                type TEXT NOT NULL,
                owner UUID NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                data JSONB NOT NULL,
                index_meta JSONB NOT NULL
            );
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_type_owner
                ON objects(type, owner, id DESC)
                STORING (created_at, updated_at);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_type_owner_created
                ON objects(type, owner, created_at DESC)
                STORING (updated_at);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_type_owner_updated
                ON objects(type, owner, updated_at DESC)
                STORING (created_at);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        // CockroachDB uses INVERTED INDEX instead of GIN for JSONB
        sqlx::query(
            r#"
            CREATE INVERTED INDEX IF NOT EXISTS idx_objects_index_meta ON public.objects (index_meta);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS public.edges (
                "from" UUID NOT NULL,
                "to" UUID NOT NULL,
                type TEXT NOT NULL,
                data JSONB NOT NULL,
                index_meta JSONB NOT NULL,
                PRIMARY KEY ("from", "to", type)
            );
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_edges_from_type ON public.edges("from", type);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_edges_to_type ON public.edges("to", type);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INVERTED INDEX IF NOT EXISTS idx_edges_index_meta ON public.edges (index_meta);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
                    CREATE TABLE IF NOT EXISTS unique_constraints (
                        id UUID NOT NULL,
                        type TEXT NOT NULL,
                        key TEXT NOT NULL UNIQUE,
                        field TEXT NOT NULL,
                        PRIMARY KEY (type, key)
                    )
                    "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
                    CREATE INDEX IF NOT EXISTS idx_unique_id
                    ON unique_constraints(id)
                    "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
                    CREATE INDEX IF NOT EXISTS idx_unique_type_key
                    ON unique_constraints(type, key)
                    "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS sequences (
                name TEXT PRIMARY KEY,
                value BIGINT NOT NULL DEFAULT 1
            )
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        // #[cfg(feature = "ledger")]
        // {
        //     self.init_ledger_schema().await.map_err(|me| match me {
        //         MoneyError::Storage(e) => Error::Storage(e),
        //         _ => Error::Storage(me.to_string()),
        //     })?;
        // }
        Ok(())
    }
}

impl CockroachAdapter {
    fn map_row_to_object_record_slim(row: PgRow) -> Result<ObjectRecord, Error> {
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

    fn map_row_to_edge_record(row: PgRow) -> Result<EdgeRecord, Error> {
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
}

impl CockroachAdapter {
    // // ── Shared SQL builder helpers ───────────────────────────────────────────

    fn index_type_str(value: &IndexValue) -> &'static str {
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

    fn make_eq_json(field: &str, val: serde_json::Value) -> serde_json::Value {
        let mut map = serde_json::Map::with_capacity(1);
        map.insert(field.to_string(), val);
        serde_json::Value::Object(map)
    }

    fn inner_to_json(elem: &IndexValueInner) -> serde_json::Value {
        match elem {
            IndexValueInner::String(s) => serde_json::Value::String(s.clone()),
            IndexValueInner::Int(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
            IndexValueInner::Float(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
        }
    }

    fn index_value_to_json(value: &IndexValue) -> serde_json::Value {
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

    fn build_filter_condition(
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

        // INVERTED INDEX @> path
        match (&qs.comparison, &filter.value) {
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
            (ContainsAll, IndexValue::Array(arr)) if !arr.is_empty() => {
                let cond = format!("{}.index_meta @> ${}", alias, param_idx);
                *param_idx += 1;
                return Some((cond, operator));
            }
            (Contains | ContainsAll, IndexValue::Array(arr)) if arr.is_empty() => {
                return None;
            }
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

    fn join_conditions(conditions: &[(String, &str)]) -> String {
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

    fn build_object_query_conditions(filters: &[QueryFilter], cursor: Option<Cursor>) -> String {
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
    fn build_edge_query_conditions(
        filters: &[QueryFilter],
        cursor: Option<Cursor>,
        direction: TraversalDirection,
    ) -> String {
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

    fn build_order_clause(filters: &[QueryFilter]) -> String {
        Self::build_order_clause_aliased(filters, "", false)
    }

    fn build_edge_order_clause(filters: &[QueryFilter]) -> String {
        Self::build_order_clause_aliased(filters, "e", true)
    }

    fn build_order_clause_aliased(filters: &[QueryFilter], alias: &str, is_edge: bool) -> String {
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
                let dir = if s.mode.as_sort().unwrap().ascending {
                    "ASC"
                } else {
                    "DESC"
                };
                // Native columns: use direct column reference so composite indexes are hit
                if matches!(s.field.name, "created_at" | "updated_at") {
                    return format!("{}{} {}", prefix, s.field.name, dir);
                }
                let t = match &s.value {
                    IndexValue::String(_) => "text",
                    IndexValue::Int(_) => "bigint",
                    IndexValue::Float(_) => "double precision",
                    IndexValue::Bool(_) => "boolean",
                    IndexValue::Timestamp(_) => "timestamptz",
                    _ => "text",
                };
                format!("({}index_meta->>'{}')::{} {}", prefix, s.field.name, t, dir)
            })
            .collect();
        format!("ORDER BY {}", order_terms.join(", "))
    }

    fn build_object_traversal_query_conditions(
        direction: TraversalDirection,
        obj_filters: &[QueryFilter],
        edge_filters: &[QueryFilter],
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

    fn query_bind_filters<'a>(
        mut query: PgQuery<'a, Postgres, PgArguments>,
        filters: &'a [QueryFilter],
    ) -> PgQuery<'a, Postgres, PgArguments> {
        use crate::query::Comparison::*;
        for filter in filters.iter().filter(|f| f.mode.as_search().is_some()) {
            let search = filter.mode.as_search().unwrap();
            match (&search.comparison, &filter.value) {
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

    fn query_scalar_bind_filters<'a, O>(
        mut query: QueryScalar<'a, Postgres, O, PgArguments>,
        filters: &'a [QueryFilter],
    ) -> QueryScalar<'a, Postgres, O, PgArguments> {
        use crate::query::Comparison::*;
        for filter in filters.iter().filter(|f| f.mode.as_search().is_some()) {
            let search = filter.mode.as_search().unwrap();
            match (&search.comparison, &filter.value) {
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

impl CockroachAdapter {
    async fn edge_traversal_inner(
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

        let mut query = sqlx::query(&sql)
            .bind(type_name)
            .bind(edge_type_name)
            .bind(owner);

        if let Some(cursor) = plan.cursor {
            query = query.bind(cursor.last_id);
        }

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

    async fn query_edges_internal(
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
            SELECT e."from" AS "from", e."to" AS "to", e.type AS "type", e.data, e.index_meta
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
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(rows
            .into_iter()
            .filter_map(|row| Self::map_row_to_edge_record(row).ok())
            .collect())
    }
}

#[async_trait::async_trait]
impl Adapter for CockroachAdapter {
    async fn insert_object(&self, record: ObjectRecord) -> Result<(), Error> {
        let ObjectRecord {
            id,
            type_name,
            owner,
            created_at,
            updated_at,
            data,
            index_meta,
        } = record;
        let _ = sqlx::query(
            r#"
            INSERT INTO public.objects (id, type, owner, created_at, updated_at, data, index_meta)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(id)
        .bind(type_name.as_ref())
        .bind(owner)
        .bind(created_at)
        .bind(updated_at)
        .bind(data)
        .bind(index_meta)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| {
            if err.to_string().contains("unique") {
                Error::UniqueConstraintViolation("id".to_string())
            } else {
                Error::Storage(err.to_string())
            }
        })?;
        Ok(())
    }

    async fn fetch_object(
        &self,
        type_name: &'static str,
        id: Uuid,
    ) -> Result<Option<ObjectRecord>, Error> {
        let row = sqlx::query(
            r#"
            SELECT o.id, o.type, o.owner, o.created_at, o.updated_at, o.data
            FROM objects o
            WHERE id = $1 AND type = $2
            "#,
        )
        .bind(id)
        .bind(type_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record_slim(r).map(|o| Some(o)),
            None => Ok(None),
        }
    }

    async fn fetch_bulk_objects(
        &self,
        type_name: &'static str,
        ids: Vec<Uuid>,
    ) -> Result<Vec<ObjectRecord>, Error> {
        let rows = sqlx::query(
            r#"
            SELECT o.id, o.type, o.owner, o.created_at, o.updated_at, o.data
            FROM objects o
            WHERE id = ANY($1) AND type = $2
            "#,
        )
        .bind(ids.into_iter().map(|id| id).collect::<Vec<Uuid>>())
        .bind(type_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        rows.into_iter()
            .map(Self::map_row_to_object_record_slim)
            .collect()
    }

    async fn update_object(&self, record: ObjectRecord) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE objects
            SET updated_at = $2, data = $3, index_meta = $4
            WHERE id = $1
            "#,
        )
        .bind(record.id)
        .bind(record.updated_at)
        .bind(record.data)
        .bind(record.index_meta)
        .execute(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        Ok(())
    }

    async fn transfer_object(
        &self,
        type_name: &'static str,
        id: Uuid,
        from_owner: Uuid,
        to_owner: Uuid,
    ) -> Result<ObjectRecord, Error> {
        let row = sqlx::query(
            r#"
            UPDATE objects
            SET updated_at = $3, owner = $4
            WHERE id = $1 AND owner = $2 AND type = $5
            RETURNING id, type, owner, created_at, updated_at, data
            "#,
        )
        .bind(id)
        .bind(from_owner)
        .bind(Utc::now())
        .bind(to_owner)
        .bind(type_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|err| match err {
            sqlx::Error::RowNotFound => Error::NotFound,
            _ => Error::Storage(err.to_string()),
        })?;

        Self::map_row_to_object_record_slim(row)
    }

    async fn delete_object(
        &self,
        type_name: &'static str,
        id: Uuid,
        owner: Uuid,
    ) -> Result<Option<ObjectRecord>, Error> {
        let row = sqlx::query(
            r#"
            DELETE FROM objects
            WHERE id = $1 AND type = $2 AND owner = $3
            RETURNING id, type, owner, created_at, updated_at, data
            "#,
        )
        .bind(id)
        .bind(type_name)
        .bind(owner)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| match err {
            sqlx::Error::RowNotFound => Error::NotFound,
            _ => Error::Storage(err.to_string()),
        })?;

        match row {
            Some(r) => Self::map_row_to_object_record_slim(r).map(|o| Some(o)),
            None => Ok(None),
        }
    }

    async fn delete_bulk_objects(
        &self,
        type_name: &'static str,
        ids: Vec<Uuid>,
        owner: Uuid,
    ) -> Result<u64, Error> {
        let result =
            sqlx::query("DELETE FROM objects WHERE id = ANY($1) AND type = $2 AND owner = $3")
                .bind(ids)
                .bind(type_name)
                .bind(owner)
                .execute(&self.pool)
                .await
                .map_err(|err| Error::Storage(err.to_string()))?;
        Ok(result.rows_affected())
    }

    async fn delete_owned_objects(
        &self,
        type_name: &'static str,
        owner: Uuid,
    ) -> Result<u64, Error> {
        let result = sqlx::query("DELETE FROM objects WHERE type = $1 AND owner = $2")
            .bind(type_name)
            .bind(owner)
            .execute(&self.pool)
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;

        Ok(result.rows_affected())
    }

    async fn find_object(
        &self,
        type_name: &'static str,
        owner: Uuid,
        filters: &[QueryFilter],
    ) -> Result<Option<ObjectRecord>, Error> {
        let where_clause = Self::build_object_query_conditions(filters, None);
        let order_clause = Self::build_order_clause(filters);

        let sql = format!(
            r#"
            SELECT o.id, o.type, o.owner, o.created_at, o.updated_at, o.data
            FROM objects o
            {}
            {}
            "#,
            where_clause, order_clause
        );

        let mut query = sqlx::query(&sql).bind(type_name).bind(owner);
        query = Self::query_bind_filters(query, filters);

        let row = query
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;

        Ok(row
            .map(|row| Self::map_row_to_object_record_slim(row).ok())
            .unwrap_or_default())
    }

    async fn query_objects(
        &self,
        type_name: &'static str,
        plan: Query,
    ) -> Result<Vec<ObjectRecord>, Error> {
        let mut where_clause = Self::build_object_query_conditions(&plan.filters, plan.cursor);
        let order_clause = Self::build_order_clause(&plan.filters);

        if plan.owner.is_nil() {
            where_clause = where_clause.replace("o.owner = ", "o.owner > ");
        }

        let mut sql = format!(
            r#"
                SELECT o.id, o.type, o.owner, o.created_at, o.updated_at, o.data
                FROM objects o
                {}
                {}
                "#,
            where_clause, order_clause
        );

        if let Some(limit) = plan.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        let mut query = sqlx::query(&sql).bind(type_name).bind(plan.owner);

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
            .filter_map(|row| Self::map_row_to_object_record_slim(row).ok())
            .collect())
    }

    async fn count_objects(
        &self,
        type_name: &'static str,
        plan: Option<Query>,
    ) -> Result<u64, Error> {
        match plan {
            Some(plan) => {
                let where_clause = Self::build_object_query_conditions(&plan.filters, None);

                let mut sql = format!(
                    r#"
                    SELECT COUNT(*) FROM objects o
                    {}
                    "#,
                    where_clause
                );

                if let Some(limit) = plan.limit {
                    sql.push_str(&format!(" LIMIT {}", limit));
                }

                let mut query = sqlx::query_scalar::<_, i64>(&sql)
                    .bind(type_name)
                    .bind(plan.owner);

                query = Self::query_scalar_bind_filters(query, &plan.filters);

                let count = query
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;

                Ok(count as u64)
            }
            None => {
                let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM objects WHERE type = $1")
                    .bind(type_name)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|err| Error::Storage(err.to_string()))?;

                Ok(count as u64)
            }
        }
    }

    async fn fetch_owned_objects(
        &self,
        type_name: &'static str,
        owner: Uuid,
    ) -> Result<Vec<ObjectRecord>, Error> {
        let rows = sqlx::query(
            r#"
            SELECT o.id, o.type, o.owner, o.created_at, o.updated_at, o.data
            FROM objects o
            WHERE owner = $1 AND type = $2
            "#,
        )
        .bind(owner)
        .bind(type_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        rows.into_iter()
            .map(Self::map_row_to_object_record_slim)
            .collect()
    }

    async fn fetch_owned_object(
        &self,
        type_name: &'static str,
        owner: Uuid,
    ) -> Result<Option<ObjectRecord>, Error> {
        let row = sqlx::query(
            r#"
            SELECT o.id, o.type, o.owner, o.created_at, o.updated_at, o.data
            FROM objects o
            WHERE owner = $1 AND type = $2
            "#,
        )
        .bind(owner)
        .bind(type_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record_slim(r).map(|o| Some(o)),
            None => Ok(None),
        }
    }

    async fn fetch_union_object(
        &self,
        a_type_name: &'static str,
        b_type_name: &'static str,
        id: Uuid,
    ) -> Result<Option<ObjectRecord>, Error> {
        let row = sqlx::query(
            r#"
            SELECT o.id, o.type, o.owner, o.created_at, o.updated_at, o.data
            FROM objects o
            WHERE id = $1 AND (type = $2 OR type = $3)
            "#,
        )
        .bind(id)
        .bind(a_type_name)
        .bind(b_type_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record_slim(r).map(|o| Some(o)),
            None => Ok(None),
        }
    }

    async fn fetch_union_objects(
        &self,
        a_type_name: &'static str,
        b_type_name: &'static str,
        ids: Vec<Uuid>,
    ) -> Result<Vec<ObjectRecord>, Error> {
        let rows = sqlx::query(
            r#"
            SELECT o.id, o.type, o.owner, o.created_at, o.updated_at, o.data
            FROM objects o
            WHERE id = ANY($1) AND (type = $2 OR type = $3)
            "#,
        )
        .bind(ids.into_iter().map(|id| id).collect::<Vec<Uuid>>())
        .bind(a_type_name)
        .bind(b_type_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        rows.into_iter()
            .map(Self::map_row_to_object_record_slim)
            .collect()
    }

    async fn fetch_owned_union_object(
        &self,
        a_type_name: &'static str,
        b_type_name: &'static str,
        owner: Uuid,
    ) -> Result<Option<ObjectRecord>, Error> {
        let row = sqlx::query(
            r#"
            SELECT o.id, o.type, o.owner, o.created_at, o.updated_at, o.data
            FROM objects o
            WHERE owner = $1 AND (type = $2 OR type = $3)
            "#,
        )
        .bind(owner)
        .bind(a_type_name)
        .bind(b_type_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record_slim(r).map(|o| Some(o)),
            None => Ok(None),
        }
    }

    async fn fetch_owned_union_objects(
        &self,
        a_type_name: &'static str,
        b_type_name: &'static str,
        owner: Uuid,
    ) -> Result<Vec<ObjectRecord>, Error> {
        let rows = sqlx::query(
            r#"
            SELECT o.id, o.type, o.owner, o.created_at, o.updated_at, o.data
            FROM objects o
            WHERE owner = $1 AND (type = $2 OR type = $3)
            "#,
        )
        .bind(owner)
        .bind(a_type_name)
        .bind(b_type_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        rows.into_iter()
            .map(Self::map_row_to_object_record_slim)
            .collect()
    }

    /* ---------------- EDGES ---------------- */
    async fn insert_edge(&self, record: EdgeRecord) -> Result<(), Error> {
        let EdgeRecord {
            from,
            to,
            type_name,
            data,
            index_meta,
        } = record;
        let _ = sqlx::query(
            r#"
            INSERT INTO edges ("from", "to", type, data, index_meta)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT ("from", type, "to")
            DO UPDATE SET data = $4, index_meta = $5;
            "#,
        )
        .bind(from)
        .bind(to)
        .bind(type_name.as_ref())
        .bind(data)
        .bind(index_meta)
        .execute(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        Ok(())
    }

    async fn update_edge(
        &self,
        record: EdgeRecord,
        old_to: Uuid,
        to: Option<Uuid>,
    ) -> Result<(), Error> {
        let EdgeRecord {
            from,
            type_name,
            data,
            ..
        } = record;
        let _ = sqlx::query(
            r#"
        UPDATE edges SET data = $1, "to" = $2
        WHERE "from" = $3 AND type = $4 AND "to" = $5
        "#,
        )
        .bind(data)
        .bind(to.unwrap_or(old_to))
        .bind(from)
        .bind(type_name.as_ref())
        .bind(old_to)
        .execute(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        Ok(())
    }

    async fn delete_edge(
        &self,
        type_name: &'static str,
        from: Uuid,
        to: Uuid,
    ) -> Result<(), Error> {
        let _ = sqlx::query(
            r#"
            DELETE FROM edges
            WHERE type = $1 AND "from" = $2 AND "to" = $3
            "#,
        )
        .bind(type_name)
        .bind(from)
        .bind(to)
        .execute(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        Ok(())
    }

    async fn delete_object_edge(&self, type_name: &'static str, from: Uuid) -> Result<(), Error> {
        let _ = sqlx::query(
            r#"
            DELETE FROM edges
            WHERE type = $1 AND "from" = $2
            "#,
        )
        .bind(type_name)
        .bind(from)
        .execute(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        Ok(())
    }

    async fn query_edges(
        &self,
        type_name: &'static str,
        owner: Uuid,
        plan: EdgeQuery,
    ) -> Result<Vec<EdgeRecord>, Error> {
        self.query_edges_internal(type_name, owner, plan, TraversalDirection::Forward)
            .await
    }

    async fn query_reverse_edges(
        &self,
        type_name: &'static str,
        owner_reverse: Uuid,
        plan: EdgeQuery,
    ) -> Result<Vec<EdgeRecord>, Error> {
        self.query_edges_internal(type_name, owner_reverse, plan, TraversalDirection::Reverse)
            .await
    }

    async fn count_edges(
        &self,
        type_name: &'static str,
        owner: Uuid,
        plan: Option<EdgeQuery>,
    ) -> Result<u64, Error> {
        match plan {
            Some(plan) => {
                let where_clause = Self::build_edge_query_conditions(
                    &plan.filters,
                    None,
                    TraversalDirection::Forward,
                );

                let mut sql = format!(
                    r#"
                SELECT COUNT(*) FROM edges
                {}
                "#,
                    where_clause
                );

                if let Some(limit) = plan.limit {
                    sql.push_str(&format!(" LIMIT {}", limit));
                }

                let mut query = sqlx::query_scalar::<_, i64>(&sql)
                    .bind(type_name)
                    .bind(owner);

                query = Self::query_scalar_bind_filters(query, &plan.filters);

                let count = query
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;

                Ok(count as u64)
            }
            None => {
                let count: i64 = sqlx::query_scalar(
                    r#"SELECT COUNT(*) FROM edges WHERE type = $1 AND "from" = $2"#,
                )
                .bind(type_name)
                .bind(owner)
                .fetch_one(&self.pool)
                .await
                .map_err(|err| Error::Storage(err.to_string()))?;

                Ok(count as u64)
            }
        }
    }

    async fn count_reverse_edges(
        &self,
        type_name: &'static str,
        to: Uuid,
        plan: Option<EdgeQuery>,
    ) -> Result<u64, Error> {
        match plan {
            Some(plan) => {
                let where_clause = Self::build_edge_query_conditions(
                    &plan.filters,
                    None,
                    TraversalDirection::Reverse,
                );

                let mut sql = format!(
                    r#"
                SELECT COUNT(*) FROM edges
                {}
                "#,
                    where_clause
                );

                if let Some(limit) = plan.limit {
                    sql.push_str(&format!(" LIMIT {}", limit));
                }

                let mut query = sqlx::query_scalar::<_, i64>(&sql).bind(type_name).bind(to);

                query = Self::query_scalar_bind_filters(query, &plan.filters);

                let count = query
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;

                Ok(count as u64)
            }
            None => {
                let count: i64 = sqlx::query_scalar(
                    r#"
                    SELECT COUNT(*) FROM edges WHERE type = $1 AND "to" = $2
                    "#,
                )
                .bind(type_name)
                .bind(to)
                .fetch_one(&self.pool)
                .await
                .map_err(|err| Error::Storage(err.to_string()))?;

                Ok(count as u64)
            }
        }
    }

    async fn sequence_value(&self, sq: String) -> u64 {
        let val: i64 =
            sqlx::query_scalar("SELECT COALESCE((SELECT value FROM sequences WHERE name = $1), 1)")
                .bind(&sq)
                .fetch_one(&self.pool)
                .await
                .expect("Failed to fetch the current sequence value");
        val as u64
    }

    async fn sequence_next_value(&self, sq: String) -> u64 {
        let next_val: i64 = sqlx::query_scalar(
            "INSERT INTO sequences (name, value) VALUES ($1, 2)
             ON CONFLICT (name) DO UPDATE SET value = sequences.value + 1
             RETURNING value",
        )
        .bind(&sq)
        .fetch_one(&self.pool)
        .await
        .expect("Failed to fetch the next sequence value");
        next_val as u64
    }
}

#[async_trait::async_trait]
impl UniqueAdapter for CockroachAdapter {
    async fn insert_unique_hashes(
        &self,
        type_name: &str,
        object_id: Uuid,
        hashes: Vec<(String, &str)>,
    ) -> Result<(), Error> {
        if hashes.is_empty() {
            return Ok(());
        }
        let keys: Vec<&str> = hashes.iter().map(|(k, _)| k.as_str()).collect();
        let fields: Vec<&str> = hashes.iter().map(|(_, f)| *f).collect();

        let result = sqlx::query(
            r#"
            INSERT INTO unique_constraints (id, type, key, field)
            SELECT $1, $2, unnest($3::text[]), unnest($4::text[])
            "#,
        )
        .bind(object_id)
        .bind(type_name)
        .bind(&keys)
        .bind(&fields)
        .execute(&self.pool)
        .await;

        match result {
            Ok(_) => Ok(()),
            Err(err) => {
                let msg = err.to_string();
                if msg.contains("unique constraint") || msg.contains("duplicate") {
                    // Identify which field caused the conflict
                    let existing: Option<String> = sqlx::query_scalar(
                        "SELECT field FROM unique_constraints WHERE key = ANY($1) LIMIT 1",
                    )
                    .bind(&keys)
                    .fetch_optional(&self.pool)
                    .await
                    .unwrap_or(None);
                    let field = existing.unwrap_or_else(|| "unknown".to_string());
                    Err(Error::UniqueConstraintViolation(field))
                } else {
                    Err(Error::Storage(msg))
                }
            }
        }
    }

    async fn delete_unique(&self, hash: &str) -> Result<(), Error> {
        sqlx::query(
            r#"
            DELETE FROM unique_constraints WHERE key = $1
            "#,
        )
        .bind(hash)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn delete_unique_hashes(&self, hashes: Vec<String>) -> Result<(), Error> {
        sqlx::query(
            r#"
        DELETE FROM unique_constraints WHERE key = ANY($1)
        "#,
        )
        .bind(hashes)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn get_hashes_for_object(&self, object_id: Uuid) -> Result<Vec<String>, Error> {
        let rows = sqlx::query(
            r#"
            SELECT key FROM unique_constraints WHERE id = $1
            "#,
        )
        .bind(object_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|row| row.try_get("key").unwrap())
            .collect())
    }
}

#[async_trait::async_trait]
impl EdgeTraversal for CockroachAdapter {
    async fn fetch_object_from_edge_traversal_internal(
        &self,
        edge_type_name: &str,
        type_name: &str,
        owner: Uuid,
        filters: &[QueryFilter],
        plan: EdgeQuery,
    ) -> Result<Vec<ObjectRecord>, Error> {
        self.edge_traversal_inner(
            edge_type_name,
            type_name,
            owner,
            filters,
            plan,
            TraversalDirection::Forward,
        )
        .await
    }

    async fn fetch_object_from_edge_reverse_traversal_internal(
        &self,
        edge_type_name: &str,
        type_name: &str,
        owner: Uuid,
        filters: &[QueryFilter],
        plan: EdgeQuery,
    ) -> Result<Vec<ObjectRecord>, Error> {
        self.edge_traversal_inner(
            edge_type_name,
            type_name,
            owner,
            filters,
            plan,
            TraversalDirection::Reverse,
        )
        .await
    }
}
