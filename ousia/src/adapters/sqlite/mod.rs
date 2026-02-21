use chrono::Utc;
use sqlx::{
    Row, Sqlite,
    query::{Query as SqlxQuery, QueryScalar},
    sqlite::{SqliteArguments, SqlitePool, SqlitePoolOptions, SqliteRow},
};
use uuid::Uuid;

use crate::{
    adapters::{
        Adapter, EdgeQuery, EdgeRecord, EdgeTraversal, Error, ObjectRecord, Query,
        TraversalDirection, UniqueAdapter,
    },
    query::{Cursor, IndexValue, IndexValueInner, QueryFilter},
};

/// SQLite adapter using a unified JSON storage model
///
/// Schema:
/// ```sql
/// CREATE TABLE objects (
///     id BLOB PRIMARY KEY,
///     type TEXT NOT NULL,
///     owner BLOB NOT NULL,
///     created_at TEXT NOT NULL,
///     updated_at TEXT NOT NULL,
///     data TEXT NOT NULL,
///     index_meta TEXT NOT NULL
/// );
///
/// CREATE INDEX idx_objects_type_owner ON objects(type, owner, id DESC);
/// CREATE INDEX idx_objects_type_owner_created ON objects(type, owner, created_at DESC);
/// CREATE INDEX idx_objects_type_owner_updated ON objects(type, owner, updated_at DESC);
/// ```
pub struct SqliteAdapter {
    pub(crate) pool: SqlitePool,
}

impl SqliteAdapter {
    /// Create a new SQLite adapter with a file-based database
    pub async fn new_file(path: &str) -> Result<Self, Error> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&format!("sqlite:{}", path))
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(Self { pool })
    }

    /// Create a new SQLite adapter with an in-memory database
    pub async fn new_memory() -> Result<Self, Error> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(Self { pool })
    }

    /// Create from an existing pool
    pub fn from_pool(pool: SqlitePool) -> Self {
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
            CREATE TABLE IF NOT EXISTS objects (
                id BLOB PRIMARY KEY,
                type TEXT NOT NULL,
                owner BLOB NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                data TEXT NOT NULL,
                index_meta TEXT NOT NULL
            )
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_type_owner ON objects(type, owner, id DESC)
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_type_owner_created ON objects(type, owner, created_at DESC)
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_type_owner_updated ON objects(type, owner, updated_at DESC)
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS edges (
                "from" BLOB NOT NULL,
                "to" BLOB NOT NULL,
                type TEXT NOT NULL,
                data TEXT NOT NULL,
                index_meta TEXT NOT NULL,
                PRIMARY KEY ("from", "to", type)
            )
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_edges_from ON edges("from", type)
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_edges_to ON edges("to", type)
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
                    CREATE TABLE IF NOT EXISTS unique_constraints (
                        id BLOB NOT NULL,
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
                value INTEGER NOT NULL DEFAULT 1
            )
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }
}

impl SqliteAdapter {
    fn map_row_to_object_record_slim(row: SqliteRow) -> Result<ObjectRecord, Error> {
        let data_str: String = row
            .try_get("data")
            .map_err(|e| Error::Deserialize(e.to_string()))?;

        let data_json: serde_json::Value =
            serde_json::from_str(&data_str).map_err(|e| Error::Deserialize(e.to_string()))?;

        let type_name = row
            .try_get::<String, _>("type")
            .map_err(|e| Error::Deserialize(e.to_string()))?;

        let id = row
            .try_get::<Uuid, _>("id")
            .map_err(|e| Error::Deserialize(e.to_string()))?;

        let owner = row
            .try_get::<Uuid, _>("owner")
            .map_err(|e| Error::Deserialize(e.to_string()))?;

        let created_at_str: String = row
            .try_get("created_at")
            .map_err(|e| Error::Deserialize(e.to_string()))?;

        let updated_at_str: String = row
            .try_get("updated_at")
            .map_err(|e| Error::Deserialize(e.to_string()))?;

        let created_at = chrono::DateTime::parse_from_rfc3339(&created_at_str)
            .map_err(|e| Error::Deserialize(e.to_string()))?
            .with_timezone(&chrono::Utc);

        let updated_at = chrono::DateTime::parse_from_rfc3339(&updated_at_str)
            .map_err(|e| Error::Deserialize(e.to_string()))?
            .with_timezone(&chrono::Utc);

        Ok(ObjectRecord {
            id,
            type_name: std::borrow::Cow::Owned(type_name),
            owner,
            created_at,
            updated_at,
            data: data_json,
            index_meta: serde_json::Value::Null,
        })
    }

    fn map_row_to_edge_record(row: SqliteRow) -> Result<EdgeRecord, Error> {
        let data_str: String = row
            .try_get("data")
            .map_err(|e| Error::Deserialize(e.to_string()))?;

        let index_meta_str: String = row
            .try_get("index_meta")
            .map_err(|e| Error::Deserialize(e.to_string()))?;

        let data_json: serde_json::Value =
            serde_json::from_str(&data_str).map_err(|e| Error::Deserialize(e.to_string()))?;

        let index_meta_json: serde_json::Value =
            serde_json::from_str(&index_meta_str).map_err(|e| Error::Deserialize(e.to_string()))?;

        let type_name = row
            .try_get::<String, _>("type")
            .map_err(|e| Error::Deserialize(e.to_string()))?;

        let from = row
            .try_get::<Uuid, _>("from")
            .map_err(|e| Error::Deserialize(e.to_string()))?;

        let to = row
            .try_get::<Uuid, _>("to")
            .map_err(|e| Error::Deserialize(e.to_string()))?;

        Ok(EdgeRecord {
            type_name: std::borrow::Cow::Owned(type_name),
            from,
            to,
            data: data_json,
            index_meta: index_meta_json,
        })
    }
    fn map_row_to_edge_and_object(row: SqliteRow) -> Result<(EdgeRecord, ObjectRecord), Error> {
        let de = |e: sqlx::Error| Error::Deserialize(e.to_string());
        let ds = |e: serde_json::Error| Error::Deserialize(e.to_string());

        let edge_data_str: String = row.try_get("edge_data").map_err(de)?;
        let edge_im_str: String = row.try_get("edge_index_meta").map_err(de)?;
        let obj_data_str: String = row.try_get("obj_data").map_err(de)?;

        let obj_created_str: String = row.try_get("obj_created_at").map_err(de)?;
        let obj_updated_str: String = row.try_get("obj_updated_at").map_err(de)?;

        let edge = EdgeRecord {
            type_name: std::borrow::Cow::Owned(row.try_get::<String, _>("edge_type").map_err(de)?),
            from: row.try_get::<Uuid, _>("edge_from").map_err(de)?,
            to: row.try_get::<Uuid, _>("edge_to").map_err(de)?,
            data: serde_json::from_str(&edge_data_str).map_err(ds)?,
            index_meta: serde_json::from_str(&edge_im_str).map_err(ds)?,
        };
        let obj = ObjectRecord {
            id: row.try_get::<Uuid, _>("obj_id").map_err(de)?,
            type_name: std::borrow::Cow::Owned(row.try_get::<String, _>("obj_type").map_err(de)?),
            owner: row.try_get::<Uuid, _>("obj_owner").map_err(de)?,
            created_at: chrono::DateTime::parse_from_rfc3339(&obj_created_str)
                .map_err(|e| Error::Deserialize(e.to_string()))?
                .with_timezone(&chrono::Utc),
            updated_at: chrono::DateTime::parse_from_rfc3339(&obj_updated_str)
                .map_err(|e| Error::Deserialize(e.to_string()))?
                .with_timezone(&chrono::Utc),
            data: serde_json::from_str(&obj_data_str).map_err(ds)?,
            index_meta: serde_json::Value::Null,
        };
        Ok((edge, obj))
    }

    async fn query_edges_with_objects_inner(
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

    // // ── Shared SQL builder helpers ───────────────────────────────────────────

    fn build_filter_condition(alias: &str, filter: &QueryFilter) -> Option<(String, &'static str)> {
        let crate::query::QueryMode::Search(ref qs) = filter.mode else {
            return None;
        };
        let comparison = match qs.comparison {
            crate::query::Comparison::Equal => "=",
            crate::query::Comparison::NotEqual => "!=",
            crate::query::Comparison::GreaterThan => ">",
            crate::query::Comparison::LessThan => "<",
            crate::query::Comparison::GreaterThanOrEqual => ">=",
            crate::query::Comparison::LessThanOrEqual => "<=",
            crate::query::Comparison::BeginsWith => "LIKE",
            crate::query::Comparison::Contains | crate::query::Comparison::ContainsAll => {
                if matches!(filter.value, IndexValue::Array(_)) {
                    "ARRAY_CONTAINS"
                } else {
                    "LIKE"
                }
            }
        };
        let col = format!(
            "json_extract({}.index_meta, '$.{}')",
            alias, filter.field.name
        );
        let condition = if comparison == "ARRAY_CONTAINS" {
            format!(
                "EXISTS (SELECT 1 FROM json_each({col}) WHERE value IN (SELECT value FROM json_each(?)))",
                col = col
            )
        } else {
            format!("{} {} ?", col, comparison)
        };
        let operator = match qs.operator {
            crate::query::Operator::And => "AND",
            _ => "OR",
        };
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
            ("o.type = ?".to_string(), "AND"),
            ("o.owner = ?".to_string(), "AND"),
        ];
        if cursor.is_some() {
            conditions.push(("o.id < ?".to_string(), "AND"));
        }
        for filter in filters {
            if let Some((cond, op)) = Self::build_filter_condition("o", filter) {
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
            ("e.type = ?".to_string(), "AND"),
            (format!("{} = ?", anchor_col), "AND"),
        ];
        if cursor.is_some() {
            conditions.push((format!("{} < ?", cursor_col), "AND"));
        }
        for filter in filters {
            if let Some((cond, op)) = Self::build_filter_condition("e", filter) {
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
                format!(
                    "json_extract({}index_meta, '$.{}') {}",
                    prefix, s.field.name, dir
                )
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

        // ── Object conditions ────────────────────────────────────────────────────
        let mut obj_conditions: Vec<(String, &str)> = vec![("o.type = ?".to_string(), "AND")];

        if cursor.is_some() {
            obj_conditions.push(("o.id < ?".to_string(), "AND"));
        }

        for filter in obj_filters {
            if let Some((cond, op)) = Self::build_filter_condition("o", filter) {
                obj_conditions.push((cond, op));
            }
        }

        // ── Edge conditions ──────────────────────────────────────────────────────
        let owner_col = match direction {
            TraversalDirection::Forward => r#"e."from""#,
            TraversalDirection::Reverse => r#"e."to""#,
        };

        let mut edge_conditions: Vec<(String, &str)> = vec![
            ("e.type = ?".to_string(), "AND"),
            (format!("{} = ?", owner_col), "AND"),
        ];

        for filter in edge_filters {
            if let Some((cond, op)) = Self::build_filter_condition("e", filter) {
                edge_conditions.push((cond, op));
            }
        }

        // ── Combine: obj AND edge ────────────────────────────────────────────────
        let obj_clause = Self::join_conditions(&obj_conditions);
        let edge_clause = Self::join_conditions(&edge_conditions);

        format!("WHERE {} AND ({})", obj_clause, edge_clause)
    }

    fn query_bind_filters<'a>(
        mut query: SqlxQuery<'a, Sqlite, SqliteArguments<'a>>,
        filters: &'a [QueryFilter],
    ) -> SqlxQuery<'a, Sqlite, SqliteArguments<'a>> {
        for filter in filters.iter().filter(|f| f.mode.as_search().is_some()) {
            query = match &filter.value {
                IndexValue::String(s) => {
                    use crate::query::Comparison::*;
                    match filter.mode.as_search().unwrap().comparison {
                        BeginsWith => query.bind(format!("{}%", s)),
                        Contains => query.bind(format!("%{}%", s)),
                        _ => query.bind(s),
                    }
                }
                IndexValue::Int(i) => query.bind(i),
                IndexValue::Float(f) => query.bind(f),
                IndexValue::Bool(b) => query.bind(b),
                IndexValue::Timestamp(t) => query.bind(t.to_rfc3339()),
                IndexValue::Uuid(uid) => query.bind(uid),
                IndexValue::Array(arr) => {
                    // Convert array to JSON string for SQLite
                    if let Some(first) = arr.first() {
                        match first {
                            IndexValueInner::String(_) => {
                                let values: Vec<&str> = arr
                                    .iter()
                                    .map(|s| s.as_string().unwrap_or_default())
                                    .collect();
                                query.bind(
                                    serde_json::to_string(&values)
                                        .unwrap_or_else(|_| "[]".to_string()),
                                )
                            }
                            IndexValueInner::Int(_) => {
                                let values: Vec<i64> =
                                    arr.iter().map(|s| s.as_int().unwrap_or_default()).collect();
                                query.bind(
                                    serde_json::to_string(&values)
                                        .unwrap_or_else(|_| "[]".to_string()),
                                )
                            }
                            IndexValueInner::Float(_) => {
                                let values: Vec<f64> = arr
                                    .iter()
                                    .map(|s| s.as_float().unwrap_or_default())
                                    .collect();
                                query.bind(
                                    serde_json::to_string(&values)
                                        .unwrap_or_else(|_| "[]".to_string()),
                                )
                            }
                        }
                    } else {
                        query.bind("[]".to_string())
                    }
                }
            };
        }
        query
    }

    fn query_scalar_bind_filters<'a, O>(
        mut query: QueryScalar<'a, Sqlite, O, SqliteArguments<'a>>,
        filters: &'a [QueryFilter],
    ) -> QueryScalar<'a, Sqlite, O, SqliteArguments<'a>> {
        for filter in filters.iter().filter(|f| f.mode.as_search().is_some()) {
            query = match &filter.value {
                IndexValue::String(s) => {
                    use crate::query::Comparison::*;
                    match filter.mode.as_search().unwrap().comparison {
                        BeginsWith => query.bind(format!("{}%", s)),
                        Contains => query.bind(format!("%{}%", s)),
                        _ => query.bind(s),
                    }
                }
                IndexValue::Int(i) => query.bind(i),
                IndexValue::Float(f) => query.bind(f),
                IndexValue::Bool(b) => query.bind(b),
                IndexValue::Timestamp(t) => query.bind(t.to_rfc3339()),
                IndexValue::Uuid(uid) => query.bind(uid),
                IndexValue::Array(arr) => {
                    // Convert array to JSON string for SQLite
                    if let Some(first) = arr.first() {
                        match first {
                            IndexValueInner::String(_) => {
                                let values: Vec<&str> = arr
                                    .iter()
                                    .map(|s| s.as_string().unwrap_or_default())
                                    .collect();
                                query.bind(
                                    serde_json::to_string(&values)
                                        .unwrap_or_else(|_| "[]".to_string()),
                                )
                            }
                            IndexValueInner::Int(_) => {
                                let values: Vec<i64> =
                                    arr.iter().map(|s| s.as_int().unwrap_or_default()).collect();
                                query.bind(
                                    serde_json::to_string(&values)
                                        .unwrap_or_else(|_| "[]".to_string()),
                                )
                            }
                            IndexValueInner::Float(_) => {
                                let values: Vec<f64> = arr
                                    .iter()
                                    .map(|s| s.as_float().unwrap_or_default())
                                    .collect();
                                query.bind(
                                    serde_json::to_string(&values)
                                        .unwrap_or_else(|_| "[]".to_string()),
                                )
                            }
                        }
                    } else {
                        query.bind("[]".to_string())
                    }
                }
            };
        }
        query
    }
}

impl SqliteAdapter {
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

    /// Build WHERE clause for batch traversal with object JOIN.
    /// Bindings: obj_type(?), edge_type(?), id1(?), id2(?), ..., obj_filters, edge_filters.
    fn build_batch_traversal_conditions(
        direction: TraversalDirection,
        obj_filters: &[QueryFilter],
        edge_filters: &[QueryFilter],
        n: usize,
    ) -> String {
        let placeholders = std::iter::repeat("?")
            .take(n)
            .collect::<Vec<_>>()
            .join(", ");
        let anchor = match direction {
            TraversalDirection::Forward => r#"e."from""#,
            TraversalDirection::Reverse => r#"e."to""#,
        };
        let mut obj_conditions: Vec<(String, &str)> = vec![("o.type = ?".to_string(), "AND")];
        for f in obj_filters {
            if let Some((c, op)) = Self::build_filter_condition("o", f) {
                obj_conditions.push((c, op));
            }
        }
        let mut edge_conditions: Vec<(String, &str)> = vec![
            ("e.type = ?".to_string(), "AND"),
            (format!("{} IN ({})", anchor, placeholders), "AND"),
        ];
        for f in edge_filters {
            if let Some((c, op)) = Self::build_filter_condition("e", f) {
                edge_conditions.push((c, op));
            }
        }
        format!(
            "WHERE {} AND ({})",
            Self::join_conditions(&obj_conditions),
            Self::join_conditions(&edge_conditions)
        )
    }

    /// Build WHERE clause for batch edge-only queries.
    /// Bindings: edge_type(?), id1(?), id2(?), ..., edge_filters.
    fn build_batch_edge_only_conditions(
        direction: TraversalDirection,
        edge_filters: &[QueryFilter],
        n: usize,
    ) -> String {
        let placeholders = std::iter::repeat("?")
            .take(n)
            .collect::<Vec<_>>()
            .join(", ");
        let anchor = match direction {
            TraversalDirection::Forward => r#"e."from""#,
            TraversalDirection::Reverse => r#"e."to""#,
        };
        let mut conditions: Vec<(String, &str)> = vec![
            ("e.type = ?".to_string(), "AND"),
            (format!("{} IN ({})", anchor, placeholders), "AND"),
        ];
        for f in edge_filters {
            if let Some((c, op)) = Self::build_filter_condition("e", f) {
                conditions.push((c, op));
            }
        }
        format!("WHERE {}", Self::join_conditions(&conditions))
    }

    /// Build WHERE clause for one branch of a UNION both-directions query with object JOIN.
    /// Each branch has its own bindings: obj_type(?), edge_type(?), pivot(?), obj_filters, edge_filters.
    fn build_union_branch_with_obj_conditions(
        direction: TraversalDirection,
        obj_filters: &[QueryFilter],
        edge_filters: &[QueryFilter],
    ) -> String {
        let anchor = match direction {
            TraversalDirection::Forward => r#"e."from""#,
            TraversalDirection::Reverse => r#"e."to""#,
        };
        let mut obj_conditions: Vec<(String, &str)> = vec![("o.type = ?".to_string(), "AND")];
        for f in obj_filters {
            if let Some((c, op)) = Self::build_filter_condition("o", f) {
                obj_conditions.push((c, op));
            }
        }
        let mut edge_conditions: Vec<(String, &str)> = vec![
            ("e.type = ?".to_string(), "AND"),
            (format!("{} = ?", anchor), "AND"),
        ];
        for f in edge_filters {
            if let Some((c, op)) = Self::build_filter_condition("e", f) {
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
    /// Each branch has its own bindings: edge_type(?), pivot(?), edge_filters.
    fn build_union_branch_edge_only_conditions(
        direction: TraversalDirection,
        edge_filters: &[QueryFilter],
    ) -> String {
        let anchor = match direction {
            TraversalDirection::Forward => r#"e."from""#,
            TraversalDirection::Reverse => r#"e."to""#,
        };
        let mut conditions: Vec<(String, &str)> = vec![
            ("e.type = ?".to_string(), "AND"),
            (format!("{} = ?", anchor), "AND"),
        ];
        for f in edge_filters {
            if let Some((c, op)) = Self::build_filter_condition("e", f) {
                conditions.push((c, op));
            }
        }
        format!("WHERE {}", Self::join_conditions(&conditions))
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
        rows.into_iter().map(Self::map_row_to_edge_record).collect()
    }
}

#[async_trait::async_trait]
impl Adapter for SqliteAdapter {
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
            INSERT INTO objects (id, type, owner, created_at, updated_at, data, index_meta)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(id)
        .bind(type_name.as_ref())
        .bind(owner)
        .bind(created_at.to_rfc3339())
        .bind(updated_at.to_rfc3339())
        .bind(serde_json::to_string(&data).map_err(|e| Error::Serialize(e.to_string()))?)
        .bind(serde_json::to_string(&index_meta).map_err(|e| Error::Serialize(e.to_string()))?)
        .execute(&self.pool)
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
            WHERE id = ? AND type = ?
            "#,
        )
        .bind(id)
        .bind(type_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record_slim(r).map(Some),
            None => Ok(None),
        }
    }

    async fn fetch_bulk_objects(
        &self,
        type_name: &'static str,
        ids: Vec<Uuid>,
    ) -> Result<Vec<ObjectRecord>, Error> {
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!(
            "SELECT o.id, o.type, o.owner, o.created_at, o.updated_at, o.data FROM objects o WHERE id IN ({}) AND type = ?",
            placeholders
        );

        let mut query = sqlx::query(&sql);
        for id in ids {
            query = query.bind(id);
        }
        query = query.bind(type_name);

        let rows = query
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
            SET updated_at = ?, data = ?, index_meta = ?
            WHERE id = ?
            "#,
        )
        .bind(record.updated_at.to_rfc3339())
        .bind(serde_json::to_string(&record.data).map_err(|e| Error::Serialize(e.to_string()))?)
        .bind(
            serde_json::to_string(&record.index_meta)
                .map_err(|e| Error::Serialize(e.to_string()))?,
        )
        .bind(record.id)
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
        // SQLite doesn't support RETURNING, so we update then fetch
        let result = sqlx::query(
            r#"
            UPDATE objects
            SET updated_at = ?, owner = ?
            WHERE id = ? AND owner = ? AND type = ?
            "#,
        )
        .bind(Utc::now().to_rfc3339())
        .bind(to_owner)
        .bind(id)
        .bind(from_owner)
        .bind(type_name)
        .execute(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(Error::NotFound);
        }

        self.fetch_object(type_name, id)
            .await?
            .ok_or(Error::NotFound)
    }

    async fn delete_object(
        &self,
        type_name: &'static str,
        id: Uuid,
        owner: Uuid,
    ) -> Result<Option<ObjectRecord>, Error> {
        // Fetch first, then delete (SQLite doesn't have RETURNING)
        let record = self.fetch_object(type_name, id).await?;

        if let Some(ref rec) = record {
            if rec.owner != owner {
                return Ok(None);
            }

            sqlx::query(
                r#"
                DELETE FROM objects
                WHERE id = ? AND owner = ?
                "#,
            )
            .bind(id)
            .bind(owner)
            .execute(&self.pool)
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;
        }

        Ok(record)
    }

    async fn delete_bulk_objects(
        &self,
        type_name: &'static str,
        ids: Vec<Uuid>,
        owner: Uuid,
    ) -> Result<u64, Error> {
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!(
            "DELETE FROM objects WHERE id IN ({}) AND type = ? AND owner = ?",
            placeholders
        );

        let mut query = sqlx::query(&sql);
        for id in ids {
            query = query.bind(id);
        }
        query = query.bind(type_name);

        let result = query
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
        let result = sqlx::query("DELETE FROM objects WHERE type = ? AND owner = ?")
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
            LIMIT 1
            "#,
            where_clause, order_clause
        );

        let mut query = sqlx::query(&sql).bind(type_name).bind(owner);
        query = Self::query_bind_filters(query, filters);

        let row = query
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record_slim(r).map(Some),
            None => Ok(None),
        }
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

        rows.into_iter()
            .map(Self::map_row_to_object_record_slim)
            .collect()
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
                let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM objects WHERE type = ?")
                    .bind(type_name)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|err| Error::Storage(err.to_string()))?;

                Ok(count as u64)
            }
        }
    }

    async fn fetch_owned_objects_batch(
        &self,
        type_name: &'static str,
        owner_ids: &[Uuid],
    ) -> Result<Vec<ObjectRecord>, Error> {
        if owner_ids.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = owner_ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let sql = format!(
            "SELECT o.id, o.type, o.owner, o.created_at, o.updated_at, o.data FROM objects o WHERE type = ? AND owner IN ({})",
            placeholders
        );
        let mut query = sqlx::query(&sql).bind(type_name);
        for id in owner_ids {
            query = query.bind(*id);
        }
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;

        rows.into_iter()
            .map(Self::map_row_to_object_record_slim)
            .collect()
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
            WHERE owner = ? AND type = ?
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
            WHERE owner = ? AND type = ?
            LIMIT 1
            "#,
        )
        .bind(owner)
        .bind(type_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record_slim(r).map(Some),
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
            WHERE id = ? AND (type = ? OR type = ?)
            "#,
        )
        .bind(id)
        .bind(a_type_name)
        .bind(b_type_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record_slim(r).map(Some),
            None => Ok(None),
        }
    }

    async fn fetch_union_objects(
        &self,
        a_type_name: &'static str,
        b_type_name: &'static str,
        ids: Vec<Uuid>,
    ) -> Result<Vec<ObjectRecord>, Error> {
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");

        let sql = format!(
            r#"
            SELECT o.id, o.type, o.owner, o.created_at, o.updated_at, o.data
            FROM objects o
            WHERE id IN ({}) AND (type = ? OR type = ?)
            "#,
            placeholders
        );
        let mut query = sqlx::query(&sql);

        for id in ids {
            query = query.bind(id);
        }
        let rows = query
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
            WHERE owner = ? AND (type = ? OR type = ?)
            "#,
        )
        .bind(owner)
        .bind(a_type_name)
        .bind(b_type_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record_slim(r).map(Some),
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
            WHERE owner = ? AND (type = ? OR type = ?)
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
        let data_str = serde_json::to_string(&data).map_err(|e| Error::Serialize(e.to_string()))?;
        let index_meta_str =
            serde_json::to_string(&index_meta).map_err(|e| Error::Serialize(e.to_string()))?;

        let _ = sqlx::query(
            r#"
            INSERT INTO edges ("from", "to", type, data, index_meta)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT ("from", type, "to")
            DO UPDATE SET data = ?, index_meta = ?;
            "#,
        )
        .bind(from)
        .bind(to)
        .bind(type_name.as_ref())
        .bind(&data_str)
        .bind(&index_meta_str)
        .bind(&data_str)
        .bind(&index_meta_str)
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
        UPDATE edges SET data = ?, "to" = ?
        WHERE "from" = ? AND type = ? AND "to" = ?
        "#,
        )
        .bind(serde_json::to_string(&data).map_err(|e| Error::Serialize(e.to_string()))?)
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
            WHERE type = ? AND "from" = ? AND "to" = ?
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
            WHERE type = ? AND "from" = ?
            "#,
        )
        .bind(type_name)
        .bind(from.to_string())
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

    async fn query_edges_with_targets(
        &self,
        edge_type: &'static str,
        obj_type: &'static str,
        owner: Uuid,
        obj_filters: &[QueryFilter],
        plan: EdgeQuery,
    ) -> Result<Vec<(EdgeRecord, ObjectRecord)>, Error> {
        self.query_edges_with_objects_inner(
            edge_type,
            obj_type,
            owner,
            obj_filters,
            plan,
            TraversalDirection::Forward,
        )
        .await
    }

    async fn query_reverse_edges_with_sources(
        &self,
        edge_type: &'static str,
        obj_type: &'static str,
        owner: Uuid,
        obj_filters: &[QueryFilter],
        plan: EdgeQuery,
    ) -> Result<Vec<(EdgeRecord, ObjectRecord)>, Error> {
        self.query_edges_with_objects_inner(
            edge_type,
            obj_type,
            owner,
            obj_filters,
            plan,
            TraversalDirection::Reverse,
        )
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
                    r#"SELECT COUNT(*) FROM edges WHERE type = ? AND "from" = ?"#,
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
                let count: i64 =
                    sqlx::query_scalar(r#"SELECT COUNT(*) FROM edges WHERE type = ? AND "to" = ?"#)
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
            sqlx::query_scalar("SELECT COALESCE((SELECT value FROM sequences WHERE name = ?), 1)")
                .bind(&sq)
                .fetch_one(&self.pool)
                .await
                .expect("Failed to fetch the current sequence value");
        val as u64
    }

    async fn sequence_next_value(&self, sq: String) -> u64 {
        let mut tx = self
            .pool
            .begin()
            .await
            .expect("Failed to begin transaction");

        sqlx::query(
            "INSERT INTO sequences (name, value) VALUES (?, 2)
             ON CONFLICT (name) DO UPDATE SET value = sequences.value + 1",
        )
        .bind(&sq)
        .execute(&mut *tx)
        .await
        .expect("Failed to upsert sequence");

        let next_val: i64 = sqlx::query_scalar("SELECT value FROM sequences WHERE name = ?")
            .bind(&sq)
            .fetch_one(&mut *tx)
            .await
            .expect("Failed to fetch the next sequence value");

        tx.commit().await.expect("Failed to commit transaction");

        next_val as u64
    }
}

#[async_trait::async_trait]
impl UniqueAdapter for SqliteAdapter {
    async fn insert_unique_hashes(
        &self,
        type_name: &str,
        object_id: Uuid,
        hashes: Vec<(String, &str)>,
    ) -> Result<(), Error> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;
        for (hash, field) in hashes {
            sqlx::query(
                r#"
                INSERT INTO unique_constraints (id, type, key, field)
                VALUES (?, ?, ?, ?)
                "#,
            )
            .bind(object_id)
            .bind(type_name)
            .bind(hash)
            .bind(&field)
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                // Check if it's a uniqueness violation
                if err.to_string().contains("unique") {
                    Error::UniqueConstraintViolation(field.to_string())
                } else {
                    Error::Storage(err.to_string())
                }
            })?;
        }

        tx.commit()
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;
        Ok(())
    }

    async fn delete_unique(&self, hash: &str) -> Result<(), Error> {
        sqlx::query(
            r#"
            DELETE FROM unique_constraints WHERE key = ?
            "#,
        )
        .bind(hash)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn delete_unique_hashes(&self, hashes: Vec<String>) -> Result<(), Error> {
        let placeholders = hashes.iter().map(|_| "?").collect::<Vec<_>>().join(",");

        let sql = format!(
            "DELETE FROM unique_constraints WHERE key IN ({})",
            placeholders
        );

        let mut query = sqlx::query(&sql);
        for id in hashes {
            query = query.bind(id);
        }

        query
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn get_hashes_for_object(&self, object_id: Uuid) -> Result<Vec<String>, Error> {
        let rows = sqlx::query(
            r#"
            SELECT key FROM unique_constraints WHERE id = ?
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
impl EdgeTraversal for SqliteAdapter {
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

    async fn query_edges_with_targets_batch(
        &self,
        edge_type: &'static str,
        obj_type: &'static str,
        from_ids: &[Uuid],
        obj_filters: &[QueryFilter],
        plan: EdgeQuery,
    ) -> Result<Vec<(EdgeRecord, ObjectRecord)>, Error> {
        if from_ids.is_empty() {
            return Ok(Vec::new());
        }
        let where_clause = Self::build_batch_traversal_conditions(
            TraversalDirection::Forward,
            obj_filters,
            &plan.filters,
            from_ids.len(),
        );
        let order_clause = Self::build_edge_order_clause(&plan.filters);
        let mut sql = format!(
            r#"
            SELECT
                e."from" AS edge_from, e."to" AS edge_to, e.type AS edge_type,
                e.data AS edge_data, e.index_meta AS edge_index_meta,
                o.id AS obj_id, o.type AS obj_type, o.owner AS obj_owner,
                o.created_at AS obj_created_at, o.updated_at AS obj_updated_at, o.data AS obj_data
            FROM edges e
            JOIN objects o ON e."to" = o.id
            {where_clause}
            {order_clause}
            "#,
        );
        if let Some(limit) = plan.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }
        let mut query = sqlx::query(&sql).bind(obj_type).bind(edge_type);
        for id in from_ids {
            query = query.bind(*id);
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

    async fn query_reverse_edges_with_sources_batch(
        &self,
        edge_type: &'static str,
        obj_type: &'static str,
        to_ids: &[Uuid],
        obj_filters: &[QueryFilter],
        plan: EdgeQuery,
    ) -> Result<Vec<(EdgeRecord, ObjectRecord)>, Error> {
        if to_ids.is_empty() {
            return Ok(Vec::new());
        }
        let where_clause = Self::build_batch_traversal_conditions(
            TraversalDirection::Reverse,
            obj_filters,
            &plan.filters,
            to_ids.len(),
        );
        let order_clause = Self::build_edge_order_clause(&plan.filters);
        let mut sql = format!(
            r#"
            SELECT
                e."from" AS edge_from, e."to" AS edge_to, e.type AS edge_type,
                e.data AS edge_data, e.index_meta AS edge_index_meta,
                o.id AS obj_id, o.type AS obj_type, o.owner AS obj_owner,
                o.created_at AS obj_created_at, o.updated_at AS obj_updated_at, o.data AS obj_data
            FROM edges e
            JOIN objects o ON e."from" = o.id
            {where_clause}
            {order_clause}
            "#,
        );
        if let Some(limit) = plan.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }
        let mut query = sqlx::query(&sql).bind(obj_type).bind(edge_type);
        for id in to_ids {
            query = query.bind(*id);
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

    async fn query_edges_batch(
        &self,
        edge_type: &'static str,
        from_ids: &[Uuid],
        plan: EdgeQuery,
    ) -> Result<Vec<EdgeRecord>, Error> {
        if from_ids.is_empty() {
            return Ok(Vec::new());
        }
        let where_clause = Self::build_batch_edge_only_conditions(
            TraversalDirection::Forward,
            &plan.filters,
            from_ids.len(),
        );
        let order_clause = Self::build_edge_order_clause(&plan.filters);
        let mut sql = format!(
            r#"
            SELECT e."from", e."to", e.type, e.data, e.index_meta
            FROM edges e
            {where_clause}
            {order_clause}
            "#,
        );
        if let Some(limit) = plan.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }
        let mut query = sqlx::query(&sql).bind(edge_type);
        for id in from_ids {
            query = query.bind(*id);
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

    async fn query_reverse_edges_batch(
        &self,
        edge_type: &'static str,
        to_ids: &[Uuid],
        plan: EdgeQuery,
    ) -> Result<Vec<EdgeRecord>, Error> {
        if to_ids.is_empty() {
            return Ok(Vec::new());
        }
        let where_clause = Self::build_batch_edge_only_conditions(
            TraversalDirection::Reverse,
            &plan.filters,
            to_ids.len(),
        );
        let order_clause = Self::build_edge_order_clause(&plan.filters);
        let mut sql = format!(
            r#"
            SELECT e."from", e."to", e.type, e.data, e.index_meta
            FROM edges e
            {where_clause}
            {order_clause}
            "#,
        );
        if let Some(limit) = plan.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }
        let mut query = sqlx::query(&sql).bind(edge_type);
        for id in to_ids {
            query = query.bind(*id);
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

    async fn query_edges_both_directions_with_objects(
        &self,
        edge_type: &'static str,
        obj_type: &'static str,
        pivot: Uuid,
        obj_filters: &[QueryFilter],
        plan: EdgeQuery,
    ) -> Result<
        (
            Vec<(EdgeRecord, ObjectRecord)>,
            Vec<(EdgeRecord, ObjectRecord)>,
        ),
        Error,
    > {
        let fwd_where = Self::build_union_branch_with_obj_conditions(
            TraversalDirection::Forward,
            obj_filters,
            &plan.filters,
        );
        let rev_where = Self::build_union_branch_with_obj_conditions(
            TraversalDirection::Reverse,
            obj_filters,
            &plan.filters,
        );
        let sel = r#"
            SELECT
                e."from" AS edge_from, e."to" AS edge_to, e.type AS edge_type,
                e.data AS edge_data, e.index_meta AS edge_index_meta,
                o.id AS obj_id, o.type AS obj_type, o.owner AS obj_owner,
                o.created_at AS obj_created_at, o.updated_at AS obj_updated_at, o.data AS obj_data
        "#;
        let sql = format!(
            "{sel} FROM edges e JOIN objects o ON e.\"to\" = o.id {fwd_where}
            UNION ALL
            {sel} FROM edges e JOIN objects o ON e.\"from\" = o.id {rev_where}",
        );
        // SQLite ?-params are positional — bind each branch independently
        let mut query = sqlx::query(&sql).bind(obj_type).bind(edge_type).bind(pivot);
        query = Self::query_bind_filters(query, obj_filters);
        query = Self::query_bind_filters(query, &plan.filters);
        // reverse branch
        query = query.bind(obj_type).bind(edge_type).bind(pivot);
        query = Self::query_bind_filters(query, obj_filters);
        query = Self::query_bind_filters(query, &plan.filters);

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let mut fwd: Vec<(EdgeRecord, ObjectRecord)> = Vec::new();
        let mut rev: Vec<(EdgeRecord, ObjectRecord)> = Vec::new();
        for row in rows {
            let edge_from: Uuid = row
                .try_get::<Uuid, _>("edge_from")
                .map_err(|e| Error::Deserialize(e.to_string()))?;
            let pair = Self::map_row_to_edge_and_object(row)?;
            if edge_from == pivot {
                fwd.push(pair);
            } else {
                rev.push(pair);
            }
        }
        Ok((fwd, rev))
    }

    async fn query_edges_both_directions(
        &self,
        edge_type: &'static str,
        pivot: Uuid,
        plan: EdgeQuery,
    ) -> Result<(Vec<EdgeRecord>, Vec<EdgeRecord>), Error> {
        let fwd_where = Self::build_union_branch_edge_only_conditions(
            TraversalDirection::Forward,
            &plan.filters,
        );
        let rev_where = Self::build_union_branch_edge_only_conditions(
            TraversalDirection::Reverse,
            &plan.filters,
        );
        let sql = format!(
            r#"SELECT e."from", e."to", e.type, e.data, e.index_meta FROM edges e {fwd_where}
            UNION ALL
            SELECT e."from", e."to", e.type, e.data, e.index_meta FROM edges e {rev_where}"#,
        );
        // Bind each branch separately (positional ?)
        let mut query = sqlx::query(&sql).bind(edge_type).bind(pivot);
        query = Self::query_bind_filters(query, &plan.filters);
        query = query.bind(edge_type).bind(pivot);
        query = Self::query_bind_filters(query, &plan.filters);

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let mut fwd: Vec<EdgeRecord> = Vec::new();
        let mut rev: Vec<EdgeRecord> = Vec::new();
        for row in rows {
            let edge_from: Uuid = row
                .try_get::<Uuid, _>("from")
                .map_err(|e| Error::Deserialize(e.to_string()))?;
            let record = Self::map_row_to_edge_record(row)?;
            if edge_from == pivot {
                fwd.push(record);
            } else {
                rev.push(record);
            }
        }
        Ok((fwd, rev))
    }

    async fn count_edges_batch(
        &self,
        edge_type: &'static str,
        from_ids: &[Uuid],
        plan: EdgeQuery,
    ) -> Result<Vec<(Uuid, u64)>, Error> {
        if from_ids.is_empty() {
            return Ok(Vec::new());
        }
        let where_clause = Self::build_batch_edge_only_conditions(
            TraversalDirection::Forward,
            &plan.filters,
            from_ids.len(),
        );
        let sql = format!(
            r#"SELECT e."from", COUNT(*) AS cnt FROM edges e {where_clause} GROUP BY e."from""#,
        );
        let mut query = sqlx::query(&sql).bind(edge_type);
        for id in from_ids {
            query = query.bind(*id);
        }
        query = Self::query_bind_filters(query, &plan.filters);
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        rows.into_iter()
            .map(|row| {
                let id: Uuid = row
                    .try_get("from")
                    .map_err(|e| Error::Deserialize(e.to_string()))?;
                let cnt: i64 = row
                    .try_get("cnt")
                    .map_err(|e| Error::Deserialize(e.to_string()))?;
                Ok((id, cnt as u64))
            })
            .collect()
    }

    async fn count_reverse_edges_batch(
        &self,
        edge_type: &'static str,
        to_ids: &[Uuid],
        plan: EdgeQuery,
    ) -> Result<Vec<(Uuid, u64)>, Error> {
        if to_ids.is_empty() {
            return Ok(Vec::new());
        }
        let where_clause = Self::build_batch_edge_only_conditions(
            TraversalDirection::Reverse,
            &plan.filters,
            to_ids.len(),
        );
        let sql = format!(
            r#"SELECT e."to", COUNT(*) AS cnt FROM edges e {where_clause} GROUP BY e."to""#,
        );
        let mut query = sqlx::query(&sql).bind(edge_type);
        for id in to_ids {
            query = query.bind(*id);
        }
        query = Self::query_bind_filters(query, &plan.filters);
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        rows.into_iter()
            .map(|row| {
                let id: Uuid = row
                    .try_get("to")
                    .map_err(|e| Error::Deserialize(e.to_string()))?;
                let cnt: i64 = row
                    .try_get("cnt")
                    .map_err(|e| Error::Deserialize(e.to_string()))?;
                Ok((id, cnt as u64))
            })
            .collect()
    }
}
