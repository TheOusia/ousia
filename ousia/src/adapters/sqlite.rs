use chrono::Utc;
use sqlx::{
    Row, Sqlite,
    query::{Query as SqlxQuery, QueryScalar},
    sqlite::{SqliteArguments, SqlitePool, SqlitePoolOptions, SqliteRow},
};
use uuid::Uuid;

use crate::{
    adapters::{Adapter, EdgeQuery, EdgeRecord, Error, ObjectRecord, Query, UniqueAdapter},
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
/// CREATE INDEX idx_objects_type_owner ON objects(type, owner);
/// CREATE INDEX idx_objects_owner ON objects(owner);
/// CREATE INDEX idx_objects_created_at ON objects(created_at);
/// CREATE INDEX idx_objects_updated_at ON objects(updated_at);
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
            CREATE INDEX IF NOT EXISTS idx_objects_type_owner ON objects(type, owner)
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_owner ON objects(owner)
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_created_at ON objects(created_at)
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_updated_at ON objects(updated_at)
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

        tx.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn ensure_sequence_exists(&self, sq: String) {
        // Create the sequences table if it doesn't exist
        let _ = sqlx::query(
            "CREATE TABLE IF NOT EXISTS sequences (
            name TEXT PRIMARY KEY,
            value INTEGER NOT NULL DEFAULT 1
        )",
        )
        .execute(&self.pool.clone())
        .await
        .unwrap();

        // Insert the sequence if it doesn't exist
        let sql = format!(
            "INSERT OR IGNORE INTO sequences (name, value) VALUES ('{}', 1)",
            sq
        );

        let _ = sqlx::query(&sql).execute(&self.pool.clone()).await.unwrap();
    }
}

impl SqliteAdapter {
    fn map_row_to_object_record(row: SqliteRow) -> Result<ObjectRecord, Error> {
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
            type_name,
            owner,
            created_at,
            updated_at,
            data: data_json,
            index_meta: index_meta_json,
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
            type_name,
            from,
            to,
            data: data_json,
            index_meta: index_meta_json,
        })
    }
    fn build_object_query_conditions(filters: &Vec<QueryFilter>, cursor: Option<Cursor>) -> String {
        let mut conditions = vec![
            ("type = ?".to_string(), "AND"),
            ("owner = ?".to_string(), "AND"),
        ];

        if cursor.is_some() {
            conditions.push(("id < ?".to_string(), "AND"));
        }

        for filter in filters {
            match &filter.mode {
                crate::query::QueryMode::Search(query_search) => {
                    let comparison = match query_search.comparison {
                        crate::query::Comparison::Equal => "=",
                        crate::query::Comparison::BeginsWith => "LIKE",
                        crate::query::Comparison::Contains
                        | crate::query::Comparison::ContainsAll => {
                            if matches!(filter.value, IndexValue::Array(_)) {
                                // For arrays, we'll use a custom check
                                "ARRAY_CONTAINS"
                            } else {
                                "LIKE"
                            }
                        }
                        crate::query::Comparison::GreaterThan => ">",
                        crate::query::Comparison::LessThan => "<",
                        crate::query::Comparison::GreaterThanOrEqual => ">=",
                        crate::query::Comparison::LessThanOrEqual => "<=",
                        crate::query::Comparison::NotEqual => "!=",
                    };

                    let condition = if comparison == "ARRAY_CONTAINS" {
                        // For array contains, we need to check if any element in the stored JSON array
                        // is present in the query array. We'll use a placeholder that gets expanded later.
                        format!(
                            "EXISTS (SELECT 1 FROM json_each(json_extract(index_meta, '$.{}')) WHERE value IN (SELECT value FROM json_each(?)))",
                            filter.field.name
                        )
                    } else {
                        format!(
                            "json_extract(index_meta, '$.{}') {} ?",
                            filter.field.name, comparison
                        )
                    };

                    let operator = match query_search.operator {
                        crate::query::Operator::And => "AND",
                        _ => "OR",
                    };
                    conditions.push((condition, operator));
                }
                crate::query::QueryMode::Sort(_) => continue,
            }
        }

        let mut query = String::new();
        for (i, (cond, joiner)) in conditions.iter().enumerate() {
            query.push_str(cond);
            if i < conditions.len() - 1 && !joiner.is_empty() {
                query.push(' ');
                query.push_str(joiner);
                query.push(' ');
            }
        }

        format!("WHERE {}", query)
    }

    fn build_edge_query_conditions(filters: &Vec<QueryFilter>, cursor: Option<Cursor>) -> String {
        let mut conditions = vec![
            ("type = ?".to_string(), "AND"),
            (r#""from" = ?"#.to_string(), "AND"),
        ];

        if cursor.is_some() {
            conditions.push((r#""to" > ?"#.to_string(), "AND"));
        }

        for filter in filters {
            match &filter.mode {
                crate::query::QueryMode::Search(query_search) => {
                    let comparison = match query_search.comparison {
                        crate::query::Comparison::Equal => "=",
                        crate::query::Comparison::BeginsWith => "LIKE",
                        crate::query::Comparison::Contains
                        | crate::query::Comparison::ContainsAll => {
                            if matches!(filter.value, IndexValue::Array(_)) {
                                "ARRAY_CONTAINS"
                            } else {
                                "LIKE"
                            }
                        }
                        crate::query::Comparison::GreaterThan => ">",
                        crate::query::Comparison::LessThan => "<",
                        crate::query::Comparison::GreaterThanOrEqual => ">=",
                        crate::query::Comparison::LessThanOrEqual => "<=",
                        crate::query::Comparison::NotEqual => "!=",
                    };

                    let condition = if comparison == "ARRAY_CONTAINS" {
                        format!(
                            "EXISTS (SELECT 1 FROM json_each(json_extract(index_meta, '$.{}')) WHERE value IN (SELECT value FROM json_each(?)))",
                            filter.field.name
                        )
                    } else {
                        format!(
                            "json_extract(index_meta, '$.{}') {} ?",
                            filter.field.name, comparison
                        )
                    };

                    let operator = match query_search.operator {
                        crate::query::Operator::And => "AND",
                        _ => "OR",
                    };
                    conditions.push((condition, operator));
                }
                crate::query::QueryMode::Sort(_) => continue,
            }
        }

        let mut query = String::new();
        for (i, (cond, joiner)) in conditions.iter().enumerate() {
            query.push_str(cond);
            if i < conditions.len() - 1 && !joiner.is_empty() {
                query.push(' ');
                query.push_str(joiner);
                query.push(' ');
            }
        }

        format!("WHERE {}", query)
    }

    fn build_edge_reverse_query_conditions(
        filters: &Vec<QueryFilter>,
        cursor: Option<Cursor>,
    ) -> String {
        let mut conditions = vec![
            ("type = ?".to_string(), "AND"),
            (r#""to" = ?"#.to_string(), "AND"),
        ];

        if cursor.is_some() {
            conditions.push((r#""from" > ?"#.to_string(), "AND"));
        }

        for filter in filters {
            match &filter.mode {
                crate::query::QueryMode::Search(query_search) => {
                    let comparison = match query_search.comparison {
                        crate::query::Comparison::Equal => "=",
                        crate::query::Comparison::BeginsWith => "LIKE",
                        crate::query::Comparison::Contains
                        | crate::query::Comparison::ContainsAll => {
                            if matches!(filter.value, IndexValue::Array(_)) {
                                "ARRAY_CONTAINS"
                            } else {
                                "LIKE"
                            }
                        }
                        crate::query::Comparison::GreaterThan => ">",
                        crate::query::Comparison::LessThan => "<",
                        crate::query::Comparison::GreaterThanOrEqual => ">=",
                        crate::query::Comparison::LessThanOrEqual => "<=",
                        crate::query::Comparison::NotEqual => "!=",
                    };

                    let condition = if comparison == "ARRAY_CONTAINS" {
                        format!(
                            "EXISTS (SELECT 1 FROM json_each(json_extract(index_meta, '$.{}')) WHERE value IN (SELECT value FROM json_each(?)))",
                            filter.field.name
                        )
                    } else {
                        format!(
                            "json_extract(index_meta, '$.{}') {} ?",
                            filter.field.name, comparison
                        )
                    };

                    let operator = match query_search.operator {
                        crate::query::Operator::And => "AND",
                        _ => "OR",
                    };
                    conditions.push((condition, operator));
                }
                crate::query::QueryMode::Sort(_) => continue,
            }
        }

        let mut query = String::new();
        for (i, (cond, joiner)) in conditions.iter().enumerate() {
            query.push_str(cond);
            if i < conditions.len() - 1 && !joiner.is_empty() {
                query.push(' ');
                query.push_str(joiner);
                query.push(' ');
            }
        }

        format!("WHERE {}", query)
    }

    fn build_order_clause(filters: &Vec<QueryFilter>) -> String {
        let sort: Vec<&QueryFilter> = filters
            .iter()
            .filter(|f| f.mode.as_sort().is_some())
            .collect();

        if sort.is_empty() {
            return "ORDER BY id DESC".to_string();
        }

        let order_terms: Vec<String> = sort
            .iter()
            .filter(|s| s.value.as_array().is_none()) // Filter out array types from sorting
            .map(|s| {
                let direction = if s.mode.as_sort().unwrap().ascending {
                    "ASC"
                } else {
                    "DESC"
                };

                format!(
                    "json_extract(index_meta, '$.{}') {}",
                    s.field.name, direction
                )
            })
            .collect();

        format!("ORDER BY {}", order_terms.join(", "))
    }

    fn build_edge_order_clause(filters: &Vec<QueryFilter>) -> String {
        let sort: Vec<&QueryFilter> = filters
            .iter()
            .filter(|f| f.mode.as_sort().is_some())
            .collect();

        if sort.is_empty() {
            return "".to_string();
        }

        let order_terms: Vec<String> = sort
            .iter()
            .filter(|s| s.value.as_array().is_none()) // Filter out array types from sorting
            .map(|s| {
                let direction = if s.mode.as_sort().unwrap().ascending {
                    "ASC"
                } else {
                    "DESC"
                };

                format!(
                    "json_extract(index_meta, '$.{}') {}",
                    s.field.name, direction
                )
            })
            .collect();

        format!("ORDER BY {}", order_terms.join(", "))
    }

    fn query_bind_filters<'a>(
        mut query: SqlxQuery<'a, Sqlite, SqliteArguments<'a>>,
        filters: &'a Vec<QueryFilter>,
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
        filters: &'a Vec<QueryFilter>,
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

#[async_trait::async_trait]
impl Adapter for SqliteAdapter {
    async fn insert_object(&self, record: ObjectRecord) -> Result<(), Error> {
        let pool = self.pool.clone();
        let _ = sqlx::query(
            r#"
            INSERT INTO objects (id, type, owner, created_at, updated_at, data, index_meta)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(record.id)
        .bind(record.type_name)
        .bind(record.owner)
        .bind(record.created_at.to_rfc3339())
        .bind(record.updated_at.to_rfc3339())
        .bind(serde_json::to_string(&record.data).map_err(|e| Error::Serialize(e.to_string()))?)
        .bind(
            serde_json::to_string(&record.index_meta)
                .map_err(|e| Error::Serialize(e.to_string()))?,
        )
        .execute(&pool)
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
        let pool = self.pool.clone();
        let row = sqlx::query(
            r#"
            SELECT id, type, owner, created_at, updated_at, data, index_meta
            FROM objects
            WHERE id = ? AND type = ?
            "#,
        )
        .bind(id)
        .bind(type_name)
        .fetch_optional(&pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record(r).map(Some),
            None => Ok(None),
        }
    }

    async fn fetch_bulk_objects(
        &self,
        type_name: &'static str,
        ids: Vec<Uuid>,
    ) -> Result<Vec<ObjectRecord>, Error> {
        let pool = self.pool.clone();
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!(
            "SELECT id, type, owner, created_at, updated_at, data, index_meta FROM objects WHERE id IN ({}) AND type = ?",
            placeholders
        );

        let mut query = sqlx::query(&sql);
        for id in ids {
            query = query.bind(id);
        }
        query = query.bind(type_name);

        let rows = query
            .fetch_all(&pool)
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;

        rows.into_iter()
            .map(Self::map_row_to_object_record)
            .collect()
    }

    async fn update_object(&self, record: ObjectRecord) -> Result<(), Error> {
        let pool = self.pool.clone();
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
        .execute(&pool)
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
        let pool = self.pool.clone();

        // SQLite doesn't support RETURNING, so we need to update then fetch
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
        .execute(&pool)
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
        let pool = self.pool.clone();

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
            .execute(&pool)
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
        let pool = self.pool.clone();

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
            .execute(&pool)
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
        let where_clause = Self::build_object_query_conditions(&filters.to_vec(), None);
        let order_clause = Self::build_order_clause(&filters.to_vec());

        let sql = format!(
            r#"
            SELECT id, type, owner, created_at, updated_at, data, index_meta
            FROM objects
            {}
            {}
            LIMIT 1
            "#,
            where_clause, order_clause
        );

        let mut query = sqlx::query(&sql).bind(type_name).bind(owner);

        let f = filters.to_vec();
        query = Self::query_bind_filters(query, &f);

        let pool = self.pool.clone();
        let row = query
            .fetch_optional(&pool)
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record(r).map(Some),
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
            where_clause = where_clause.replace("owner = ", "owner > ");
        }

        let mut sql = format!(
            r#"
            SELECT id, type, owner, created_at, updated_at, data, index_meta
            FROM objects
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

        let pool = self.pool.clone();
        let rows = query
            .fetch_all(&pool)
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;

        rows.into_iter()
            .map(Self::map_row_to_object_record)
            .collect()
    }

    async fn count_objects(
        &self,
        type_name: &'static str,
        plan: Option<Query>,
    ) -> Result<u64, Error> {
        let pool = self.pool.clone();

        match plan {
            Some(plan) => {
                let where_clause = Self::build_object_query_conditions(&plan.filters, None);

                let mut sql = format!(
                    r#"
                    SELECT COUNT(*) FROM objects
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
                    .fetch_one(&pool)
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;

                Ok(count as u64)
            }
            None => {
                let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM objects WHERE type = ?")
                    .bind(type_name)
                    .fetch_one(&pool)
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
        let pool = self.pool.clone();
        let rows = sqlx::query(
            r#"
            SELECT id, type, owner, created_at, updated_at, data, index_meta
            FROM objects
            WHERE owner = ? AND type = ?
            "#,
        )
        .bind(owner)
        .bind(type_name)
        .fetch_all(&pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        rows.into_iter()
            .map(Self::map_row_to_object_record)
            .collect()
    }

    async fn fetch_owned_object(
        &self,
        type_name: &'static str,
        owner: Uuid,
    ) -> Result<Option<ObjectRecord>, Error> {
        let pool = self.pool.clone();
        let row = sqlx::query(
            r#"
            SELECT id, type, owner, created_at, updated_at, data, index_meta
            FROM objects
            WHERE owner = ? AND type = ?
            LIMIT 1
            "#,
        )
        .bind(owner)
        .bind(type_name)
        .fetch_optional(&pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record(r).map(Some),
            None => Ok(None),
        }
    }

    async fn fetch_union_object(
        &self,
        a_type_name: &'static str,
        b_type_name: &'static str,
        id: Uuid,
    ) -> Result<Option<ObjectRecord>, Error> {
        let pool = self.pool.clone();
        let row = sqlx::query(
            r#"
            SELECT id, type, owner, created_at, updated_at, data, index_meta
            FROM objects
            WHERE id = ? AND (type = ? OR type = ?)
            "#,
        )
        .bind(id)
        .bind(a_type_name)
        .bind(b_type_name)
        .fetch_optional(&pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record(r).map(|o| Some(o)),
            None => Ok(None),
        }
    }

    async fn fetch_union_objects(
        &self,
        a_type_name: &'static str,
        b_type_name: &'static str,
        ids: Vec<Uuid>,
    ) -> Result<Vec<ObjectRecord>, Error> {
        let pool = self.pool.clone();

        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");

        let sql = format!(
            r#"
            SELECT id, type, owner, created_at, updated_at, data, index_meta
            FROM objects
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
            .fetch_all(&pool)
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;

        rows.into_iter()
            .map(Self::map_row_to_object_record)
            .collect()
    }

    async fn fetch_owned_union_object(
        &self,
        a_type_name: &'static str,
        b_type_name: &'static str,
        owner: Uuid,
    ) -> Result<Option<ObjectRecord>, Error> {
        let pool = self.pool.clone();
        let row = sqlx::query(
            r#"
            SELECT id, type, owner, created_at, updated_at, data, index_meta
            FROM objects
            WHERE owner = ? AND (type = ? OR type = ?)
            "#,
        )
        .bind(owner)
        .bind(a_type_name)
        .bind(b_type_name)
        .fetch_optional(&pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record(r).map(|o| Some(o)),
            None => Ok(None),
        }
    }

    async fn fetch_owned_union_objects(
        &self,
        a_type_name: &'static str,
        b_type_name: &'static str,
        owner: Uuid,
    ) -> Result<Vec<ObjectRecord>, Error> {
        let pool = self.pool.clone();
        let rows = sqlx::query(
            r#"
            SELECT id, type, owner, created_at, updated_at, data, index_meta
            FROM objects
            WHERE owner = ? AND (type = ? OR type = ?)
            "#,
        )
        .bind(owner)
        .bind(a_type_name)
        .bind(b_type_name)
        .fetch_all(&pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        rows.into_iter()
            .map(Self::map_row_to_object_record)
            .collect()
    }

    /* ---------------- EDGES ---------------- */
    async fn insert_edge(&self, record: EdgeRecord) -> Result<(), Error> {
        let pool = self.pool.clone();

        let data =
            serde_json::to_string(&record.data).map_err(|e| Error::Serialize(e.to_string()))?;
        let index_meta = serde_json::to_string(&record.index_meta)
            .map_err(|e| Error::Serialize(e.to_string()))?;

        let _ = sqlx::query(
            r#"
            INSERT INTO edges ("from", "to", type, data, index_meta)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT ("from", type, "to")
            DO UPDATE SET data = ?, index_meta = ?;
            "#,
        )
        .bind(record.from)
        .bind(record.to)
        .bind(record.type_name)
        .bind(&data)
        .bind(&index_meta)
        .bind(&data)
        .bind(&index_meta)
        .execute(&pool)
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
        let pool = self.pool.clone();
        let _ = sqlx::query(
            r#"
        UPDATE edges SET data = ?, "to" = ?
        WHERE "from" = ? AND type = ? AND "to" = ?
        "#,
        )
        .bind(record.data)
        .bind(to.unwrap_or(old_to))
        .bind(record.from)
        .bind(record.type_name)
        .bind(old_to)
        .execute(&pool)
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
        let pool = self.pool.clone();
        let _ = sqlx::query(
            r#"
            DELETE FROM edges
            WHERE type = ? AND "from" = ? AND "to" = ?
            "#,
        )
        .bind(type_name)
        .bind(from)
        .bind(to)
        .execute(&pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        Ok(())
    }

    async fn delete_object_edge(&self, type_name: &'static str, from: Uuid) -> Result<(), Error> {
        let pool = self.pool.clone();
        let _ = sqlx::query(
            r#"
            DELETE FROM edges
            WHERE type = $1 AND "from" = $2
            "#,
        )
        .bind(type_name)
        .bind(from.to_string())
        .execute(&pool)
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
        let where_clause = Self::build_edge_query_conditions(&plan.filters, plan.cursor);
        let order_clause = Self::build_edge_order_clause(&plan.filters);

        let mut sql = format!(
            r#"
            SELECT "from", "to", "type", data, index_meta
            FROM edges
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

        let pool = self.pool.clone();
        let rows = query
            .fetch_all(&pool)
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;

        rows.into_iter().map(Self::map_row_to_edge_record).collect()
    }

    async fn query_reverse_edges(
        &self,
        type_name: &'static str,
        owner_reverse: Uuid,
        plan: EdgeQuery,
    ) -> Result<Vec<EdgeRecord>, Error> {
        let where_clause = Self::build_edge_reverse_query_conditions(&plan.filters, plan.cursor);
        let order_clause = Self::build_edge_order_clause(&plan.filters);

        let mut sql = format!(
            r#"
            SELECT "from", "to", "type", data, index_meta
            FROM edges
            {}
            {}
            "#,
            where_clause, order_clause
        );

        if let Some(limit) = plan.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        let mut query = sqlx::query(&sql).bind(type_name).bind(owner_reverse);

        if let Some(cursor) = plan.cursor {
            query = query.bind(cursor.last_id);
        }
        query = Self::query_bind_filters(query, &plan.filters);

        let pool = self.pool.clone();
        let rows = query
            .fetch_all(&pool)
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;

        rows.into_iter().map(Self::map_row_to_edge_record).collect()
    }

    async fn count_edges(
        &self,
        type_name: &'static str,
        owner: Uuid,
        plan: Option<EdgeQuery>,
    ) -> Result<u64, Error> {
        let pool = self.pool.clone();

        match plan {
            Some(plan) => {
                let where_clause = Self::build_edge_query_conditions(&plan.filters, None);

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
                    .fetch_one(&pool)
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
                .fetch_one(&pool)
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
        let pool = self.pool.clone();

        match plan {
            Some(plan) => {
                let where_clause = Self::build_edge_reverse_query_conditions(&plan.filters, None);

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
                    .fetch_one(&pool)
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;

                Ok(count as u64)
            }
            None => {
                let count: i64 =
                    sqlx::query_scalar(r#"SELECT COUNT(*) FROM edges WHERE type = ? AND "to" = ?"#)
                        .bind(type_name)
                        .bind(to)
                        .fetch_one(&pool)
                        .await
                        .map_err(|err| Error::Storage(err.to_string()))?;

                Ok(count as u64)
            }
        }
    }

    async fn sequence_value(&self, sq: String) -> u64 {
        self.ensure_sequence_exists(sq.clone()).await;

        let current_val: i64 = sqlx::query_scalar("SELECT value FROM sequences WHERE name = ?")
            .bind(sq)
            .fetch_one(&self.pool.clone())
            .await
            .expect("Failed to fetch the current sequence value");

        current_val as u64
    }

    async fn sequence_next_value(&self, sq: String) -> u64 {
        self.ensure_sequence_exists(sq.clone()).await;

        // SQLite doesn't support RETURNING in UPDATE until version 3.35.0
        // So we'll do it in a transaction for atomicity
        let mut tx = self
            .pool
            .begin()
            .await
            .expect("Failed to begin transaction");

        sqlx::query("UPDATE sequences SET value = value + 1 WHERE name = ?")
            .bind(&sq)
            .execute(&mut *tx)
            .await
            .expect("Failed to increment sequence");

        let next_val: i64 = sqlx::query_scalar("SELECT value FROM sequences WHERE name = ?")
            .bind(sq)
            .fetch_one(&mut *tx)
            .await
            .expect("Failed to fetch the next sequence value");

        tx.commit().await.expect("Failed to commit transaction");

        next_val as u64
    }
}

#[async_trait::async_trait]
impl UniqueAdapter for SqliteAdapter {
    async fn insert_unique(
        &self,
        object_id: Uuid,
        type_name: &str,
        hash: &str,
        field: &str,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            INSERT INTO unique_constraints (id, type, key, field)
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(object_id)
        .bind(type_name)
        .bind(hash)
        .bind(field)
        .execute(&self.pool)
        .await
        .map_err(|err| {
            // Check if it's a uniqueness violation
            if err.to_string().contains("unique") {
                Error::UniqueConstraintViolation(field.to_string())
            } else {
                Error::Storage(err.to_string())
            }
        })?;

        Ok(())
    }

    async fn insert_unique_hashes(
        &self,
        object_id: Uuid,
        type_name: &str,
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
                VALUES ($1, $2, $3, $4)
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
        let pool = self.pool.clone();
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
            .execute(&pool)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn delete_all_for_object(&self, object_id: Uuid) -> Result<(), Error> {
        sqlx::query(
            r#"
            DELETE FROM unique_constraints WHERE id = $1
            "#,
        )
        .bind(object_id)
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
