use chrono::Utc;
use sqlx::{
    Execute, PgPool, Postgres, Row,
    postgres::{PgArguments, PgRow},
    query::{Query as PgQuery, QueryScalar},
};
use uuid::Uuid;

use crate::{
    adapters::{Adapter, EdgeQuery, EdgeRecord, Error, ObjectRecord, Query, UniquenessAdapter},
    query::{Cursor, IndexValue, IndexValueInner, QueryFilter},
};

/// PostgreSQL adapter using a unified JSON storage model
///
/// Schema:
/// ```sql
/// CREATE TABLE public.objects (
///     id uuid PRIMARY KEY,
///     type TEXT NOT NULL,
///     owner uuid NOT NULL,
///     created_at TIMESTAMPTZ NOT NULL,
///     updated_at TIMESTAMPTZ NOT NULL,
///     data JSONB NOT NULL,
///     index_meta JSONB NOT NULL,
///     CONSTRAINT fk_owner FOREIGN KEY (owner) REFERENCES objects(id) ON DELETE CASCADE
/// );
///
/// CREATE INDEX idx_objects_type_owner ON objects(type, owner);
/// CREATE INDEX idx_objects_owner ON objects(owner);
/// CREATE INDEX idx_objects_created_at ON objects(created_at);
/// CREATE INDEX idx_objects_updated_at ON objects(updated_at);
///
/// -- GIN index for flexible JSONB querying
/// CREATE INDEX idx_objects_index_meta ON public.objects USING GIN (index_meta);
/// ```
pub struct PostgresAdapter {
    pub(crate) pool: PgPool,
}

impl PostgresAdapter {
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
                id uuid PRIMARY KEY,
                type TEXT NOT NULL,
                owner uuid NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
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
            CREATE INDEX IF NOT EXISTS idx_objects_type_owner ON objects(type, owner);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_owner ON objects(owner);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_created_at ON objects(created_at);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_updated_at ON objects(updated_at);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_index_meta ON public.objects USING GIN (index_meta);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS public.edges (
                "from" uuid NOT NULL,
                "to" uuid NOT NULL,
                type TEXT NOT NULL,
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
            CREATE UNIQUE INDEX IF NOT EXISTS idx_edges_key ON public.edges("from", "to", type);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_edges_from_key ON public.edges("from", type);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_edges_to_key ON public.edges("to", type);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_edges_index_meta ON public.edges USING GIN (index_meta);
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

        tx.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        #[cfg(feature = "ledger")]
        {
            self.init_ledger_schema().await.map_err(|me| match me {
                MoneyError::Storage(e) => Error::Storage(e),
                _ => Error::Storage(me.to_string()),
            })?;
        }
        Ok(())
    }

    async fn ensure_sequence_exists(&self, sq: String) {
        let _ = sqlx::query(
            "DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_sequence
                    WHERE sequencename = $1
                ) THEN
                    CREATE SEQUENCE $1;
                END IF;
            END $$;",
        )
        .bind(sq)
        .execute(&self.pool.clone())
        .await
        .unwrap();
    }
}

impl PostgresAdapter {
    fn map_row_to_object_record(row: PgRow) -> Result<ObjectRecord, Error> {
        let data_json: serde_json::Value = row
            .try_get("data")
            .map_err(|e| Error::Deserialize(e.to_string()))?;

        let index_meta_json: serde_json::Value = row
            .try_get("index_meta")
            .map_err(|e| Error::Deserialize(e.to_string()))?;

        // Reconstruct meta from separate columns

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

    fn map_row_to_edge_record(row: PgRow) -> Result<EdgeRecord, Error> {
        let data_json: serde_json::Value = row
            .try_get("data")
            .map_err(|e| Error::Deserialize(e.to_string()))?;

        let index_meta_json: serde_json::Value = row
            .try_get("index_meta")
            .map_err(|e| Error::Deserialize(e.to_string()))?;

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
            ("type = $1".to_string(), "AND"),
            ("owner = $2".to_string(), "AND"),
        ];
        let mut param_idx = 3;

        if let Some(_) = cursor {
            conditions.push(("id < $3".to_string(), "AND"));
            param_idx += 1;
        }

        for filter in filters {
            let index_type = match &filter.value {
                IndexValue::String(_) => "text",
                IndexValue::Int(_) => "bigint",
                IndexValue::Float(_) => "double precision",
                IndexValue::Bool(_) => "boolean",
                IndexValue::Timestamp(_) => "timestamptz",
                IndexValue::Uuid(_) => "uuid",
                IndexValue::Array(arr) => {
                    // Determine array element type from first element
                    if let Some(first) = arr.first() {
                        match first {
                            IndexValueInner::String(_) => "text[]",
                            IndexValueInner::Int(_) => "bigint[]",
                            IndexValueInner::Float(_) => "double precision[]",
                        }
                    } else {
                        "text[]" // default for empty arrays
                    }
                }
            };

            match &filter.mode {
                crate::query::QueryMode::Search(query_search) => {
                    let comparison = match query_search.comparison {
                        crate::query::Comparison::Equal => "=",
                        crate::query::Comparison::BeginsWith => "ILIKE",
                        crate::query::Comparison::Contains => {
                            if matches!(filter.value, IndexValue::Array(_)) {
                                "?|"
                            } else {
                                "ILIKE"
                            }
                        }
                        crate::query::Comparison::ContainsAll => {
                            if matches!(filter.value, IndexValue::Array(_)) {
                                "?&"
                            } else {
                                "ILIKE"
                            }
                        }
                        crate::query::Comparison::GreaterThan => ">",
                        crate::query::Comparison::LessThan => "<",
                        crate::query::Comparison::GreaterThanOrEqual => ">=",
                        crate::query::Comparison::LessThanOrEqual => "<=",
                        crate::query::Comparison::NotEqual => "<>",
                    };
                    let condition = if matches!(filter.value, IndexValue::Array(_)) {
                        // For JSONB arrays, use JSONB operators
                        format!(
                            "index_meta->'{}' {} ${}",
                            filter.field.name, comparison, param_idx
                        )
                    } else {
                        format!(
                            "(index_meta->>'{}')::{} {} ${}",
                            filter.field.name, index_type, comparison, param_idx
                        )
                    };

                    let operator = match query_search.operator {
                        crate::query::Operator::And => "AND",
                        _ => "OR",
                    };
                    conditions.push((condition, operator));
                    param_idx += 1;
                }
                crate::query::QueryMode::Sort(_) => continue,
            }
        }

        let mut query = String::new();
        for (i, (cond, joiner)) in conditions.iter().enumerate() {
            query.push_str(cond);
            // Only add the joiner if not the last element and joiner isn't empty
            if i < conditions.len() - 1 && !joiner.is_empty() {
                query.push(' ');
                query.push_str(joiner);
                query.push(' ');
            }
        }

        let where_clause = format!("WHERE {}", query);

        where_clause
    }

    fn build_edge_query_conditions(filters: &Vec<QueryFilter>, cursor: Option<Cursor>) -> String {
        let mut conditions = vec![
            ("type = $1".to_string(), "AND"),
            (r#""from" = $2"#.to_string(), "AND"),
        ];
        let mut param_idx = 3;

        if cursor.is_some() {
            conditions.push((r#""to" < $3"#.to_string(), "AND"));
            param_idx += 1;
        }

        for filter in filters {
            let index_type = match &filter.value {
                IndexValue::String(_) => "text",
                IndexValue::Int(_) => "bigint",
                IndexValue::Float(_) => "double precision",
                IndexValue::Bool(_) => "boolean",
                IndexValue::Timestamp(_) => "timestamptz",
                IndexValue::Uuid(_) => "uuid",
                IndexValue::Array(arr) => {
                    // Determine array element type from first element
                    if let Some(first) = arr.first() {
                        match first {
                            IndexValueInner::String(_) => "text[]",
                            IndexValueInner::Int(_) => "bigint[]",
                            IndexValueInner::Float(_) => "double precision[]",
                        }
                    } else {
                        "text[]" // default for empty arrays
                    }
                }
            };

            match &filter.mode {
                crate::query::QueryMode::Search(query_search) => {
                    let comparison = match query_search.comparison {
                        crate::query::Comparison::Equal => "=",
                        crate::query::Comparison::BeginsWith => "ILIKE",
                        crate::query::Comparison::Contains => {
                            if matches!(filter.value, IndexValue::Array(_)) {
                                "?|"
                            } else {
                                "ILIKE"
                            }
                        }
                        crate::query::Comparison::ContainsAll => {
                            if matches!(filter.value, IndexValue::Array(_)) {
                                "?&"
                            } else {
                                "ILIKE"
                            }
                        }
                        crate::query::Comparison::GreaterThan => ">",
                        crate::query::Comparison::LessThan => "<",
                        crate::query::Comparison::GreaterThanOrEqual => ">=",
                        crate::query::Comparison::LessThanOrEqual => "<=",
                        crate::query::Comparison::NotEqual => "<>",
                    };
                    let condition = if matches!(filter.value, IndexValue::Array(_)) {
                        // For JSONB arrays, use JSONB operators
                        format!(
                            "index_meta->'{}' {} ${}",
                            filter.field.name, comparison, param_idx
                        )
                    } else {
                        format!(
                            "(index_meta->>'{}')::{} {} ${}",
                            filter.field.name, index_type, comparison, param_idx
                        )
                    };

                    let operator = match query_search.operator {
                        crate::query::Operator::And => "AND",
                        _ => "OR",
                    };
                    conditions.push((condition, operator));
                    param_idx += 1;
                }
                crate::query::QueryMode::Sort(_) => continue,
            }
        }

        let mut query = String::new();
        for (i, (cond, joiner)) in conditions.iter().enumerate() {
            query.push_str(cond);
            // Only add the joiner if not the last element and joiner isn't empty
            if i < conditions.len() - 1 && !joiner.is_empty() {
                query.push(' ');
                query.push_str(joiner);
                query.push(' ');
            }
        }

        let where_clause = format!("WHERE {}", query);

        where_clause
    }

    fn build_edge_reverse_query_conditions(
        filters: &Vec<QueryFilter>,
        cursor: Option<Cursor>,
    ) -> String {
        let mut conditions = vec![
            ("type = $1".to_string(), "AND"),
            (r#""to" = $2"#.to_string(), "AND"),
        ];
        let mut param_idx = 3;

        if let Some(_) = cursor {
            conditions.push((r#""from" < $3"#.to_string(), "AND"));
            param_idx += 1;
        }

        for filter in filters {
            let index_type = match &filter.value {
                IndexValue::String(_) => "text",
                IndexValue::Int(_) => "bigint",
                IndexValue::Float(_) => "double precision",
                IndexValue::Bool(_) => "boolean",
                IndexValue::Timestamp(_) => "timestamptz",
                IndexValue::Uuid(_) => "uuid",
                IndexValue::Array(arr) => {
                    // Determine array element type from first element
                    if let Some(first) = arr.first() {
                        match first {
                            IndexValueInner::String(_) => "text[]",
                            IndexValueInner::Int(_) => "bigint[]",
                            IndexValueInner::Float(_) => "double precision[]",
                        }
                    } else {
                        "text[]" // default for empty arrays
                    }
                }
            };

            match &filter.mode {
                crate::query::QueryMode::Search(query_search) => {
                    let comparison = match query_search.comparison {
                        crate::query::Comparison::Equal => "=",
                        crate::query::Comparison::BeginsWith => "ILIKE",
                        crate::query::Comparison::Contains => {
                            if matches!(filter.value, IndexValue::Array(_)) {
                                "?|"
                            } else {
                                "ILIKE"
                            }
                        }
                        crate::query::Comparison::ContainsAll => {
                            if matches!(filter.value, IndexValue::Array(_)) {
                                "?&"
                            } else {
                                "ILIKE"
                            }
                        }
                        crate::query::Comparison::GreaterThan => ">",
                        crate::query::Comparison::LessThan => "<",
                        crate::query::Comparison::GreaterThanOrEqual => ">=",
                        crate::query::Comparison::LessThanOrEqual => "<=",
                        crate::query::Comparison::NotEqual => "<>",
                    };
                    let condition = if matches!(filter.value, IndexValue::Array(_)) {
                        // For JSONB arrays, use JSONB operators
                        format!(
                            "index_meta->'{}' {} ${}",
                            filter.field.name, comparison, param_idx
                        )
                    } else {
                        format!(
                            "(index_meta->>'{}')::{} {} ${}",
                            filter.field.name, index_type, comparison, param_idx
                        )
                    };

                    let operator = match query_search.operator {
                        crate::query::Operator::And => "AND",
                        _ => "OR",
                    };
                    conditions.push((condition, operator));
                    param_idx += 1;
                }
                crate::query::QueryMode::Sort(_) => continue,
            }
        }

        let mut query = String::new();
        for (i, (cond, joiner)) in conditions.iter().enumerate() {
            query.push_str(cond);
            // Only add the joiner if not the last element and joiner isn't empty
            if i < conditions.len() - 1 && !joiner.is_empty() {
                query.push(' ');
                query.push_str(joiner);
                query.push(' ');
            }
        }

        let where_clause = format!("WHERE {}", query);

        where_clause
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
            .filter(|s| s.value.as_array().is_none())
            .map(|s| {
                let direction = if s.mode.as_sort().unwrap().ascending {
                    "ASC"
                } else {
                    "DESC"
                };

                let index_type = match &s.value {
                    IndexValue::String(_) => "text",
                    IndexValue::Int(_) => "bigint",
                    IndexValue::Float(_) => "double precision",
                    IndexValue::Bool(_) => "boolean",
                    IndexValue::Timestamp(_) => "timestamptz",
                    _ => "text",
                };
                format!(
                    "(index_meta->>'{}')::{} {}",
                    s.field.name, index_type, direction
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
            .filter(|s| s.value.as_array().is_none())
            .map(|s| {
                let direction = if s.mode.as_sort().unwrap().ascending {
                    "ASC"
                } else {
                    "DESC"
                };

                let index_type = match &s.value {
                    IndexValue::String(_) => "text",
                    IndexValue::Int(_) => "bigint",
                    IndexValue::Float(_) => "double precision",
                    IndexValue::Bool(_) => "boolean",
                    IndexValue::Uuid(_) => "uuid",
                    IndexValue::Timestamp(_) => "timestamptz",
                    _ => "text",
                };
                format!(
                    "(index_meta->>'{}')::{} {}",
                    s.field.name, index_type, direction
                )
            })
            .collect();

        format!("ORDER BY {}", order_terms.join(", "))
    }

    fn query_bind_filters<'a>(
        mut query: PgQuery<'a, Postgres, PgArguments>,
        filters: &'a Vec<QueryFilter>,
    ) -> PgQuery<'a, Postgres, PgArguments> {
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
                IndexValue::Timestamp(t) => query.bind(t),
                IndexValue::Uuid(uid) => query.bind(uid),
                IndexValue::Array(arr) => {
                    // Determine array element type from first element
                    if let Some(first) = arr.first() {
                        match first {
                            IndexValueInner::String(_) => query.bind(
                                arr.iter()
                                    .map(|s| s.as_string().unwrap_or_default().to_string())
                                    .collect::<Vec<String>>(),
                            ),
                            IndexValueInner::Int(_) => query.bind(
                                arr.iter()
                                    .map(|s| s.as_int().unwrap_or_default().to_string())
                                    .collect::<Vec<String>>(),
                            ),
                            IndexValueInner::Float(_) => query.bind(
                                arr.iter()
                                    .map(|s| s.as_float().unwrap_or_default().to_string())
                                    .collect::<Vec<String>>(),
                            ),
                        }
                    } else {
                        query.bind(vec![] as Vec<String>)
                    }
                }
            };
        }
        query
    }

    fn query_scalar_bind_filters<'a, O>(
        mut query: QueryScalar<'a, Postgres, O, PgArguments>,
        filters: &'a Vec<QueryFilter>,
    ) -> QueryScalar<'a, Postgres, O, PgArguments> {
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
                IndexValue::Timestamp(t) => query.bind(t),
                IndexValue::Uuid(uid) => query.bind(uid),
                IndexValue::Array(arr) => {
                    // Determine array element type from first element
                    if let Some(first) = arr.first() {
                        match first {
                            IndexValueInner::String(_) => query.bind(
                                arr.iter()
                                    .map(|s| s.as_string().unwrap_or_default().to_string())
                                    .collect::<Vec<String>>(),
                            ),
                            IndexValueInner::Int(_) => query.bind(
                                arr.iter()
                                    .map(|s| s.as_int().unwrap_or_default().to_string())
                                    .collect::<Vec<String>>(),
                            ),
                            IndexValueInner::Float(_) => query.bind(
                                arr.iter()
                                    .map(|s| s.as_float().unwrap_or_default().to_string())
                                    .collect::<Vec<String>>(),
                            ),
                        }
                    } else {
                        query.bind(vec![] as Vec<String>)
                    }
                }
            };
        }
        query
    }
}

#[async_trait::async_trait]
impl Adapter for PostgresAdapter {
    async fn insert_object(&self, record: ObjectRecord) -> Result<(), Error> {
        let pool = self.pool.clone();
        let _ = sqlx::query(
            r#"
            INSERT INTO public.objects (id, type, owner, created_at, updated_at, data, index_meta)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(record.id)
        .bind(record.type_name)
        .bind(record.owner)
        .bind(record.created_at)
        .bind(record.updated_at)
        .bind(record.data)
        .bind(record.index_meta)
        .fetch_optional(&pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;
        Ok(())
    }

    async fn fetch_object(&self, id: Uuid) -> Result<Option<ObjectRecord>, Error> {
        let pool = self.pool.clone();
        let row = sqlx::query(
            r#"
            SELECT id, type, owner, created_at, updated_at, data, index_meta
            FROM objects
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record(r).map(|o| Some(o)),
            None => Ok(None),
        }
    }

    async fn fetch_bulk_objects(&self, ids: Vec<Uuid>) -> Result<Vec<ObjectRecord>, Error> {
        let pool = self.pool.clone();
        let rows = sqlx::query(
            r#"
            SELECT id, type, owner, created_at, updated_at, data, index_meta
            FROM objects
            WHERE id = ANY($1)
            "#,
        )
        .bind(ids.into_iter().map(|id| id).collect::<Vec<Uuid>>())
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
            SET updated_at = $2, data = $3, index_meta = $4
            WHERE id = $1
            "#,
        )
        .bind(record.id)
        .bind(record.updated_at)
        .bind(record.data)
        .bind(record.index_meta)
        .execute(&pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        Ok(())
    }

    async fn transfer_object(
        &self,
        id: Uuid,
        from_owner: Uuid,
        to_owner: Uuid,
    ) -> Result<ObjectRecord, Error> {
        let pool = self.pool.clone();
        let row = sqlx::query(
            r#"
            UPDATE objects
            SET updated_at = $3, owner = $4
            WHERE id = $1 AND owner = $2
            RETURNING *
            "#,
        )
        .bind(id)
        .bind(from_owner)
        .bind(Utc::now())
        .bind(to_owner)
        .fetch_one(&pool)
        .await
        .map_err(|err| match err {
            sqlx::Error::RowNotFound => Error::NotFound,
            _ => Error::Storage(err.to_string()),
        })?;

        Self::map_row_to_object_record(row)
    }

    async fn delete_object(&self, id: Uuid, owner: Uuid) -> Result<Option<ObjectRecord>, Error> {
        let pool = self.pool.clone();
        let row = sqlx::query(
            r#"
            DELETE FROM objects
            WHERE id = $1 AND owner = $2
            RETURNING *
            "#,
        )
        .bind(id)
        .bind(owner)
        .fetch_optional(&pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record(r).map(|o| Some(o)),
            None => Ok(None),
        }
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

        Ok(row
            .map(|row| Self::map_row_to_object_record(row).ok())
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

        Ok(rows
            .into_iter()
            .filter_map(|row| Self::map_row_to_object_record(row).ok())
            .collect())
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
                let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM objects WHERE type = $1")
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
            WHERE owner = $1 AND type = $2
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
            WHERE owner = $1 AND type = $2
            "#,
        )
        .bind(owner)
        .bind(type_name)
        .fetch_optional(&pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record(r).map(|o| Some(o)),
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
            WHERE id = $1 AND (type = $2 OR type = $3)
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
        let rows = sqlx::query(
            r#"
            SELECT id, type, owner, created_at, updated_at, data, index_meta
            FROM objects
            WHERE id = ANY($1) AND (type = $2 OR type = $3)
            "#,
        )
        .bind(ids.into_iter().map(|id| id).collect::<Vec<Uuid>>())
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
            WHERE owner = $1 AND (type = $2 OR type = $3)
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
            WHERE owner = $1 AND (type = $2 OR type = $3)
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
        let _ = sqlx::query(
            r#"
            INSERT INTO edges ("from", "to", type, data, index_meta)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT ("from", type, "to")
            DO UPDATE SET data = $4, index_meta = $5;
            "#,
        )
        .bind(record.from)
        .bind(record.to)
        .bind(record.type_name)
        .bind(record.data)
        .bind(record.index_meta)
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
        UPDATE edges SET data = $1, "to" = $2
        WHERE "from" = $3 AND type = $4 AND "to" = $6
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
            WHERE type = $1 AND "from" = $2 AND "to" = $3
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
        .bind(from)
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

        Ok(rows
            .into_iter()
            .filter_map(|row| Self::map_row_to_edge_record(row).ok())
            .collect())
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

        Ok(rows
            .into_iter()
            .filter_map(|row| Self::map_row_to_edge_record(row).ok())
            .collect())
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

                let pool = self.pool.clone();
                let count = query
                    .fetch_one(&pool)
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

                let pool = self.pool.clone();
                let count = query
                    .fetch_one(&pool)
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
                .fetch_one(&pool)
                .await
                .map_err(|err| Error::Storage(err.to_string()))?;

                Ok(count as u64)
            }
        }
    }

    #[cfg(feature = "sequence")]
    async fn sequence_value(&self, sq: String) -> u64 {
        self.ensure_sequence_exists(sq.clone()).await;

        let next_val: i64 = sqlx::query_scalar("SELECT currval($1);")
            .bind(sq)
            .fetch_one(&self.pool.clone())
            .await
            .expect("Failed to fetch the next sequence value");

        next_val as u64
    }

    #[cfg(feature = "sequence")]
    async fn sequence_next_value(&self, sq: String) -> u64 {
        self.ensure_sequence_exists(sq.clone()).await;

        let next_val: i64 = sqlx::query_scalar("SELECT nextval($1);")
            .bind(sq)
            .fetch_one(&self.pool.clone())
            .await
            .expect("Failed to fetch the next sequence value");

        next_val as u64
    }
}

#[async_trait::async_trait]
impl UniquenessAdapter for PostgresAdapter {
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

#[cfg(feature = "ledger")]
use ledger::{
    Asset, Balance, LedgerAdapter, MoneyError, Transaction, ValueObject, ValueObjectState,
};
#[cfg(feature = "ledger")]
const MAX_FRAGMENTS: i64 = 1000;

#[cfg(feature = "ledger")]
impl PostgresAdapter {
    /// Initialize the ledger schema
    async fn init_ledger_schema(&self) -> Result<(), MoneyError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| MoneyError::Storage(e.to_string()))?;

        // Create assets table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS assets (
                id TEXT PRIMARY KEY,
                code TEXT UNIQUE NOT NULL,
                unit BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        // Create value_objects table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS value_objects (
                id TEXT PRIMARY KEY,
                asset TEXT NOT NULL,
                owner TEXT NOT NULL,
                amount BIGINT NOT NULL CHECK (amount > 0),
                state TEXT NOT NULL CHECK (state IN ('alive', 'reserved', 'burned')),
                reserved_for TEXT,
                created_at TIMESTAMPTZ NOT NULL,
                burned_at TIMESTAMPTZ,
                CONSTRAINT fk_asset FOREIGN KEY (asset) REFERENCES assets(id)
            )
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        // Create indexes for value_objects
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_vo_owner_state
            ON value_objects(owner, state) WHERE state = 'alive'
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_vo_asset_owner
            ON value_objects(asset, owner, state)
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_vo_reserved
            ON value_objects(reserved_for, state)
            WHERE reserved_for IS NOT NULL AND state = 'reserved'
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        // Create transactions table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS transactions (
                id TEXT PRIMARY KEY,
                asset TEXT NOT NULL,
                sender TEXT,
                receiver TEXT,
                burned_amount BIGINT NOT NULL DEFAULT 0,
                minted_amount BIGINT NOT NULL DEFAULT 0,
                metadata TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                reverted_at TIMESTAMPTZ,
                reverted_by TEXT,
                CONSTRAINT fk_asset FOREIGN KEY (asset) REFERENCES assets(id),
                CONSTRAINT fk_revert FOREIGN KEY (reverted_by) REFERENCES transactions(id)
            )
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        // Create indexes for transactions
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_tx_asset ON transactions(asset, created_at DESC)
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_tx_sender ON transactions(sender, created_at DESC) WHERE sender IS NOT NULL
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_tx_receiver ON transactions(receiver, created_at DESC) WHERE receiver IS NOT NULL
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        // Create idempotency_keys table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS idempotency_keys (
                key TEXT PRIMARY KEY,
                transaction_id TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT fk_transaction FOREIGN KEY (transaction_id) REFERENCES transactions(id)
            )
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_idem_created ON idempotency_keys(created_at DESC)
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| MoneyError::Storage(e.to_string()))?;

        Ok(())
    }

    fn state_to_string(state: ValueObjectState) -> &'static str {
        match state {
            ValueObjectState::Alive => "alive",
            ValueObjectState::Reserved => "reserved",
            ValueObjectState::Burned => "burned",
        }
    }

    fn string_to_state(s: &str) -> Result<ValueObjectState, MoneyError> {
        match s {
            "alive" => Ok(ValueObjectState::Alive),
            "reserved" => Ok(ValueObjectState::Reserved),
            "burned" => Ok(ValueObjectState::Burned),
            _ => Err(MoneyError::Storage(format!("Invalid state: {}", s))),
        }
    }

    async fn get_asset_by_id(&self, id: Uuid) -> Result<Asset, MoneyError> {
        let row = sqlx::query(
            r#"
            SELECT id, code, unit
            FROM assets
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?
        .ok_or_else(|| MoneyError::AssetNotFound(id.to_string()))?;

        self.row_to_asset(row)
    }

    fn row_to_value_object(&self, row: sqlx::postgres::PgRow) -> Result<ValueObject, MoneyError> {
        let id = row
            .try_get::<Uuid, _>("id")
            .map_err(|e| MoneyError::Storage(e.to_string()))?;
        let asset = Uuid::parse_str(&row.try_get::<String, _>("asset").unwrap())
            .map_err(|e| MoneyError::Storage(e.to_string()))?;
        let owner = row
            .try_get::<Uuid, _>("owner")
            .map_err(|e| MoneyError::Storage(e.to_string()))?;
        let amount: i64 = row.try_get("amount").unwrap();
        let state_str: String = row.try_get("state").unwrap();
        let state = Self::string_to_state(&state_str)?;
        let reserved_for = row
            .try_get::<Option<String>, _>("reserved_for")
            .unwrap()
            .map(|s| Uuid::parse_str(&s))
            .transpose()
            .map_err(|e| MoneyError::Storage(e.to_string()))?;
        let created_at = row.try_get("created_at").unwrap();

        Ok(ValueObject {
            id,
            asset,
            owner,
            amount,
            state,
            reserved_for,
            created_at,
        })
    }

    fn row_to_transaction(&self, row: sqlx::postgres::PgRow) -> Result<Transaction, MoneyError> {
        let id = row
            .try_get::<Uuid, _>("id")
            .map_err(|e| MoneyError::Storage(e.to_string()))?;
        let asset = Uuid::parse_str(&row.try_get::<String, _>("asset").unwrap())
            .map_err(|e| MoneyError::Storage(e.to_string()))?;
        let sender = row
            .try_get::<Option<Uuid>, _>("sender")
            .map_err(|e| MoneyError::Storage(e.to_string()))?;
        let receiver = row
            .try_get::<Option<Uuid>, _>("receiver")
            .map_err(|e| MoneyError::Storage(e.to_string()))?;
        let burned_amount: i64 = row.try_get("burned_amount").unwrap();
        let minted_amount: i64 = row.try_get("minted_amount").unwrap();
        let metadata: String = row.try_get("metadata").unwrap();
        let created_at = row.try_get("created_at").unwrap();

        Ok(Transaction {
            id,
            asset,
            sender,
            receiver,
            burned_amount,
            minted_amount,
            metadata,
            created_at,
        })
    }

    fn row_to_asset(&self, row: sqlx::postgres::PgRow) -> Result<Asset, MoneyError> {
        let id = row
            .try_get::<Uuid, _>("id")
            .map_err(|e| MoneyError::Storage(e.to_string()))?;
        let code: String = row.try_get("code").unwrap();
        let unit: i64 = row.try_get("unit").unwrap();

        Ok(Asset { id, code, unit })
    }

    /// Check and handle idempotency key
    async fn check_idempotency(&self, key: &str) -> Result<Option<Uuid>, MoneyError> {
        let result = sqlx::query(
            r#"
            SELECT transaction_id FROM idempotency_keys WHERE key = $1
            "#,
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        match result {
            Some(row) => {
                let tx_id = Uuid::parse_str(&row.try_get::<String, _>("transaction_id").unwrap())
                    .map_err(|e| MoneyError::Storage(e.to_string()))?;
                Ok(Some(tx_id))
            }
            None => Ok(None),
        }
    }

    /// Store idempotency key
    async fn store_idempotency(&self, key: &str, tx_id: Uuid) -> Result<(), MoneyError> {
        sqlx::query(
            r#"
            INSERT INTO idempotency_keys (key, transaction_id)
            VALUES ($1, $2)
            ON CONFLICT (key) DO NOTHING
            "#,
        )
        .bind(key)
        .bind(tx_id)
        .execute(&self.pool)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        Ok(())
    }
}

// #[cfg(feature = "ledger")]
// #[async_trait::async_trait]
// impl LedgerAdapter for PostgresAdapter {
//     async fn mint_value_objects(
//         &self,
//         asset: Uuid,
//         owner: Uuid,
//         amount: i64,
//         metadata: String,
//     ) -> Result<Vec<ValueObject>, MoneyError> {
//         if amount <= 0 {
//             return Err(MoneyError::InvalidAmount);
//         }

//         // Get asset to determine unit size
//         let asset = self.get_asset_by_id(asset).await?;

//         let effective_chunk_size = if amount > asset.unit * MAX_FRAGMENTS {
//             // Calculate minimum chunk size to stay within MAX_FRAGMENTS
//             (amount + MAX_FRAGMENTS - 1) / MAX_FRAGMENTS // Ceiling division
//         } else {
//             asset.unit
//         };

//         // Fragment the amount
//         let mut value_objects = Vec::new();
//         let mut remaining = amount;

//         while remaining > 0 {
//             let chunk = remaining.min(effective_chunk_size);

//             // Determine if this is a reserved mint
//             let vo = if metadata.starts_with("reserve:") {
//                 // For reserved mints, the metadata should contain the authority
//                 // This is a simplified version - in production you'd parse this properly
//                 ValueObject::new_alive(asset.id, owner, chunk)
//             } else {
//                 ValueObject::new_alive(asset.id, owner, chunk)
//             };

//             // Insert ValueObject
//             sqlx::query(
//                 r#"
//                 INSERT INTO value_objects (id, asset, owner, amount, state, reserved_for, created_at)
//                 VALUES ($1, $2, $3, $4, $5, $6, $7)
//                 "#,
//             )
//             .bind(vo.id)
//             .bind(vo.asset.to_string())
//             .bind(vo.owner)
//             .bind(vo.amount)
//             .bind(Self::state_to_string(vo.state))
//             .bind(vo.reserved_for.map(|id| id))
//             .bind(vo.created_at)
//             .execute(&self.pool)
//             .await
//             .map_err(|e| MoneyError::Storage(e.to_string()))?;

//             value_objects.push(vo);
//             remaining -= chunk;
//         }

//         Ok(value_objects)
//     }

//     async fn burn_value_objects(&self, ids: Vec<Uuid>, metadata: String) -> Result<(), MoneyError> {
//         if ids.is_empty() {
//             return Ok(());
//         }

//         let ids: Vec<Uuid> = ids.into_iter().map(|id| id).collect();

//         let result = sqlx::query(
//             r#"
//             UPDATE value_objects
//             SET state = 'burned', burned_at = NOW()
//             WHERE id = ANY($1) AND state != 'burned'
//             "#,
//         )
//         .bind(ids)
//         .execute(&self.pool)
//         .await
//         .map_err(|e| MoneyError::Storage(e.to_string()))?;

//         if result.rows_affected() == 0 {
//             return Err(MoneyError::Storage(
//                 "No value objects were burned".to_string(),
//             ));
//         }

//         Ok(())
//     }

//     async fn select_for_burn(
//         &self,
//         asset: Uuid,
//         owner: Uuid,
//         amount: i64,
//     ) -> Result<Vec<ValueObject>, MoneyError> {
//         if amount <= 0 {
//             return Err(MoneyError::InvalidAmount);
//         }

//         let rows = sqlx::query(
//             r#"
//             SELECT id, asset, owner, amount, state, reserved_for, created_at
//             FROM value_objects
//             WHERE asset = $1
//               AND owner = $2
//               AND state = 'alive'
//             ORDER BY created_at ASC
//             FOR UPDATE SKIP LOCKED
//             "#,
//         )
//         .bind(asset.to_string())
//         .bind(owner)
//         .fetch_all(&self.pool)
//         .await
//         .map_err(|e| MoneyError::Storage(e.to_string()))?;

//         let mut selected = Vec::new();
//         let mut total = 0i64;

//         for row in rows {
//             let vo = self.row_to_value_object(row)?;
//             total += vo.amount;
//             selected.push(vo);

//             if total >= amount {
//                 break;
//             }
//         }

//         if total < amount {
//             return Err(MoneyError::InsufficientFunds);
//         }

//         Ok(selected)
//     }

//     async fn select_reserved(
//         &self,
//         asset: Uuid,
//         owner: Uuid,
//         authority: Uuid,
//         amount: i64,
//     ) -> Result<Vec<ValueObject>, MoneyError> {
//         if amount <= 0 {
//             return Err(MoneyError::InvalidAmount);
//         }

//         let rows = sqlx::query(
//             r#"
//             SELECT id, asset, owner, amount, state, reserved_for, created_at
//             FROM value_objects
//             WHERE asset = $1
//               AND owner = $2
//               AND state = 'reserved'
//               AND reserved_for = $3
//             ORDER BY created_at ASC
//             FOR UPDATE SKIP LOCKED
//             "#,
//         )
//         .bind(asset.to_string())
//         .bind(owner)
//         .bind(authority.to_string())
//         .fetch_all(&self.pool)
//         .await
//         .map_err(|e| MoneyError::Storage(e.to_string()))?;

//         let mut selected = Vec::new();
//         let mut total = 0i64;

//         for row in rows {
//             let vo = self.row_to_value_object(row)?;
//             total += vo.amount;
//             selected.push(vo);

//             if total >= amount {
//                 break;
//             }
//         }

//         if total < amount {
//             return Err(MoneyError::ReservationNotFound);
//         }

//         Ok(selected)
//     }

//     async fn change_state(
//         &self,
//         ids: Vec<Uuid>,
//         new_state: ValueObjectState,
//     ) -> Result<(), MoneyError> {
//         if ids.is_empty() {
//             return Ok(());
//         }

//         let ids: Vec<Uuid> = ids.into_iter().map(|id| id).collect();

//         // Validate state transitions
//         let current_states = sqlx::query(
//             r#"
//             SELECT id, state FROM value_objects WHERE id = ANY($1)
//             "#,
//         )
//         .bind(&ids)
//         .fetch_all(&self.pool)
//         .await
//         .map_err(|e| MoneyError::Storage(e.to_string()))?;

//         for row in current_states {
//             let state_str: String = row.try_get("state").unwrap();
//             let current = Self::string_to_state(&state_str)?;

//             if !current.can_transition_to(new_state) {
//                 return Err(MoneyError::InvalidAuthority);
//             }
//         }

//         sqlx::query(
//             r#"
//             UPDATE value_objects
//             SET state = $1
//             WHERE id = ANY($2)
//             "#,
//         )
//         .bind(Self::state_to_string(new_state))
//         .bind(ids)
//         .execute(&self.pool)
//         .await
//         .map_err(|e| MoneyError::Storage(e.to_string()))?;

//         Ok(())
//     }

//     async fn get_balance(&self, asset: Uuid, owner: Uuid) -> Result<Balance, MoneyError> {
//         let row = sqlx::query(
//             r#"
//             SELECT
//                 COALESCE(SUM(CASE WHEN state = 'alive' THEN amount ELSE 0 END), 0) as available,
//                 COALESCE(SUM(CASE WHEN state = 'reserved' THEN amount ELSE 0 END), 0) as reserved
//             FROM value_objects
//             WHERE asset = $1 AND owner = $2
//             "#,
//         )
//         .bind(asset.to_string())
//         .bind(owner)
//         .fetch_optional(&self.pool)
//         .await
//         .map_err(|e| MoneyError::Storage(e.to_string()))?;

//         let (available, reserved): (i64, i64) = row
//             .map(|row| {
//                 (
//                     row.try_get("available").unwrap_or(0),
//                     row.try_get("reserved").unwrap_or(0),
//                 )
//             })
//             .unwrap_or_default();

//         Ok(Balance::from_value_objects(
//             owner, asset, available, reserved,
//         ))
//     }

//     async fn record_transaction(&self, transaction: Transaction) -> Result<Uuid, MoneyError> {
//         sqlx::query(
//             r#"
//             INSERT INTO transactions (id, asset, sender, receiver, burned_amount, minted_amount, metadata, created_at)
//             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
//             "#,
//         )
//         .bind(transaction.id)
//         .bind(transaction.asset.to_string())
//         .bind(transaction.sender.map(|id| id))
//         .bind(transaction.receiver.map(|id| id))
//         .bind(transaction.burned_amount)
//         .bind(transaction.minted_amount)
//         .bind(&transaction.metadata)
//         .bind(transaction.created_at)
//         .execute(&self.pool)
//         .await
//         .map_err(|e| MoneyError::Storage(e.to_string()))?;

//         Ok(transaction.id)
//     }

//     async fn get_transaction(&self, tx_id: Uuid) -> Result<Transaction, MoneyError> {
//         let row = sqlx::query(
//             r#"
//             SELECT id, asset, sender, receiver, burned_amount, minted_amount, metadata, created_at
//             FROM transactions
//             WHERE id = $1
//             "#,
//         )
//         .bind(tx_id)
//         .fetch_optional(&self.pool)
//         .await
//         .map_err(|e| MoneyError::Storage(e.to_string()))?
//         .ok_or(MoneyError::TransactionNotFound)?;

//         Ok(self.row_to_transaction(row)?)
//     }

//     async fn get_asset(&self, code: &str) -> Result<Asset, MoneyError> {
//         let row = sqlx::query(
//             r#"
//             SELECT id, code, unit
//             FROM assets
//             WHERE code = $1
//             "#,
//         )
//         .bind(code)
//         .fetch_optional(&self.pool)
//         .await
//         .map_err(|e| MoneyError::Storage(e.to_string()))?
//         .ok_or_else(|| MoneyError::AssetNotFound(code.to_string()))?;

//         Ok(self.row_to_asset(row)?)
//     }

//     async fn create_asset(&self, asset: Asset) -> Result<(), MoneyError> {
//         sqlx::query(
//             r#"
//             INSERT INTO assets (id, code, unit, created_at)
//             VALUES ($1, $2, $3, NOW())
//             ON CONFLICT (code) DO NOTHING
//             "#,
//         )
//         .bind(asset.id)
//         .bind(&asset.code)
//         .bind(asset.unit)
//         .execute(&self.pool)
//         .await
//         .map_err(|e| MoneyError::Storage(e.to_string()))?;

//         Ok(())
//     }

//     async fn begin_transaction(&self) -> Result<(), MoneyError> {
//         // For now, we'll rely on connection-level transactions
//         // In a more advanced implementation, you'd manage explicit transactions
//         Ok(())
//     }

//     async fn commit_transaction(&self) -> Result<(), MoneyError> {
//         Ok(())
//     }

//     async fn rollback_transaction(&self) -> Result<(), MoneyError> {
//         Ok(())
//     }
// }

// #[cfg(feature = "ledger")]
// impl PostgresAdapter {
//     /// Mint with idempotency key support
//     pub async fn mint_idempotent(
//         &self,
//         idempotency_key: &str,
//         asset: Uuid,
//         owner: Uuid,
//         amount: i64,
//         metadata: String,
//     ) -> Result<Uuid, MoneyError> {
//         // Check if this key was already processed
//         if let Some(existing_tx_id) = self.check_idempotency(idempotency_key).await? {
//             return Ok(existing_tx_id);
//         }

//         // Perform the mint
//         let _vos = self
//             .mint_value_objects(asset, owner, amount, metadata.clone())
//             .await?;

//         // Record transaction
//         let transaction = Transaction::new(asset, None, Some(owner), 0, amount, metadata);
//         let tx_id = self.record_transaction(transaction).await?;

//         // Store idempotency key
//         self.store_idempotency(idempotency_key, tx_id).await?;

//         Ok(tx_id)
//     }

//     /// Revert a transaction
//     pub async fn revert_transaction(
//         &self,
//         tx_id: Uuid,
//         reason: String,
//     ) -> Result<Uuid, MoneyError> {
//         // Get original transaction
//         let original = self.get_transaction(tx_id).await?;

//         // Create compensating transaction (reverse the flow)
//         let compensating = Transaction::new(
//             original.asset,
//             original.receiver,      // Reverse: receiver becomes sender
//             original.sender,        // Reverse: sender becomes receiver
//             original.minted_amount, // Burn what was minted
//             original.burned_amount, // Mint what was burned
//             format!("revert:{}:{}", tx_id, reason),
//         );

//         // If there was a receiver, burn from them
//         if let Some(receiver) = original.receiver {
//             let to_burn = self
//                 .select_for_burn(original.asset, receiver, original.minted_amount)
//                 .await?;
//             let burn_ids: Vec<Uuid> = to_burn.iter().map(|vo| vo.id).collect();
//             self.burn_value_objects(burn_ids, format!("revert:{}", tx_id))
//                 .await?;
//         }

//         // If there was a sender, mint back to them
//         if let Some(sender) = original.sender {
//             self.mint_value_objects(
//                 original.asset,
//                 sender,
//                 original.burned_amount,
//                 format!("revert:{}", tx_id),
//             )
//             .await?;
//         }

//         // Record the compensating transaction
//         let revert_tx_id = self.record_transaction(compensating).await?;

//         // Mark original as reverted
//         sqlx::query(
//             r#"
//             UPDATE transactions
//             SET reverted_at = NOW(), reverted_by = $2
//             WHERE id = $1
//             "#,
//         )
//         .bind(tx_id)
//         .bind(revert_tx_id)
//         .execute(&self.pool)
//         .await
//         .map_err(|e| MoneyError::Storage(e.to_string()))?;

//         Ok(revert_tx_id)
//     }

//     /// Get transaction history for an owner
//     pub async fn get_transaction_history(
//         &self,
//         owner: Uuid,
//         limit: i64,
//     ) -> Result<Vec<Transaction>, MoneyError> {
//         let rows = sqlx::query(
//             r#"
//             SELECT id, asset, sender, receiver, burned_amount, minted_amount, metadata, created_at
//             FROM transactions
//             WHERE sender = $1 OR receiver = $1
//             ORDER BY created_at DESC
//             LIMIT $2
//             "#,
//         )
//         .bind(owner)
//         .bind(limit)
//         .fetch_all(&self.pool)
//         .await
//         .map_err(|e| MoneyError::Storage(e.to_string()))?;

//         rows.into_iter()
//             .map(|row| self.row_to_transaction(row))
//             .collect()
//     }

//     /// Get all assets
//     pub async fn list_assets(&self) -> Result<Vec<Asset>, MoneyError> {
//         let rows = sqlx::query(
//             r#"
//             SELECT id, code, unit FROM assets ORDER BY code
//             "#,
//         )
//         .fetch_all(&self.pool)
//         .await
//         .map_err(|e| MoneyError::Storage(e.to_string()))?;

//         rows.into_iter().map(|row| self.row_to_asset(row)).collect()
//     }

//     /// Cleanup old idempotency keys (run periodically)
//     pub async fn cleanup_idempotency_keys(&self, older_than_days: i64) -> Result<u64, MoneyError> {
//         let result = sqlx::query(
//             r#"
//             DELETE FROM idempotency_keys
//             WHERE created_at < NOW() - INTERVAL '1 day' * $1
//             "#,
//         )
//         .bind(older_than_days)
//         .execute(&self.pool)
//         .await
//         .map_err(|e| MoneyError::Storage(e.to_string()))?;

//         Ok(result.rows_affected())
//     }
// }
