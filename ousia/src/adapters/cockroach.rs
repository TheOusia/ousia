use chrono::Utc;
use sqlx::{
    PgPool, Postgres, Row,
    postgres::{PgArguments, PgRow},
    query::{Query as PgQuery, QueryScalar},
};
use uuid::Uuid;

use crate::{
    adapters::{Adapter, EdgeQuery, EdgeRecord, Error, ObjectRecord, Query},
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

impl CockroachAdapter {
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
}

impl CockroachAdapter {
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
impl Adapter for CockroachAdapter {
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
        .map_err(|err| match err {
            sqlx::Error::RowNotFound => Error::NotFound,
            _ => Error::Storage(err.to_string()),
        })?;

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
        let row = query.fetch_optional(&pool).await.map_err(|err| match err {
            sqlx::Error::RowNotFound => Error::NotFound,
            _ => Error::Storage(err.to_string()),
        })?;

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

//     async fn ensure_sequence_exists(&self, sq: String) {
//         let _ = sqlx::query("CREATE SEQUENCE IF NOT EXISTS $1")
//             .bind(sq)
//             .execute(&self.pool.clone())
//             .await;
//     }

//     fn map_row_to_object_record(row: PgRow) -> Result<ObjectRecord, Error> {
//         let data_json: serde_json::Value = row
//             .try_get("data")
//             .map_err(|e| Error::Deserialize(e.to_string()))?;

//         let index_meta_json: serde_json::Value = row
//             .try_get("index_meta")
//             .map_err(|e| Error::Deserialize(e.to_string()))?;

//         let type_name = row
//             .try_get::<String, _>("type")
//             .map_err(|e| Error::Deserialize(e.to_string()))?;

//         let id = row
//             .try_get::<Uuid, _>("id")
//             .map_err(|e| Error::Deserialize(e.to_string()))?;

//         let owner = row
//             .try_get::<Uuid, _>("owner")
//             .map_err(|e| Error::Deserialize(e.to_string()))?;

//         let created_at = row
//             .try_get("created_at")
//             .map_err(|e| Error::Deserialize(e.to_string()))?;

//         let updated_at = row
//             .try_get("updated_at")
//             .map_err(|e| Error::Deserialize(e.to_string()))?;

//         Ok(ObjectRecord {
//             id,
//             type_name,
//             owner,
//             created_at,
//             updated_at,
//             data: data_json,
//             index_meta: index_meta_json,
//         })
//     }

//     fn map_row_to_edge_record(row: PgRow) -> Result<EdgeRecord, Error> {
//         let data_json: serde_json::Value = row
//             .try_get("data")
//             .map_err(|e| Error::Deserialize(e.to_string()))?;

//         let index_meta_json: serde_json::Value = row
//             .try_get("index_meta")
//             .map_err(|e| Error::Deserialize(e.to_string()))?;

//         let type_name = row
//             .try_get::<String, _>("type")
//             .map_err(|e| Error::Deserialize(e.to_string()))?;

//         let from = row
//             .try_get::<Uuid, _>("from")
//             .map_err(|e| Error::Deserialize(e.to_string()))?;

//         let to = row
//             .try_get::<Uuid, _>("to")
//             .map_err(|e| Error::Deserialize(e.to_string()))?;

//         Ok(EdgeRecord {
//             type_name,
//             from,
//             to,
//             data: data_json,
//             index_meta: index_meta_json,
//         })
//     }

//     fn build_object_query_conditions(filters: &Vec<QueryFilter>, cursor: Option<Cursor>) -> String {
//         let mut conditions = vec![
//             ("type = $1".to_string(), "AND"),
//             ("owner = $2".to_string(), "AND"),
//         ];
//         let mut param_idx = 3;

//         if let Some(_) = cursor {
//             conditions.push(("id < $3".to_string(), "AND"));
//             param_idx += 1;
//         }

//         for filter in filters {
//             let index_type = match &filter.value {
//                 IndexValue::String(_) => "text",
//                 IndexValue::Int(_) => "bigint",
//                 IndexValue::Float(_) => "double precision",
//                 IndexValue::Bool(_) => "boolean",
//                 IndexValue::Timestamp(_) => "timestamptz",
//                 IndexValue::Uuid(_) => "uuid",
//                 IndexValue::Array(arr) => {
//                     if let Some(first) = arr.first() {
//                         match first {
//                             IndexValueInner::String(_) => "text[]",
//                             IndexValueInner::Int(_) => "bigint[]",
//                             IndexValueInner::Float(_) => "double precision[]",
//                         }
//                     } else {
//                         "text[]"
//                     }
//                 }
//             };

//             match &filter.mode {
//                 crate::query::QueryMode::Search(query_search) => {
//                     let comparison = match query_search.comparison {
//                         crate::query::Comparison::Equal => "=",
//                         crate::query::Comparison::BeginsWith => "ILIKE",
//                         crate::query::Comparison::Contains => {
//                             if matches!(filter.value, IndexValue::Array(_)) {
//                                 "&&"
//                             } else {
//                                 "ILIKE"
//                             }
//                         }
//                         crate::query::Comparison::GreaterThan => ">",
//                         crate::query::Comparison::LessThan => "<",
//                         crate::query::Comparison::GreaterThanOrEqual => ">=",
//                         crate::query::Comparison::LessThanOrEqual => "<=",
//                         crate::query::Comparison::NotEqual => "<>",
//                     };
//                     let condition = format!(
//                         "(index_meta->>'{}')::{} {} ${}",
//                         filter.field.name, index_type, comparison, param_idx
//                     );

//                     let operator = match query_search.operator {
//                         crate::query::Operator::And => "AND",
//                         _ => "OR",
//                     };
//                     conditions.push((condition, operator));
//                     param_idx += 1;
//                 }
//                 crate::query::QueryMode::Sort(_) => continue,
//             }
//         }

//         let mut query = sqlx::query(&sql).bind(type_name).bind(owner);

//         let f = filters.to_vec();
//         query = Self::query_bind_filters(query, &f);

//         let pool = self.pool.clone();
//         let row = query.fetch_optional(&pool).await.map_err(|err| match err {
//             sqlx::Error::RowNotFound => Error::NotFound,
//             _ => Error::Storage(err.to_string()),
//         })?;

//         Ok(row
//             .map(|row| Self::map_row_to_object_record(row).ok())
//             .unwrap_or_default())
//     }

//     async fn query_objects(
//         &self,
//         type_name: &'static str,
//         plan: Query,
//     ) -> Result<Vec<ObjectRecord>, Error> {
//         let mut where_clause = Self::build_object_query_conditions(&plan.filters, plan.cursor);
//         let order_clause = Self::build_order_clause(&plan.filters);

//         if plan.owner.is_nil() {
//             where_clause = where_clause.replace("owner = ", "owner > ");
//         }

//         let mut sql = format!(
//             r#"
//                 SELECT id, type, owner, created_at, updated_at, data, index_meta
//                 FROM objects
//                 {}
//                 {}
//                 "#,
//             where_clause, order_clause
//         );

//         if let Some(limit) = plan.limit {
//             sql.push_str(&format!(" LIMIT {}", limit));
//         }

//         let mut query = sqlx::query(&sql).bind(type_name).bind(plan.owner);

//         if let Some(cursor) = plan.cursor {
//             query = query.bind(cursor.last_id);
//         }

//         query = Self::query_bind_filters(query, &plan.filters);

//         let pool = self.pool.clone();
//         let rows = query
//             .fetch_all(&pool)
//             .await
//             .map_err(|err| Error::Storage(err.to_string()))?;

//         Ok(rows
//             .into_iter()
//             .filter_map(|row| Self::map_row_to_object_record(row).ok())
//             .collect())
//     }

//     async fn count_objects(
//         &self,
//         type_name: &'static str,
//         plan: Option<Query>,
//     ) -> Result<u64, Error> {
//         let pool = self.pool.clone();

//         match plan {
//             Some(plan) => {
//                 let where_clause = Self::build_object_query_conditions(&plan.filters, None);

//                 let mut sql = format!(
//                     r#"
//                     SELECT COUNT(*) FROM objects
//                     {}
//                     "#,
//                     where_clause
//                 );

//                 if let Some(limit) = plan.limit {
//                     sql.push_str(&format!(" LIMIT {}", limit));
//                 }

//                 let mut query = sqlx::query_scalar::<_, i64>(&sql)
//                     .bind(type_name)
//                     .bind(plan.owner);

//                 query = Self::query_scalar_bind_filters(query, &plan.filters);

//                 let count = query
//                     .fetch_one(&pool)
//                     .await
//                     .map_err(|e| Error::Storage(e.to_string()))?;

//                 Ok(count as u64)
//             }
//             None => {
//                 let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM objects WHERE type = $1")
//                     .bind(type_name)
//                     .fetch_one(&pool)
//                     .await
//                     .map_err(|err| Error::Storage(err.to_string()))?;

//                 Ok(count as u64)
//             }
//         }
//     }

//     async fn fetch_owned_objects(
//         &self,
//         type_name: &'static str,
//         owner: Uuid,
//     ) -> Result<Vec<ObjectRecord>, Error> {
//         let pool = self.pool.clone();
//         let rows = sqlx::query(
//             r#"
//             SELECT id, type, owner, created_at, updated_at, data, index_meta
//             FROM objects
//             WHERE owner = $1 AND type = $2
//             "#,
//         )
//         .bind(owner)
//         .bind(type_name)
//         .fetch_all(&pool)
//         .await
//         .map_err(|err| Error::Storage(err.to_string()))?;

//         rows.into_iter()
//             .map(Self::map_row_to_object_record)
//             .collect()
//     }

//     async fn fetch_owned_object(
//         &self,
//         type_name: &'static str,
//         owner: Uuid,
//     ) -> Result<Option<ObjectRecord>, Error> {
//         let pool = self.pool.clone();
//         let row = sqlx::query(
//             r#"
//             SELECT id, type, owner, created_at, updated_at, data, index_meta
//             FROM objects
//             WHERE owner = $1 AND type = $2
//             "#,
//         )
//         .bind(owner)
//         .bind(type_name)
//         .fetch_optional(&pool)
//         .await
//         .map_err(|err| Error::Storage(err.to_string()))?;

//         match row {
//             Some(r) => Self::map_row_to_object_record(r).map(|o| Some(o)),
//             None => Ok(None),
//         }
//     }

//     async fn fetch_union_object(
//         &self,
//         a_type_name: &'static str,
//         b_type_name: &'static str,
//         id: Uuid,
//     ) -> Result<Option<ObjectRecord>, Error> {
//         let pool = self.pool.clone();
//         let row = sqlx::query(
//             r#"
//             SELECT id, type, owner, created_at, updated_at, data, index_meta
//             FROM objects
//             WHERE id = $1 AND (type = $2 OR type = $3)
//             "#,
//         )
//         .bind(id)
//         .bind(a_type_name)
//         .bind(b_type_name)
//         .fetch_optional(&pool)
//         .await
//         .map_err(|err| Error::Storage(err.to_string()))?;

//         match row {
//             Some(r) => Self::map_row_to_object_record(r).map(|o| Some(o)),
//             None => Ok(None),
//         }
//     }

//     async fn fetch_union_objects(
//         &self,
//         a_type_name: &'static str,
//         b_type_name: &'static str,
//         ids: Vec<Uuid>,
//     ) -> Result<Vec<ObjectRecord>, Error> {
//         let pool = self.pool.clone();
//         let rows = sqlx::query(
//             r#"
//             SELECT id, type, owner, created_at, updated_at, data, index_meta
//             FROM objects
//             WHERE id = ANY($1) AND (type = $2 OR type = $3)
//             "#,
//         )
//         .bind(ids.into_iter().map(|id| id).collect::<Vec<Uuid>>())
//         .bind(a_type_name)
//         .bind(b_type_name)
//         .fetch_all(&pool)
//         .await
//         .map_err(|err| Error::Storage(err.to_string()))?;

//         rows.into_iter()
//             .map(Self::map_row_to_object_record)
//             .collect()
//     }

//     async fn fetch_owned_union_object(
//         &self,
//         a_type_name: &'static str,
//         b_type_name: &'static str,
//         owner: Uuid,
//     ) -> Result<Option<ObjectRecord>, Error> {
//         let pool = self.pool.clone();
//         let row = sqlx::query(
//             r#"
//             SELECT id, type, owner, created_at, updated_at, data, index_meta
//             FROM objects
//             WHERE owner = $1 AND (type = $2 OR type = $3)
//             "#,
//         )
//         .bind(owner)
//         .bind(a_type_name)
//         .bind(b_type_name)
//         .fetch_optional(&pool)
//         .await
//         .map_err(|err| Error::Storage(err.to_string()))?;

//         match row {
//             Some(r) => Self::map_row_to_object_record(r).map(|o| Some(o)),
//             None => Ok(None),
//         }
//     }

//     async fn fetch_owned_union_objects(
//         &self,
//         a_type_name: &'static str,
//         b_type_name: &'static str,
//         owner: Uuid,
//     ) -> Result<Vec<ObjectRecord>, Error> {
//         let pool = self.pool.clone();
//         let rows = sqlx::query(
//             r#"
//             SELECT id, type, owner, created_at, updated_at, data, index_meta
//             FROM objects
//             WHERE owner = $1 AND (type = $2 OR type = $3)
//             "#,
//         )
//         .bind(owner)
//         .bind(a_type_name)
//         .bind(b_type_name)
//         .fetch_all(&pool)
//         .await
//         .map_err(|err| Error::Storage(err.to_string()))?;

//         rows.into_iter()
//             .map(Self::map_row_to_object_record)
//             .collect()
//     }

//     /* ---------------- EDGES ---------------- */
//     async fn insert_edge(&self, record: EdgeRecord) -> Result<(), Error> {
//         let pool = self.pool.clone();
//         let _ = sqlx::query(
//             r#"
//             INSERT INTO edges ("from", "to", type, data, index_meta)
//             VALUES ($1, $2, $3, $4, $5)
//             ON CONFLICT ("from", "to", type)
//             DO UPDATE SET data = $4, index_meta = $5;
//             "#,
//         )
//         .bind(record.from)
//         .bind(record.to)
//         .bind(record.type_name)
//         .bind(record.data)
//         .bind(record.index_meta)
//         .execute(&pool)
//         .await
//         .map_err(|err| Error::Storage(err.to_string()))?;

//         Ok(())
//     }

//     async fn update_edge(
//         &self,
//         record: EdgeRecord,
//         old_to: Uuid,
//         to: Option<Uuid>,
//     ) -> Result<(), Error> {
//         let pool = self.pool.clone();
//         let _ = sqlx::query(
//             r#"
//         UPDATE edges SET data = $1, "to" = $2, index_meta = $5
//         WHERE "from" = $3 AND type = $4 AND "to" = $6
//         "#,
//         )
//         .bind(record.data)
//         .bind(to.unwrap_or(old_to))
//         .bind(record.from)
//         .bind(record.type_name)
//         .bind(record.index_meta)
//         .bind(old_to)
//         .execute(&pool)
//         .await
//         .map_err(|err| Error::Storage(err.to_string()))?;

//         Ok(())
//     }

//     async fn delete_edge(
//         &self,
//         type_name: &'static str,
//         from: Uuid,
//         to: Uuid,
//     ) -> Result<(), Error> {
//         let pool = self.pool.clone();
//         let _ = sqlx::query(
//             r#"
//             DELETE FROM edges
//             WHERE type = $1 AND "from" = $2 AND "to" = $3
//             "#,
//         )
//         .bind(type_name)
//         .bind(from)
//         .bind(to)
//         .execute(&pool)
//         .await
//         .map_err(|err| Error::Storage(err.to_string()))?;

//         Ok(())
//     }

//     async fn delete_object_edge(&self, type_name: &'static str, from: Uuid) -> Result<(), Error> {
//         let pool = self.pool.clone();
//         let _ = sqlx::query(
//             r#"
//             DELETE FROM edges
//             WHERE type = $1 AND "from" = $2
//             "#,
//         )
//         .bind(type_name)
//         .bind(from)
//         .execute(&pool)
//         .await
//         .map_err(|err| Error::Storage(err.to_string()))?;

//         Ok(())
//     }

//     async fn query_edges(
//         &self,
//         type_name: &'static str,
//         owner: Uuid,
//         plan: EdgeQuery,
//     ) -> Result<Vec<EdgeRecord>, Error> {
//         let where_clause = Self::build_edge_query_conditions(&plan.filters, plan.cursor);
//         let order_clause = Self::build_edge_order_clause(&plan.filters);

//         let mut sql = format!(
//             r#"
//             SELECT "from", "to", "type", data, index_meta
//             FROM edges
//             {}
//             {}
//             "#,
//             where_clause, order_clause
//         );

//         if let Some(limit) = plan.limit {
//             sql.push_str(&format!(" LIMIT {}", limit));
//         }

//         let mut query = sqlx::query(&sql).bind(type_name).bind(owner);
//         if let Some(cursor) = plan.cursor {
//             query = query.bind(cursor.last_id);
//         }

//         query = Self::query_bind_filters(query, &plan.filters);

//         let pool = self.pool.clone();
//         let rows = query
//             .fetch_all(&pool)
//             .await
//             .map_err(|err| Error::Storage(err.to_string()))?;

//         Ok(rows
//             .into_iter()
//             .filter_map(|row| Self::map_row_to_edge_record(row).ok())
//             .collect())
//     }

//     async fn query_reverse_edges(
//         &self,
//         type_name: &'static str,
//         owner_reverse: Uuid,
//         plan: EdgeQuery,
//     ) -> Result<Vec<EdgeRecord>, Error> {
//         let where_clause = Self::build_edge_reverse_query_conditions(&plan.filters, plan.cursor);
//         let order_clause = Self::build_edge_order_clause(&plan.filters);

//         let mut sql = format!(
//             r#"
//             SELECT "from", "to", "type", data, index_meta
//             FROM edges
//             {}
//             {}
//             "#,
//             where_clause, order_clause
//         );

//         if let Some(limit) = plan.limit {
//             sql.push_str(&format!(" LIMIT {}", limit));
//         }

//         let mut query = sqlx::query(&sql).bind(type_name).bind(owner_reverse);
//         if let Some(cursor) = plan.cursor {
//             query = query.bind(cursor.last_id);
//         }

//         query = Self::query_bind_filters(query, &plan.filters);

//         let pool = self.pool.clone();
//         let rows = query
//             .fetch_all(&pool)
//             .await
//             .map_err(|err| Error::Storage(err.to_string()))?;

//         Ok(rows
//             .into_iter()
//             .filter_map(|row| Self::map_row_to_edge_record(row).ok())
//             .collect())
//     }

//     async fn count_edges(
//         &self,
//         type_name: &'static str,
//         owner: Uuid,
//         plan: Option<EdgeQuery>,
//     ) -> Result<u64, Error> {
//         let pool = self.pool.clone();

//         match plan {
//             Some(plan) => {
//                 let where_clause = Self::build_edge_query_conditions(&plan.filters, None);

//                 let mut sql = format!(
//                     r#"
//                 SELECT COUNT(*) FROM edges
//                 {}
//                 "#,
//                     where_clause
//                 );

//                 if let Some(limit) = plan.limit {
//                     sql.push_str(&format!(" LIMIT {}", limit));
//                 }

//                 let mut query = sqlx::query_scalar::<_, i64>(&sql)
//                     .bind(type_name)
//                     .bind(owner);

//                 query = Self::query_scalar_bind_filters(query, &plan.filters);

//                 let pool = self.pool.clone();
//                 let count = query
//                     .fetch_one(&pool)
//                     .await
//                     .map_err(|e| Error::Storage(e.to_string()))?;

//                 Ok(count as u64)
//             }
//             None => {
//                 let count: i64 = sqlx::query_scalar(
//                     r#"SELECT COUNT(*) FROM edges WHERE type = $1 AND "from" = $2"#,
//                 )
//                 .bind(type_name)
//                 .bind(owner)
//                 .fetch_one(&pool)
//                 .await
//                 .map_err(|err| Error::Storage(err.to_string()))?;

//                 Ok(count as u64)
//             }
//         }
//     }

//     #[cfg(feature = "sequence")]
//     async fn sequence_value(&self, sq: String) -> u64 {
//         self.ensure_sequence_exists(sq.clone()).await;

//         let next_val: i64 = sqlx::query_scalar("SELECT currval($1);")
//             .bind(sq)
//             .fetch_one(&self.pool.clone())
//             .await
//             .expect("Failed to fetch the next sequence value");

//         next_val as u64
//     }

//     #[cfg(feature = "sequence")]
//     async fn sequence_next_value(&self, sq: String) -> u64 {
//         self.ensure_sequence_exists(sq.clone()).await;

//         let next_val: i64 = sqlx::query_scalar("SELECT nextval($1);")
//             .bind(sq)
//             .fetch_one(&self.pool.clone())
//             .await
//             .expect("Failed to fetch the next sequence value");

//         next_val as u64
//     }
//     // } String::new();
//     //         for (i, (cond, joiner)) in conditions.iter().enumerate() {
//     //             query.push_str(cond);
//     //             if i < conditions.len() - 1 && !joiner.is_empty() {
//     //                 query.push(' ');
//     //                 query.push_str(joiner);
//     //                 query.push(' ');
//     //             }
//     //         }

//     //         format!("WHERE {}", query)
//     //     }

//     fn build_edge_query_conditions(filters: &Vec<QueryFilter>, cursor: Option<Cursor>) -> String {
//         let mut conditions = vec![
//             ("type = $1".to_string(), "AND"),
//             (r#""from" = $2"#.to_string(), "AND"),
//         ];
//         let mut param_idx = 3;

//         if cursor.is_some() {
//             conditions.push((r#""to" < $3"#.to_string(), "AND"));
//             param_idx += 1;
//         }

//         for filter in filters {
//             let index_type = match &filter.value {
//                 IndexValue::String(_) => "text",
//                 IndexValue::Int(_) => "bigint",
//                 IndexValue::Float(_) => "double precision",
//                 IndexValue::Bool(_) => "boolean",
//                 IndexValue::Timestamp(_) => "timestamptz",
//                 IndexValue::Uuid(_) => "uuid",
//                 IndexValue::Array(arr) => {
//                     if let Some(first) = arr.first() {
//                         match first {
//                             IndexValueInner::String(_) => "text[]",
//                             IndexValueInner::Int(_) => "bigint[]",
//                             IndexValueInner::Float(_) => "double precision[]",
//                         }
//                     } else {
//                         "text[]"
//                     }
//                 }
//             };

//             match &filter.mode {
//                 crate::query::QueryMode::Search(query_search) => {
//                     let comparison = match query_search.comparison {
//                         crate::query::Comparison::Equal => "=",
//                         crate::query::Comparison::BeginsWith => "ILIKE",
//                         crate::query::Comparison::Contains => {
//                             if matches!(filter.value, IndexValue::Array(_)) {
//                                 "&&"
//                             } else {
//                                 "ILIKE"
//                             }
//                         }
//                         crate::query::Comparison::GreaterThan => ">",
//                         crate::query::Comparison::LessThan => "<",
//                         crate::query::Comparison::GreaterThanOrEqual => ">=",
//                         crate::query::Comparison::LessThanOrEqual => "<=",
//                         crate::query::Comparison::NotEqual => "<>",
//                     };
//                     let condition = format!(
//                         "(index_meta->>'{}')::{} {} ${}",
//                         filter.field.name, index_type, comparison, param_idx
//                     );

//                     let operator = match query_search.operator {
//                         crate::query::Operator::And => "AND",
//                         _ => "OR",
//                     };
//                     conditions.push((condition, operator));
//                     param_idx += 1;
//                 }
//                 crate::query::QueryMode::Sort(_) => continue,
//             }
//         }

//         let mut query = String::new();
//         for (i, (cond, joiner)) in conditions.iter().enumerate() {
//             query.push_str(cond);
//             if i < conditions.len() - 1 && !joiner.is_empty() {
//                 query.push(' ');
//                 query.push_str(joiner);
//                 query.push(' ');
//             }
//         }

//         format!("WHERE {}", query)
//     }

//     fn build_edge_reverse_query_conditions(
//         filters: &Vec<QueryFilter>,
//         cursor: Option<Cursor>,
//     ) -> String {
//         let mut conditions = vec![
//             ("type = $1".to_string(), "AND"),
//             (r#""to" = $2"#.to_string(), "AND"),
//         ];
//         let mut param_idx = 3;

//         if let Some(_) = cursor {
//             conditions.push((r#""from" < $3"#.to_string(), "AND"));
//             param_idx += 1;
//         }

//         for filter in filters {
//             let index_type = match &filter.value {
//                 IndexValue::String(_) => "text",
//                 IndexValue::Int(_) => "bigint",
//                 IndexValue::Float(_) => "double precision",
//                 IndexValue::Bool(_) => "boolean",
//                 IndexValue::Timestamp(_) => "timestamptz",
//                 IndexValue::Uuid(_) => "uuid",
//                 IndexValue::Array(arr) => {
//                     if let Some(first) = arr.first() {
//                         match first {
//                             IndexValueInner::String(_) => "text[]",
//                             IndexValueInner::Int(_) => "bigint[]",
//                             IndexValueInner::Float(_) => "double precision[]",
//                         }
//                     } else {
//                         "text[]"
//                     }
//                 }
//             };

//             match &filter.mode {
//                 crate::query::QueryMode::Search(query_search) => {
//                     let comparison = match query_search.comparison {
//                         crate::query::Comparison::Equal => "=",
//                         crate::query::Comparison::BeginsWith => "ILIKE",
//                         crate::query::Comparison::Contains => {
//                             if matches!(filter.value, IndexValue::Array(_)) {
//                                 "&&"
//                             } else {
//                                 "ILIKE"
//                             }
//                         }
//                         crate::query::Comparison::GreaterThan => ">",
//                         crate::query::Comparison::LessThan => "<",
//                         crate::query::Comparison::GreaterThanOrEqual => ">=",
//                         crate::query::Comparison::LessThanOrEqual => "<=",
//                         crate::query::Comparison::NotEqual => "<>",
//                     };
//                     let condition = format!(
//                         "(index_meta->>'{}')::{} {} ${}",
//                         filter.field.name, index_type, comparison, param_idx
//                     );

//                     let operator = match query_search.operator {
//                         crate::query::Operator::And => "AND",
//                         _ => "OR",
//                     };
//                     conditions.push((condition, operator));
//                     param_idx += 1;
//                 }
//                 crate::query::QueryMode::Sort(_) => continue,
//             }
//         }

//         let mut query = String::new();
//         for (i, (cond, joiner)) in conditions.iter().enumerate() {
//             query.push_str(cond);
//             if i < conditions.len() - 1 && !joiner.is_empty() {
//                 query.push(' ');
//                 query.push_str(joiner);
//                 query.push(' ');
//             }
//         }

//         format!("WHERE {}", query)
//     }

//     fn build_order_clause(filters: &Vec<QueryFilter>) -> String {
//         let sort: Vec<&QueryFilter> = filters
//             .iter()
//             .filter(|f| f.mode.as_sort().is_some())
//             .collect();

//         if sort.is_empty() {
//             return "ORDER BY id DESC".to_string();
//         }

//         let order_terms: Vec<String> = sort
//             .iter()
//             .filter(|s| s.value.as_array().is_none())
//             .map(|s| {
//                 let direction = if s.mode.as_sort().unwrap().ascending {
//                     "ASC"
//                 } else {
//                     "DESC"
//                 };

//                 let index_type = match &s.value {
//                     IndexValue::String(_) => "text",
//                     IndexValue::Int(_) => "bigint",
//                     IndexValue::Float(_) => "double precision",
//                     IndexValue::Bool(_) => "boolean",
//                     IndexValue::Timestamp(_) => "timestamptz",
//                     _ => "text",
//                 };
//                 format!(
//                     "(index_meta->>'{}')::{} {}",
//                     s.field.name, index_type, direction
//                 )
//             })
//             .collect();

//         format!("ORDER BY {}", order_terms.join(", "))
//     }

//     fn build_edge_order_clause(filters: &Vec<QueryFilter>) -> String {
//         let sort: Vec<&QueryFilter> = filters
//             .iter()
//             .filter(|f| f.mode.as_sort().is_some())
//             .collect();

//         if sort.is_empty() {
//             return "".to_string();
//         }

//         let order_terms: Vec<String> = sort
//             .iter()
//             .filter(|s| s.value.as_array().is_none())
//             .map(|s| {
//                 let direction = if s.mode.as_sort().unwrap().ascending {
//                     "ASC"
//                 } else {
//                     "DESC"
//                 };

//                 let index_type = match &s.value {
//                     IndexValue::String(_) => "text",
//                     IndexValue::Int(_) => "bigint",
//                     IndexValue::Float(_) => "double precision",
//                     IndexValue::Bool(_) => "boolean",
//                     IndexValue::Uuid(_) => "uuid",
//                     IndexValue::Timestamp(_) => "timestamptz",
//                     _ => "text",
//                 };
//                 format!(
//                     "(index_meta->>'{}')::{} {}",
//                     s.field.name, index_type, direction
//                 )
//             })
//             .collect();

//         format!("ORDER BY {}", order_terms.join(", "))
//     }

//     fn query_bind_filters<'a>(
//         mut query: PgQuery<'a, Postgres, PgArguments>,
//         filters: &'a Vec<QueryFilter>,
//     ) -> PgQuery<'a, Postgres, PgArguments> {
//         for filter in filters.iter().filter(|f| f.mode.as_search().is_some()) {
//             query = match &filter.value {
//                 IndexValue::String(s) => {
//                     use crate::query::Comparison::*;
//                     match filter.mode.as_search().unwrap().comparison {
//                         BeginsWith => query.bind(format!("{}%", s)),
//                         Contains => query.bind(format!("%{}%", s)),
//                         _ => query.bind(s),
//                     }
//                 }
//                 IndexValue::Int(i) => query.bind(i),
//                 IndexValue::Float(f) => query.bind(f),
//                 IndexValue::Bool(b) => query.bind(b),
//                 IndexValue::Timestamp(t) => query.bind(t),
//                 IndexValue::Uuid(uid) => query.bind(uid),
//                 IndexValue::Array(arr) => {
//                     if let Some(first) = arr.first() {
//                         match first {
//                             IndexValueInner::String(_) => query.bind(
//                                 arr.iter()
//                                     .map(|s| s.as_string().unwrap_or_default())
//                                     .collect::<Vec<&str>>(),
//                             ),
//                             IndexValueInner::Int(_) => query.bind(
//                                 arr.iter()
//                                     .map(|s| s.as_int().unwrap_or_default())
//                                     .collect::<Vec<i64>>(),
//                             ),
//                             IndexValueInner::Float(_) => query.bind(
//                                 arr.iter()
//                                     .map(|s| s.as_float().unwrap_or_default())
//                                     .collect::<Vec<f64>>(),
//                             ),
//                         }
//                     } else {
//                         query.bind(vec![] as Vec<String>)
//                     }
//                 }
//             };
//         }
//         query
//     }

//     fn query_scalar_bind_filters<'a, O>(
//         mut query: QueryScalar<'a, Postgres, O, PgArguments>,
//         filters: &'a Vec<QueryFilter>,
//     ) -> QueryScalar<'a, Postgres, O, PgArguments> {
//         for filter in filters.iter().filter(|f| f.mode.as_search().is_some()) {
//             query = match &filter.value {
//                 IndexValue::String(s) => {
//                     use crate::query::Comparison::*;
//                     match filter.mode.as_search().unwrap().comparison {
//                         BeginsWith => query.bind(format!("{}%", s)),
//                         Contains => query.bind(format!("%{}%", s)),
//                         _ => query.bind(s),
//                     }
//                 }
//                 IndexValue::Int(i) => query.bind(i),
//                 IndexValue::Float(f) => query.bind(f),
//                 IndexValue::Bool(b) => query.bind(b),
//                 IndexValue::Timestamp(t) => query.bind(t),
//                 IndexValue::Uuid(uid) => query.bind(uid),
//                 IndexValue::Array(arr) => {
//                     if let Some(first) = arr.first() {
//                         match first {
//                             IndexValueInner::String(_) => query.bind(
//                                 arr.iter()
//                                     .map(|s| s.as_string().unwrap_or_default())
//                                     .collect::<Vec<&str>>(),
//                             ),
//                             IndexValueInner::Int(_) => query.bind(
//                                 arr.iter()
//                                     .map(|s| s.as_int().unwrap_or_default())
//                                     .collect::<Vec<i64>>(),
//                             ),
//                             IndexValueInner::Float(_) => query.bind(
//                                 arr.iter()
//                                     .map(|s| s.as_float().unwrap_or_default())
//                                     .collect::<Vec<f64>>(),
//                             ),
//                         }
//                     } else {
//                         query.bind(vec![] as Vec<String>)
//                     }
//                 }
//             };
//         }
//         query
//     }
// }

// #[async_trait::async_trait]
// impl Adapter for CockroachAdapter {
//     async fn insert_object(&self, record: ObjectRecord) -> Result<(), Error> {
//         let pool = self.pool.clone();
//         let _ = sqlx::query(
//             r#"
//             INSERT INTO public.objects (id, type, owner, created_at, updated_at, data, index_meta)
//             VALUES ($1, $2, $3, $4, $5, $6, $7)
//             "#,
//         )
//         .bind(record.id)
//         .bind(record.type_name)
//         .bind(record.owner)
//         .bind(record.created_at)
//         .bind(record.updated_at)
//         .bind(record.data)
//         .bind(record.index_meta)
//         .fetch_optional(&pool)
//         .await
//         .map_err(|err| Error::Storage(err.to_string()))?;
//         Ok(())
//     }

//     async fn fetch_object(&self, id: Uuid) -> Result<Option<ObjectRecord>, Error> {
//         let pool = self.pool.clone();
//         let row = sqlx::query(
//             r#"
//             SELECT id, type, owner, created_at, updated_at, data, index_meta
//             FROM objects
//             WHERE id = $1
//             "#,
//         )
//         .bind(id)
//         .fetch_optional(&pool)
//         .await
//         .map_err(|err| Error::Storage(err.to_string()))?;

//         match row {
//             Some(r) => Self::map_row_to_object_record(r).map(|o| Some(o)),
//             None => Ok(None),
//         }
//     }

//     async fn fetch_bulk_objects(&self, ids: Vec<Uuid>) -> Result<Vec<ObjectRecord>, Error> {
//         let pool = self.pool.clone();
//         let rows = sqlx::query(
//             r#"
//             SELECT id, type, owner, created_at, updated_at, data, index_meta
//             FROM objects
//             WHERE id = ANY($1)
//             "#,
//         )
//         .bind(ids.into_iter().map(|id| id).collect::<Vec<Uuid>>())
//         .fetch_all(&pool)
//         .await
//         .map_err(|err| Error::Storage(err.to_string()))?;

//         rows.into_iter()
//             .map(Self::map_row_to_object_record)
//             .collect()
//     }

//     async fn update_object(&self, record: ObjectRecord) -> Result<(), Error> {
//         let pool = self.pool.clone();
//         sqlx::query(
//             r#"
//             UPDATE objects
//             SET updated_at = $2, data = $3, index_meta = $4
//             WHERE id = $1
//             "#,
//         )
//         .bind(record.id)
//         .bind(record.updated_at)
//         .bind(record.data)
//         .bind(record.index_meta)
//         .execute(&pool)
//         .await
//         .map_err(|err| Error::Storage(err.to_string()))?;

//         Ok(())
//     }

//     async fn transfer_object(
//         &self,
//         id: Uuid,
//         from_owner: Uuid,
//         to_owner: Uuid,
//     ) -> Result<ObjectRecord, Error> {
//         let pool = self.pool.clone();
//         let row = sqlx::query(
//             r#"
//             UPDATE objects
//             SET updated_at = $3, owner = $4
//             WHERE id = $1 AND owner = $2
//             RETURNING *
//             "#,
//         )
//         .bind(id)
//         .bind(from_owner)
//         .bind(Utc::now())
//         .bind(to_owner)
//         .fetch_one(&pool)
//         .await
//         .map_err(|err| match err {
//             sqlx::Error::RowNotFound => Error::NotFound,
//             _ => Error::Storage(err.to_string()),
//         })?;

//         Self::map_row_to_object_record(row)
//     }

//     async fn delete_object(&self, id: Uuid, owner: Uuid) -> Result<Option<ObjectRecord>, Error> {
//         let pool = self.pool.clone();
//         let row = sqlx::query(
//             r#"
//             DELETE FROM objects
//             WHERE id = $1 AND owner = $2
//             RETURNING *
//             "#,
//         )
//         .bind(id)
//         .bind(owner)
//         .fetch_optional(&pool)
//         .await
//         .map_err(|err| match err {
//             sqlx::Error::RowNotFound => Error::NotFound,
//             _ => Error::Storage(err.to_string()),
//         })?;

//         match row {
//             Some(r) => Self::map_row_to_object_record(r).map(|o| Some(o)),
//             None => Ok(None),
//         }
//     }

//     async fn find_object(
//         &self,
//         type_name: &'static str,
//         owner: Uuid,
//         filters: &[QueryFilter],
//     ) -> Result<Option<ObjectRecord>, Error> {
//         let where_clause = Self::build_object_query_conditions(&filters.to_vec(), None);
//         let order_clause = Self::build_order_clause(&filters.to_vec());

//         let sql = format!(
//             r#"
//             SELECT id, type, owner, created_at, updated_at, data, index_meta
//             FROM objects
//             {}
//             {}
//             "#,
//             where_clause, order_clause
//         );

//         // let mut query =
//     }
// }
