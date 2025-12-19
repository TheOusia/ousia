use chrono::Utc;
use sqlx::{
    PgPool, Postgres, Row,
    postgres::{PgArguments, PgRow},
    query::{Query as PgQuery, QueryScalar},
};
use ulid::Ulid;

use crate::{
    adapters::{Adapter, EdgeQuery, EdgeRecord, Error, ObjectRecord, Query},
    query::{IndexValue, QueryFilter},
};

/// PostgreSQL adapter using a unified JSON storage model
///
/// Schema:
/// ```sql
/// CREATE TABLE public.objects (
///     id TEXT PRIMARY KEY,
///     type TEXT NOT NULL,
///     owner TEXT NOT NULL,
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
    pub fn new(pool: PgPool) -> Self {
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
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                owner TEXT NOT NULL,
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
                "from" TEXT NOT NULL,
                "to" TEXT NOT NULL,
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
            CREATE UNIQUE INDEX IF NOT EXISTS idx_edges_key ON edges("from", "to", type);
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

        tx.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

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

        let id = Ulid::from_string(
            &row.try_get::<String, _>("id")
                .map_err(|e| Error::Deserialize(e.to_string()))?,
        )
        .map_err(|e| Error::Deserialize(e.to_string()))?;

        let owner = Ulid::from_string(
            &row.try_get::<String, _>("owner")
                .map_err(|e| Error::Deserialize(e.to_string()))?,
        )
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

        let from = Ulid::from_string(
            &row.try_get::<String, _>("from")
                .map_err(|e| Error::Deserialize(e.to_string()))?,
        )
        .map_err(|e| Error::Deserialize(e.to_string()))?;

        let to = Ulid::from_string(
            &row.try_get::<String, _>("to")
                .map_err(|e| Error::Deserialize(e.to_string()))?,
        )
        .map_err(|e| Error::Deserialize(e.to_string()))?;

        Ok(EdgeRecord {
            type_name,
            from,
            to,
            data: data_json,
            index_meta: index_meta_json,
        })
    }

    fn build_object_query_conditions(filters: &Vec<QueryFilter>) -> String {
        let mut conditions = vec![
            ("type = $1".to_string(), "AND"),
            ("owner = $2".to_string(), "AND"),
        ];
        let mut param_idx = 3;

        for filter in filters {
            let index_type = match &filter.value {
                IndexValue::String(_) => "text",
                IndexValue::Int(_) => "bigint",
                IndexValue::Float(_) => "double",
                IndexValue::Bool(_) => "boolean",
                IndexValue::Timestamp(_) => "timestamptz",
            };

            match &filter.mode {
                crate::query::QueryMode::Search(query_search) => {
                    let comparison = match query_search.comparison {
                        crate::query::Comparison::Equal => "=",
                        crate::query::Comparison::BeginsWith => "ILIKE",
                        crate::query::Comparison::Contains => "ILIKE",
                        crate::query::Comparison::GreaterThan => ">",
                        crate::query::Comparison::LessThan => "<",
                        crate::query::Comparison::GreaterThanOrEqual => ">=",
                        crate::query::Comparison::LessThanOrEqual => "<=",
                        crate::query::Comparison::NotEqual => "<>",
                    };
                    let condition = format!(
                        "index_meta->>'{}'::{} {} ${}",
                        filter.field.name, index_type, comparison, param_idx
                    );

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

    fn build_edge_query_conditions(filters: &Vec<QueryFilter>) -> String {
        let mut conditions = vec![
            ("type = $1".to_string(), "AND"),
            ("from = $2".to_string(), "AND"),
        ];
        let mut param_idx = 3;
        for filter in filters {
            let index_type = match &filter.value {
                IndexValue::String(_) => "text",
                IndexValue::Int(_) => "bigint",
                IndexValue::Float(_) => "double",
                IndexValue::Bool(_) => "boolean",
                IndexValue::Timestamp(_) => "timestamptz",
            };

            match &filter.mode {
                crate::query::QueryMode::Search(query_search) => {
                    let comparison = match query_search.comparison {
                        crate::query::Comparison::Equal => "=",
                        crate::query::Comparison::BeginsWith => "ILIKE",
                        crate::query::Comparison::Contains => "ILIKE",
                        crate::query::Comparison::GreaterThan => ">",
                        crate::query::Comparison::LessThan => "<",
                        crate::query::Comparison::GreaterThanOrEqual => ">=",
                        crate::query::Comparison::LessThanOrEqual => "<=",
                        crate::query::Comparison::NotEqual => "<>",
                    };
                    let condition = format!(
                        "index_meta->>'{}'::{} {} ${}",
                        filter.field.name, index_type, comparison, param_idx
                    );

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

    fn build_edge_reverse_query_conditions(filters: &Vec<QueryFilter>) -> String {
        let mut conditions = vec![
            ("type = $1".to_string(), "AND"),
            ("to = $2".to_string(), "AND"),
        ];
        let mut param_idx = 3;
        for filter in filters {
            let index_type = match &filter.value {
                IndexValue::String(_) => "text",
                IndexValue::Int(_) => "bigint",
                IndexValue::Float(_) => "double",
                IndexValue::Bool(_) => "boolean",
                IndexValue::Timestamp(_) => "timestamptz",
            };

            match &filter.mode {
                crate::query::QueryMode::Search(query_search) => {
                    let comparison = match query_search.comparison {
                        crate::query::Comparison::Equal => "=",
                        crate::query::Comparison::BeginsWith => "ILIKE",
                        crate::query::Comparison::Contains => "ILIKE",
                        crate::query::Comparison::GreaterThan => ">",
                        crate::query::Comparison::LessThan => "<",
                        crate::query::Comparison::GreaterThanOrEqual => ">=",
                        crate::query::Comparison::LessThanOrEqual => "<=",
                        crate::query::Comparison::NotEqual => "<>",
                    };
                    let condition = format!(
                        "index_meta->>'{}'::{} {} ${}",
                        filter.field.name, index_type, comparison, param_idx
                    );

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
            return "ORDER BY created_at DESC".to_string();
        }

        let order_terms: Vec<String> = sort
            .iter()
            .map(|s| {
                let direction = if s.mode.as_sort().unwrap().ascending {
                    "ASC"
                } else {
                    "DESC"
                };

                let index_type = match &s.value {
                    IndexValue::String(_) => "text",
                    IndexValue::Int(_) => "bigint",
                    IndexValue::Float(_) => "double",
                    IndexValue::Bool(_) => "boolean",
                    IndexValue::Timestamp(_) => "timestamptz",
                };
                format!(
                    "index_meta->>'{}'::{} {}",
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
        .bind(record.id.to_string())
        .bind(record.type_name)
        .bind(record.owner.to_string())
        .bind(record.created_at)
        .bind(record.updated_at)
        .bind(record.data)
        .bind(record.index_meta)
        .fetch_optional(&pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;
        Ok(())
    }

    async fn fetch_object(&self, id: Ulid) -> Result<Option<ObjectRecord>, Error> {
        let pool = self.pool.clone();
        let row = sqlx::query(
            r#"
            SELECT id, type, owner, created_at, updated_at, data, index_meta
            FROM objects
            WHERE id = $1
            "#,
        )
        .bind(id.to_string())
        .fetch_optional(&pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record(r).map(|o| Some(o)),
            None => Ok(None),
        }
    }

    async fn fetch_bulk_objects(&self, ids: Vec<Ulid>) -> Result<Vec<ObjectRecord>, Error> {
        let pool = self.pool.clone();
        let rows = sqlx::query(
            r#"
            SELECT id, type, owner, created_at, updated_at, data, index_meta
            FROM objects
            WHERE id = ANY($1)
            "#,
        )
        .bind(ids.iter().map(|id| id.to_string()).collect::<Vec<String>>())
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
        .bind(record.id.to_string())
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
        id: Ulid,
        from_owner: Ulid,
        to_owner: Ulid,
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
        .bind(id.to_string())
        .bind(from_owner.to_string())
        .bind(Utc::now())
        .bind(to_owner.to_string())
        .fetch_one(&pool)
        .await
        .map_err(|err| match err {
            sqlx::Error::RowNotFound => Error::NotFound,
            _ => Error::Storage(err.to_string()),
        })?;

        Self::map_row_to_object_record(row)
    }

    async fn delete_object(&self, id: Ulid, owner: Ulid) -> Result<Option<ObjectRecord>, Error> {
        let pool = self.pool.clone();
        let row = sqlx::query(
            r#"
            DELETE FROM objects
            WHERE id = $1 AND owner = $2
            RETURNING *
            "#,
        )
        .bind(id.to_string())
        .bind(owner.to_string())
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

    async fn query_objects(
        &self,
        type_name: &'static str,
        plan: Query,
    ) -> Result<Vec<ObjectRecord>, Error> {
        let where_clause = Self::build_object_query_conditions(&plan.filters);
        let order_clause = Self::build_order_clause(&plan.filters);

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

        if let Some(offset) = plan.offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }

        let mut query = sqlx::query(&sql)
            .bind(type_name)
            .bind(plan.owner.to_string());

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
                let where_clause = Self::build_object_query_conditions(&plan.filters);
                let order_clause = Self::build_order_clause(&plan.filters);

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

                if let Some(offset) = plan.offset {
                    sql.push_str(&format!(" OFFSET {}", offset));
                }

                let mut query = sqlx::query_scalar::<_, i64>(&sql)
                    .bind(type_name)
                    .bind(plan.owner.to_string());

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
        owner: Ulid,
    ) -> Result<Vec<ObjectRecord>, Error> {
        let pool = self.pool.clone();
        let rows = sqlx::query(
            r#"
            SELECT id, type, owner, created_at, updated_at, data, index_meta
            FROM objects
            WHERE owner = $1 AND type = $2
            "#,
        )
        .bind(owner.to_string())
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
        owner: Ulid,
    ) -> Result<Option<ObjectRecord>, Error> {
        let pool = self.pool.clone();
        let row = sqlx::query(
            r#"
            SELECT id, type, owner, created_at, updated_at, data, index_meta
            FROM objects
            WHERE owner = $1 AND type = $2
            "#,
        )
        .bind(owner.to_string())
        .bind(type_name)
        .fetch_optional(&pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record(r).map(|o| Some(o)),
            None => Ok(None),
        }
    }

    /* ---------------- EDGES ---------------- */
    async fn insert_edge(&self, record: EdgeRecord) -> Result<(), Error> {
        let pool = self.pool.clone();
        let _ = sqlx::query(
            r#"
            INSERT INTO edges ("from", "to", type, data, index_meta)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(record.from.to_string())
        .bind(record.to.to_string())
        .bind(record.type_name)
        .bind(record.data)
        .bind(record.index_meta)
        .execute(&pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        Ok(())
    }

    async fn delete_edge(
        &self,
        type_name: &'static str,
        from: Ulid,
        to: Ulid,
    ) -> Result<(), Error> {
        let pool = self.pool.clone();
        let _ = sqlx::query(
            r#"
            DELETE FROM edges
            WHERE type = $1 AND "from" = $2 AND "to" = $3
            "#,
        )
        .bind(type_name)
        .bind(from.to_string())
        .bind(to.to_string())
        .execute(&pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        Ok(())
    }

    async fn query_edges(
        &self,
        type_name: &'static str,
        owner: Ulid,
        plan: EdgeQuery,
    ) -> Result<Vec<EdgeRecord>, Error> {
        let where_clause = Self::build_edge_query_conditions(&plan.filters);
        let order_clause = Self::build_order_clause(&plan.filters);

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

        if let Some(offset) = plan.offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }

        let mut query = sqlx::query(&sql).bind(type_name).bind(owner.to_string());

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
        owner: Ulid,
        plan: Option<EdgeQuery>,
    ) -> Result<u64, Error> {
        let pool = self.pool.clone();

        match plan {
            Some(plan) => {
                let where_clause = Self::build_edge_query_conditions(&plan.filters);
                let order_clause = Self::build_order_clause(&plan.filters);

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

                if let Some(offset) = plan.offset {
                    sql.push_str(&format!(" OFFSET {}", offset));
                }

                let mut query = sqlx::query_scalar::<_, i64>(&sql)
                    .bind(type_name)
                    .bind(owner.to_string());

                query = Self::query_scalar_bind_filters(query, &plan.filters);

                let pool = self.pool.clone();
                let count = query
                    .fetch_one(&pool)
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;

                Ok(count as u64)
            }
            None => {
                let count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM edges WHERE type = $1 AND from = $2")
                        .bind(type_name)
                        .bind(owner.to_string())
                        .fetch_one(&pool)
                        .await
                        .map_err(|err| Error::Storage(err.to_string()))?;

                Ok(count as u64)
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::{Meta, Object, ObjectMeta, query::QueryMode};
    use ousia_derive::{OusiaDefault, OusiaObject};
    use serde::{Deserialize, Serialize};
    use testcontainers::ContainerAsync;
    use testcontainers_modules::postgres::Postgres;

    use super::*;

    /// Example: Blog Post object
    #[derive(OusiaObject, OusiaDefault, Debug, Clone)]
    #[ousia(
        type_name = "Post",
        index = "title:search+sort",
        index = "status:search"
    )]
    pub struct Post {
        _meta: Meta,

        pub title: String,
        pub content: String,
        pub status: PostStatus,
        pub published_at: Option<chrono::DateTime<chrono::Utc>>,
        pub tags: Vec<String>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    pub enum PostStatus {
        Draft,
        Published,
        Archived,
    }

    impl Default for PostStatus {
        fn default() -> Self {
            PostStatus::Draft
        }
    }

    // Implement ToIndexValue for custom enum
    impl crate::query::ToIndexValue for PostStatus {
        fn to_index_value(&self) -> crate::query::IndexValue {
            let s = match self {
                PostStatus::Draft => "draft",
                PostStatus::Published => "published",
                PostStatus::Archived => "archived",
            };
            crate::query::IndexValue::String(s.to_string())
        }
    }

    /// Example: User object
    #[derive(OusiaObject, OusiaDefault, Debug, Clone)]
    #[ousia(
        type_name = "User",
        index = "email:search",
        index = "username:search+sort"
    )]
    pub struct User {
        _meta: Meta,

        pub username: String,
        pub email: String,
        pub display_name: String,
    }

    #[cfg(test)]
    pub(crate) async fn setup_test_db() -> (ContainerAsync<Postgres>, PgPool) {
        use sqlx::postgres::PgPoolOptions;
        use testcontainers::{ImageExt, runners::AsyncRunner as _};

        let postgres = match Postgres::default()
            .with_password("postgres")
            .with_user("postgres")
            .with_db_name("postgres")
            .with_tag("16-alpine")
            .start()
            .await
        {
            Ok(postgres) => postgres,
            Err(err) => panic!("Failed to start Postgres: {}", err),
        };
        // Give DB time to start
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let port = postgres.get_host_port_ipv4(5432).await.unwrap();
        let db_url = format!("postgres://postgres:postgres@localhost:{}/postgres", port);

        let pool = match PgPoolOptions::new()
            .max_connections(5)
            .connect(&db_url)
            .await
        {
            Ok(pool) => pool,
            Err(err) => panic!("Failed to connect to Postgres: {}", err),
        };

        (postgres, pool)
    }

    #[tokio::test]
    async fn test_adapter_insert() {
        let (_resource, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::new(pool);

        if let Err(err) = adapter.init_schema().await {
            panic!("Error: {:#?}", err);
        }

        let user = User::default();
        if let Err(err) = adapter
            .insert_object(ObjectRecord::from_object(&user))
            .await
        {
            panic!("Error: {:#?}", err);
        };
    }

    #[tokio::test]
    async fn test_adapter_get() {
        let (_resource, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::new(pool);

        if let Err(err) = adapter.init_schema().await {
            panic!("Error: {:#?}", err);
        }

        let mut user = User::default();
        user.username = "test_user".to_string();
        if let Err(err) = adapter
            .insert_object(ObjectRecord::from_object(&user))
            .await
        {
            panic!("Error: {:#?}", err);
        };

        let user_result = adapter.fetch_object(user.id()).await.unwrap();
        assert!(user_result.is_some());

        let _user: User = user_result.unwrap().to_object().unwrap();
        assert_eq!(_user.id(), user.id());
        assert_eq!(_user.username, user.username);
    }

    #[tokio::test]
    async fn test_adapter_update() {
        let (_resource, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::new(pool);

        if let Err(err) = adapter.init_schema().await {
            panic!("Error: {:#?}", err);
        }

        let mut user = User::default();
        user.username = "test_user".to_string();
        if let Err(err) = adapter
            .insert_object(ObjectRecord::from_object(&user))
            .await
        {
            panic!("Error: {:#?}", err);
        } else {
            let user_result = adapter.fetch_object(user.id()).await.unwrap();
            assert!(user_result.is_some());

            let _user: User = user_result.unwrap().to_object().unwrap();
            assert_eq!(_user.id(), user.id());
            assert_eq!(_user.username, user.username);
        }

        user.username = "new_username".to_string();
        if let Err(err) = adapter
            .update_object(ObjectRecord::from_object(&user))
            .await
        {
            panic!("Error: {:#?}", err);
        } else {
            let user_result = adapter.fetch_object(user.id()).await.unwrap();
            assert!(user_result.is_some());

            let _user: User = user_result.unwrap().to_object().unwrap();
            assert_eq!(_user.id(), user.id());
            assert_eq!(_user.username, user.username);
        }
    }

    #[tokio::test]
    async fn test_adapter_query() {
        let (_resource, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::new(pool);

        if let Err(err) = adapter.init_schema().await {
            panic!("Error: {:#?}", err);
        }

        let mut user = User::default();
        user.username = "test_user".to_string();
        user.email = "test@gmail.com".to_string();
        if let Err(err) = adapter
            .insert_object(ObjectRecord::from_object(&user))
            .await
        {
            panic!("Error: {:#?}", err);
        }
        let user_result = adapter.fetch_object(user.id()).await.unwrap();
        assert!(user_result.is_some());

        let users = adapter
            .query_objects(
                User::TYPE,
                Query::default().where_eq(&User::FIELDS.email, "efedua.bell@gmail.com"),
            )
            .await
            .unwrap();
        assert_eq!(users.len(), 0);

        let users = adapter
            .query_objects(
                User::TYPE,
                Query::default().where_eq(&User::FIELDS.email, "test@gmail.com"),
            )
            .await
            .unwrap();
        assert_eq!(users.len(), 1);

        let mut post_1 = Post::default();
        post_1.set_owner(user.id());

        adapter
            .insert_object(ObjectRecord::from_object(&post_1))
            .await
            .unwrap();

        post_1.status = PostStatus::Published;
        adapter
            .update_object(ObjectRecord::from_object(&post_1))
            .await
            .unwrap();

        let _post: Post = adapter
            .fetch_object(post_1.id())
            .await
            .unwrap()
            .expect("Post not found")
            .to_object()
            .unwrap();
        assert_eq!(_post.id(), post_1.id());

        let posts = adapter
            .query_objects(
                Post::TYPE,
                Query::new(user.id()).where_eq(&Post::FIELDS.status, PostStatus::Published),
            )
            .await
            .unwrap();
        assert_eq!(posts.len(), 1);

        let posts = adapter
            .query_objects(
                Post::TYPE,
                Query::new(user.id()).where_eq(&Post::FIELDS.status, PostStatus::Draft),
            )
            .await
            .unwrap();
        assert_eq!(posts.len(), 0);
    }
}
