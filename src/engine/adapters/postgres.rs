use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row, postgres::PgRow};
use ulid::Ulid;

use crate::{
    Object,
    engine::adapters::{Adapter, AdapterError, QueryFilter, QueryPlan, QuerySort},
    object::query::{IndexMeta, IndexValue},
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
    pool: PgPool,
}

impl PostgresAdapter {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Initialize the database schema
    pub async fn init_schema(&self) -> Result<(), AdapterError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| AdapterError::Storage(err.to_string()))?;

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
        .map_err(|e| AdapterError::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_type_owner ON objects(type, owner);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| AdapterError::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_owner ON objects(owner);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| AdapterError::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_created_at ON objects(created_at);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| AdapterError::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_updated_at ON objects(updated_at);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| AdapterError::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_index_meta ON public.objects USING GIN (index_meta);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| AdapterError::Storage(e.to_string()))?;
        tx.commit()
            .await
            .map_err(|e| AdapterError::Storage(e.to_string()))?;

        Ok(())
    }

    fn map_row_to_object<T: Object>(row: PgRow) -> Result<T, AdapterError> {
        let data_json: serde_json::Value = row
            .try_get("data")
            .map_err(|e| AdapterError::Serialization(e.to_string()))?;

        let mut obj: T = serde_json::from_value(data_json)
            .map_err(|e| AdapterError::Serialization(e.to_string()))?;

        // Reconstruct meta from separate columns
        let meta = obj.meta_mut();
        meta.id = Ulid::from_string(
            &row.try_get::<String, _>("id")
                .map_err(|e| AdapterError::Serialization(e.to_string()))?,
        )
        .map_err(|e| AdapterError::Serialization(e.to_string()))?;

        meta.owner = Ulid::from_string(
            &row.try_get::<String, _>("owner")
                .map_err(|e| AdapterError::Serialization(e.to_string()))?,
        )
        .map_err(|e| AdapterError::Serialization(e.to_string()))?;

        meta.created_at = row
            .try_get("created_at")
            .map_err(|e| AdapterError::Serialization(e.to_string()))?;

        meta.updated_at = row
            .try_get("updated_at")
            .map_err(|e| AdapterError::Serialization(e.to_string()))?;

        Ok(obj)
    }

    fn build_query_conditions(plan: &QueryPlan) -> (String, Vec<String>) {
        let mut conditions = vec!["type = $1".to_string(), "owner = $2".to_string()];
        let mut param_idx = 3;

        for filter in &plan.filters {
            let condition = match &filter.value {
                IndexValue::String(_) => {
                    format!("index_meta->>'{}'::text = ${}", filter.field, param_idx)
                }
                IndexValue::Int(_) => {
                    format!("(index_meta->>'{}')::bigint = ${}", filter.field, param_idx)
                }
                IndexValue::Float(_) => {
                    format!(
                        "(index_meta->>'{}')::double precision = ${}",
                        filter.field, param_idx
                    )
                }
                IndexValue::Bool(_) => {
                    format!(
                        "(index_meta->>'{}')::boolean = ${}",
                        filter.field, param_idx
                    )
                }
                IndexValue::Timestamp(_) => {
                    format!(
                        "(index_meta->>'{}')::timestamptz = ${}",
                        filter.field, param_idx
                    )
                }
            };
            conditions.push(condition);
            param_idx += 1;
        }

        let where_clause = format!("WHERE {}", conditions.join(" AND "));

        (where_clause, conditions)
    }

    fn build_order_clause(plan: &QueryPlan) -> String {
        if plan.sort.is_empty() {
            return "ORDER BY created_at DESC".to_string();
        }

        let order_terms: Vec<String> = plan
            .sort
            .iter()
            .map(|s| {
                let direction = if s.ascending { "ASC" } else { "DESC" };
                format!("index_meta->>'{}'::text {}", s.field, direction)
            })
            .collect();

        format!("ORDER BY {}", order_terms.join(", "))
    }
}

#[async_trait]
impl Adapter for PostgresAdapter {
    async fn fetch_by_id<T: Object>(&self, id: Ulid) -> Option<T> {
        let row = sqlx::query(
            r#"
            SELECT id, type, owner, created_at, updated_at, data, index_meta
            FROM objects
            WHERE id = $1
            "#,
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await
        .ok()?;

        row.and_then(|r| Self::map_row_to_object(r).ok())
    }

    async fn insert<T: Object>(&self, obj: &T) -> Result<(), AdapterError> {
        let meta = obj.meta();
        let index_meta = obj.index_meta();

        // Serialize the object (excluding meta which is stored separately)
        let data_json =
            serde_json::to_value(obj).map_err(|e| AdapterError::Serialization(e.to_string()))?;

        let index_meta_json = serde_json::to_value(&index_meta)
            .map_err(|e| AdapterError::Serialization(e.to_string()))?;

        sqlx::query(
            r#"
            INSERT INTO public.objects (id, type, owner, created_at, updated_at, data, index_meta)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(meta.id().to_string())
        .bind(T::TYPE)
        .bind(meta.owner().to_string())
        .bind(meta.created_at())
        .bind(meta.updated_at())
        .bind(data_json)
        .bind(index_meta_json)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            if e.to_string().contains("duplicate key") {
                AdapterError::Conflict
            } else {
                AdapterError::Storage(e.to_string())
            }
        })?;

        Ok(())
    }

    async fn update<T: Object>(&self, obj: &mut T) -> Result<(), AdapterError> {
        // Update the updated_at timestamp
        obj.meta_mut().updated_at = chrono::Utc::now();

        let meta = obj.meta();
        let index_meta = obj.index_meta();

        let data_json = serde_json::to_value(obj.clone())
            .map_err(|e| AdapterError::Serialization(e.to_string()))?;

        let index_meta_json = serde_json::to_value(&index_meta)
            .map_err(|e| AdapterError::Serialization(e.to_string()))?;

        let result = sqlx::query(
            r#"
            UPDATE objects
            SET updated_at = $1, data = $2, index_meta = $3
            WHERE id = $4 AND type = $5
            "#,
        )
        .bind(meta.updated_at())
        .bind(data_json)
        .bind(index_meta_json)
        .bind(meta.id().to_string())
        .bind(T::TYPE)
        .execute(&self.pool)
        .await
        .map_err(|e| AdapterError::Storage(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(AdapterError::NotFound);
        }

        Ok(())
    }

    async fn query<T: Object>(&self, plan: QueryPlan) -> Vec<T> {
        let (where_clause, _) = Self::build_query_conditions(&plan);
        let order_clause = Self::build_order_clause(&plan);

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

        let mut query = sqlx::query(&sql).bind(T::TYPE).bind(plan.owner.to_string());

        // Bind filter values
        for filter in &plan.filters {
            query = match &filter.value {
                IndexValue::String(s) => query.bind(s),
                IndexValue::Int(i) => query.bind(i),
                IndexValue::Float(f) => query.bind(f),
                IndexValue::Bool(b) => query.bind(b),
                IndexValue::Timestamp(t) => query.bind(t),
            };
        }

        let rows = query.fetch_all(&self.pool).await.unwrap_or_default();

        rows.into_iter()
            .filter_map(|row| Self::map_row_to_object(row).ok())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use ousia_derive::{OusiaDefault, OusiaObject};
    #[cfg(test)]
    use testcontainers::ContainerAsync;
    #[cfg(test)]
    use testcontainers_modules::postgres::Postgres;

    use crate::{Meta, ObjectMeta, engine::adapters::Field};

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
    impl crate::object::query::ToIndexValue for PostStatus {
        fn to_index_value(&self) -> crate::object::query::IndexValue {
            let s = match self {
                PostStatus::Draft => "draft",
                PostStatus::Published => "published",
                PostStatus::Archived => "archived",
            };
            crate::object::query::IndexValue::String(s.to_string())
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
    async fn setup_test_db() -> (ContainerAsync<Postgres>, PgPool) {
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
        if let Err(err) = adapter.insert(&user).await {
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
        if let Err(err) = adapter.insert(&user).await {
            panic!("Error: {:#?}", err);
        };

        let user_result = adapter.fetch_by_id::<User>(user.id()).await;
        assert!(user_result.is_some());

        let _user = user_result.unwrap();
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
        if let Err(err) = adapter.insert(&user).await {
            panic!("Error: {:#?}", err);
        } else {
            let user_result = adapter.fetch_by_id::<User>(user.id()).await;
            assert!(user_result.is_some());

            let _user = user_result.unwrap();
            assert_eq!(_user.id(), user.id());
            assert_eq!(_user.username, user.username);
        }

        user.username = "new_username".to_string();
        if let Err(err) = adapter.update(&mut user).await {
            panic!("Error: {:#?}", err);
        } else {
            let user_result = adapter.fetch_by_id::<User>(user.id()).await;
            assert!(user_result.is_some());

            let _user = user_result.unwrap();
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
        if let Err(err) = adapter.insert(&user).await {
            panic!("Error: {:#?}", err);
        }
        let user_result = adapter.fetch_by_id::<User>(user.id()).await;
        assert!(user_result.is_some());

        UserIndexes {
            email: Field::new("email"),
            username: Field::new("username"),
        };
        let users = adapter
            .query::<User>(
                QueryPlan::default().with_filter(User::INDEXES.email, "efedua.bell@gmail.com"),
            )
            .await;
        assert_eq!(users.len(), 0);

        let users = adapter
            .query::<User>(QueryPlan::default().with_filter(User::INDEXES.email, "test@gmail.com"))
            .await;
        assert_eq!(users.len(), 1);

        let mut post_1 = Post::default();
        post_1.set_owner(user.id());

        adapter.insert(&post_1).await.unwrap();

        post_1.status = PostStatus::Published;
        adapter.update(&mut post_1).await.unwrap();

        let _post = adapter.fetch_by_id::<Post>(post_1.id()).await.unwrap();
        assert_eq!(_post.id(), post_1.id());

        let posts = adapter
            .query::<Post>(
                QueryPlan::new(user.id()).with_filter(Post::INDEXES.status, PostStatus::Published),
            )
            .await;
        assert_eq!(posts.len(), 1);
    }
}
