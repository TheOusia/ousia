#[cfg(feature = "ledger")]
use std::sync::Arc;

use chrono::Utc;

use super::PostgresAdapter;
use uuid::Uuid;

use crate::{
    adapters::{Adapter, EdgeQuery, EdgeRecord, Error, ObjectRecord, Query, TraversalDirection},
    query::QueryFilter,
};

#[async_trait::async_trait]
impl Adapter for PostgresAdapter {
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
        .bind(ids)
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
            WHERE id = $1 AND owner = $2 AND type = $3
            RETURNING id, type, owner, created_at, updated_at, data
            "#,
        )
        .bind(id)
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
        let order_clause = Self::build_order_clause(filters, false);

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
        let order_clause = Self::build_order_clause(&plan.filters, false);

        if plan.owner.is_nil() {
            where_clause = where_clause.replace("owner = ", "owner > ");
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

    async fn fetch_owned_objects_batch(
        &self,
        type_name: &'static str,
        owner_ids: &[Uuid],
    ) -> Result<Vec<ObjectRecord>, Error> {
        let rows = sqlx::query(
            r#"
            SELECT o.id, o.type, o.owner, o.created_at, o.updated_at, o.data
            FROM objects o
            WHERE type = $1 AND owner = ANY($2)
            "#,
        )
        .bind(type_name)
        .bind(owner_ids)
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
        .bind(ids)
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
        owner: Uuid,
        plan: EdgeQuery,
    ) -> Result<Vec<EdgeRecord>, Error> {
        self.query_edges_internal(type_name, owner, plan, TraversalDirection::Reverse)
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
                .expect("Failed to fetch sequence value");
        val as u64
    }

    async fn sequence_next_value(&self, sq: String) -> u64 {
        // Upsert: insert with value=2 on first call, otherwise increment.
        // This matches SQLite semantics: first sequence_value = 1, first next = 2.
        let next_val: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO sequences (name, value) VALUES ($1, 2)
            ON CONFLICT (name) DO UPDATE SET value = sequences.value + 1
            RETURNING value
            "#,
        )
        .bind(&sq)
        .fetch_one(&self.pool)
        .await
        .expect("Failed to fetch next sequence value");
        next_val as u64
    }

    #[cfg(feature = "ledger")]
    fn ledger_adapter(&self) -> Option<Arc<dyn ledger::LedgerAdapter>> {
        Some(Arc::new(PostgresAdapter::from_pool(self.pool.clone())))
    }
}
