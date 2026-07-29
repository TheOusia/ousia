#[cfg(feature = "ledger")]
use std::sync::Arc;

use chrono::Utc;

use super::PostgresAdapter;
use uuid::Uuid;

use crate::{
    adapters::{Adapter, EdgeQuery, EdgeRecord, Error, ObjectRecord, Query, TraversalDirection},
    query::QueryFilter,
};
use sqlx::Row;

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
            SELECT o.id, o.owner, o.created_at, o.updated_at, o.data
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
            Some(r) => Self::map_row_to_object_record_slim(r, Some(type_name)).map(Some),
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
            SELECT o.id, o.owner, o.created_at, o.updated_at, o.data
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
            .map(|row| Self::map_row_to_object_record_slim(row, Some(type_name)))
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
            RETURNING id, owner, created_at, updated_at, data
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

        Self::map_row_to_object_record_slim(row, Some(type_name))
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
            RETURNING id, owner, created_at, updated_at, data
            "#,
        )
        .bind(id)
        .bind(owner)
        .bind(type_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        match row {
            Some(r) => Self::map_row_to_object_record_slim(r, Some(type_name)).map(Some),
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
            SELECT o.id, o.owner, o.created_at, o.updated_at, o.data
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
            .map(|row| Self::map_row_to_object_record_slim(row, Some(type_name)).ok())
            .unwrap_or_default())
    }

    async fn query_objects(
        &self,
        type_name: &'static str,
        plan: Query,
    ) -> Result<Vec<ObjectRecord>, Error> {
        // ── Plan params (WHERE side) ────────────────────────────────────────
        let mut param_idx = 3;
        let (mut where_clause, geo_plan) = Self::build_object_query_conditions_with_geo(
            &plan.filters,
            plan.cursor,
            &plan.geo_filters,
            plan.geo_order.as_ref(),
            &mut param_idx,
        );
        // `param_idx` now points to the slot where bind_geo_filters will start
        // emitting its bindings. Reconstruct the per-geo-order lon/lat slot
        // indices so we can splice them into the ORDER BY suffix.
        let (order_lon_p, order_lat_p) =
            Self::compute_geo_order_param_slots(param_idx, &plan.geo_filters, &geo_plan);

        let scalar_order = Self::build_order_clause(&plan.filters, false);
        if plan.owner.is_nil() {
            where_clause = where_clause.replace("owner = ", "owner > ");
        }

        // ── Compose ORDER BY: geo distance first, then any scalar sort terms ─
        let order_clause = match Self::build_geo_order_suffix(
            plan.geo_order.as_ref(),
            &geo_plan,
            order_lon_p,
            order_lat_p,
        ) {
            Some(geo_term) => {
                if scalar_order.starts_with("ORDER BY ") {
                    format!(
                        "ORDER BY {}, {}",
                        geo_term,
                        &scalar_order["ORDER BY ".len()..]
                    )
                } else {
                    format!("ORDER BY {}", geo_term)
                }
            }
            None => scalar_order,
        };

        // ── Build full SQL once ─────────────────────────────────────────────
        let mut sql = format!(
            r#"
                SELECT o.id, o.owner, o.created_at, o.updated_at, o.data
                FROM objects o
                {}
                {}
                {}
                "#,
            geo_plan.joins, where_clause, order_clause
        );
        if let Some(limit) = plan.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        // ── Bind in the exact same order the placeholders were assigned ─────
        let mut query = sqlx::query(&sql).bind(type_name).bind(plan.owner);
        if let Some(cursor) = plan.cursor {
            query = query.bind(cursor.last_id);
        }
        query = Self::query_bind_filters(query, &plan.filters);
        query =
            Self::bind_geo_filters(query, &plan.geo_filters, plan.geo_order.as_ref(), &geo_plan);

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;

        Ok(rows
            .into_iter()
            .filter_map(|row| Self::map_row_to_object_record_slim(row, Some(type_name)).ok())
            .collect())
    }

    async fn query_objects_with_distance(
        &self,
        type_name: &'static str,
        plan: Query,
    ) -> Result<Vec<(ObjectRecord, f64)>, Error> {
        if plan.geo_order.is_none() {
            return Err(Error::InvalidQuery(
                "query_objects_with_distance requires order_by_distance(...) to be set".to_string(),
            ));
        }

        let mut param_idx = 3;
        let (mut where_clause, geo_plan) = Self::build_object_query_conditions_with_geo(
            &plan.filters,
            plan.cursor,
            &plan.geo_filters,
            plan.geo_order.as_ref(),
            &mut param_idx,
        );
        let scalar_order = Self::build_order_clause(&plan.filters, false);

        if plan.owner.is_nil() {
            where_clause = where_clause.replace("owner = ", "owner > ");
        }

        let (lon_p, lat_p) =
            Self::compute_geo_order_param_slots(param_idx, &plan.geo_filters, &geo_plan);
        let order_alias = geo_plan
            .order_alias
            .as_deref()
            .expect("geo_order set → order_alias planned");

        let geo_term =
            Self::build_geo_order_suffix(plan.geo_order.as_ref(), &geo_plan, lon_p, lat_p)
                .expect("geo_order set");
        let tail = if scalar_order.starts_with("ORDER BY ") {
            format!(", {}", &scalar_order["ORDER BY ".len()..])
        } else {
            String::new()
        };

        let sql = format!(
            r#"
                SELECT o.id, o.owner, o.created_at, o.updated_at, o.data,
                       ST_Distance({alias}.location, ST_SetSRID(ST_MakePoint(${lon}, ${lat}), 4326)::geography) AS __distance
                FROM objects o
                {joins}
                {where_}
                ORDER BY {geo_term}{tail}{limit}
            "#,
            alias = order_alias,
            lon = lon_p,
            lat = lat_p,
            joins = geo_plan.joins,
            where_ = where_clause,
            geo_term = geo_term,
            tail = tail,
            limit = plan
                .limit
                .map(|l| format!(" LIMIT {}", l))
                .unwrap_or_default(),
        );

        let mut query = sqlx::query(&sql).bind(type_name).bind(plan.owner);
        if let Some(cursor) = plan.cursor {
            query = query.bind(cursor.last_id);
        }
        query = Self::query_bind_filters(query, &plan.filters);
        query =
            Self::bind_geo_filters(query, &plan.geo_filters, plan.geo_order.as_ref(), &geo_plan);

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let distance: f64 = row
                .try_get("__distance")
                .map_err(|e| Error::Deserialize(e.to_string()))?;
            let rec = Self::map_row_to_object_record_slim(row, Some(type_name))?;
            out.push((rec, distance));
        }
        Ok(out)
    }

    async fn count_objects(
        &self,
        type_name: &'static str,
        plan: Option<Query>,
    ) -> Result<u64, Error> {
        match plan {
            Some(plan) => {
                let mut param_idx = 3;
                let (mut where_clause, geo_plan) = Self::build_object_query_conditions_with_geo(
                    &plan.filters,
                    None,
                    &plan.geo_filters,
                    plan.geo_order.as_ref(),
                    &mut param_idx,
                );

                if plan.owner.is_nil() {
                    where_clause = where_clause.replace("owner = ", "owner > ");
                }

                let mut sql = format!(
                    r#"
                    SELECT COUNT(*) FROM objects o
                    {}
                    {}
                    "#,
                    geo_plan.joins, where_clause
                );

                if let Some(limit) = plan.limit {
                    sql.push_str(&format!(" LIMIT {}", limit));
                }

                let mut query = sqlx::query_scalar::<_, i64>(&sql)
                    .bind(type_name)
                    .bind(plan.owner);

                query = Self::query_scalar_bind_filters(query, &plan.filters);
                query = Self::bind_geo_filters_scalar(
                    query,
                    &plan.geo_filters,
                    plan.geo_order.as_ref(),
                    &geo_plan,
                );

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
            SELECT o.id, o.owner, o.created_at, o.updated_at, o.data
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
            .map(|row| Self::map_row_to_object_record_slim(row, Some(type_name)))
            .collect()
    }

    async fn count_owned_objects_batch(
        &self,
        type_name: &'static str,
        owner_ids: &[Uuid],
    ) -> Result<Vec<(Uuid, u64)>, Error> {
        let rows = sqlx::query(
            r#"
            SELECT owner, COUNT(*) AS cnt
            FROM objects
            WHERE type = $1 AND owner = ANY($2)
            GROUP BY owner
            "#,
        )
        .bind(type_name)
        .bind(owner_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        rows.into_iter()
            .map(|row| {
                let id: Uuid = row
                    .try_get("owner")
                    .map_err(|e| Error::Deserialize(e.to_string()))?;
                let cnt: i64 = row
                    .try_get("cnt")
                    .map_err(|e| Error::Deserialize(e.to_string()))?;
                Ok((id, cnt as u64))
            })
            .collect()
    }

    async fn fetch_owned_objects(
        &self,
        type_name: &'static str,
        owner: Uuid,
    ) -> Result<Vec<ObjectRecord>, Error> {
        let rows = sqlx::query(
            r#"
            SELECT o.id, o.owner, o.created_at, o.updated_at, o.data
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
            .map(|row| Self::map_row_to_object_record_slim(row, Some(type_name)))
            .collect()
    }

    async fn fetch_owned_object(
        &self,
        type_name: &'static str,
        owner: Uuid,
    ) -> Result<Option<ObjectRecord>, Error> {
        let row = sqlx::query(
            r#"
            SELECT o.id, o.owner, o.created_at, o.updated_at, o.data
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
            Some(r) => Self::map_row_to_object_record_slim(r, Some(type_name)).map(Some),
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
            Some(r) => Self::map_row_to_object_record_slim(r, None).map(Some),
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
            .map(|row| Self::map_row_to_object_record_slim(row, None))
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
            Some(r) => Self::map_row_to_object_record_slim(r, None).map(Some),
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
            .map(|row| Self::map_row_to_object_record_slim(row, None))
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
            created_at,
            updated_at,
        } = record;
        let _ = sqlx::query(
            r#"
            INSERT INTO object_edges ("from", "to", type, data, index_meta, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT ("from", type, "to")
            DO UPDATE SET data = $4, index_meta = $5, updated_at = $7;
            "#,
        )
        .bind(from)
        .bind(to)
        .bind(type_name.as_ref())
        .bind(data)
        .bind(index_meta)
        .bind(created_at)
        .bind(updated_at)
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
        UPDATE object_edges SET data = $1, "to" = $2, updated_at = $3
        WHERE "from" = $4 AND type = $5 AND "to" = $6
        "#,
        )
        .bind(data)
        .bind(to.unwrap_or(old_to))
        .bind(Utc::now())
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
            DELETE FROM object_edges
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
            DELETE FROM object_edges
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

    async fn fetch_edge(
        &self,
        type_name: &'static str,
        from: Uuid,
        to: Uuid,
    ) -> Result<Option<EdgeRecord>, Error> {
        let row = sqlx::query(
            r#"
        SELECT e."from", e."to", e.data, e.created_at, e.updated_at
        FROM object_edges e
        WHERE type = $1 AND "from" = $2 AND "to" = $3
        "#,
        )
        .bind(type_name)
        .bind(from)
        .bind(to)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| Error::Storage(err.to_string()))?;

        let Some(row) = row else {
            return Ok(None);
        };

        Self::map_row_to_edge_record(row, type_name).map(Some)
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
                SELECT COUNT(*) FROM object_edges e
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
                    r#"SELECT COUNT(*) FROM object_edges WHERE type = $1 AND "from" = $2"#,
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
                SELECT COUNT(*) FROM object_edges
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
                    SELECT COUNT(*) FROM object_edges WHERE type = $1 AND "to" = $2
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
        // Convention: first `sequence_value` returns 1, first `sequence_next_value` returns 2.
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
