use super::PostgresAdapter;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    adapters::{EdgeQuery, EdgeRecord, EdgeTraversal, Error, ObjectRecord, TraversalDirection},
    query::QueryFilter,
};

#[async_trait::async_trait]
impl EdgeTraversal for PostgresAdapter {
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
        let where_clause = Self::build_batch_traversal_conditions(
            TraversalDirection::Forward,
            obj_filters,
            &plan.filters,
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
        let mut query = sqlx::query(&sql)
            .bind(obj_type)
            .bind(edge_type)
            .bind(from_ids);
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
        let where_clause = Self::build_batch_traversal_conditions(
            TraversalDirection::Reverse,
            obj_filters,
            &plan.filters,
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
        let mut query = sqlx::query(&sql)
            .bind(obj_type)
            .bind(edge_type)
            .bind(to_ids);
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
        let where_clause =
            Self::build_batch_edge_only_conditions(TraversalDirection::Forward, &plan.filters);
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
        let mut query = sqlx::query(&sql).bind(edge_type).bind(from_ids);
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
        let where_clause =
            Self::build_batch_edge_only_conditions(TraversalDirection::Reverse, &plan.filters);
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
        let mut query = sqlx::query(&sql).bind(edge_type).bind(to_ids);
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
            r#"
            {sel} FROM edges e JOIN objects o ON e."to" = o.id {fwd_where}
            UNION ALL
            {sel} FROM edges e JOIN objects o ON e."from" = o.id {rev_where}
            "#,
        );
        let mut query = sqlx::query(&sql).bind(obj_type).bind(edge_type).bind(pivot);
        query = Self::query_bind_filters(query, obj_filters);
        query = Self::query_bind_filters(query, &plan.filters);
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let mut fwd: Vec<(EdgeRecord, ObjectRecord)> = Vec::new();
        let mut rev: Vec<(EdgeRecord, ObjectRecord)> = Vec::new();
        for row in rows {
            // forward: edge.from == pivot; reverse: edge.to == pivot
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
            r#"
            SELECT e."from", e."to", e.type, e.data, e.index_meta
            FROM edges e {fwd_where}
            UNION ALL
            SELECT e."from", e."to", e.type, e.data, e.index_meta
            FROM edges e {rev_where}
            "#,
        );
        let mut query = sqlx::query(&sql).bind(edge_type).bind(pivot);
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
        let where_clause =
            Self::build_batch_edge_only_conditions(TraversalDirection::Forward, &plan.filters);
        let sql = format!(
            r#"
            SELECT e."from", COUNT(*) AS cnt
            FROM edges e
            {where_clause}
            GROUP BY e."from"
            "#,
        );
        let mut query = sqlx::query(&sql).bind(edge_type).bind(from_ids);
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
        let where_clause =
            Self::build_batch_edge_only_conditions(TraversalDirection::Reverse, &plan.filters);
        let sql = format!(
            r#"
            SELECT e."to", COUNT(*) AS cnt
            FROM edges e
            {where_clause}
            GROUP BY e."to"
            "#,
        );
        let mut query = sqlx::query(&sql).bind(edge_type).bind(to_ids);
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
