use super::PostgresAdapter;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    adapters::{Error, GeoAdapter},
    query::GeoPoint,
};

#[async_trait::async_trait]
impl GeoAdapter for PostgresAdapter {
    async fn upsert_geo_points(
        &self,
        type_name: &str,
        object_id: Uuid,
        points: Vec<GeoPoint>,
    ) -> Result<(), Error> {
        if points.is_empty() {
            return Ok(());
        }

        let fields: Vec<&str> = points.iter().map(|p| p.field).collect();
        let lons: Vec<f64> = points.iter().map(|p| p.lon).collect();
        let lats: Vec<f64> = points.iter().map(|p| p.lat).collect();
        let hashes: Vec<&str> = points.iter().map(|p| p.hash.as_str()).collect();

        sqlx::query(
            r#"
            INSERT INTO public.object_geo (object_id, type, field, location, hash)
            SELECT $1, $2,
                   f.field,
                   ST_SetSRID(ST_MakePoint(f.lon, f.lat), 4326)::geography,
                   f.hash
            FROM UNNEST($3::text[], $4::float8[], $5::float8[], $6::text[])
                AS f(field, lon, lat, hash)
            ON CONFLICT (object_id, field) DO UPDATE
                SET location = EXCLUDED.location,
                    hash     = EXCLUDED.hash,
                    type     = EXCLUDED.type
            "#,
        )
        .bind(object_id)
        .bind(type_name)
        .bind(&fields)
        .bind(&lons)
        .bind(&lats)
        .bind(&hashes)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn get_geo_hashes(&self, object_id: Uuid) -> Result<Vec<(String, String)>, Error> {
        let rows = sqlx::query(
            r#"
            SELECT field, hash FROM public.object_geo WHERE object_id = $1
            "#,
        )
        .bind(object_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let field: String = row.try_get("field").unwrap();
                let hash: String = row.try_get("hash").unwrap();
                (field, hash)
            })
            .collect())
    }

    async fn delete_geo_fields(&self, object_id: Uuid, fields: Vec<String>) -> Result<(), Error> {
        if fields.is_empty() {
            return Ok(());
        }
        sqlx::query(
            r#"
            DELETE FROM public.object_geo WHERE object_id = $1 AND field = ANY($2)
            "#,
        )
        .bind(object_id)
        .bind(&fields)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    async fn delete_geo_for_object(&self, object_id: Uuid) -> Result<(), Error> {
        sqlx::query(
            r#"
            DELETE FROM public.object_geo WHERE object_id = $1
            "#,
        )
        .bind(object_id)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }
}
