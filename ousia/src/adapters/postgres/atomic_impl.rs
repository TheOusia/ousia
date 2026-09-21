//! Object writes that touch `objects`, `object_constraints` and `object_geo`
//! together, each applied in a single transaction.

use sqlx::{PgConnection, postgres::PgRow};
use uuid::Uuid;

use super::PostgresAdapter;
use crate::{
    adapters::{Error, ObjectRecord},
    query::GeoPoint,
};

fn storage(e: sqlx::Error) -> Error {
    Error::Storage(e.to_string())
}

impl PostgresAdapter {
    pub(super) async fn create_object_tx(
        &self,
        record: ObjectRecord,
        unique: Vec<(String, &'static str)>,
        geo: Vec<GeoPoint>,
    ) -> Result<(), Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        Self::insert_object_row(&mut tx, &record).await?;
        Self::add_constraints(&mut tx, &record.type_name, record.id, &unique).await?;
        let points: Vec<&GeoPoint> = geo.iter().collect();
        Self::upsert_geo_rows(&mut tx, &record.type_name, record.id, &points).await?;
        tx.commit().await.map_err(storage)
    }

    /// `unique` / `geo` are the full desired sets; `None` leaves that table untouched.
    pub(super) async fn update_object_tx(
        &self,
        record: ObjectRecord,
        unique: Option<Vec<(String, &'static str)>>,
        geo: Option<Vec<GeoPoint>>,
    ) -> Result<(), Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        // The UPDATE takes the row lock, so every later read in this
        // transaction sees this object's constraints/geo rows stably.
        if !Self::update_object_row(&mut tx, &record).await? {
            return Err(Error::NotFound);
        }
        if let Some(unique) = unique {
            Self::sync_constraints(&mut tx, &record.type_name, record.id, &unique).await?;
        }
        if let Some(geo) = geo {
            Self::sync_geo(&mut tx, &record.type_name, record.id, &geo).await?;
        }
        tx.commit().await.map_err(storage)
    }

    /// Moves the object only if its stored `data` still equals `snapshot` (the
    /// bytes `unique` was derived from). `Ok(None)` means it changed; re-read and retry.
    pub(super) async fn transfer_object_tx(
        &self,
        type_name: &'static str,
        id: Uuid,
        from_owner: Uuid,
        to_owner: Uuid,
        snapshot: Vec<u8>,
        unique: Vec<(String, &'static str)>,
    ) -> Result<Option<ObjectRecord>, Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let row: Option<PgRow> = sqlx::query(
            r#"
            UPDATE objects
            SET updated_at = now(), owner = $4
            WHERE id = $1 AND owner = $2 AND type = $3 AND data = $5
            RETURNING id, owner, created_at, updated_at, data
            "#,
        )
        .bind(id)
        .bind(from_owner)
        .bind(type_name)
        .bind(to_owner)
        .bind(&snapshot)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?;

        let Some(row) = row else {
            let still_owned: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM objects WHERE id = $1 AND owner = $2 AND type = $3)",
            )
            .bind(id)
            .bind(from_owner)
            .bind(type_name)
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
            return if still_owned { Ok(None) } else { Err(Error::NotFound) };
        };

        Self::sync_constraints(&mut tx, type_name, id, &unique).await?;
        tx.commit().await.map_err(storage)?;
        Self::map_row_to_object_record_slim(row, Some(type_name)).map(Some)
    }

    pub(super) async fn insert_object_row(
        conn: &mut PgConnection,
        record: &ObjectRecord,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            INSERT INTO objects (id, type, owner, created_at, updated_at, data, index_meta)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(record.id)
        .bind(record.type_name.as_ref())
        .bind(record.owner)
        .bind(record.created_at)
        .bind(record.updated_at)
        .bind(&record.data)
        .bind(&record.index_meta)
        .execute(conn)
        .await
        .map_err(|err| {
            if err.to_string().contains("unique") {
                Error::UniqueConstraintViolation("id".to_string())
            } else {
                storage(err)
            }
        })?;
        Ok(())
    }

    /// Returns false when no object with this id and type exists.
    pub(super) async fn update_object_row(
        conn: &mut PgConnection,
        record: &ObjectRecord,
    ) -> Result<bool, Error> {
        let result = sqlx::query(
            r#"
            UPDATE objects
            SET updated_at = $3, data = $4, index_meta = $5
            WHERE id = $1 AND type = $2
            "#,
        )
        .bind(record.id)
        .bind(record.type_name.as_ref())
        .bind(record.updated_at)
        .bind(&record.data)
        .bind(&record.index_meta)
        .execute(conn)
        .await
        .map_err(storage)?;
        Ok(result.rows_affected() > 0)
    }

    async fn sync_constraints(
        conn: &mut PgConnection,
        type_name: &str,
        id: Uuid,
        wanted: &[(String, &'static str)],
    ) -> Result<(), Error> {
        let current: Vec<String> =
            sqlx::query_scalar("SELECT key FROM object_constraints WHERE type = $1 AND id = $2")
                .bind(type_name)
                .bind(id)
                .fetch_all(&mut *conn)
                .await
                .map_err(storage)?;

        let stale: Vec<&str> = current
            .iter()
            .filter(|k| !wanted.iter().any(|(w, _)| w == *k))
            .map(String::as_str)
            .collect();
        if !stale.is_empty() {
            sqlx::query(
                "DELETE FROM object_constraints WHERE type = $1 AND id = $2 AND key = ANY($3)",
            )
            .bind(type_name)
            .bind(id)
            .bind(&stale)
            .execute(&mut *conn)
            .await
            .map_err(storage)?;
        }

        let missing: Vec<(String, &'static str)> = wanted
            .iter()
            .filter(|(k, _)| !current.contains(k))
            .cloned()
            .collect();
        Self::add_constraints(conn, type_name, id, &missing).await
    }

    /// Fails with `UniqueConstraintViolation` naming the first key another object holds.
    async fn add_constraints(
        conn: &mut PgConnection,
        type_name: &str,
        id: Uuid,
        keys: &[(String, &'static str)],
    ) -> Result<(), Error> {
        if keys.is_empty() {
            return Ok(());
        }
        let hashes: Vec<&str> = keys.iter().map(|(k, _)| k.as_str()).collect();
        let fields: Vec<&str> = keys.iter().map(|(_, f)| *f).collect();
        // DO NOTHING (not an error) keeps the transaction usable, and a
        // concurrent insert of the same key blocks until it commits.
        let inserted: Vec<String> = sqlx::query_scalar(
            r#"
            INSERT INTO object_constraints (id, type, key, field)
            SELECT $1, $2, k, f FROM UNNEST($3::text[], $4::text[]) AS t(k, f)
            ON CONFLICT (type, key) DO NOTHING
            RETURNING key
            "#,
        )
        .bind(id)
        .bind(type_name)
        .bind(&hashes)
        .bind(&fields)
        .fetch_all(conn)
        .await
        .map_err(storage)?;

        match keys.iter().find(|(k, _)| !inserted.contains(k)) {
            Some((_, field)) => Err(Error::UniqueConstraintViolation(field.to_string())),
            None => Ok(()),
        }
    }

    async fn sync_geo(
        conn: &mut PgConnection,
        type_name: &str,
        id: Uuid,
        wanted: &[GeoPoint],
    ) -> Result<(), Error> {
        let current: Vec<(String, String)> = sqlx::query_as(
            "SELECT field, hash FROM object_geo WHERE type = $1 AND object_id = $2",
        )
        .bind(type_name)
        .bind(id)
        .fetch_all(&mut *conn)
        .await
        .map_err(storage)?;

        let stale: Vec<&str> = current
            .iter()
            .filter(|(f, _)| !wanted.iter().any(|p| p.field == f))
            .map(|(f, _)| f.as_str())
            .collect();
        if !stale.is_empty() {
            sqlx::query(
                "DELETE FROM object_geo WHERE type = $1 AND object_id = $2 AND field = ANY($3)",
            )
            .bind(type_name)
            .bind(id)
            .bind(&stale)
            .execute(&mut *conn)
            .await
            .map_err(storage)?;
        }

        let changed: Vec<&GeoPoint> = wanted
            .iter()
            .filter(|p| !current.iter().any(|(f, h)| f == p.field && *h == p.hash))
            .collect();
        Self::upsert_geo_rows(conn, type_name, id, &changed).await
    }

    async fn upsert_geo_rows(
        conn: &mut PgConnection,
        type_name: &str,
        id: Uuid,
        points: &[&GeoPoint],
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
            INSERT INTO object_geo (object_id, type, field, location, hash)
            SELECT $1, $2, f.field,
                   ST_SetSRID(ST_MakePoint(f.lon, f.lat), 4326)::geography,
                   f.hash
            FROM UNNEST($3::text[], $4::float8[], $5::float8[], $6::text[])
                AS f(field, lon, lat, hash)
            ON CONFLICT (type, object_id, field) DO UPDATE
                SET location = EXCLUDED.location,
                    hash     = EXCLUDED.hash
            "#,
        )
        .bind(id)
        .bind(type_name)
        .bind(&fields)
        .bind(&lons)
        .bind(&lats)
        .bind(&hashes)
        .execute(conn)
        .await
        .map_err(storage)?;
        Ok(())
    }
}
