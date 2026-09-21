use super::PostgresAdapter;
use sqlx::Row;
use uuid::Uuid;

use crate::adapters::{Error, UniqueAdapter};

#[async_trait::async_trait]
impl UniqueAdapter for PostgresAdapter {
    async fn insert_unique_hashes(
        &self,
        type_name: &str,
        object_id: Uuid,
        hashes: Vec<(String, &str)>,
    ) -> Result<(), Error> {
        if hashes.is_empty() {
            return Ok(());
        }

        let keys: Vec<&str> = hashes.iter().map(|(k, _)| k.as_str()).collect();
        let fields: Vec<&str> = hashes.iter().map(|(_, f)| *f).collect();

        let result = sqlx::query(
            r#"
            INSERT INTO unique_constraints (id, type, key, field)
            SELECT $1, $2, unnest($3::text[]), unnest($4::text[])
            "#,
        )
        .bind(object_id)
        .bind(type_name)
        .bind(&keys)
        .bind(&fields)
        .execute(&self.pool)
        .await;

        match result {
            Ok(_) => Ok(()),
            Err(err) if err.to_string().contains("unique constraint") => {
                // Find which key already exists to report the correct field name.
                let conflicting: Vec<String> =
                    sqlx::query_scalar("SELECT key FROM unique_constraints WHERE key = ANY($1)")
                        .bind(&keys)
                        .fetch_all(&self.pool)
                        .await
                        .unwrap_or_default();

                let field = hashes
                    .iter()
                    .find(|(k, _)| conflicting.iter().any(|c| c == k))
                    .map(|(_, f)| *f)
                    .unwrap_or("unknown");

                Err(Error::UniqueConstraintViolation(field.to_string()))
            }
            Err(err) => Err(Error::Storage(err.to_string())),
        }
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

    async fn delete_unique_for_object(&self, object_id: Uuid) -> Result<(), Error> {
        sqlx::query("DELETE FROM unique_constraints WHERE id = $1")
            .bind(object_id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    async fn delete_unique_for_objects(
        &self,
        type_name: &str,
        owner: Uuid,
        ids: Vec<Uuid>,
    ) -> Result<(), Error> {
        if ids.is_empty() {
            return Ok(());
        }
        sqlx::query(
            r#"
            DELETE FROM unique_constraints uc
            USING objects o
            WHERE uc.id = o.id
              AND o.id = ANY($1)
              AND o.type = $2
              AND o.owner = $3
            "#,
        )
        .bind(&ids)
        .bind(type_name)
        .bind(owner)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    async fn delete_unique_for_owned(
        &self,
        type_name: &str,
        owner: Uuid,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            DELETE FROM unique_constraints uc
            USING objects o
            WHERE uc.id = o.id
              AND o.type = $1
              AND o.owner = $2
            "#,
        )
        .bind(type_name)
        .bind(owner)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }
}
