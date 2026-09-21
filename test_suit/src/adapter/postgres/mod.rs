//! Postgres integration tests, one file per area. Each test starts its own
//! container via `setup_test_db`.

mod object_crud;
mod object_queries;
mod object_query_values;
mod sorting_pagination;
mod ownership;
mod unions;
mod edge_crud;
mod edge_timestamps;
mod edge_queries;
mod traversal;
mod multi_pivot;
mod unique;
mod sequences;
mod schema_evolution;
mod geo;
mod batch_traversal;
mod two_hop;
mod field_drift;

use std::time::Duration;

use super::*;
use ousia::{
    EdgeMeta, EdgeMetaTrait, EdgeQuery, Engine, Error, Object, ObjectMeta, ObjectOwnership, Query,
    Union,
    adapters::{ObjectRecord, postgres::PostgresAdapter},
    filter, system_owner,
};
use sqlx::PgPool;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

use ousia::adapters::Adapter;

async fn setup_test_db() -> (ContainerAsync<Postgres>, PgPool) {
    use sqlx::postgres::PgPoolOptions;
    use testcontainers::{ImageExt, runners::AsyncRunner as _};

    let postgres = Postgres::default()
        .with_password("postgres")
        .with_user("postgres")
        .with_db_name("postgres")
        .with_name("imresamu/postgis")
        .with_tag("16-3.6-alpine")
        .start()
        .await
        .expect("Failed to start Postgres");

    tokio::time::sleep(Duration::from_secs(3)).await;

    let port = postgres.get_host_port_ipv4(5432).await.unwrap();
    let db_url = format!("postgres://postgres:postgres@localhost:{}/postgres", port);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .expect("Failed to connect to Postgres");

    (postgres, pool)
}

async fn seed_variants(engine: &Engine, rows: &[(&str, i64, &[&str])]) {
    for (name, count, tags) in rows {
        let mut v = Variants::default();
        v.name = (*name).into();
        v.count = *count;
        v.tags = tags.iter().map(|t| t.to_string()).collect();
        engine.create_object(&v).await.unwrap();
    }
}

fn sorted_names(rows: &[Variants]) -> Vec<String> {
    let mut names: Vec<String> = rows.iter().map(|v| v.name.clone()).collect();
    names.sort();
    names
}
