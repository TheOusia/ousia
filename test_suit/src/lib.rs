mod adapter;

#[cfg(test)]
#[tokio::test]
async fn test_view() {
    use std::time::Duration;
    use ousia::ObjectMeta;
    use ousia::{Engine, Meta, OusiaDefault, OusiaObject, adapters::postgres::PostgresAdapter};
    use sqlx::postgres::PgPoolOptions;
    use testcontainers::{ImageExt, runners::AsyncRunner as _};
    use testcontainers_modules::postgres::Postgres;

    #[derive(OusiaObject, OusiaDefault, Debug, Clone)]
    pub struct User {
        #[ousia_meta(view(dashboard = "id, owner, created_at, updated_at"))]
        #[ousia_meta(view(api = "id"))]
        _meta: Meta,

        #[ousia(view(dashboard))]
        pub username: String,

        #[ousia(view(dashboard))]
        pub email: String,

        #[ousia(view(dashboard), view(api))]
        pub display_name: String,

        #[ousia(private)]
        password: String,
    }

    let postgres = Postgres::default()
        .with_password("postgres")
        .with_user("postgres")
        .with_db_name("postgres")
        .with_name("postgis/postgis")
        .with_tag("16-3.4-alpine")
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

    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut user = User::default();
    user.display_name = "Owner".to_string();
    user.email = "owner@example.com".to_string();
    user.username = "user1".to_string();
    user.password = "encrypted_password".to_string();
    engine.create_object(&user).await.unwrap();

    let api_view = user._api();
    assert_eq!(api_view.id, user.id());
    assert_eq!(&api_view.display_name, &user.display_name);

    let dashboard_view = user._dashboard();

    assert_eq!(&dashboard_view.id, &user.id());
    assert_eq!(&dashboard_view.username, &user.username);
    assert_eq!(&dashboard_view.email, &user.email);
    assert_eq!(&dashboard_view.display_name, &user.display_name);
    assert_eq!(&dashboard_view.created_at, &user.created_at());
}
