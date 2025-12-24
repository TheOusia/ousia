mod adapter;

mod test_view {
    use ousia::{
        Engine, Meta, ObjectMeta, OusiaDefault, OusiaObject, adapters::sqlite::SqliteAdapter,
    };

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

    #[tokio::test]
    async fn test_view() {
        let adapter = SqliteAdapter::new_memory().await.unwrap();
        adapter.init_schema().await.unwrap();

        let engine = Engine::new(Box::new(adapter));

        // Create owner
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
}
