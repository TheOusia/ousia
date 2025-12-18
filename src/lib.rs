mod adapters;
mod edges;
pub mod error;
mod object;
pub(crate) mod query;

use crate::adapters::Adapter;
use crate::adapters::ObjectRecord;
use crate::adapters::QueryContext;
pub use crate::edges::meta::*;
pub use crate::edges::traits::*;
use crate::error::Error;
pub use crate::object::meta::*;
pub use crate::object::traits::*;
use chrono::Utc;
use ulid::Ulid;

#[cfg(feature = "derive")]
pub use ousia_derive::*;

pub struct Engine {
    adapter: Box<dyn Adapter>,
}

impl Engine {
    pub fn new(adapter: Box<dyn Adapter>) -> Self {
        Self { adapter }
    }

    pub async fn fetch_object<T: Object>(&self, id: Ulid) -> Result<Option<T>, Error> {
        let val = self.adapter.fetch_object(id).await?;
        match val {
            Some(record) => record.to_object().map(|o| Some(o)),
            None => Ok(None),
        }
    }

    async fn update_object<T: Object>(&self, i: &mut T) -> Result<(), Error> {
        let meta = i.meta_mut();
        meta.updated_at = Utc::now();

        self.adapter
            .update_object(ObjectRecord::from_object(i))
            .await
    }

    async fn preload_object<'a, T: Object>(&'a self, id: Ulid) -> QueryContext<'a, T> {
        self.adapter.preload_object(id).await
    }
}

#[cfg(test)]
mod test {
    use serde::{Deserialize, Serialize};

    use crate::{
        adapters::{Adapter, postgres::PostgresAdapter},
        edges::{Edge, meta::EdgeMeta},
        object::SYSTEM_OWNER,
        query::IndexMeta,
    };

    use super::*;

    #[derive(OusiaObject, OusiaDefault, Debug, Clone)]
    #[ousia(type_name = "User", index = "name:search")]
    struct User {
        _meta: Meta,
        name: String,
    }

    #[test]
    fn test_object_ownership() {
        let user = User::default();
        assert!(!user.is_system_owned());
    }

    #[test]
    fn test_index_meta() {
        let mut user = User::default();
        user.name = "John Doe".to_string();

        assert_eq!(
            user.index_meta()
                .meta()
                .get("name")
                .map(|ik| ik.as_string().unwrap()),
            Some("John Doe")
        );
    }

    #[test]
    fn test_query() {
        let mut user = User::default();
        user.name = "John Doe".to_string();

        assert_eq!(User::FIELDS.name.name, "name");
    }

    #[tokio::test]
    async fn test_transfer_wrong_owner_fails() {
        // create object owned by A
        // attempt transfer from B → C
        // expect Error::NotFound or Error::Unauthorized
    }

    #[tokio::test]
    async fn engine_test() {
        #[derive(Debug, Serialize, Deserialize)] // #[derive(Debug, OusiaEdge)]
        // #[edge(type_name = "Follow", from = User, to = User)]
        struct Follow {
            // #[edge(meta)] // explicitly mark the meta field
            _meta: EdgeMeta, // default meta field is _meta
            notification: bool,
        }

        // This is macro derived
        impl Edge for Follow {
            const TYPE: &'static str = "Follow";

            type From = User;
            type To = User;

            fn meta(&self) -> &EdgeMeta {
                &self._meta
            }
            fn meta_mut(&mut self) -> &mut EdgeMeta {
                &mut self._meta
            }

            fn index_meta(&self) -> IndexMeta {
                IndexMeta::default()
            }
        }

        let (_resource, pool) = super::adapters::postgres::tests::setup_test_db().await;

        let adapter = PostgresAdapter::new(pool);
        adapter.init_schema().await.unwrap();

        let mut user = User::default();
        user.name = "John Doe".to_string();
        sqlx::query(
            "
        INSERT INTO public.objects (id, type, owner, created_at, updated_at, data, index_meta)
            VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(user.id().to_string())
        .bind(User::TYPE)
        .bind(user.owner().to_string())
        .bind(user.created_at())
        .bind(user.updated_at())
        .bind(serde_json::to_value(&user).unwrap())
        .bind(serde_json::to_value(&user.index_meta()).unwrap())
        .execute(&adapter.pool)
        .await
        .unwrap();

        let mut user_follower = User::default();
        user_follower.name = "Eric Alleman".to_string();
        sqlx::query(
            "
        INSERT INTO public.objects (id, type, owner, created_at, updated_at, data, index_meta)
            VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(user_follower.id().to_string())
        .bind(User::TYPE)
        .bind(user_follower.owner().to_string())
        .bind(user_follower.created_at())
        .bind(user_follower.updated_at())
        .bind(serde_json::to_value(&user_follower).unwrap())
        .bind(serde_json::to_value(&user_follower.index_meta()).unwrap())
        .execute(&adapter.pool)
        .await
        .unwrap();

        let follow = Follow {
            _meta: EdgeMeta::new(user.id(), user_follower.id()),
            notification: true,
        };

        sqlx::query(
            r#"
        INSERT INTO public.edges ("from", "to", type, data, index_meta)
            VALUES ($1, $2, $3, $4, $5)"#,
        )
        .bind(follow.from().to_string())
        .bind(follow.to().to_string())
        .bind(Follow::TYPE)
        .bind(serde_json::to_value(&follow).unwrap())
        .bind(serde_json::to_value(&follow.index_meta()).unwrap())
        .execute(&adapter.pool)
        .await
        .unwrap();

        let engine = Engine::new(Box::new(adapter));

        let user_option = engine.fetch_object::<User>(user.id()).await.unwrap(); // easiest way to get the user.
        println!("Loaded user: {:#?}", user_option); // Success

        let query_context = engine.preload_object::<User>(user.id()).await; // We return a context that allows further querying.
        let user_option = query_context.get().await.unwrap(); // Option<User>

        let followers = query_context
            .edge::<Follow, User>()
            .where_eq(&User::FIELDS.name, "John Doe")
            .collect() // unimplemented
            .await;

        engine.update_object(&mut user).await.unwrap();
    }
}
