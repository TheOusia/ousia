mod adapters;
mod edge;
pub mod error;
mod object;
pub(crate) mod query;

use crate::adapters::{Adapter, EdgeRecord, ObjectRecord, Query, QueryContext};
pub use crate::edge::meta::*;
pub use crate::edge::query::EdgeQuery;
pub use crate::edge::traits::*;
use crate::error::Error;
pub use crate::object::*;
use chrono::Utc;
pub use query::IndexQuery;
use ulid::Ulid;

#[cfg(feature = "derive")]
pub use ousia_derive::*;

/// The Engine is the primary interface for interacting with domain objects and edges.
/// It abstracts away storage details and provides a type-safe API.
pub struct Engine {
    adapter: Box<dyn Adapter>,
}

impl Engine {
    pub fn new(adapter: Box<dyn Adapter>) -> Self {
        Self { adapter }
    }

    // ==================== Object CRUD ====================

    /// Create a new object in storage
    pub async fn create_object<T: Object>(&self, obj: &T) -> Result<(), Error> {
        self.adapter
            .insert_object(ObjectRecord::from_object(obj))
            .await
    }

    /// Fetch an object by ID
    pub async fn fetch_object<T: Object>(&self, id: Ulid) -> Result<Option<T>, Error> {
        let val = self.adapter.fetch_object(id).await?;
        match val {
            Some(record) => record.to_object().map(Some),
            None => Ok(None),
        }
    }

    /// Fetch multiple objects by IDs
    pub async fn fetch_objects<T: Object>(&self, ids: Vec<Ulid>) -> Result<Vec<T>, Error> {
        let records = self.adapter.fetch_bulk_objects(ids).await?;
        records.into_iter().map(|r| r.to_object()).collect()
    }

    /// Update an existing object
    pub async fn update_object<T: Object>(&self, obj: &mut T) -> Result<(), Error> {
        let meta = obj.meta_mut();
        meta.updated_at = Utc::now();

        self.adapter
            .update_object(ObjectRecord::from_object(obj))
            .await
    }

    /// Delete an object
    pub async fn delete_object<T: Object>(
        &self,
        id: Ulid,
        owner: Ulid,
    ) -> Result<Option<T>, Error> {
        let record = self.adapter.delete_object(id, owner).await?;
        match record {
            Some(r) => r.to_object().map(Some),
            None => Ok(None),
        }
    }

    /// Transfer ownership of an object
    pub async fn transfer_object<T: Object>(
        &self,
        id: Ulid,
        from_owner: Ulid,
        to_owner: Ulid,
    ) -> Result<T, Error> {
        let record = self
            .adapter
            .transfer_object(id, from_owner, to_owner)
            .await?;
        record.to_object()
    }

    // ==================== Object Queries ====================

    /// Query objects with filters
    pub async fn query_objects<T: Object>(&self, query: Query) -> Result<Vec<T>, Error> {
        let records = self.adapter.query_objects(T::TYPE, query).await?;
        records.into_iter().map(|r| r.to_object()).collect()
    }

    /// Count objects matching query
    pub async fn count_objects<T: Object>(&self, query: Option<Query>) -> Result<u64, Error> {
        self.adapter.count_objects(T::TYPE, query).await
    }

    /// Fetch all objects owned by a specific owner
    pub async fn fetch_owned_objects<T: Object>(&self, owner: Ulid) -> Result<Vec<T>, Error> {
        let records = self.adapter.fetch_owned_objects(T::TYPE, owner).await?;
        records.into_iter().map(|r| r.to_object()).collect()
    }

    /// Fetch a single owned object (for one-to-one relationships)
    pub async fn fetch_owned_object<T: Object>(&self, owner: Ulid) -> Result<Option<T>, Error> {
        let record = self.adapter.fetch_owned_object(T::TYPE, owner).await?;
        match record {
            Some(r) => r.to_object().map(Some),
            None => Ok(None),
        }
    }

    // ==================== Edge Operations ====================

    /// Create a new edge
    pub async fn create_edge<E: Edge>(&self, edge: &E) -> Result<(), Error> {
        self.adapter.insert_edge(EdgeRecord::from_edge(edge)).await
    }

    /// Delete an edge
    pub async fn delete_edge<E: Edge>(&self, from: Ulid, to: Ulid) -> Result<(), Error> {
        self.adapter.delete_edge(E::TYPE, from, to).await
    }

    /// Query edges
    pub async fn query_edges<E: Edge>(
        &self,
        from: Ulid,
        query: EdgeQuery,
    ) -> Result<Vec<E>, Error> {
        let records = self.adapter.query_edges(E::TYPE, from, query).await?;
        records.into_iter().map(|r| r.to_edge()).collect()
    }

    /// Count edges
    pub async fn count_edges<E: Edge>(
        &self,
        from: Ulid,
        query: Option<EdgeQuery>,
    ) -> Result<u64, Error> {
        self.adapter.count_edges(E::TYPE, from, query).await
    }

    // ==================== Advanced Query API ====================

    /// Start a query context for complex traversals
    pub async fn preload_object<'a, T: Object>(&'a self, id: Ulid) -> QueryContext<'a, T> {
        self.adapter.preload_object(id).await
    }
}

#[cfg(test)]
mod test {
    use crate::{adapters::postgres::PostgresAdapter, edge::meta::EdgeMeta};

    use super::*;

    #[derive(OusiaObject, OusiaDefault, Debug, Clone)]
    #[ousia(type_name = "User", index = "name:search", index = "email:search")]
    struct User {
        _meta: Meta,
        name: String,
        email: String,
    }

    #[derive(OusiaObject, OusiaDefault, Debug, Clone)]
    #[ousia(type_name = "Post", index = "title:search+sort")]
    struct Post {
        _meta: Meta,
        title: String,
        content: String,
    }

    #[derive(Debug, OusiaEdge)]
    #[ousia(type_name = "Follow", index = "notification:search")]
    struct Follow {
        _meta: EdgeMeta,
        notification: bool,
    }

    impl Default for Follow {
        fn default() -> Self {
            Self {
                _meta: EdgeMeta::new(Ulid::nil(), Ulid::nil()),
                notification: false,
            }
        }
    }

    #[test]
    fn test_object_ownership_is_system_owned() {
        let user = User::default();
        assert!(user.is_system_owned());
    }

    #[test]
    fn test_object_ownership_not_system_owned() {
        let user = User {
            _meta: Meta::new_with_owner(Ulid::new()),
            name: "John Doe".to_string(),
            email: "john.doe@example.com".to_string(),
        };
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
    fn test_query_fields() {
        assert_eq!(User::FIELDS.name.name, "name");
        assert_eq!(User::FIELDS.email.name, "email");
    }

    #[tokio::test]
    async fn test_engine_create_and_fetch() {
        let (_resource, pool) = super::adapters::postgres::tests::setup_test_db().await;
        let adapter = PostgresAdapter::new(pool);
        adapter.init_schema().await.unwrap();

        let engine = Engine::new(Box::new(adapter));

        let mut user = User::default();
        user.name = "Alice".to_string();
        user.email = "alice@example.com".to_string();

        // Create
        engine.create_object(&user).await.unwrap();

        // Fetch
        let fetched: Option<User> = engine.fetch_object(user.id()).await.unwrap();
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.name, "Alice");
        assert_eq!(fetched.email, "alice@example.com");
    }

    #[tokio::test]
    async fn test_engine_update() {
        let (_resource, pool) = super::adapters::postgres::tests::setup_test_db().await;
        let adapter = PostgresAdapter::new(pool);
        adapter.init_schema().await.unwrap();

        let engine = Engine::new(Box::new(adapter));

        let mut user = User::default();
        user.name = "Bob".to_string();
        user.email = "bob@example.com".to_string();

        engine.create_object(&user).await.unwrap();

        // Update
        user.name = "Robert".to_string();
        engine.update_object(&mut user).await.unwrap();

        // Verify
        let fetched: Option<User> = engine.fetch_object(user.id()).await.unwrap();
        assert_eq!(fetched.unwrap().name, "Robert");
    }

    #[tokio::test]
    async fn test_engine_delete() {
        let (_resource, pool) = super::adapters::postgres::tests::setup_test_db().await;
        let adapter = PostgresAdapter::new(pool);
        adapter.init_schema().await.unwrap();

        let engine = Engine::new(Box::new(adapter));

        let mut user = User::default();
        user.name = "Charlie".to_string();
        user.email = "charlie@example.com".to_string();

        engine.create_object(&user).await.unwrap();

        // Delete
        let deleted: Option<User> = engine.delete_object(user.id(), user.owner()).await.unwrap();
        assert!(deleted.is_some());

        // Verify deleted
        let fetched: Option<User> = engine.fetch_object(user.id()).await.unwrap();
        assert!(fetched.is_none());
    }

    #[tokio::test]
    async fn test_engine_query() {
        let (_resource, pool) = super::adapters::postgres::tests::setup_test_db().await;
        let adapter = PostgresAdapter::new(pool);
        adapter.init_schema().await.unwrap();

        let engine = Engine::new(Box::new(adapter));

        // Create multiple users
        let mut alice = User::default();
        alice.name = "Alice".to_string();
        alice.email = "alice@example.com".to_string();

        let mut bob = User::default();
        bob.name = "Bob".to_string();
        bob.email = "bob@example.com".to_string();

        engine.create_object(&alice).await.unwrap();
        engine.create_object(&bob).await.unwrap();

        // Query by name
        let users: Vec<User> = engine
            .query_objects(Query::default().where_eq(&User::FIELDS.name, "Alice"))
            .await
            .unwrap();

        assert_eq!(users.len(), 1);
        assert_eq!(users[0].name, "Alice");
    }

    #[tokio::test]
    async fn test_engine_ownership() {
        let (_resource, pool) = super::adapters::postgres::tests::setup_test_db().await;
        let adapter = PostgresAdapter::new(pool);
        adapter.init_schema().await.unwrap();

        let engine = Engine::new(Box::new(adapter));

        // Create owner
        let mut owner = User::default();
        owner.name = "Owner".to_string();
        owner.email = "owner@example.com".to_string();
        engine.create_object(&owner).await.unwrap();

        // Create owned post
        let mut post = Post::default();
        post.set_owner(owner.id());
        post.title = "My First Post".to_string();
        post.content = "Hello, world!".to_string();
        engine.create_object(&post).await.unwrap();

        // Verify ownership
        assert!(post.is_owned_by(&owner));

        // Fetch owned objects
        let posts: Vec<Post> = engine.fetch_owned_objects(owner.id()).await.unwrap();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].title, "My First Post");
    }

    #[tokio::test]
    async fn test_engine_transfer_ownership() {
        let (_resource, pool) = super::adapters::postgres::tests::setup_test_db().await;
        let adapter = PostgresAdapter::new(pool);
        adapter.init_schema().await.unwrap();

        let engine = Engine::new(Box::new(adapter));

        // Create two users
        let mut alice = User::default();
        alice.name = "Alice".to_string();
        alice.email = "alice@example.com".to_string();
        engine.create_object(&alice).await.unwrap();

        let mut bob = User::default();
        bob.name = "Bob".to_string();
        bob.email = "bob@example.com".to_string();
        engine.create_object(&bob).await.unwrap();

        // Create post owned by Alice
        let mut post = Post::default();
        post.set_owner(alice.id());
        post.title = "Alice's Post".to_string();
        post.content = "Original content".to_string();
        engine.create_object(&post).await.unwrap();

        // Transfer to Bob
        let transferred: Post = engine
            .transfer_object(post.id(), alice.id(), bob.id())
            .await
            .unwrap();

        assert_eq!(transferred.owner(), bob.id());
    }

    #[tokio::test]
    async fn test_engine_edges() {
        let (_resource, pool) = super::adapters::postgres::tests::setup_test_db().await;
        let adapter = PostgresAdapter::new(pool);
        adapter.init_schema().await.unwrap();

        let engine = Engine::new(Box::new(adapter));

        // Create two users
        let mut alice = User::default();
        alice.name = "Alice".to_string();
        alice.email = "alice@example.com".to_string();
        engine.create_object(&alice).await.unwrap();

        let mut bob = User::default();
        bob.name = "Bob".to_string();
        bob.email = "bob@example.com".to_string();
        engine.create_object(&bob).await.unwrap();

        // Create follow edge: Alice follows Bob
        let follow = Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        };
        engine.create_edge(&follow).await.unwrap();

        // Query edges
        let follows: Vec<Follow> = engine
            .query_edges(alice.id(), EdgeQuery::default())
            .await
            .unwrap();

        assert_eq!(follows.len(), 1);
        assert_eq!(follows[0].from(), alice.id());
        assert_eq!(follows[0].to(), bob.id());
        assert!(follows[0].notification);

        // Delete edge
        engine
            .delete_edge::<Follow>(alice.id(), bob.id())
            .await
            .unwrap();

        // Verify deleted
        let follows: Vec<Follow> = engine
            .query_edges(alice.id(), EdgeQuery::default())
            .await
            .unwrap();
        assert_eq!(follows.len(), 0);
    }

    #[tokio::test]
    async fn test_engine_count_objects() {
        let (_resource, pool) = super::adapters::postgres::tests::setup_test_db().await;
        let adapter = PostgresAdapter::new(pool);
        adapter.init_schema().await.unwrap();

        let engine = Engine::new(Box::new(adapter));

        // Create multiple users
        for i in 0..5 {
            let mut user = User::default();
            user.name = format!("User{}", i);
            user.email = format!("user{}@example.com", i);
            engine.create_object(&user).await.unwrap();
        }

        // Count all users
        let count: u64 = engine.count_objects::<User>(None).await.unwrap();
        assert_eq!(count, 5);

        // Count with filter
        let count: u64 = engine
            .count_objects::<User>(Some(Query::default().where_eq(&User::FIELDS.name, "User0")))
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_engine_bulk_fetch() {
        let (_resource, pool) = super::adapters::postgres::tests::setup_test_db().await;
        let adapter = PostgresAdapter::new(pool);
        adapter.init_schema().await.unwrap();

        let engine = Engine::new(Box::new(adapter));

        // Create multiple users
        let mut ids = Vec::new();
        for i in 0..3 {
            let mut user = User::default();
            user.name = format!("User{}", i);
            user.email = format!("user{}@example.com", i);
            ids.push(user.id());
            engine.create_object(&user).await.unwrap();
        }

        // Fetch in bulk
        let users: Vec<User> = engine.fetch_objects(ids).await.unwrap();
        assert_eq!(users.len(), 3);
    }

    #[tokio::test]
    async fn test_engine_complex_query() {
        let (_resource, pool) = super::adapters::postgres::tests::setup_test_db().await;
        let adapter = PostgresAdapter::new(pool);
        adapter.init_schema().await.unwrap();

        let engine = Engine::new(Box::new(adapter));

        // Create owner
        let mut owner = User::default();
        owner.name = "Owner".to_string();
        owner.email = "owner@example.com".to_string();
        engine.create_object(&owner).await.unwrap();

        // Create multiple posts
        for i in 0..10 {
            let mut post = Post::default();
            post.set_owner(owner.id());
            post.title = format!("Post {}", i);
            post.content = format!("Content {}", i);
            engine.create_object(&post).await.unwrap();
        }

        // Query with limit
        let posts: Vec<Post> = engine
            .query_objects(Query::new(owner.id()).with_limit(5))
            .await
            .unwrap();
        assert_eq!(posts.len(), 5);

        // Query with offset
        let posts: Vec<Post> = engine
            .query_objects(Query::new(owner.id()).with_offset(5).with_limit(3))
            .await
            .unwrap();
        assert_eq!(posts.len(), 3);
    }

    #[tokio::test]
    async fn test_transfer_wrong_owner_fails() {
        let (_resource, pool) = super::adapters::postgres::tests::setup_test_db().await;
        let adapter = PostgresAdapter::new(pool);
        adapter.init_schema().await.unwrap();

        let engine = Engine::new(Box::new(adapter));

        // Create users
        let mut alice = User::default();
        alice.name = "Alice".to_string();
        engine.create_object(&alice).await.unwrap();

        let mut bob = User::default();
        bob.name = "Bob".to_string();
        engine.create_object(&bob).await.unwrap();

        let mut charlie = User::default();
        charlie.name = "Charlie".to_string();
        engine.create_object(&charlie).await.unwrap();

        // Create object owned by Alice
        let mut post = Post::default();
        post.set_owner(alice.id());
        post.title = "Alice's Post".to_string();
        engine.create_object(&post).await.unwrap();

        // Try to transfer from Bob to Charlie (should fail - Bob doesn't own it)
        let result: Result<Post, Error> = engine
            .transfer_object(post.id(), bob.id(), charlie.id())
            .await;

        assert!(matches!(result, Err(Error::NotFound)));
    }
}
