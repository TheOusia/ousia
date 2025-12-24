# Ousia

**Ousia** is a lightweight, type-safe domain modeling framework for Rust that provides elegant object persistence, relationships, and controlled data visibility through a powerful view system.

<!--[![Crates.io](https://img.shields.io/crates/v/ousia.svg)](https://crates.io/crates/ousia)-->
<!--[![Documentation](https://docs.rs/ousia/badge.svg)](https://docs.rs/ousia)-->
<!--[![License](https://img.shields.io/crates/l/ousia.svg)](LICENSE)-->

## Features

- 🎯 **Type-Safe Domain Objects** - Define your domain models with derive macros
- 🔍 **Flexible Indexing** - Built-in support for searchable and sortable fields
- 🔐 **View System** - Control data visibility with compile-time guarantees
- 🗄️ **Multiple Backends** - PostgreSQL and SQLite adapters included
- 🔗 **Graph Relationships** - First-class support for edges and traversals
- ⚡ **Zero Overhead** - Minimal runtime cost, maximum type safety
- 🎨 **Clean API** - Intuitive builder patterns and async-first design

## Quick Start

Add Ousia to your `Cargo.toml`:

```toml
[dependencies]
ousia = { version = "1.0", features = ["derive", "postgres"] }
tokio = { version = "1", features = ["full"] }
serde_json = "1"
```

## Basic Usage

### Defining Objects

```rust
use ousia::{OusiaObject, OusiaDefault, Meta};

#[derive(OusiaObject, OusiaDefault, Debug, Clone)]
#[ousia(
    type_name = "User",
    index = "email:search",
    index = "username:search+sort"
)]
pub struct User {
    #[ousia(meta)]
    _meta: Meta,

    pub username: String,
    pub email: String,
    pub display_name: String,
    
    #[ousia(private)]
    password: String,
}
```

### Creating and Querying Objects

```rust
use ousia::{Engine, Query};
use ousia::adapters::postgres::PostgresAdapter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize database adapter
    let pool = sqlx::PgPool::connect("postgresql://localhost/mydb").await?;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await?;
    
    // Create engine
    let engine = Engine::new(Box::new(adapter));
    
    // Create a user
    let mut user = User {
        username: "alice".to_string(),
        email: "alice@example.com".to_string(),
        display_name: "Alice".to_string(),
        password: "secret123".to_string(),
        ..Default::default()
    };
    
    engine.create_object(&user).await?;
    
    // Query users
    let query = Query::new(user.owner())
        .where_eq(&User::FIELDS.username, "alice")
        .with_limit(10);
    
    let users: Vec<User> = engine.query_objects(query).await?;
    
    Ok(())
}
```

## View System

The view system allows you to control which fields are visible in different contexts, providing compile-time safety against accidental data leaks.

### Three Visibility Levels

1. **Internal** - Engine-only view that includes ALL fields (used for persistence)
2. **Default** - Safe user-facing view (excludes private fields, owner hidden by default)
3. **Custom Views** - Explicitly defined projections for specific use cases

### Defining Views

```rust
#[derive(OusiaObject, OusiaDefault, Debug, Clone)]
#[ousia(type_name = "User")]
pub struct User {
    // Control meta field visibility per view
    #[ousia_meta(view(default="id, created_at, updated_at"))]
    #[ousia_meta(view(dashboard="id, owner, created_at, updated_at"))]
    #[ousia_meta(view(api="id, created_at"))]
    _meta: Meta,

    #[ousia(view(dashboard))]
    pub username: String,
    
    #[ousia(view(dashboard))]
    pub email: String,
    
    #[ousia(view(dashboard), view(api))]
    pub display_name: String,
    
    #[ousia(view(dashboard))]
    pub status: String,
    
    // Private field - only visible internally
    #[ousia(private)]
    password: String,
}
```

### Using Views

```rust
// Default serialization (excludes private fields)
let json = serde_json::to_value(&user)?;
// Output: { "id": "...", "created_at": "...", "username": "...", "email": "...", "display_name": "...", "status": "..." }
// Note: password is NOT included

// Use custom view
let api_view = user._api();
let json = serde_json::to_value(&api_view)?;
// Output: { "id": "...", "created_at": "...", "display_name": "..." }

// Use dashboard view
let dashboard_view = user._dashboard();
let json = serde_json::to_value(&dashboard_view)?;
// Output: { "id": "...", "owner": "...", "created_at": "...", "updated_at": "...", "username": "...", "email": "...", "display_name": "...", "status": "..." }
```

### View Benefits

- ✅ **Compile-time safety** - Invalid views won't compile
- ✅ **Zero runtime overhead** - Views are generated at compile time
- ✅ **No DTOs needed** - One object definition, multiple projections
- ✅ **Explicit over implicit** - Views must be explicitly selected
- ✅ **Prevents data leaks** - Private fields never exposed accidentally

## Edges and Relationships

Model relationships between objects using edges:

```rust
use ousia::{OusiaEdge, OusiaDefault, EdgeMeta};

#[derive(OusiaEdge, OusiaDefault, Debug, Clone)]
#[ousia(
    type_name = "Follow",
    index = "created_at:sort"
)]
pub struct Follow {
    #[ousia(meta)]
    _meta: EdgeMeta,
    
    pub created_at: chrono::DateTime<chrono::Utc>,
}

// Create a follow relationship
let follow = Follow {
    _meta: EdgeMeta::new(alice_id, bob_id),
    created_at: chrono::Utc::now(),
};

engine.create_edge(&follow).await?;

// Query followers
let followers: Vec<User> = engine
    .preload_object::<User>(bob_id)
    .await
    .edge::<Follow, User>()
    .collect()
    .await?;
```

## Indexing

Define indexes for efficient querying:

```rust
#[derive(OusiaObject, OusiaDefault)]
#[ousia(
    type_name = "Post",
    index = "title:search",           // Searchable
    index = "created_at:sort",         // Sortable
    index = "tags:search+sort"         // Both
)]
pub struct Post {
    #[ousia(meta)]
    _meta: Meta,
    
    pub title: String,
    pub content: String,
    pub tags: String,
}

// Search posts
let query = Query::new(owner)
    .filter(
        &Post::FIELDS.title,
        "rust",
        QueryMode::search(Comparison::Contains, None)
    )
    .with_limit(20);

let posts: Vec<Post> = engine.query_objects(query).await?;
```

## Advanced Queries

### Complex Filtering

```rust
use ousia::query::{Comparison, Operator, QueryMode};

let query = Query::new(owner)
    .filter(
        &User::FIELDS.username,
        "alice",
        QueryMode::search(Comparison::BeginsWith, Some(Operator::Or))
    )
    .filter(
        &User::FIELDS.email,
        "@example.com",
        QueryMode::search(Comparison::Contains, None)
    )
    .with_limit(50)
    .with_offset(0);

let users: Vec<User> = engine.query_objects(query).await?;
```

### Sorting

```rust
let query = Query::new(owner)
    .filter(
        &Post::FIELDS.created_at,
        chrono::Utc::now(),
        QueryMode::sort(false) // descending
    );

let posts: Vec<Post> = engine.query_objects(query).await?;
```

### Edge Traversal

```rust
// Get all posts by users that alice follows
let posts: Vec<Post> = engine
    .preload_object::<User>(alice_id)
    .await
    .edge::<Follow, User>()
    .edge::<AuthorOf, Post>()
    .with_limit(20)
    .collect()
    .await?;
```

## Ownership Model

Ousia has a built-in ownership model where every object has an owner:

```rust
use ousia::object::SYSTEM_OWNER;

// System-owned object
let mut config = Config {
    _meta: Meta::default(), // Uses SYSTEM_OWNER
    key: "app.version".to_string(),
    value: "1.0.0".to_string(),
};

// User-owned object
let mut post = Post {
    _meta: Meta::new_with_owner(user_id),
    title: "Hello World".to_string(),
    content: "...".to_string(),
};

// Transfer ownership
let transferred = engine.transfer_object::<Post>(
    post_id,
    old_owner_id,
    new_owner_id
).await?;
```

## Database Adapters

### PostgreSQL

```rust
use ousia::adapters::postgres::PostgresAdapter;

let pool = sqlx::PgPool::connect("postgresql://localhost/mydb").await?;
let adapter = PostgresAdapter::from_pool(pool);
adapter.init_schema().await?;

let engine = Engine::new(Box::new(adapter));
```

### SQLite

```rust
use ousia::adapters::sqlite::SqliteAdapter;

let adapter = SqliteAdapter::new_file("./data.db").await?;
// or in-memory:
// let adapter = SqliteAdapter::new_memory().await?;

adapter.init_schema().await?;
let engine = Engine::new(Box::new(adapter));
```

## Architecture

Ousia follows a clean architecture pattern:

```
┌─────────────────────────────────────────┐
│          Domain Layer                   │
│  (Your Objects & Edges with derive)    │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│          Engine Layer                   │
│  (Type-safe CRUD & Query API)          │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│          Adapter Layer                  │
│  (PostgreSQL / SQLite / Custom)        │
└─────────────────────────────────────────┘
```

### Key Concepts

- **Objects** - Domain entities with metadata, indexes, and views
- **Edges** - Typed relationships between objects
- **Engine** - High-level API for object operations
- **Adapters** - Storage backend implementations
- **Views** - Compile-time projections for controlled visibility
- **Meta** - Built-in metadata (id, owner, timestamps)

## Best Practices

### 1. Use Private Fields for Sensitive Data

```rust
#[derive(OusiaObject, OusiaDefault)]
pub struct User {
    #[ousia(meta)]
    _meta: Meta,
    
    pub email: String,
    
    #[ousia(private)]
    password_hash: String, // Never exposed in default serialization
    
    #[ousia(private)]
    api_key: String,
}
```

### 2. Define Views for Different Contexts

```rust
// Public API view - minimal data
#[ousia_meta(view(api="id, created_at"))]

// Admin dashboard view - more data
#[ousia_meta(view(admin="id, owner, created_at, updated_at"))]

// Internal processing view - everything
// (automatically available via ObjectInternal trait)
```

### 3. Index Fields You Query

```rust
#[ousia(
    index = "email:search",      // For WHERE email = ?
    index = "created_at:sort",   // For ORDER BY created_at
    index = "status:search+sort" // For both
)]
```

### 4. Use Edges for Relationships

```rust
// Instead of storing foreign keys in objects, use edges:

#[derive(OusiaEdge)]
#[ousia(type_name = "Member")]
pub struct Member {
    #[ousia(meta)]
    _meta: EdgeMeta, // from = user_id, to = team_id
    
    pub role: String,
    pub joined_at: chrono::DateTime<chrono::Utc>,
}
```

### 5. Leverage Type Safety

```rust
// The engine enforces type safety at compile time
engine.create_object(&user).await?;   // ✅ User implements ObjectInternal
engine.create_object(&"string").await?; // ❌ Compile error

// Queries return the correct type
let users: Vec<User> = engine.query_objects(query).await?;
let posts: Vec<Post> = engine.query_objects(query).await?; // Different type
```

## Performance

Ousia is designed for performance:

- **Zero-cost abstractions** - No runtime overhead for type safety
- **Efficient serialization** - Direct JSON mapping via serde
- **Optimized queries** - SQL generation with proper indexing
- **Minimal allocations** - Careful use of references and moves
- **Async-first** - Built on tokio for high concurrency

### Profiling

Enable the `profiling` feature to measure query performance:

```toml
[dependencies]
ousia = { version = "1.0", features = ["profiling"] }
```

```rust
#[cfg(feature = "profiling")]
{
    let (users, profile) = adapter.query_objects_profiled::<User>(
        User::TYPE,
        query
    ).await?;
    
    println!("Query time: {:?}", profile.query_ms);
    println!("Serialization time: {:?}", profile.serialize_ms);
}
```

## Testing

Ousia includes utilities for testing:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use ousia::adapters::sqlite::SqliteAdapter;

    #[tokio::test]
    async fn test_user_creation() -> Result<(), Box<dyn std::error::Error>> {
        // Use in-memory SQLite for tests
        let adapter = SqliteAdapter::new_memory().await?;
        adapter.init_schema().await?;
        let engine = Engine::new(Box::new(adapter));
        
        let user = User {
            username: "test".to_string(),
            email: "test@example.com".to_string(),
            ..Default::default()
        };
        
        engine.create_object(&user).await?;
        
        let found = engine.fetch_object::<User>(user.id()).await?;
        assert!(found.is_some());
        
        Ok(())
    }
}
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Ousia draws inspiration from:
- **GraphQL** - Field-level visibility and type safety
- **DynamoDB** - Flexible indexing patterns
- **Datomic** - Immutable data and temporal queries
- **Entity Framework** - Clean domain modeling

## Support

- 📚 [Documentation](https://docs.rs/ousia)
- 💬 [Discussions](https://github.com/theousia/ousia/discussions)
- 🐛 [Issue Tracker](https://github.com/theousia/ousia/issues)

---

**Built with ❤️ in Rust**
