# Ousia [![Build Status]][actions] [![Latest Version]][crates.io] [![ousia msrv]][Rust 1.85] [![ousia_derive msrv]][Rust 1.85]

[Build Status]: https://img.shields.io/github/actions/workflow/status/TheOusia/ousia/rust.yml?branch=main
[actions]: https://github.com/TheOusia/ousia/actions?query=branch%3Amain
[Latest Version]: https://img.shields.io/crates/v/ousia.svg
[crates.io]: https://crates.io/crates/ousia
[ousia msrv]: https://img.shields.io/crates/msrv/ousia.svg?label=ousia%20msrv&color=lightgray
[ousia_derive msrv]: https://img.shields.io/crates/msrv/ousia_derive.svg?label=ousia_derive%20msrv&color=lightgray
[Rust 1.85]: https://blog.rust-lang.org/2025/02/20/Rust-1.85.0/

A graph-relational ORM with built-in double-entry ledger for Rust. Zero migrations, compile-time safety, and atomic payment splits — all in one framework.

---

## Table of Contents

- [Why Ousia?](#why-ousia)
- [Architecture Overview](#architecture-overview)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [Objects](#objects)
  - [Defining Objects](#defining-objects)
  - [Object CRUD](#object-crud)
  - [Object Queries](#object-queries)
  - [Query Builder Reference](#query-builder-reference)
  - [Ownership Queries](#ownership-queries)
  - [Uniqueness Constraints](#uniqueness-constraints)
  - [Union Types](#union-types)
  - [View System](#view-system)
  - [Owner-Based Multitenancy](#owner-based-multitenancy)
- [Edges (Graph Relationships)](#edges-graph-relationships)
  - [Defining Edges](#defining-edges)
  - [Edge CRUD](#edge-crud)
  - [Edge Queries](#edge-queries)
  - [EdgeQuery Builder Reference](#edgequery-builder-reference)
- [Graph Traversal](#graph-traversal)
  - [Single-Pivot: `preload_object`](#single-pivot-preload_object)
  - [Multi-Pivot: `preload_objects`](#multi-pivot-preload_objects)
- [Sequence Counters](#sequence-counters)
- [Ledger (Money)](#ledger-money)
- [Design Philosophy](#design-philosophy)
- [Benchmarks](#benchmarks)

---

## Why Ousia?

Most Rust ORMs give you tables and rows. Ousia gives you a typed graph with money semantics baked in.

|                               | Ousia               | SeaORM / Diesel | SQLx        |
| ----------------------------- | ------------------- | --------------- | ----------- |
| Graph edges with properties   | First-class      | Manual joins | Raw SQL  |
| No migrations                 | Struct IS schema | Required     | Required |
| Compile-time query validation | `const FIELDS`   | Partial         |           |
| Owner-based multitenancy      | Built-in         | Manual       | Manual   |
| Atomic payment splits         | Built-in ledger  | External     | External |
| View system                   | Derive macro     |              |          |

---

## Architecture Overview

```
┌─────────────────────────────────────────────┐
│                  Engine                     │
│   (type-safe interface for all operations)  │
├─────────────────┬───────────────────────────┤
│   Object Store  │      Edge Store           │
│   (JSONB data   │  (typed graph with        │
│    + indexes)   │   index meta)             │
├─────────────────┴───────────────────────────┤
│             Adapter (Postgres / Memory)     │
├─────────────────────────────────────────────┤
│           Ledger (optional feature)         │
│  (double-entry, two-phase, value objects)   │
└─────────────────────────────────────────────┘
```

**Objects** hold structured data. Each has a `Meta` (id, owner, created_at, updated_at) plus your fields serialized as JSONB. Indexes are declared with `#[ousia(...)]` and validated at compile time.

**Edges** are first-class typed relationships between objects. They carry their own data fields and indexes, and support both forward and reverse traversal.

**The Ledger** handles money as immutable `ValueObject` fragments. Transfers are two-phase: a pure-memory planning stage followed by a single atomic execution with microsecond locks.

---

## Installation

```toml
[dependencies]
// ousia = "1" -- enables "derive", "postgres" and "ledger"
ousia = { version = "1", features = ["derive", "ledger"] }
```

The `derive` feature enables `#[derive(OusiaObject, OusiaEdge)]`. The `ledger` feature re-exports the `ledger` crate under `ousia::ledger`.

---

## Quickstart

```rust
use ousia::{Engine, Meta, OusiaDefault, OusiaObject, ObjectMeta, ObjectOwnership, Query};
use ousia::adapters::postgres::PostgresAdapter;

// 1. Define your type
#[derive(OusiaObject, OusiaDefault, Debug)]
#[ousia(
    unique = "username",
    index = "username:search+sort",
    index = "email:search"
)]
pub struct User {
    _meta: Meta,
    pub username: String,
    pub email: String,
    pub display_name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 2. Connect
    let adapter = PostgresAdapter::from_url("postgres://localhost/mydb").await?;
    adapter.init_schema().await?;
    let engine = Engine::new(Box::new(adapter));

    // 3. Create
    let mut user = User::default();
    user.username = "alice".to_string();
    user.email = "alice@example.com".to_string();
    user.display_name = "Alice".to_string();
    engine.create_object(&user).await?;

    // 4. Fetch
    let fetched: Option<User> = engine.fetch_object(user.id()).await?;

    // 5. Query
    let users: Vec<User> = engine
        .query_objects(Query::default().where_eq(&User::FIELDS.username, "alice"))
        .await?;

    Ok(())
}
```

---

## Objects

### Defining Objects

Every object has a `Meta` field (by convention `_meta`) that holds `id`, `owner`, `created_at`, and `updated_at`. All other fields are yours.

```rust
use ousia::{Meta, OusiaDefault, OusiaObject};

#[derive(OusiaObject, OusiaDefault, Debug)]
#[ousia(
    type_name = "Post",          // optional — defaults to struct name
    index = "status:search",
    index = "created_at:sort",
    index = "tags:search"        // Vec<String> supports contains queries
)]
pub struct Post {
    _meta: Meta,
    pub title: String,
    pub content: String,
    pub status: PostStatus,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub enum PostStatus { #[default] Draft, Published, Archived }

// Implement ToIndexValue for PostStatus to enable indexing for custom types
impl ToIndexValue for PostStatus {
    fn to_index_value(&self) -> IndexValue {
        match self {
            PostStatus::Draft => IndexValue::String("draft".to_string()),
            PostStatus::Published => IndexValue::String("published".to_string()),
            PostStatus::Archived => IndexValue::String("archived".to_string()),
        }
    }
}
```

The `OusiaObject` derive generates:

- `impl Object` — type name, meta accessors, index metadata
- `impl Unique` — uniqueness hash derivation
- `const FIELDS` — a `PostFields` struct with one `IndexField` per indexed field, used in query builder calls
- Custom `Serialize`/`Deserialize` that respects private fields and views

The `OusiaDefault` derive generates `impl Default` with a fresh `Meta`.

**Reserved field names** (used by Meta — don't declare these yourself): `id`, `owner`, `type`, `created_at`, `updated_at`.

---

### Object CRUD

#### `create_object`

```rust
engine.create_object(&post).await?;
```

Inserts the object. If the type declares `unique` fields, the uniqueness hash is checked atomically before insertion. Returns `Err(Error::UniqueConstraintViolation)` on conflict.

#### `fetch_object`

```rust
let post: Option<Post> = engine.fetch_object(post_id).await?;
```

Fetches a single object by its UUID. Returns `None` if not found.

#### `fetch_objects`

```rust
let posts: Vec<Post> = engine.fetch_objects(vec![id1, id2, id3]).await?;
```

Batch-fetches multiple objects by their UUIDs in a single query. Order of results is not guaranteed to match input order.

#### `update_object`

```rust
post.title = "New Title".to_string();
engine.update_object(&mut post).await?;
```

Updates the object in storage. Automatically sets `updated_at` to now. If unique fields changed, the old uniqueness hashes are removed and new ones are checked — rollback happens atomically if the new value is already taken.

#### `delete_object`

```rust
let deleted: Option<Post> = engine.delete_object(post_id, owner_id).await?;
```

Deletes an object by ID, requiring the correct `owner`. Returns the deleted object, or `None` if no match was found. Mismatched owner returns `None`.

#### `delete_objects`

```rust
let count: u64 = engine.delete_objects::<Post>(vec![id1, id2], owner_id).await?;
```

Bulk-deletes objects by ID, all requiring the same owner. Returns the number of deleted rows.

#### `delete_owned_objects`

```rust
let count: u64 = engine.delete_owned_objects::<Post>(owner_id).await?;
```

Deletes all objects of type `Post` owned by the given owner. Useful for cascading cleanup.

#### `transfer_object`

```rust
let post: Post = engine.transfer_object::<Post>(post_id, from_owner, to_owner).await?;
```

Transfers ownership from `from_owner` to `to_owner`. The `from_owner` must match the current owner. Returns the updated object with its new owner.

---

### Object Queries

#### `find_object`

```rust
let user: Option<User> = engine
    .find_object::<User>(&[filter!(&User::FIELDS.email, "alice@example.com")])
    .await?;
```

Finds a single object matching the given filters, scoped to `SYSTEM_OWNER`. Useful for looking up globally-unique records like users by email.

#### `find_object_with_owner`

```rust
let profile: Option<Profile> = engine
    .find_object_with_owner::<Profile>(user_id, &[filter!(&Profile::FIELDS.slug, "main")])
    .await?;
```

Like `find_object`, but restricts the search to a specific owner.

#### `query_objects`

```rust
let posts: Vec<Post> = engine
    .query_objects(
        Query::new(owner_id)
            .where_eq(&Post::FIELDS.status, PostStatus::Published)
            .sort_desc(&Post::FIELDS.created_at)
            .with_limit(20)
            .with_cursor(last_seen_id),
    )
    .await?;
```

The primary query method. Takes a `Query` builder and returns all matching objects. Supports filtering, sorting, pagination, and scoping by owner.

#### `count_objects`

```rust
// Count all posts
let total: u64 = engine.count_objects::<Post>(None).await?;

// Count filtered
let published: u64 = engine
    .count_objects::<Post>(Some(
        Query::new(owner_id).where_eq(&Post::FIELDS.status, PostStatus::Published),
    ))
    .await?;
```

Returns the number of objects matching the query. Pass `None` to count all objects of the type.

---

### Query Builder Reference

`Query` is the fluent builder for object queries.

```rust
// Scope to system-owned objects (default owner)
Query::default()

// Scope to a specific owner
Query::new(owner_id)

// Global search — no owner filter (use sparingly on large tables)
Query::wide()
```

**AND filters** (default operator — all conditions must match):

| Method                  | SQL equivalent              |
| ----------------------- | --------------------------- |
| `.where_eq(f, v)`       | `field = v`                 |
| `.where_ne(f, v)`       | `field != v`                |
| `.where_gt(f, v)`       | `field > v`                 |
| `.where_gte(f, v)`      | `field >= v`                |
| `.where_lt(f, v)`       | `field < v`                 |
| `.where_lte(f, v)`      | `field <= v`                |
| `.where_contains(f, v)` | `field @> v` (array/GIN)    |
| `.where_contains_all(f, v)` | all elements present    |
| `.where_begins_with(f, v)`  | `field LIKE 'v%'`       |

**OR filters** (any one condition matches — prefix `or_`):

`.or_eq`, `.or_ne`, `.or_gt`, `.or_gte`, `.or_lt`, `.or_lte`, `.or_contains`, `.or_contains_all`, `.or_begins_with`

**Sorting:**

```rust
.sort_asc(&Post::FIELDS.created_at)   // ORDER BY created_at ASC
.sort_desc(&Post::FIELDS.created_at)  // ORDER BY created_at DESC
```

**Pagination:**

```rust
.with_limit(20)              // LIMIT 20
.with_cursor(last_seen_id)   // cursor-based (keyset) pagination — no OFFSET
```

**Example — compound filter with OR:**

```rust
// Posts that are published OR archived, sorted newest first, page 2
let posts: Vec<Post> = engine
    .query_objects(
        Query::new(owner_id)
            .where_eq(&Post::FIELDS.status, PostStatus::Published)
            .or_eq(&Post::FIELDS.status, PostStatus::Archived)
            .sort_desc(&Post::FIELDS.created_at)
            .with_limit(20)
            .with_cursor(last_cursor_id),
    )
    .await?;
```

**Example — array contains:**

```rust
// Posts tagged with "rust"
let tagged: Vec<Post> = engine
    .query_objects(
        Query::new(owner_id).where_contains(&Post::FIELDS.tags, vec!["rust"]),
    )
    .await?;
```

---

### Ownership Queries

#### `fetch_owned_objects`

```rust
let posts: Vec<Post> = engine.fetch_owned_objects::<Post>(user_id).await?;
```

Fetches all objects of type `Post` owned by the given owner. No filtering — returns everything.

#### `fetch_owned_object`

```rust
let profile: Option<Profile> = engine.fetch_owned_object::<Profile>(user_id).await?;
```

Fetches a single owned object. Designed for one-to-one ownership relationships (e.g., each user has one profile). Returns `None` if no object is owned by that owner.

---

### Uniqueness Constraints

```rust
// Single field unique globally
#[ousia(unique = "username")]

// Composite unique (both fields together must be unique)
#[ousia(unique = "username+email")]

// Singleton per owner (e.g., one profile per user)
#[ousia(unique = "owner")]
```

On violation, `create_object` or `update_object` returns `Err(Error::UniqueConstraintViolation(field_name))`. Updates are handled cleanly: old hashes are removed, new ones checked, and rollback happens if the new hash is already taken.

---

### Union Types

Union types let you fetch from two different object types with a single query. Useful when a relationship points to one of two possible types (e.g., a post authored by either a `User` or an `Organization`).

```rust
// Fetch by ID — checks type A first, then type B
let result: Option<Union<User, Organization>> =
    engine.fetch_union_object::<User, Organization>(id).await?;

match result {
    Some(Union::A(user)) => println!("User: {}", user.username),
    Some(Union::B(org))  => println!("Org: {}", org.name),
    None => println!("Not found"),
}

// Batch fetch
let results: Vec<Union<User, Organization>> =
    engine.fetch_union_objects::<User, Organization>(vec![id1, id2]).await?;

// Fetch one owned by a specific owner
let result: Option<Union<User, Organization>> =
    engine.fetch_owned_union_object::<User, Organization>(owner_id).await?;

// Fetch all owned by a specific owner
let results: Vec<Union<User, Organization>> =
    engine.fetch_owned_union_objects::<User, Organization>(owner_id).await?;
```

---

### View System

Views let you generate multiple serialization shapes from one struct without duplicating types. Ideal for public vs. admin API responses.

```rust
#[derive(OusiaObject, OusiaDefault)]
pub struct User {
    #[ousia_meta(view(public = "id,created_at"))]        // meta fields in "public" view
    #[ousia_meta(view(admin = "id,owner,created_at"))]   // meta fields in "admin" view
    _meta: Meta,

    #[ousia(view(public))]   // included in public view
    #[ousia(view(admin))]    // included in admin view
    pub username: String,

    #[ousia(view(admin))]    // admin only
    pub email: String,

    #[ousia(private)]        // never serialized (e.g. password hash)
    pub password_hash: String,
}

// Usage — auto-generated structs and methods:
let public_view: UserPublicView = user._public();   // { id, created_at, username }
let admin_view: UserAdminView  = user._admin();     // { id, owner, created_at, username, email }
```

Private fields are excluded from all serialization (including the default `Serialize` impl) but are included in the internal database representation via `__serialize_internal`.

---

### Owner-Based Multitenancy

Every object has an `owner` UUID in its Meta. The `SYSTEM_OWNER` constant (`00000000-0000-7000-8000-000000000001`) is the default for unowned objects.

```rust
use ousia::{ObjectMeta, ObjectOwnership, system_owner};

// Set owner at creation
post.set_owner(user.id());

// Check ownership
assert!(post.is_owned_by(&user));
assert!(!post.is_system_owned());

// Fetch everything owned by a user
let posts: Vec<Post> = engine.fetch_owned_objects(user.id()).await?;

// Fetch single owned object (useful for one-to-one, e.g., user profile)
let profile: Option<Profile> = engine.fetch_owned_object(user.id()).await?;
```

Delete and transfer operations require the correct owner — mismatched owner returns `Err(Error::NotFound)`.

---

## Edges (Graph Relationships)

### Defining Edges

```rust
use ousia::{EdgeMeta, OusiaDefault, OusiaEdge};

#[derive(OusiaEdge, OusiaDefault, Debug)]
#[ousia(
    type_name = "Follow",
    index = "status:search",
    index = "created_at:sort"
)]
pub struct Follow {
    _meta: EdgeMeta,            // holds `from` and `to` UUIDs
    pub status: String,         // "pending" | "accepted"
    pub notifications: bool,
}
```

`EdgeMeta` stores the `from` and `to` object IDs. The `OusiaEdge` derive generates `impl Edge`, `const FIELDS`, and custom serde that keeps `_meta` out of the serialized data payload.

`from` and `to` are always available as indexed fields (no need to declare them).

---

### Edge CRUD

#### `create_edge`

```rust
let follow = Follow {
    _meta: EdgeMeta::new(alice.id(), bob.id()),
    status: "accepted".to_string(),
    notifications: true,
};
engine.create_edge(&follow).await?;
```

#### `update_edge`

```rust
// Update edge data, keep the same `to` target
engine.update_edge(&mut follow, None).await?;

// Update edge data AND retarget to a different object
engine.update_edge(&mut follow, Some(new_target_id)).await?;
```

The second argument is `Option<Uuid>` — if `Some`, the `to` field is updated to point to the new target.

#### `fetch_edge`

```rust
let edge: Option<Follow> = engine.fetch_edge::<Follow>(alice.id(), bob.id()).await?;
```

Fetches a specific edge by its `(from, to)` pair. Returns `None` if no such edge exists.

#### `delete_edge`

```rust
engine.delete_edge::<Follow>(alice.id(), bob.id()).await?;
```

#### `delete_object_edge`

```rust
// Delete all Follow edges originating from alice
engine.delete_object_edge::<Follow>(alice.id()).await?;
```

---

### Edge Queries

#### `query_edges`

```rust
// Forward: edges where `from` = alice
let follows: Vec<Follow> = engine
    .query_edges(alice.id(), EdgeQuery::default())
    .await?;
```

#### `query_reverse_edges`

```rust
// Reverse: edges where `to` = bob (who follows bob?)
let followers: Vec<Follow> = engine
    .query_reverse_edges(bob.id(), EdgeQuery::default())
    .await?;
```

#### `count_edges` / `count_reverse_edges`

```rust
let following_count: u64 = engine.count_edges::<Follow>(alice.id(), None).await?;
let follower_count: u64  = engine.count_reverse_edges::<Follow>(bob.id(), None).await?;

// With filters
let accepted_count: u64 = engine
    .count_edges::<Follow>(
        alice.id(),
        Some(EdgeQuery::default().where_eq(&Follow::FIELDS.status, "accepted")),
    )
    .await?;
```

---

### EdgeQuery Builder Reference

`EdgeQuery` is the fluent builder for edge queries. It has the same filter, sort, and pagination methods as `Query`, but without an owner scope.

```rust
EdgeQuery::default()
    .where_eq(&Follow::FIELDS.status, "accepted")
    .sort_desc(&Follow::FIELDS.created_at)
    .with_limit(50)
    .with_cursor(last_seen_id)
```

**AND filters:**

| Method                  | Description              |
| ----------------------- | ------------------------ |
| `.where_eq(f, v)`       | field = v                |
| `.where_ne(f, v)`       | field != v               |
| `.where_gt(f, v)`       | field > v                |
| `.where_gte(f, v)`      | field >= v               |
| `.where_lt(f, v)`       | field < v                |
| `.where_lte(f, v)`      | field <= v               |
| `.where_contains(f, v)` | array contains v         |
| `.where_begins_with(f, v)` | prefix match          |

**OR variants:** `.or_eq`, `.or_ne`, `.or_gt`, `.or_gte`, `.or_lt`, `.or_lte`, `.or_contains`, `.or_begins_with`

**Sorting:** `.sort_asc(field)`, `.sort_desc(field)`

**Pagination:** `.with_limit(n)`, `.with_cursor(uuid)`

---

## Graph Traversal

Ousia provides two traversal APIs: a **single-pivot** API (`preload_object`) for working from one known node, and a **multi-pivot** API (`preload_objects`) that eliminates N+1 by batching across many nodes in exactly 2 queries.

---

### Single-Pivot: `preload_object`

`engine.preload_object::<T>(id)` returns a `QueryContext` — a builder rooted at a single object ID.

```rust
let ctx = engine.preload_object::<User>(alice.id());
```

#### Fetch the pivot object

```rust
let user: Option<User> = engine.preload_object::<User>(alice.id()).get().await?;
```

#### Traverse edges: `.edge::<E, O>()`

Transitions the context into an `EdgeQueryContext` that can filter both the edges and their target/source objects.

```rust
engine.preload_object::<User>(alice.id())
    .edge::<Follow, User>()
```

**Object filters** (applied to the connected objects):

```rust
.where_eq(&User::FIELDS.status, "active")
.where_gt(&User::FIELDS.created_at, cutoff)
.where_contains(&User::FIELDS.tags, vec!["vip"])
// + where_ne, where_gte, where_lt, where_lte, where_begins_with, where_contains_all
// + or_* variants: or_eq, or_ne, or_gt, ...
```

**Edge filters** (applied to the edge properties):

```rust
.edge_eq(&Follow::FIELDS.status, "accepted")
.edge_gt(&Follow::FIELDS.created_at, cutoff)
// + edge_ne, edge_gte, edge_lt, edge_lte, edge_contains, edge_begins_with, edge_contains_all
// + edge_or_* variants
```

**Sorting:**

```rust
.sort_asc(&User::FIELDS.username)       // sort target objects
.sort_desc(&User::FIELDS.created_at)
.edge_sort_asc(&Follow::FIELDS.created_at)   // sort edges
.edge_sort_desc(&Follow::FIELDS.created_at)
```

**Pagination:**

```rust
.with_limit(50)
.with_cursor(last_id)
.paginate(Some(cursor_uuid))   // alternative — accepts Option<impl Into<Cursor>>
```

**Terminal methods:**

| Method                      | Returns              | Direction      | Includes  |
| --------------------------- | -------------------- | -------------- | --------- |
| `.collect()`                | `Vec<O>`             | forward        | objects   |
| `.collect_reverse()`        | `Vec<O>`             | reverse        | objects   |
| `.collect_edges()`          | `Vec<E>`             | forward        | edges     |
| `.collect_reverse_edges()`  | `Vec<E>`             | reverse        | edges     |
| `.collect_with_target()`    | `Vec<ObjectEdge<E,O>>` | forward      | edge+obj  |
| `.collect_reverse_with_target()` | `Vec<ObjectEdge<E,O>>` | reverse | edge+obj  |
| `.collect_both()`           | `(Vec<O>, Vec<O>)`   | both (UNION)   | objects   |
| `.collect_both_with_target()` | `(Vec<ObjectEdge<E,O>>, Vec<ObjectEdge<E,O>>)` | both | edge+obj |
| `.collect_both_edges()`     | `(Vec<E>, Vec<E>)`   | both (UNION)   | edges     |

The `collect_both*` methods issue a single UNION query for both forward and reverse directions simultaneously.

**Example — accepted followers of alice, created after last month:**

```rust
let users: Vec<User> = engine
    .preload_object::<User>(alice.id())
    .edge::<Follow, User>()
    .where_gt(&User::FIELDS.created_at, last_month)
    .edge_eq(&Follow::FIELDS.status, "accepted")
    .sort_desc(&User::FIELDS.created_at)
    .with_limit(20)
    .collect_reverse()
    .await?;
```

**Example — get both alice's follows and her followers in one query:**

```rust
let (following, followers): (Vec<User>, Vec<User>) = engine
    .preload_object::<User>(alice.id())
    .edge::<Follow, User>()
    .edge_eq(&Follow::FIELDS.status, "accepted")
    .collect_both()
    .await?;
```

**Example — edge + object pairs (inspect edge properties alongside the object):**

```rust
let pairs: Vec<ObjectEdge<Follow, User>> = engine
    .preload_object::<User>(alice.id())
    .edge::<Follow, User>()
    .collect_with_target()
    .await?;

for pair in pairs {
    println!("{} (notifs: {})", pair.object().username, pair.edge().notifications);
}
```

#### Fetch owned children: `.preload::<C>()`

Fetches a parent and all objects it owns in 2 queries.

```rust
let result: Option<(User, Vec<Post>)> = engine
    .preload_object::<User>(alice.id())
    .preload::<Post>()
    .collect()
    .await?;

if let Some((user, posts)) = result {
    println!("{} has {} posts", user.username, posts.len());
}
```

---

### Multi-Pivot: `preload_objects`

`engine.preload_objects::<P>(query)` fetches a page of parent objects, then batch-joins their edges or children — **always exactly 2 queries, never N+1**.

```rust
let ctx = engine.preload_objects::<User>(Query::new(owner_id).with_limit(100));
```

#### Traverse edges: `.edge::<E, C>()`

Returns a `MultiEdgeContext` configured with the parent query.

```rust
engine.preload_objects::<User>(parent_query)
    .edge::<Follow, User>()
```

**Edge query configuration:**

```rust
// Replace the entire EdgeQuery on the context
.with_edge_query(
    EdgeQuery::default()
        .where_eq(&Follow::FIELDS.status, "accepted")
        .with_limit(50),
)

// Filter connected objects
.obj_eq(&User::FIELDS.status, "active")
```

**Terminal methods (all return `Vec<(P, ...)>`):**

| Method                       | Returns                           | Direction | Includes  |
| ---------------------------- | --------------------------------- | --------- | --------- |
| `.collect()`                 | `Vec<(P, Vec<C>)>`               | forward   | objects   |
| `.collect_reverse()`         | `Vec<(P, Vec<C>)>`               | reverse   | objects   |
| `.collect_edges()`           | `Vec<(P, Vec<E>)>`               | forward   | edges     |
| `.collect_reverse_edges()`   | `Vec<(P, Vec<E>)>`               | reverse   | edges     |
| `.collect_with_target()`     | `Vec<(P, Vec<ObjectEdge<E,C>>)>` | forward   | edge+obj  |
| `.collect_reverse_with_target()` | `Vec<(P, Vec<ObjectEdge<E,C>>)>` | reverse | edge+obj |
| `.count()`                   | `Vec<(P, u64)>`                  | forward   | counts    |
| `.count_reverse()`           | `Vec<(P, u64)>`                  | reverse   | counts    |

**Example — load 100 users and their accepted followers in 2 queries:**

```rust
let results: Vec<(User, Vec<User>)> = engine
    .preload_objects::<User>(Query::wide().with_limit(100))
    .edge::<Follow, User>()
    .with_edge_query(EdgeQuery::default().where_eq(&Follow::FIELDS.status, "accepted"))
    .collect_reverse()
    .await?;

for (user, followers) in &results {
    println!("{}: {} followers", user.username, followers.len());
}
```

**Example — follower counts for a page of users:**

```rust
let counts: Vec<(User, u64)> = engine
    .preload_objects::<User>(Query::wide().with_limit(100))
    .edge::<Follow, User>()
    .count_reverse()
    .await?;
```

#### Fetch owned children: `.preload::<C>()`

Batch-fetches all children owned by each parent — exactly 2 queries.

```rust
let results: Vec<(User, Vec<Post>)> = engine
    .preload_objects::<User>(Query::new(tenant_id).with_limit(50))
    .preload::<Post>()
    .collect()
    .await?;

for (user, posts) in results {
    println!("{} owns {} posts", user.username, posts.len());
}
```

---

## Sequence Counters

Named counters backed by the database. Useful for order numbers, invoice IDs, and similar monotonically increasing values.

```rust
// Read the current value without incrementing
let current: u64 = engine.counter_value("order_number".to_string()).await;

// Increment and return the new value
let next: u64 = engine.counter_next_value("order_number".to_string()).await;
```

Counter keys are arbitrary strings. The counter is created on first use.

---

## Ledger (Money)

Ousia includes a full double-entry ledger. See [`ledger/README.md`](ledger/README.md) for the complete API. Here's the shape:

#### Installation

```bash
cargo add ousia --features ledger
```

or

```toml
ousia = { version = "1", features = ["derive", "postgres"] }
```

```rust
use ousia::ledger::{Asset, LedgerContext, LedgerSystem, Money, Balance};

// Setup
let system = Arc::new(LedgerSystem::new(Box::new(adapter)));
let ctx = LedgerContext::new(system.adapter_arc());

// Create an asset
let usd = Asset::new("USD", 10_000, 2);   // unit = $100, 2 decimals
system.adapter().create_asset(usd).await?;

// Atomic payment split: buyer pays $100, splits to seller/platform/charity
Money::atomic(&ctx, |tx| async move {
    let money = tx.money("USD", buyer_id, 100_00).await?;
    let mut slice = money.slice(100_00)?;

    let seller_cut   = slice.slice(70_00)?;
    let platform_fee = slice.slice(20_00)?;
    let charity      = slice.slice(10_00)?;

    seller_cut.transfer_to(seller_id, "sale".to_string()).await?;
    platform_fee.transfer_to(platform_id, "fee".to_string()).await?;
    charity.transfer_to(charity_id, "donation".to_string()).await?;

    Ok(())
}).await?;

// Check balance
let balance = Balance::get("USD", seller_id, &ctx).await?;
println!("Seller balance: {}", balance.available);
```

---

## Design Philosophy

**What Ousia does:**

- Type safety enforced at compile time via `const FIELDS` and derive macros
- Typed graph edges with indexed properties
- Atomic money transfers with double-entry guarantees
- Owner-based multitenancy as a first-class concept
- Automatic change handling — over-selection in payments returns the diff

**Idempotency:** Keys stored permanently. Only used for external deposit/withdrawal webhooks — not every internal transaction needs a key.

**What Ousia deliberately rejects:**

- **Explicit transactions** — the two-phase ledger handles it; locks held for microseconds only
- **ORM-layer validation** — belongs in your service layer, not your ORM
- **Soft deletes** — application-specific; implement in your domain if needed
- **Schema migrations** — the struct is the schema; add and remove fields freely
- **Early locking** — planning phase is pure memory; execution phase is atomic

---

## Benchmarks

Median latency · 10–20 samples per group · MacBook M1 Pro 32 GB · PostgreSQL 16 in Docker (localhost)

Datasets: **ousia_edges** — 10k users, 100k follows, N+1 bench over 1k pivots; **ousia_queries** — 50k users, 2k posts; **ousia_vs_raw** — 10k users, 2k posts, N+1 bench over 200 owners.

---

#### Disclaimer

This results may not accurately reflect the performance due to structure of bench functions and is expected to change when a better bench functions is implemented

```bash
cargo bench
```

---

### N+1 Elimination — the headline result

| Suite                     | Benchmark                   | ousia batch (2q) | raw N+1  | raw batch (2q) | N+1 speedup |
| ------------------------- | --------------------------- | ---------------- | -------- | -------------- | ----------- |
| ousia_edges (1k pivots)   | preload_multi_pivot_forward | 464 µs           | 461 ms   | 109 ms         | **993×**    |
| ousia_edges (1k pivots)   | preload_multi_pivot_count   | 482 µs           | 435 ms   | 20.7 ms        | **903×**    |
| ousia_vs_raw (200 owners) | preload_owned_batch         | 537 µs           | 103.9 ms | 4.46 ms        | **193×**    |

### Edge Operations (`ousia_edges` — 10k users, 100k follows)

| Benchmark                         | ousia  | raw sqlx | sea-orm |
| --------------------------------- | ------ | -------- | ------- |
| query_edges_forward               | 473 µs | 462 µs   | 458 µs  |
| query_edges_reverse               | 447 µs | 498 µs   | 468 µs  |
| count_edges                       | 471 µs | 475 µs   | 471 µs  |
| query_edges_with_filter           | 463 µs | 480 µs   | 510 µs  |
| preload_forward (1 pivot → users) | 589 µs | 525 µs   | 577 µs  |
| preload_reverse (1 pivot ← users) | 749 µs | 526 µs   | 466 µs  |
| create_edge                       | 573 µs | 536 µs   | 537 µs  |

### Object Queries (`ousia_vs_raw` — 10k users, 2k posts)

| Benchmark                | ousia   | raw sqlx | sea-orm |
| ------------------------ | ------- | -------- | ------- |
| fetch_by_pk              | 453 µs  | 613 µs   | 1.80 ms |
| eq_filter_indexed        | 1.68 ms | 1.93 ms  | 1.19 ms |
| count_aggregate          | 537 µs  | 499 µs   | 472 µs  |
| owner_scan (by owner ID) | 680 µs  | 459 µs   | 468 µs  |
| range_sort + limit 20    | 594 µs  | 556 µs   | 688 µs  |
| array_contains (GIN)     | 660 µs  | 1.19 ms  | 1.67 ms |
| begins_with prefix       | 623 µs  | 749 µs   | 691 µs  |
| bulk_fetch × 10          | 475 µs  | 456 µs   | 456 µs  |
| bulk_fetch × 50          | 639 µs  | 543 µs   | 531 µs  |
| bulk_fetch × 100         | 840 µs  | 620 µs   | 633 µs  |
| multi_sort + limit 50    | 504 µs  | 548 µs   | 596 µs  |

### Query Patterns (`ousia_queries` — 50k users, 2k posts)

| Benchmark               | ousia   | raw sqlx | sea-orm |
| ----------------------- | ------- | -------- | ------- |
| AND filter (2 fields) ¹ | 468 µs  | 22.5 ms  | 29.4 ms |
| OR / IN condition       | 621 µs  | 490 µs   | 2.56 ms |
| cursor page1 × 10       | 2.82 ms | 2.15 ms  | 3.73 ms |
| cursor mid-page × 10    | 2.29 ms | 4.18 ms  | 2.59 ms |
| cursor page1 × 50       | 455 µs  | 535 µs   | 508 µs  |
| cursor mid-page × 50    | 476 µs  | 568 µs   | 526 µs  |
| cursor page1 × 100      | 470 µs  | 605 µs   | 588 µs  |
| cursor mid-page × 100   | 516 µs  | 614 µs   | 588 µs  |
| full scan limit 100     | 470 µs  | 613 µs   | 579 µs  |
| full scan limit 500     | 507 µs  | 967 µs   | 1.11 ms |
| multi_sort + limit 50   | 487 µs  | 548 µs   | 616 µs  |
| create_object           | 635 µs  | 590 µs   | 613 µs  |

¹ At 50k rows, ousia's `index_meta` JSONB indexes turn a full-table scan into an index lookup — **48× faster** than hand-written SQL without a matching composite index.

### Joins & CTEs (`ousia_vs_raw`)

| Benchmark                            | ousia    | raw sqlx | sea-orm |
| ------------------------------------ | -------- | -------- | ------- |
| join_posts_users (published, top 20) | —        | 615 µs   | 613 µs  |
| cte_ranked_posts (window fn top-3)   | 726 µs ² | 1.20 ms  | 1.60 ms |

² ousia fetches all published posts + groups top-3 per owner in Rust.

**Key takeaways:**

- Batch preload eliminates N+1 with **193–993× speedup** — the gap grows with dataset size.
- At 50k rows, JSONB index queries beat full-table-scan SQL by **48×** for compound AND filters.
- Single-query operations (PK fetch, GIN array search, cursors ≥50) match or beat raw sqlx.
- Joins and window functions are best expressed as raw SQL; ousia provides an escape hatch for these.

---

## Metrics

- Query duration histogram
- Transaction amount histogram
- Transaction success rate histogram

---

## Buy Me a Drink

If this project saved your time, helped you ship faster, or made you say "damn, that's slick!" — consider buying me a beer

## [Send me a drink on Cointr.ee](https://cointr.ee/epikoder)

## License

MIT
