# Ousia Design Considerations & Roadmap

## Current Architecture

### Strengths
1. **Clean Abstraction**: The `Adapter` trait provides a clear contract for storage implementations
2. **Type Safety**: Compile-time validation of indexed fields via the query builder
3. **Ownership Model**: Built-in multi-tenancy through the owner field
4. **Flexible Indexing**: Declarative index definitions via derive macros
5. **Metadata Separation**: Meta fields are consistently managed and not mixed with business data

### Current Limitations
1. No transaction support
2. No delete operations
3. Count queries are inefficient (fetch all then count)
4. No support for complex queries (OR conditions, ranges, IN clauses)
5. No caching layer
6. No migration system for schema changes

## Recommended Improvements

### 1. Transaction Support

**Why**: Ensure atomicity for related operations (e.g., creating a user and their initial posts)

**Implementation**:
```rust
#[async_trait]
pub trait Adapter: Send + Sync {
    type Transaction: Transaction;
    
    async fn begin_transaction(&self) -> Result<Self::Transaction, AdapterError>;
    
    // ... existing methods
}

#[async_trait]
pub trait Transaction: Send + Sync {
    async fn insert<T: Object>(&mut self, obj: &mut T) -> Result<(), AdapterError>;
    async fn update<T: Object>(&mut self, obj: &mut T) -> Result<(), AdapterError>;
    async fn commit(self) -> Result<(), AdapterError>;
    async fn rollback(self) -> Result<(), AdapterError>;
}
```

### 2. Enhanced Query Capabilities

**Add support for**:
- Range queries: `filter_range(field, min, max)`
- IN queries: `filter_in(field, values)`
- OR conditions: `or_filter(...)`
- Text search: `search(field, term)` with full-text search support

**Example**:
```rust
engine
    .query::<Post>(user_id)
    .filter_range(Post::INDEXES.created_at.name, start_date, end_date)
    .or(|q| q
        .filter(Post::INDEXES.status.name, PostStatus::Published)
        .filter(Post::INDEXES.status.name, PostStatus::Featured)
    )
    .search(Post::INDEXES.content.name, "rust programming")
    .fetch()
    .await
```

### 3. Soft Delete Pattern

**Why**: Maintain data integrity and enable audit trails

**Implementation**:
```rust
// Add to Meta struct
pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,

// Add to Adapter trait
async fn delete<T: Object>(&self, id: Ulid) -> Result<(), AdapterError>;
async fn restore<T: Object>(&self, id: Ulid) -> Result<(), AdapterError>;
async fn hard_delete<T: Object>(&self, id: Ulid) -> Result<(), AdapterError>;
```

### 4. Efficient Count Queries

**Current Issue**: Counting requires fetching all matching records

**Solution**: Add dedicated count method to Adapter
```rust
#[async_trait]
pub trait Adapter: Send + Sync {
    async fn count<T: Object>(&self, plan: QueryPlan) -> usize;
    // ...
}
```

### 5. Caching Layer

**Why**: Reduce database load for frequently accessed objects

**Design Options**:

**Option A - Adapter-level caching**:
```rust
pub struct CachedAdapter<A: Adapter> {
    inner: A,
    cache: Arc<dyn Cache>,
}
```

**Option B - Engine-level caching**:
```rust
pub struct CachedEngine<A: Adapter> {
    engine: Engine<A>,
    cache: Arc<dyn Cache>,
}
```

### 6. Migration System

**Why**: Manage schema evolution and data migrations

**Proposed Structure**:
```rust
pub trait Migration {
    fn version(&self) -> u32;
    fn up(&self) -> String; // SQL or other commands
    fn down(&self) -> String;
}

pub struct Migrator<A: Adapter> {
    adapter: A,
    migrations: Vec<Box<dyn Migration>>,
}
```

### 7. Relationship Support

**Why**: Model object relationships (1:1, 1:N, N:N)

**Approach 1 - Explicit Loading**:
```rust
let user = engine.fetch_by_id::<User>(user_id).await?;
let posts = engine.query::<Post>(user.id()).fetch().await;
```

**Approach 2 - Declarative Relations**:
```rust
#[derive(OusiaObject)]
#[ousia(
    type_name = "User",
    has_many = "posts:Post"
)]
struct User {
    // ...
}

// Later:
let posts = user.load_posts(&engine).await?;
```

### 8. Event System

**Why**: Enable reactive patterns and audit logging

**Design**:
```rust
pub trait EventListener: Send + Sync {
    async fn on_insert<T: Object>(&self, obj: &T);
    async fn on_update<T: Object>(&self, old: &T, new: &T);
    async fn on_delete<T: Object>(&self, obj: &T);
}

impl<A: Adapter> Engine<A> {
    pub fn add_listener(&mut self, listener: Arc<dyn EventListener>) {
        // ...
    }
}
```

### 9. Validation Framework

**Why**: Ensure data consistency at the application level

**Implementation**:
```rust
pub trait Validate {
    fn validate(&self) -> Result<(), ValidationError>;
}

// In Engine:
pub async fn insert<T: Object + Validate>(&self, obj: &mut T) -> Result<(), AdapterError> {
    obj.validate()?;
    self.adapter.insert(obj).await
}
```

### 10. Multi-Adapter Support

**Why**: Support read replicas, sharding, or multi-region deployments

**Design**:
```rust
pub struct MultiAdapter {
    write: Box<dyn Adapter>,
    reads: Vec<Box<dyn Adapter>>,
    strategy: LoadBalancingStrategy,
}
```

## Performance Considerations

### Indexing Strategy
- **Current**: JSON indexes on PostgreSQL (flexible but slower)
- **Improvement**: Generate native columns for frequently queried fields
- **Trade-off**: Less flexible, but much faster queries

### Connection Pooling
- Already handled by `sqlx::PgPool`
- Consider tuning pool size based on workload

### Batch Operations
- Current implementation is sequential
- **Improvement**: Use SQL batch inserts for better performance

### Query Optimization
- Add `EXPLAIN ANALYZE` logging in development
- Monitor slow queries and add indexes as needed

## Security Considerations

### 1. Owner Validation
**Current**: Trust that the owner field is set correctly
**Improvement**: Validate ownership in engine layer

```rust
pub async fn insert<T: Object>(&self, obj: &mut T, requester: Ulid) -> Result<(), AdapterError> {
    // Verify requester can create objects for this owner
    if obj.owner() != requester && !is_admin(requester) {
        return Err(AdapterError::PermissionDenied);
    }
    self.adapter.insert(obj).await
}
```

### 2. SQL Injection Protection
- **Current**: Using parameterized queries (✓)
- **Maintain**: Always use placeholders, never string concatenation

### 3. Data Encryption
- Consider encrypting sensitive fields at rest
- Use PostgreSQL's built-in encryption or application-level encryption

## Testing Strategy

### Unit Tests
- Test derive macros with various configurations
- Test query builder validation
- Test index metadata generation

### Integration Tests
- Test full CRUD lifecycle
- Test query operations with real database
- Test concurrent operations

### Performance Tests
- Benchmark query performance with large datasets
- Test connection pool under load
- Measure index effectiveness

## Documentation Needs

1. **Getting Started Guide**: Setup, basic CRUD operations
2. **Query Builder Guide**: All query capabilities with examples
3. **Index Configuration**: When to use search vs sort indexes
4. **Ownership Model**: Best practices for multi-tenancy
5. **Adapter Implementation Guide**: How to create custom adapters
6. **Performance Tuning**: Index strategies, query optimization

## Migration Path

### Phase 1: Core Stability (Current)
- [x] Basic CRUD operations
- [x] Query builder with filters and sorting
- [x] PostgreSQL adapter
- [ ] Comprehensive tests
- [ ] Documentation

### Phase 2: Enhanced Features
- [ ] Transaction support
- [ ] Delete operations
- [ ] Efficient count queries
- [ ] Range and IN queries

### Phase 3: Production Readiness
- [ ] Caching layer
- [ ] Migration system
- [ ] Event system
- [ ] Validation framework

### Phase 4: Scale & Performance
- [ ] Read replicas support
- [ ] Query optimization
- [ ] Monitoring and observability
- [ ] Performance benchmarks

## Conclusion

The current Ousia architecture provides a solid foundation for a type-safe, multi-tenant object storage system. The suggested improvements focus on:

1. **Completeness**: Adding missing CRUD operations
2. **Performance**: Optimizing queries and adding caching
3. **Safety**: Enhanced validation and security
4. **Developer Experience**: Better APIs and documentation

The modular design allows these improvements to be added incrementally without breaking existing code.
