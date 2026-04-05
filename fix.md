
## Q&A Before The Plan

**1. Is Postgres `CACHE 100` app-dependent?**

No — `CACHE 100` is **server-side per-backend connection**. Each PG backend pre-allocates 100 values from the sequence into its own memory. Your app just calls `nextval('seq_name')` — zero app logic needed. The only "loss" is gaps on connection drop, which is fine for sequences. Fully DB-native.

**2. Should empty partitions be removed?**

Yes — an empty partition still costs: it gets its own autovacuum worker slot, shows up in `pg_class`, and the planner has to consider it. If a type is unregistered from the registry, its partition should be dropped. The `init_schema` should be **idempotent**: create missing partitions, drop orphaned ones.

**3. `index_meta` and `data` defaulting to `{}`**

Good — means `NOT NULL DEFAULT '{}'::jsonb` on both. GIN on `jsonb_path_ops` handles empty JSONB fine (it just produces no entries), so no special casing needed.

---

Now the full plan:

---

## Claude Code Plan

```markdown
# Ousia Storage Layer — Schema Scaling Plan

## Goal
Refactor the Postgres storage backend to support:
- objects: 100M rows
- edges: 100–150M rows  
- unique_constraints: 100–200M rows
- sequences: 100–1K incr/s per type, fully DB-native

All changes must be:
- Pure DB-side (no app-layer coordination logic)
- Driven by a compile-time type registry from `OusiaObject` macro
- Idempotent (safe to re-run init_schema)
- Production-grade index decisions made here, not deferred

---

## Phase 0 — Type Registry (Compile-Time)

### 0.1 — Macro Output
The `OusiaObject` derive macro must emit a file at:
  `target/ousia/type_registry`

Format: one type string per line, e.g.:
```
invoice
user
product
edge_follows
edge_owns
```

Rules:
- File is append-idempotent (no duplicates)
- Written at compile time via `build.rs` or macro `proc_macro` side-effect
- Registry is read at runtime by `init_schema`
- Edge types are prefixed with `edge_` to distinguish from object types
- If the file does not exist, `init_schema` proceeds with a `default` partition only and logs a warning

### 0.2 — Registry Reader (runtime)
Add function:
```rust
fn load_type_registry() -> Vec<String>
```
- Reads `target/ousia/type_registry`
- Deduplicates
- Returns sorted Vec<String>
- Returns empty vec (not error) if file missing

---

## Phase 1 — Sequences

### 1.1 — Drop `sequences` table approach
Remove:
- `sequences` table creation
- Any `UPDATE sequences SET value = value + 1` query

### 1.2 — Add `sequence_registry` table
```sql
CREATE TABLE IF NOT EXISTS sequence_registry (
    id    UUID PRIMARY KEY,
    name  TEXT NOT NULL UNIQUE
);
```
- Written once per sequence on first use
- Never updated
- Used for discoverability / admin

### 1.3 — Sequence creation helper
Add function:
```rust
async fn ensure_sequence(tx: &mut PgTx, name: &str) -> Result<()>
```

Executes:
```sql
-- Create sequence if not exists (PG 9.5+)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class 
        WHERE relkind = 'S' AND relname = $1
    ) THEN
        EXECUTE format('CREATE SEQUENCE %I CACHE 100', $1);
    END IF;
END$$;

-- Register in registry
INSERT INTO sequence_registry (id, name)
VALUES (gen_random_uuid(), $1)
ON CONFLICT (name) DO NOTHING;
```

### 1.4 — Public API
```rust
// increment and return next value
async fn next_val(pool: &PgPool, name: &str) -> Result<i64>
// SELECT nextval($1)

// peek without incrementing  
async fn cur_val(pool: &PgPool, name: &str) -> Result<i64>
// SELECT currval($1) -- note: requires nextval called first in session
// If currval not safe, use: SELECT last_value FROM <seq> -- direct catalog read
```

---

## Phase 2 — Objects Table (Partitioned)

### 2.1 — Drop and recreate as partitioned table
```sql
-- Run in migration, not init_schema directly
CREATE TABLE IF NOT EXISTS public.objects (
    id          UUID        NOT NULL,
    type        TEXT        NOT NULL,
    owner       UUID        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    data        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    index_meta  JSONB       NOT NULL DEFAULT '{}'::jsonb
) PARTITION BY LIST (type);
```

Note: PRIMARY KEY must include partition key.
```sql
-- PK includes type since it's the partition key
ALTER TABLE objects ADD PRIMARY KEY (type, id);
```

### 2.2 — Partition creation (called from init_schema)
For each type in registry:
```sql
CREATE TABLE IF NOT EXISTS objects_{type}
    PARTITION OF objects
    FOR VALUES IN ('{type}');
```

For safety, always ensure a default partition exists:
```sql
CREATE TABLE IF NOT EXISTS objects_default
    PARTITION OF objects DEFAULT;
```

### 2.3 — Orphan partition cleanup
After creating all known partitions:
```sql
-- Find partitions not in registry
SELECT c.relname
FROM pg_class c
JOIN pg_inherits i ON i.inhrelid = c.oid
JOIN pg_class p ON p.oid = i.inhparent
WHERE p.relname = 'objects'
  AND c.relname NOT IN ('objects_default', {known_partitions})
  AND NOT EXISTS (
      SELECT 1 FROM objects PARTITION OF ... -- skip non-empty
  );
```
Drop only if empty:
```sql
-- Check before drop
SELECT count(*) FROM objects_{orphan} LIMIT 1;
-- If 0: DROP TABLE objects_{orphan};
-- If > 0: log warning, do NOT drop, alert operator
```

### 2.4 — Indexes on parent table (PG propagates to all partitions)
```sql
-- Primary lookup: type + owner, sorted by each timestamp
CREATE INDEX IF NOT EXISTS idx_objects_owner_created
    ON objects(type, owner, created_at DESC)
    INCLUDE (id, updated_at);

CREATE INDEX IF NOT EXISTS idx_objects_owner_updated
    ON objects(type, owner, updated_at DESC)
    INCLUDE (id, created_at);

-- Range query support: owner > 'A' without losing sort
CREATE INDEX IF NOT EXISTS idx_objects_type_created
    ON objects(type, created_at DESC)
    INCLUDE (owner, id);

CREATE INDEX IF NOT EXISTS idx_objects_type_updated
    ON objects(type, updated_at DESC)
    INCLUDE (owner, id);

-- GIN for dynamic index_meta queries
CREATE INDEX IF NOT EXISTS idx_objects_index_meta
    ON objects USING GIN (index_meta jsonb_path_ops);
```

Drop old indexes that are now redundant:
```sql
DROP INDEX IF EXISTS idx_objects_type_owner;
DROP INDEX IF EXISTS idx_objects_type_owner_created;
DROP INDEX IF EXISTS idx_objects_type_owner_updated;
```

### 2.5 — Per-table storage tuning
Apply to parent (propagates to partitions):
```sql
ALTER TABLE objects SET (
    autovacuum_vacuum_scale_factor   = 0.01,
    autovacuum_analyze_scale_factor  = 0.005,
    autovacuum_vacuum_cost_delay     = 2,
    fillfactor                       = 80
);
```

---

## Phase 3 — Edges Table (Partitioned)

### 3.1 — Recreate with created_at and partitioned
```sql
CREATE TABLE IF NOT EXISTS public.edges (
    "from"      UUID        NOT NULL,
    "to"        UUID        NOT NULL,
    type        TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    data        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    index_meta  JSONB       NOT NULL DEFAULT '{}'::jsonb
) PARTITION BY LIST (type);
```

### 3.2 — Partitions from registry (edge_ prefixed types)
```sql
CREATE TABLE IF NOT EXISTS edges_{type}
    PARTITION OF edges
    FOR VALUES IN ('{type}');

CREATE TABLE IF NOT EXISTS edges_default
    PARTITION OF edges DEFAULT;
```

### 3.3 — Indexes (production decisions — no deferral)

Unique constraint (must include partition key):
```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_edges_key
    ON edges("from", "to", type);
```

Outgoing edges — covering index, index-only scan for all common reads:
```sql
CREATE INDEX IF NOT EXISTS idx_edges_from_covering
    ON edges("from", type, created_at DESC)
    INCLUDE ("to", data, index_meta);
```

Incoming edges — same treatment:
```sql
CREATE INDEX IF NOT EXISTS idx_edges_to_covering
    ON edges("to", type, created_at DESC)
    INCLUDE ("from", data, index_meta);
```

GIN for dynamic filtering:
```sql
CREATE INDEX IF NOT EXISTS idx_edges_index_meta
    ON edges USING GIN (index_meta jsonb_path_ops);
```

Drop old thin indexes:
```sql
DROP INDEX IF EXISTS idx_edges_from_key;
DROP INDEX IF EXISTS idx_edges_to_key;
```

### 3.4 — Storage tuning
```sql
ALTER TABLE edges SET (
    autovacuum_vacuum_scale_factor   = 0.01,
    autovacuum_analyze_scale_factor  = 0.005,
    autovacuum_vacuum_cost_delay     = 2,
    fillfactor                       = 80
);

ALTER INDEX idx_edges_index_meta SET (gin_pending_list_limit = 32768);
```

---

## Phase 4 — unique_constraints (Partitioned)

### 4.1 — Recreate partitioned
```sql
CREATE TABLE IF NOT EXISTS public.unique_constraints (
    id    UUID NOT NULL,
    type  TEXT NOT NULL,
    key   TEXT NOT NULL,
    field TEXT NOT NULL,
    PRIMARY KEY (type, key)
) PARTITION BY LIST (type);

CREATE TABLE IF NOT EXISTS unique_constraints_default
    PARTITION OF unique_constraints DEFAULT;
```

### 4.2 — Partitions from registry (object types only)
```sql
CREATE TABLE IF NOT EXISTS unique_constraints_{type}
    PARTITION OF unique_constraints
    FOR VALUES IN ('{type}');
```

### 4.3 — Indexes
```sql
-- Drop redundant index (PK already covers this)
DROP INDEX IF EXISTS idx_unique_type_key;

-- Reverse lookup by object id (cascade deletes, object resolution)
CREATE INDEX IF NOT EXISTS idx_unique_id
    ON unique_constraints(id);
```

### 4.4 — Storage tuning
```sql
ALTER TABLE unique_constraints SET (
    autovacuum_vacuum_scale_factor  = 0.01,
    autovacuum_vacuum_cost_delay    = 2,
    autovacuum_vacuum_threshold     = 1000,
    fillfactor                      = 80
);
```

---

## Phase 5 — init_schema Refactor

### Execution order
1. Load type registry → `Vec<String>`
2. Create `sequence_registry` table
3. Create `ousia_meta` table  
4. Create parent `objects` table (partitioned) + indexes + tuning
5. Create parent `edges` table (partitioned) + indexes + tuning
6. Create parent `unique_constraints` table (partitioned) + indexes + tuning
7. For each type in registry:
   - Create `objects_{type}` partition
   - Create `edges_{type}` partition (edge-typed entries only)
   - Create `unique_constraints_{type}` partition
8. Ensure `_default` partitions exist for all three tables
9. Drop orphaned empty partitions (objects, edges, unique_constraints)
10. Run `ensure_sequence` for any sequences needed at boot
11. Commit

### Idempotency requirements
- All `CREATE TABLE` → `CREATE TABLE IF NOT EXISTS`
- All `CREATE INDEX` → `CREATE INDEX IF NOT EXISTS`
- All partition creation → check `pg_class` before creating
- Orphan drop → check row count before dropping
- Entire `init_schema` runs in a single transaction

---

## Phase 6 — postgresql.conf Recommendations
Document these in a `TUNING.md` at repo root:

```ini
shared_buffers                    = 8GB
effective_cache_size              = 24GB
work_mem                          = 128MB
maintenance_work_mem              = 2GB
max_worker_processes              = 6
max_parallel_workers              = 4
max_parallel_workers_per_gather   = 2
max_parallel_maintenance_workers  = 2
wal_buffers                       = 64MB
checkpoint_completion_target      = 0.9
checkpoint_timeout                = 15min
max_wal_size                      = 4GB
gin_pending_list_limit            = 64MB
random_page_cost                  = 1.1
effective_io_concurrency          = 200
```

---

Do not touch query layer — all existing queries against `objects`, `edges`, 
`unique_constraints` work unchanged against partitioned tables. 
Postgres routes transparently based on type value.

And most especially WE'll only be Supporting Postgres... So ignore Sqlite and Cockroach (they are deprecated from now on)
```
