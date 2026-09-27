# PostgreSQL Tuning Recommendations

Baseline for a dedicated Postgres 16 server targeting:
- `objects`: 100M rows
- `object_edges`: 100–150M rows
- `object_constraints`: 100–200M rows

These settings are applied to `postgresql.conf` (or via `ALTER SYSTEM`).

## `postgresql.conf`

```ini
# Memory
shared_buffers                    = 8GB
effective_cache_size              = 24GB
work_mem                          = 128MB
maintenance_work_mem              = 2GB

# Parallelism
max_worker_processes              = 6
max_parallel_workers              = 4
max_parallel_workers_per_gather   = 2
max_parallel_maintenance_workers  = 2

# WAL / checkpoints
wal_buffers                       = 64MB
checkpoint_completion_target      = 0.9
checkpoint_timeout                = 15min
max_wal_size                      = 4GB

# GIN pending list
gin_pending_list_limit            = 64MB

# I/O (SSD)
random_page_cost                  = 1.1
effective_io_concurrency          = 200
```

## Per-partition storage tuning

`init_schema` creates one partition per registered type (`objects_{type}`,
`object_edges_{type}`, `object_constraints_{type}`, `object_geo_{type}`).
Postgres does not propagate `ALTER TABLE ... SET` from a partitioned parent,
so apply autovacuum and fillfactor tuning to each **leaf** partition.

Example for the `User` object type and `Follow` edge type:

```sql
ALTER TABLE objects_user SET (
    autovacuum_vacuum_scale_factor  = 0.01,
    autovacuum_analyze_scale_factor = 0.005,
    autovacuum_vacuum_cost_delay    = 2,
    fillfactor                      = 80
);

ALTER TABLE object_edges_follow SET (
    autovacuum_vacuum_scale_factor  = 0.01,
    autovacuum_analyze_scale_factor = 0.005,
    autovacuum_vacuum_cost_delay    = 2,
    fillfactor                      = 80
);

ALTER TABLE object_constraints_user SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_cost_delay   = 2,
    autovacuum_vacuum_threshold    = 1000,
    fillfactor                     = 80
);
```

Repeat for every registered type. The `_default` catch-all partitions can keep
default settings since they should stay nearly empty in production.

## GIN index pending list

The `index_meta` GIN index is declared on the parent tables; each partition gets
its own child index. Tune the hot ones individually (value is in kB):

```sql
ALTER INDEX objects_user_index_meta_idx SET (gin_pending_list_limit = 32768);
```

## Composite indexes

`index = "field:search"` copies a field into the `index_meta` JSONB column. It
does **not** give that field a Postgres index of its own. The only index on
`index_meta` is the GIN index above, and GIN serves containment (`@>`) and
nothing else — `where_eq` / `where_ne` on a scalar and `where_contains*` on an
array. `where_in`, `where_gt`/`gte`/`lt`/`lte` and `where_begins_with` read the
field as `index_meta->>'field'` and are filters, not seeks. On a small type
that's fine: Postgres walks the type newest-first through its `(type,
created_at)` index and checks each row. But that walk reads (rows in the type)
÷ (rows that match) to fill a page, so a selective filter — one account's
history, a feed that follows quiet accounts — gets slower as the type grows.

A `composite_index` declaration gives those queries a btree to seek on:

```rust
#[derive(OusiaObject)]
#[ousia(
    type_name = "Event",
    index = "author:search",
    composite_index = "author, created_at desc",
)]
pub struct Event { /* … */ }
```

`init_schema` builds it on that type's partition only (never on the parent,
which would copy it onto every type's partition):

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS objects_event_author_created_at_desc_idx
    ON objects_event ((index_meta->>'author'), created_at DESC);
```

**Elements.** Comma-separated, each an `index`-declared field of the same
struct or one of the columns `id`, `owner`, `created_at`, `updated_at`,
optionally followed by `asc` or `desc` (default `asc`), up to 32. Any mix and
order works — `"author, created_at desc"`, `"region, status"`,
`"region, tier, created_at desc"`. Anything else — a typo, a geo field,
`type` — is a compile error.

**Order matters.** A btree is searched through its leading elements. An index
on `(region, tier, created_at desc)` serves a filter on `region`, on `region` +
`tier`, and on `region` + `tier` + a `created_at` range or sort — not one on
`tier` alone, or `created_at` alone. Put the fields every query filters on
first, the ones some queries add next, and the timestamp last.

**Casts.** Each field is indexed the way the query builder reads it, by Rust
type — `String` / `Uuid` as `(index_meta->>'f')`, integers as
`((index_meta->>'f')::bigint)`, floats as `::double precision`, `bool` as
`::boolean` — because Postgres only uses an expression index for a query with
the same expression. Timestamp and array fields are compile errors: text →
`timestamptz` depends on the session time zone, so Postgres refuses to index
it, and arrays belong to GIN. A custom `ToIndexValue` type whose
`to_index_value` returns an int, float or bool must set
`const INDEX_CAST: IndexCast = IndexCast::BigInt;` (or `Double` / `Boolean`).

### How queries use it

ousia shapes each object query around the type's composite indexes; nothing
changes in how queries are written.

- **Equality.** A `where_eq` on a field in a composite index's usable prefix
  (the leading fields, each pinned by an equality or `where_in`) compiles to
  the index's expression instead of a GIN containment test, so the btree
  serves it. Equalities that no composite index can use stay on GIN.
- **Ranges.** `where_in` and `where_gt`/`lt`/… on the leading field(s) seek
  the index; a `created_at` / `updated_at` element after them serves a range
  or sort on that column.
- **Fan-in.** A `where_in` or `where_eq` on the prefix, sorted by the columns
  that follow it in the index, with a limit — a feed page, an account's
  history — is answered one value at a time: each value reads its newest (or
  oldest) `limit` entries straight off the index, and the per-value results
  are merged in the query's order. The cost is about `values × limit` index
  entries whatever the size of the type, where a single `= ANY(...)` makes
  Postgres either walk the whole type in sort order or fetch every row of
  every value and sort them. Cursors, ties and duplicate values give exactly
  the same pages as the plain query. Fan-in runs as one read-only transaction
  with bitmap scans turned off for that statement, which is what keeps each
  value's lookup an ordered, limited index read.

Not served: sorting by an `index_meta` field (the sort reads the JSON value,
not the indexed expression); `where_begins_with` (`ILIKE`, which a btree
can't); a `Uuid` field compared with `where_gt` / `where_lt` (compared as
`uuid`, indexed as text); and queries with an `or_*` filter, which are left as
written.

### When to use it

A composite index trades disk and a little write time for read times that stay
flat as the type grows. It pays off when all of these hold:

- **The type is large or will be** — past roughly a million rows. Below that the
  newest-first walk through `(type, created_at)` is already fast, and at 1M rows
  the plain query still won some of the cases measured below.
- **The query is selective on an `index_meta` field** — `where_in` / `where_eq`
  on an account, region, status or similar, where most rows of the type don't
  match: one account's history, a feed over the accounts someone follows, the
  open orders in one region.
- **It sorts by a column that comes after that field in the index** —
  typically `created_at desc` — **and takes a page** (`with_limit`). That is the
  fan-in shape above, and it is where the gain is largest.
- **Readers page deep.** Walking back months costs the plain query more rows
  per page; the index seeks straight to the cursor.

Skip it when:

- the type is small or stays small;
- most rows match the filter (a flag nearly every row has) — walking in order
  finds a page almost at once;
- the filtered values are few and very busy. Fan-in reads about
  `values × limit` index entries, while the plain walk stops as soon as it has
  one page, and accounts that each post constantly fill that page quickly;
- the type is write-heavy and read by that field rarely.

Measured end to end through the engine on a single connection (Postgres 16,
21-row page sorted by `created_at desc`, median of 15 runs, `where_in` over the
followed accounts plus a `where_eq` on a second field;
`composite_index = "author, created_at desc"`). Accounts' activity follows a
power law: the busiest 50 write about 10% of rows. Both variants returned the
same rows in every case.

| reader | 1M rows, without | 1M, with | 3M rows, without | 3M, with |
|---|---:|---:|---:|---:|
| follows 100 quiet accounts | 13.1 ms | 3.8 ms | 40.4 ms | 2.7 ms |
| follows 200 typical accounts | 3.0 ms | 8.2 ms | 6.9 ms | 5.1 ms |
| same, page 6 months back | 2.9 ms | 5.8 ms | 7.0 ms | 4.3 ms |
| follows the 50 busiest accounts | 1.2 ms | 3.4 ms | 1.2 ms | 3.0 ms |
| one account (`where_eq`) | 2.8 ms | 2.2 ms | 2.3 ms | 1.8 ms |

With the index, every case stays between 2 and 5 ms from 1M to 3M rows. Without
it, the selective cases grow with the type: 3× for the quiet feed, 2.3× for the
typical one. Following only the busiest accounts is the one case the plain
query keeps winning, by under 2 ms, and that gap didn't widen with size.

Single-row creates went from 0.90 ms to 0.98 ms. The index was 269 MB next to a
1,065 MB partition at 3M rows (89 MB / 355 MB at 1M), about 90 bytes per row for
a UUID stored as text plus a timestamp.

### Building, changing, removing

`init_schema` builds composite indexes after its transaction commits, with
`CREATE INDEX CONCURRENTLY`, so a build on a type that already has millions of
rows doesn't block its writes. Startup does wait for the build — allow for it
in any startup/liveness deadline. The builds run on a connection of their own
with `statement_timeout` lifted (that connection is closed afterwards, never
returned to the pool), so a deployment-wide timeout doesn't abort them
half-way. Each index carries a comment holding its definition, and on every
start `init_schema`:

- builds indexes that are declared but missing;
- rebuilds one whose definition changed (e.g. a field's type changed) or that a
  failed concurrent build left `INVALID`;
- drops ousia-built indexes that are no longer declared.

Indexes you add to the partition by hand have no such comment and are never
touched. During a rolling deploy that changes the declarations, an instance
started with the *old* list drops the new indexes and the next new-version
start rebuilds them — avoid restarting old instances mid-deploy.

**Names.** `objects_<type>_<element>[_desc]…_idx`, lowercased. Postgres
truncates identifiers at 63 bytes, so a longer name is cut and suffixed with a
hash of the full name; it stays unique and is the same on every start.
Composite indexes live on partitions, so they are not part of the
`schema:composed` hash, which covers the parent tables.

**Cost.** Each composite index is one more btree to update on every write to
the type (measured: about +6–9% on single-row inserts, +40% on large batched
inserts), and its size is roughly the indexed values plus a timestamp per row
(see [When to use it](#when-to-use-it) for measured sizes).

## Counters

`counter_next_value` increments a row in the `sequences` table. That is what
makes counters gap-free, and it also means increments of the same key run one
at a time. Measured on Postgres 16: one hot key tops out around 4-5K
increments/s (it plateaus rather than collapsing as clients are added), 1,000
keys spread across 32 clients reach ~34K/s, and a native sequence ~120K/s.

- Keep per-key rates well under that ceiling, or split unrelated counters into
  separate keys. Don't shard one logical counter across keys if it must stay a
  single gap-free sequence (invoice numbers).
- For high-rate IDs that may have gaps, use a native sequence
  (`CREATE SEQUENCE ... CACHE 100` + `nextval`) instead of a counter.
- A lower fillfactor keeps these single-row updates HOT:

```sql
ALTER TABLE sequences SET (fillfactor = 50);
```
