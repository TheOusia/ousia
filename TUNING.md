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

## Counters

`counter_next_value` increments a row in the `sequences` table, so every call
for the same key serialises on that row's lock. For counters incremented at
>1K/s, keep the table on a low fillfactor so updates stay HOT, and prefer
sharding hot counters by key (e.g. `invoice-seq:{shard}`) over a single row:

```sql
ALTER TABLE sequences SET (fillfactor = 50);
```
