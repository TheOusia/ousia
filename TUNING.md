# PostgreSQL Tuning Recommendations

Baseline for a dedicated Postgres 16 server targeting:
- `objects`: 100M rows
- `edges`: 100–150M rows
- `unique_constraints`: 100–200M rows

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

After `init_schema_typed` creates partitions, apply autovacuum and fillfactor
tuning to each **leaf** partition (PG does not propagate `ALTER TABLE SET` from
a partitioned parent).

Example for the `User` type:

```sql
ALTER TABLE objects_user SET (
    autovacuum_vacuum_scale_factor  = 0.01,
    autovacuum_analyze_scale_factor = 0.005,
    autovacuum_vacuum_cost_delay    = 2,
    fillfactor                      = 80
);

ALTER TABLE edges_follow SET (
    autovacuum_vacuum_scale_factor  = 0.01,
    autovacuum_analyze_scale_factor = 0.005,
    autovacuum_vacuum_cost_delay    = 2,
    fillfactor                      = 80
);

ALTER TABLE unique_constraints_user SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_cost_delay   = 2,
    autovacuum_vacuum_threshold    = 1000,
    fillfactor                     = 80
);
```

Repeat for every registered type. The `_default` catch-all partitions can use
default settings since they should remain nearly empty in production.

## GIN index pending list

Per-partition GIN index tuning (apply after `init_schema_typed`):

```sql
ALTER INDEX objects_user_index_meta_idx SET (gin_pending_list_limit = 32768);
```

## High-throughput sequences

Native PG sequences are created with `CACHE 1` by default for predictable
ordering. For counters that are incremented at >1K/s, raise the cache:

```sql
ALTER SEQUENCE "my-invoice-seq" CACHE 100;
```

Gaps on connection restart are expected and acceptable.
