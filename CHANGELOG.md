# Changelog

`ousia` and `ousia_derive` are released together; a minor release of the
derive is a minor release of `ousia`. `ousia-ledger` versions separately.

## ousia 2.5.1 — 2026-09-28

### Fixed

- `init_schema` failed at startup once an object type had been removed from
  the manifest: it dropped the type's empty `objects_<t>` partition before the
  `object_constraints_<t>`, `object_geo_<t>` and edge partitions holding
  foreign keys to it, and Postgres refused (`cannot drop table … because other
  objects depend on it`). Orphans are now dropped dependents first. An orphaned
  object partition that a table ousia keeps (or doesn't manage) still
  references is logged and skipped instead of aborting startup.

ousia 2.5.0 is yanked. The same fix is released for the 2.4 line as 2.4.3.

## ousia 2.5.0 / ousia_derive 2.3.0 — 2026-09-27

### Added

- **Composite indexes.** `#[ousia(composite_index = "author, created_at desc")]`
  on an `OusiaObject` declares a btree over `index`-declared fields and the
  columns `id`, `owner`, `created_at`, `updated_at`, each optionally `asc` /
  `desc`. `init_schema` builds it on the type's own partition with
  `CREATE INDEX CONCURRENTLY` after its transaction commits, rebuilds it when
  its definition changes or a build left it `INVALID`, and drops ousia-built
  indexes that are no longer declared. Indexes added by hand are never touched.
- **Query routing to composite indexes**, with no change to how queries are
  written:
  - a `where_eq` on a field in a composite index's usable prefix compiles to
    the index expression instead of a GIN containment test;
  - `where_in` and range comparisons on the leading fields seek the index;
  - **fan-in**: a `where_in` / `where_eq` on the prefix, sorted by the columns
    that follow it, with a limit, reads each value's first `limit` entries
    off the index and merges them. Pages, cursors and ties match the plain
    query exactly.
- `IndexCast` and `ToIndexValue::INDEX_CAST` (default `IndexCast::Text`). A
  custom `ToIndexValue` type that returns an int, float or bool sets it to
  `BigInt`, `Double` or `Boolean` so its composite index matches the query.
- `CompositeIndex`, `IndexElement`, `IndexSource` and
  `manifest::composite_indexes_for`, exported for tooling.

### Changed

- `TypeManifestEntry` has a new `composite_indexes` field. The derives fill it;
  code that builds entries by hand must add `composite_indexes: &[]`.
- The `ousia` minor version is part of the `schema:composed` hash, so the
  first start on 2.5.0 logs one schema drift warning. It is not an error.

### Compile errors

- `composite_index` on an `OusiaEdge`.
- An element that is not an `index`-declared field or one of the four columns
  (typos, `type`, geo fields), a timestamp or array field, more than 32
  elements, an unknown direction, an element listed twice, or the same
  declaration twice.

### When to use composite indexes

Use them on types past roughly a million rows, for selective `where_in` /
`where_eq` queries that page in `created_at` order. Measured end to end at 3M
rows, such queries took 2–5 ms with the index against up to 40 ms without,
and stayed flat from 1M to 3M rows. Skip them on small types, on filters most
rows match, and on filters over a few very busy values, where the plain query
stayed about 2 ms faster. Each index cost about 6–9% on single-row writes and
roughly a quarter of the partition's size in the benchmark. See
[TUNING.md](TUNING.md#when-to-use-it) for the full numbers.

## ousia 2.4.3 — 2026-09-28

### Fixed

- The same orphaned-partition startup failure as 2.5.1, backported.

## ousia 2.4.2 — 2026-09-26

### Added

- `Comparison::In`: `where_in`, `or_in`, `edge_in`, `edge_or_in`. An empty
  list matches no rows; a scalar value is treated as equality.
- `ToIndexValue` for `Vec<Uuid>`, stored as the same hyphenated text as a
  single `Uuid`.

### Fixed

- Every `created_at` / `updated_at` filter reads the column, never
  `index_meta`.

## ousia_derive 2.2.1 — 2026-09-22

### Fixed

- `created_at` / `updated_at` are no longer copied into `index_meta`.

## ousia 2.4.1 — 2026-09-22

### Fixed

- `created_at` / `updated_at` filters use the indexed columns.
- Traversals honour target sorts and page with a cursor over the same order.
