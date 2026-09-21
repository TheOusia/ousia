//! Type manifest — compile-time registry of every `Object` and `Edge`
//! linked into the binary.
//!
//! The `OusiaObject` and `OusiaEdge` derive macros each append one
//! [`TypeManifestEntry`] to the [`MANIFEST`] `linkme` slice. At runtime,
//! `init_schema` walks the slice to decide which Postgres partitions to
//! create (and drop) and to wire per-partition `ON DELETE CASCADE`
//! foreign keys for edges. It also emits the manifest to `target/ousia.json`
//! as a tooling/debugging artifact.

/// Re-export so derive expansions can name `::ousia::__linkme::distributed_slice`
/// without consumers depending on `linkme` directly.
#[doc(hidden)]
pub use linkme as __linkme;

/// Re-export so derive expansions can call
/// `::ousia::__rmp_serde::to_vec_named(...)` for msgpack encoding of the
/// `data` column without consumers depending on `rmp-serde` directly.
#[doc(hidden)]
pub use rmp_serde as __rmp_serde;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ManifestKind {
    Object,
    Edge,
}

/// One entry per registered type. `from_type` / `to_type` are populated
/// for edges (resolved via `<T as Object>::TYPE` in the derive
/// expansion) and `None` for objects.
///
/// `field_names` lists every non-meta field, in declaration order — the
/// same set the derived `Serialize`/`Deserialize` use to encode/decode
/// the `data` blob (keyed by name). Exposed here so `init_schema` can
/// sample stored rows and warn about field-name drift (a field present
/// in old data but no longer declared on the struct) without needing a
/// fully-typed decode — see `check_field_drift`.
#[derive(Debug, Clone, Copy)]
pub struct TypeManifestEntry {
    pub kind: ManifestKind,
    pub type_name: &'static str,
    pub from_type: Option<&'static str>,
    pub to_type: Option<&'static str>,
    pub field_names: &'static [&'static str],
    /// Old names still accepted on read via `#[ousia(rename = "old")]`.
    pub field_aliases: &'static [&'static str],
}

/// Compile-time set of every Object/Edge linked into the binary.
/// Populated by the `OusiaObject` / `OusiaEdge` derives via `linkme`.
#[linkme::distributed_slice]
pub static MANIFEST: [TypeManifestEntry];

/// All registered object type names. Order matches link order; for a
/// stable order use [`object_types_sorted`].
pub fn object_types() -> impl Iterator<Item = &'static str> {
    MANIFEST
        .iter()
        .filter(|e| matches!(e.kind, ManifestKind::Object))
        .map(|e| e.type_name)
}

/// All registered edge entries (full entry — callers usually need
/// `from_type` / `to_type` to wire FKs).
pub fn edge_entries() -> impl Iterator<Item = &'static TypeManifestEntry> {
    MANIFEST
        .iter()
        .filter(|e| matches!(e.kind, ManifestKind::Edge))
}

/// Object type names in deterministic sort order, **deduplicated** —
/// two struct definitions can legitimately share the same
/// `#[ousia(type_name = "...")]` (e.g. wide vs lean view types over
/// the same logical table) and both push entries, so callers should
/// see one name per logical type.
pub fn object_types_sorted() -> Vec<&'static str> {
    let mut v: Vec<&'static str> = object_types().collect();
    v.sort_unstable();
    v.dedup();
    v
}

/// Edge entries in deterministic sort order, deduplicated by the
/// `(type_name, from_type, to_type)` triple — two derives that
/// declare the same edge shape collapse to one. If two derives share
/// `type_name` but disagree on `from`/`to`, both stay (it's an edge
/// definition mismatch worth surfacing).
pub fn edge_entries_sorted() -> Vec<&'static TypeManifestEntry> {
    let mut v: Vec<&'static TypeManifestEntry> = edge_entries().collect();
    v.sort_unstable_by(|a, b| {
        a.type_name
            .cmp(b.type_name)
            .then_with(|| a.from_type.cmp(&b.from_type))
            .then_with(|| a.to_type.cmp(&b.to_type))
    });
    v.dedup_by(|a, b| {
        a.type_name == b.type_name && a.from_type == b.from_type && a.to_type == b.to_type
    });
    v
}

/// Render the manifest as a JSON string suitable for writing to
/// `target/ousia.json`. The shape is stable:
/// ```json
/// {
///   "objects": ["TypeA", "TypeB", ...],
///   "edges":   [{"type": "Follow", "from": "User", "to": "User"}, ...]
/// }
/// ```
pub fn render_json() -> String {
    let mut out = String::new();
    out.push_str("{\n  \"objects\": [");
    let objs = object_types_sorted();
    for (i, name) in objs.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push('"');
        out.push_str(name);
        out.push('"');
    }
    out.push_str("],\n  \"edges\": [");
    let edges = edge_entries_sorted();
    for (i, e) in edges.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str("{\"type\": \"");
        out.push_str(e.type_name);
        out.push_str("\", \"from\": \"");
        out.push_str(e.from_type.unwrap_or(""));
        out.push_str("\", \"to\": \"");
        out.push_str(e.to_type.unwrap_or(""));
        out.push_str("\"}");
    }
    out.push_str("]\n}\n");
    out
}
