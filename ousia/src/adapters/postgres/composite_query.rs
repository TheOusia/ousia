//! Shaping object queries so Postgres can use the type's composite indexes.
//!
//! Two rewrites, both decided from the type's `composite_index` declarations
//! in the manifest and both result-preserving:
//!
//! - **Equality through the btree.** `where_eq` on an `index_meta` field
//!   compiles to `index_meta @> {...}`, which only the GIN index serves. When
//!   the field is part of a composite index's usable prefix, the filter is
//!   rewritten to a one-value `In`, which compiles to the same expression the
//!   composite index is built on.
//! - **Fan-in.** A `where_in` (or `where_eq`) on a composite index's leading
//!   field(s), sorted by the columns that follow and limited, is answered one
//!   value at a time:
//!   each value reads the index in order and stops after `limit` rows, and the
//!   per-value results are merged. The cost is `values × limit` index entries,
//!   independent of the table's size. A single `= ANY(...)` can't do that:
//!   Postgres either walks the whole type in sort order discarding
//!   non-matching rows, or fetches every row of every value and sorts them.

use crate::adapters::Query;
use crate::manifest::{CompositeIndex, IndexSource};
use crate::query::{
    Comparison, IndexCast, IndexValue, IndexValueInner, Operator, QueryFilter, QueryMode,
};

/// A query `query_objects` answers by fan-in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FanIn {
    /// Position in `Query::filters` of the `In` filter that is fanned out.
    pub filter: usize,
    /// `ORDER BY` for one value's index scan: the query's sort keys, which
    /// the composite index already holds in order (no `id` tie-break).
    pub inner_order: String,
}

fn and_search(filter: &QueryFilter) -> Option<&Comparison> {
    let search = filter.mode.as_search()?;
    (search.operator == Operator::And).then_some(&search.comparison)
}

/// Whether `value` compiles to the cast the composite index uses for a field.
fn value_matches_cast(value: &IndexValue, cast: IndexCast) -> bool {
    match (value, cast) {
        (IndexValue::String(_) | IndexValue::Uuid(_), IndexCast::Text) => true,
        (IndexValue::Int(_), IndexCast::BigInt) => true,
        (IndexValue::Float(_), IndexCast::Double) => true,
        (IndexValue::Array(arr), cast) => match (arr.first(), cast) {
            (Some(IndexValueInner::String(_)), IndexCast::Text) => true,
            (Some(IndexValueInner::Int(_)), IndexCast::BigInt) => true,
            (Some(IndexValueInner::Float(_)), IndexCast::Double) => true,
            _ => false,
        },
        _ => false,
    }
}

/// The filter that makes `name` usable as an equality prefix of a btree: an
/// `AND` non-empty `In` whose value compiles to `cast` — or, with
/// `with_equal`, an equality that `route_equalities` would turn into one.
fn prefix_filter(
    filters: &[QueryFilter],
    name: &str,
    cast: IndexCast,
    with_equal: bool,
) -> Option<usize> {
    filters.iter().position(|f| {
        f.field.name == name
            && match (and_search(f), &f.value) {
                (Some(Comparison::Equal), value) => with_equal && value_matches_cast(value, cast),
                (Some(Comparison::In), IndexValue::Array(arr)) => {
                    !arr.is_empty() && value_matches_cast(&f.value, cast)
                }
                (Some(Comparison::In), value) => value_matches_cast(value, cast),
                _ => false,
            }
    })
}

/// The filter position for each leading `index_meta` element of `idx` that
/// the query pins (see `prefix_filter`), in index order.
fn usable_prefix(idx: &CompositeIndex, filters: &[QueryFilter], with_equal: bool) -> Vec<usize> {
    let mut prefix = Vec::new();
    for element in idx.elements {
        let IndexSource::IndexMeta(cast) = element.source else {
            break;
        };
        match prefix_filter(filters, element.name, cast, with_equal) {
            Some(pos) => prefix.push(pos),
            None => break,
        }
    }
    prefix
}

/// Rewrite `where_eq` filters on fields in a composite index's usable prefix
/// to one-value `In`s, so they compile to the index's expression instead of
/// a GIN containment test. Filters that can't use a composite index are left
/// alone — rewriting them would take them off the GIN index for nothing.
pub(super) fn route_equalities(indexes: &[&CompositeIndex], filters: &mut [QueryFilter]) {
    let mut rewrite = Vec::new();
    for idx in indexes {
        rewrite.extend(usable_prefix(idx, filters, true));
    }
    for pos in rewrite {
        let filter = &mut filters[pos];
        if and_search(filter) != Some(&Comparison::Equal) {
            continue;
        }
        let element = match &filter.value {
            IndexValue::String(s) => IndexValueInner::String(s.clone()),
            IndexValue::Uuid(u) => IndexValueInner::String(u.hyphenated().to_string()),
            IndexValue::Int(i) => IndexValueInner::Int(*i),
            IndexValue::Float(f) => IndexValueInner::Float(*f),
            _ => continue,
        };
        filter.value = IndexValue::Array(vec![element]);
        filter.mode = QueryMode::search(Comparison::In, Some(Operator::And));
    }
}

/// Whether `query` can be answered by fan-in over one of `indexes`, and how.
///
/// Requires: a limit; only `AND` filters; no geo filter, geo order or random
/// sort; a composite index whose usable prefix holds at most one `In` with
/// two or more distinct values (the one fanned out; with none, the first
/// prefix field is, over its single value); and sort keys that are exactly
/// the native columns following that prefix in the index, all in the index's
/// direction or all reversed.
///
/// A single value is worth it too: the lookup reads `limit` entries off the
/// index in order, where Postgres may instead walk the whole type in sort
/// order when it judges the value common — a cost that grows with the type.
pub(super) fn plan_fan_in(indexes: &[&CompositeIndex], query: &Query) -> Option<FanIn> {
    query.limit?;
    if !query.geo_filters.is_empty() || query.geo_order.is_some() {
        return None;
    }
    let filters = &query.filters;
    if filters.iter().any(|f| {
        f.mode.is_random_sort()
            || f.mode.as_search().is_some_and(|s| s.operator != Operator::And)
    }) {
        return None;
    }
    let sort: Vec<(&str, bool)> = filters
        .iter()
        .filter_map(|f| Some((f.field.name, f.mode.as_sort()?.ascending)))
        .collect();
    if sort.is_empty() {
        return None;
    }

    for idx in indexes {
        let prefix = usable_prefix(idx, filters, false);
        let multi: Vec<usize> = prefix
            .iter()
            .copied()
            .filter(|&pos| distinct_values(&filters[pos].value) >= 2)
            .collect();
        let fanned = match multi[..] {
            [pos] => pos,
            [] => match prefix.first() {
                Some(&pos) => pos,
                None => continue,
            },
            _ => continue,
        };

        let rest = &idx.elements[prefix.len()..];
        if rest.len() < sort.len() {
            continue;
        }
        let follows_index = sort.iter().zip(rest).all(|((name, _), element)| {
            element.source == IndexSource::Column && element.name == *name
        });
        let same = sort.iter().zip(rest).all(|((_, asc), e)| *asc != e.descending);
        let reversed = sort.iter().zip(rest).all(|((_, asc), e)| *asc == e.descending);
        if !follows_index || !(same || reversed) {
            continue;
        }

        let inner_order = sort
            .iter()
            .map(|(name, asc)| format!("o.{name} {}", if *asc { "ASC" } else { "DESC" }))
            .collect::<Vec<_>>()
            .join(", ");
        return Some(FanIn { filter: fanned, inner_order });
    }
    None
}

fn distinct_values(value: &IndexValue) -> usize {
    let IndexValue::Array(arr) = value else {
        return 1;
    };
    let mut seen: Vec<&IndexValueInner> = Vec::with_capacity(arr.len());
    for v in arr {
        if !seen.contains(&v) {
            seen.push(v);
        }
    }
    seen.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::IndexElement;
    use crate::query::{IndexField, IndexKind, SortAs};

    static ACTOR: IndexField =
        IndexField { name: "actor", kinds: &[IndexKind::Search], sort_as: SortAs::Json };
    static KIND: IndexField =
        IndexField { name: "kind", kinds: &[IndexKind::Search], sort_as: SortAs::Json };
    static CREATED: IndexField = IndexField {
        name: "created_at",
        kinds: &[IndexKind::Search, IndexKind::Sort],
        sort_as: SortAs::Timestamp,
    };
    static UPDATED: IndexField = IndexField {
        name: "updated_at",
        kinds: &[IndexKind::Search, IndexKind::Sort],
        sort_as: SortAs::Timestamp,
    };

    const ACTOR_CREATED: CompositeIndex = CompositeIndex {
        elements: &[
            IndexElement { name: "actor", source: IndexSource::IndexMeta(IndexCast::Text), descending: false },
            IndexElement { name: "created_at", source: IndexSource::Column, descending: true },
        ],
    };
    const ACTOR_KIND_CREATED: CompositeIndex = CompositeIndex {
        elements: &[
            IndexElement { name: "actor", source: IndexSource::IndexMeta(IndexCast::Text), descending: false },
            IndexElement { name: "kind", source: IndexSource::IndexMeta(IndexCast::Text), descending: false },
            IndexElement { name: "created_at", source: IndexSource::Column, descending: true },
        ],
    };

    fn feed() -> Query {
        Query::wide()
            .where_in(&ACTOR, vec!["a", "b"])
            .sort_desc(&CREATED)
            .with_limit(20)
    }

    #[test]
    fn fans_in_on_the_leading_field_sorted_by_the_next_column() {
        assert_eq!(
            plan_fan_in(&[&ACTOR_CREATED], &feed()),
            Some(FanIn { filter: 0, inner_order: "o.created_at DESC".into() })
        );
        // A btree reads backwards just as well.
        let asc = Query::wide().where_in(&ACTOR, vec!["a", "b"]).sort_asc(&CREATED).with_limit(5);
        assert!(plan_fan_in(&[&ACTOR_CREATED], &asc).is_some());
    }

    #[test]
    fn needs_a_limit_and_a_matching_sort() {
        let no_limit = Query::wide().where_in(&ACTOR, vec!["a", "b"]).sort_desc(&CREATED);
        let other_sort =
            Query::wide().where_in(&ACTOR, vec!["a", "b"]).sort_desc(&UPDATED).with_limit(5);
        let no_sort = Query::wide().where_in(&ACTOR, vec!["a", "b"]).with_limit(5);
        let no_prefix = Query::wide().where_in(&KIND, vec!["a", "b"]).sort_desc(&CREATED).with_limit(5);
        for q in [no_limit, other_sort, no_sort, no_prefix] {
            assert_eq!(plan_fan_in(&[&ACTOR_CREATED], &q), None, "{q:?}");
        }
    }

    #[test]
    fn a_single_value_is_fanned_in_too() {
        let q = Query::wide().where_in(&ACTOR, vec!["a", "a"]).sort_desc(&CREATED).with_limit(5);
        assert_eq!(plan_fan_in(&[&ACTOR_CREATED], &q).map(|f| f.filter), Some(0));
        let mut q = Query::wide().where_eq(&ACTOR, "a").sort_desc(&CREATED).with_limit(5);
        route_equalities(&[&ACTOR_CREATED], &mut q.filters);
        assert_eq!(plan_fan_in(&[&ACTOR_CREATED], &q).map(|f| f.filter), Some(0));
    }

    #[test]
    fn two_multi_value_fields_are_not_fanned_in() {
        let q = Query::wide()
            .where_in(&ACTOR, vec!["a", "b"])
            .where_in(&KIND, vec!["x", "y"])
            .sort_desc(&CREATED)
            .with_limit(5);
        assert_eq!(plan_fan_in(&[&ACTOR_KIND_CREATED], &q), None);
    }

    #[test]
    fn an_or_filter_disables_fan_in() {
        let q = feed().or_eq(&KIND, "x");
        assert_eq!(plan_fan_in(&[&ACTOR_CREATED], &q), None);
    }

    #[test]
    fn a_pinned_middle_field_extends_the_prefix() {
        let mut q = Query::wide()
            .where_in(&ACTOR, vec!["a", "b"])
            .where_eq(&KIND, "post")
            .sort_desc(&CREATED)
            .with_limit(20);
        // Without the pin the sort doesn't follow `actor` in this index.
        assert_eq!(plan_fan_in(&[&ACTOR_KIND_CREATED], &feed()), None);
        assert_eq!(plan_fan_in(&[&ACTOR_KIND_CREATED], &q), None, "kind still a GIN equality");
        route_equalities(&[&ACTOR_KIND_CREATED], &mut q.filters);
        assert!(matches!(q.filters[1].value, IndexValue::Array(ref a) if a.len() == 1));
        assert_eq!(plan_fan_in(&[&ACTOR_KIND_CREATED], &q).map(|f| f.filter), Some(0));
    }

    #[test]
    fn equalities_are_routed_only_where_a_composite_index_can_use_them() {
        // `kind` is second in the index and `actor` is not pinned: GIN stays.
        let mut filters = Query::wide().where_eq(&KIND, "post").filters;
        route_equalities(&[&ACTOR_KIND_CREATED], &mut filters);
        assert!(matches!(filters[0].value, IndexValue::String(_)));

        // `actor` leads the index: through the btree, as a one-value `In`.
        let mut filters = Query::wide().where_eq(&ACTOR, "a").filters;
        route_equalities(&[&ACTOR_CREATED], &mut filters);
        assert!(matches!(filters[0].mode.as_search().unwrap().comparison, Comparison::In));

        // A value that doesn't match the index's cast is left alone.
        let mut filters = Query::wide().where_eq(&ACTOR, 5i64).filters;
        route_equalities(&[&ACTOR_CREATED], &mut filters);
        assert!(matches!(filters[0].value, IndexValue::Int(5)));
    }
}
