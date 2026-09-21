use super::*;

// ============================================================
// Section 15: Two-hop batch traversal + batch owned-object count (2.1.0)
// ============================================================

/// Ids for the shared 2-hop fixture graph:
///
/// hub1 -[pos 1]-> s1 -> l1(req), l2(!req), l3(req)
/// hub1 -[pos 2]-> s2 -> (no leaves)
/// hub2 -[pos 1]-> s3 -> l4(req)
/// hub3 -> (no spokes)
struct TwoHopIds {
    hub1: uuid::Uuid,
    hub2: uuid::Uuid,
    hub3: uuid::Uuid,
    s1: uuid::Uuid,
    s2: uuid::Uuid,
    s3: uuid::Uuid,
    l1: uuid::Uuid,
    l2: uuid::Uuid,
    l3: uuid::Uuid,
    l4: uuid::Uuid,
}

async fn setup_two_hop_fixture() -> (ContainerAsync<Postgres>, Engine, TwoHopIds) {
    let (container, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut hubs = Vec::new();
    for name in ["hub1", "hub2", "hub3"] {
        let mut h = Hub::default();
        h.name = name.into();
        engine.create_object(&h).await.unwrap();
        hubs.push(h.id());
    }
    let mut spokes = Vec::new();
    for name in ["s1", "s2", "s3"] {
        let mut s = Spoke::default();
        s.name = name.into();
        engine.create_object(&s).await.unwrap();
        spokes.push(s.id());
    }
    let mut leaves = Vec::new();
    for name in ["l1", "l2", "l3", "l4"] {
        let mut l = Leaf::default();
        l.name = name.into();
        engine.create_object(&l).await.unwrap();
        leaves.push(l.id());
    }
    let ids = TwoHopIds {
        hub1: hubs[0],
        hub2: hubs[1],
        hub3: hubs[2],
        s1: spokes[0],
        s2: spokes[1],
        s3: spokes[2],
        l1: leaves[0],
        l2: leaves[1],
        l3: leaves[2],
        l4: leaves[3],
    };

    for (hub, spoke, position) in [
        (ids.hub1, ids.s1, 1i64),
        (ids.hub1, ids.s2, 2),
        (ids.hub2, ids.s3, 1),
    ] {
        engine
            .create_edge(&HubSpoke {
                _meta: EdgeMeta::new(hub, spoke),
                position,
            })
            .await
            .unwrap();
    }
    for (spoke, leaf, required) in [
        (ids.s1, ids.l1, true),
        (ids.s1, ids.l2, false),
        (ids.s1, ids.l3, true),
        (ids.s3, ids.l4, true),
    ] {
        engine
            .create_edge(&SpokeLeaf {
                _meta: EdgeMeta::new(spoke, leaf),
                required,
            })
            .await
            .unwrap();
    }

    (container, engine, ids)
}

#[tokio::test]
async fn test_two_hop_collect_with_target_full_chain() {
    let (_r, engine, ids) = setup_two_hop_fixture().await;

    let unknown_id = uuid::Uuid::now_v7();
    let by_hub = engine
        .batch_edge::<HubSpoke, Hub, Spoke>(&[ids.hub1, ids.hub2, ids.hub3, unknown_id])
        .then_edge::<SpokeLeaf, Leaf>()
        .collect_with_target()
        .await
        .unwrap();

    // Every pivot id appears — zero-edge and unknown ids map to empty Vecs.
    assert_eq!(by_hub.len(), 4);
    assert!(by_hub.get(&ids.hub3).unwrap().is_empty());
    assert!(by_hub.get(&unknown_id).unwrap().is_empty());

    // hub1: two spokes; s1 carries 3 leaves with edge metadata, s2 none (LEFT JOIN).
    let hub1_children = by_hub.get(&ids.hub1).unwrap();
    assert_eq!(hub1_children.len(), 2);
    let (s1, s1_leaves) = hub1_children
        .iter()
        .find(|(s, _)| s.id() == ids.s1)
        .unwrap();
    assert_eq!(s1.name, "s1");
    assert_eq!(s1_leaves.len(), 3);
    let mut leaf_ids: Vec<uuid::Uuid> = s1_leaves.iter().map(|oe| oe.object().id()).collect();
    leaf_ids.sort();
    let mut expected = vec![ids.l1, ids.l2, ids.l3];
    expected.sort();
    assert_eq!(leaf_ids, expected);
    // Edge metadata survives the join.
    let l2_edge = s1_leaves
        .iter()
        .find(|oe| oe.object().id() == ids.l2)
        .unwrap()
        .edge();
    assert!(!l2_edge.required);
    assert!(
        s1_leaves
            .iter()
            .filter(|oe| oe.object().id() != ids.l2)
            .all(|oe| oe.edge().required)
    );
    let (_, s2_leaves) = hub1_children
        .iter()
        .find(|(s, _)| s.id() == ids.s2)
        .unwrap();
    assert!(s2_leaves.is_empty());

    // hub2: one spoke with exactly its own leaf — no cross-contamination.
    let hub2_children = by_hub.get(&ids.hub2).unwrap();
    assert_eq!(hub2_children.len(), 1);
    let (s3, s3_leaves) = &hub2_children[0];
    assert_eq!(s3.id(), ids.s3);
    assert_eq!(s3_leaves.len(), 1);
    assert_eq!(s3_leaves[0].object().id(), ids.l4);
}

#[tokio::test]
async fn test_two_hop_collect_drops_edge_metadata() {
    let (_r, engine, ids) = setup_two_hop_fixture().await;

    let by_hub = engine
        .batch_edge::<HubSpoke, Hub, Spoke>(&[ids.hub1, ids.hub2])
        .then_edge::<SpokeLeaf, Leaf>()
        .collect()
        .await
        .unwrap();

    let hub1_children = by_hub.get(&ids.hub1).unwrap();
    assert_eq!(hub1_children.len(), 2);
    let (_, s1_leaves) = hub1_children
        .iter()
        .find(|(s, _)| s.id() == ids.s1)
        .unwrap();
    assert_eq!(s1_leaves.len(), 3);
    let (_, s2_leaves) = hub1_children
        .iter()
        .find(|(s, _)| s.id() == ids.s2)
        .unwrap();
    assert!(s2_leaves.is_empty());
    assert_eq!(by_hub.get(&ids.hub2).unwrap().len(), 1);
}

/// `batch_edge` (raw ids) and `preload_objects(query).edge()` must be the
/// same code path into `then_edge` — identical results on identical data.
#[tokio::test]
async fn test_two_hop_batch_edge_vs_preload_objects_parity() {
    let (_r, engine, ids) = setup_two_hop_fixture().await;

    let from_ids = engine
        .batch_edge::<HubSpoke, Hub, Spoke>(&[ids.hub1, ids.hub2, ids.hub3])
        .then_edge::<SpokeLeaf, Leaf>()
        .collect_with_target()
        .await
        .unwrap();
    let from_query = engine
        .preload_objects::<Hub>(Query::default())
        .edge::<HubSpoke, Spoke>()
        .then_edge::<SpokeLeaf, Leaf>()
        .collect_with_target()
        .await
        .unwrap();

    // Same keys, same per-hub spoke sets, same per-spoke leaf sets.
    let shape = |m: &std::collections::HashMap<
        uuid::Uuid,
        Vec<(Spoke, Vec<ousia::ObjectEdge<SpokeLeaf, Leaf>>)>,
    >| {
        let mut out: Vec<(uuid::Uuid, Vec<(uuid::Uuid, Vec<uuid::Uuid>)>)> = m
            .iter()
            .map(|(hub, children)| {
                let mut children: Vec<(uuid::Uuid, Vec<uuid::Uuid>)> = children
                    .iter()
                    .map(|(s, leaves)| {
                        let mut leaf_ids: Vec<uuid::Uuid> =
                            leaves.iter().map(|oe| oe.object().id()).collect();
                        leaf_ids.sort();
                        (s.id(), leaf_ids)
                    })
                    .collect();
                children.sort();
                (*hub, children)
            })
            .collect();
        out.sort();
        out
    };
    assert_eq!(shape(&from_ids), shape(&from_query));
}

#[tokio::test]
async fn test_two_hop_filter_passthrough() {
    let (_r, engine, ids) = setup_two_hop_fixture().await;
    let all_hubs = [ids.hub1, ids.hub2, ids.hub3];

    // Hop-2 edge filter: only required leaves survive.
    let required_only = engine
        .batch_edge::<HubSpoke, Hub, Spoke>(&all_hubs)
        .then_edge::<SpokeLeaf, Leaf>()
        .with_edge_query(EdgeQuery::default().where_eq(&SpokeLeaf::FIELDS.required, true))
        .collect_with_target()
        .await
        .unwrap();
    let (_, s1_leaves) = required_only
        .get(&ids.hub1)
        .unwrap()
        .iter()
        .find(|(s, _)| s.id() == ids.s1)
        .unwrap();
    let mut leaf_ids: Vec<uuid::Uuid> = s1_leaves.iter().map(|oe| oe.object().id()).collect();
    leaf_ids.sort();
    let mut expected = vec![ids.l1, ids.l3];
    expected.sort();
    assert_eq!(leaf_ids, expected);

    // Hop-2 object filter: an excluded leaf drops its edge with it — the
    // spoke stays, with an empty leaf Vec (not a stranded half-pair).
    let l2_only = engine
        .batch_edge::<HubSpoke, Hub, Spoke>(&all_hubs)
        .then_edge::<SpokeLeaf, Leaf>()
        .obj_eq(&Leaf::FIELDS.name, "l2")
        .collect_with_target()
        .await
        .unwrap();
    let hub1_children = l2_only.get(&ids.hub1).unwrap();
    let (_, s1_leaves) = hub1_children
        .iter()
        .find(|(s, _)| s.id() == ids.s1)
        .unwrap();
    assert_eq!(s1_leaves.len(), 1);
    assert_eq!(s1_leaves[0].object().id(), ids.l2);
    let (_, s3_leaves) = l2_only
        .get(&ids.hub2)
        .unwrap()
        .iter()
        .find(|(s, _)| s.id() == ids.s3)
        .unwrap();
    assert!(s3_leaves.is_empty());

    // Hop-1 edge filter: position=1 keeps s1/s3, drops s2 entirely.
    let pos1 = engine
        .batch_edge::<HubSpoke, Hub, Spoke>(&all_hubs)
        .with_edge_query(EdgeQuery::default().where_eq(&HubSpoke::FIELDS.position, 1i64))
        .then_edge::<SpokeLeaf, Leaf>()
        .collect_with_target()
        .await
        .unwrap();
    let hub1_children = pos1.get(&ids.hub1).unwrap();
    assert_eq!(hub1_children.len(), 1);
    assert_eq!(hub1_children[0].0.id(), ids.s1);
    assert_eq!(pos1.get(&ids.hub2).unwrap().len(), 1);

    // Hop-1 object filter: only the named spoke survives; hub2 goes empty.
    let s1_only = engine
        .batch_edge::<HubSpoke, Hub, Spoke>(&all_hubs)
        .obj_eq(&Spoke::FIELDS.name, "s1")
        .then_edge::<SpokeLeaf, Leaf>()
        .collect_with_target()
        .await
        .unwrap();
    let hub1_children = s1_only.get(&ids.hub1).unwrap();
    assert_eq!(hub1_children.len(), 1);
    assert_eq!(hub1_children[0].0.id(), ids.s1);
    assert!(s1_only.get(&ids.hub2).unwrap().is_empty());
}

/// Per-hop LIMIT is deliberately unsupported on the 2-hop path for 2.1.0 —
/// Postgres can't LIMIT one leg of a flat JOIN; a lateral-join variant is
/// deferred until a concrete need shows up (see plan_2.1.md, "SQL shape").
/// `EdgeQuery::limit`/`cursor` are ignored by
/// `query_two_hop_edges_with_targets_batch`. This test exists to document
/// that decision; enable it if lateral-join limits ever land.
#[tokio::test]
#[ignore = "per-hop LIMIT unsupported on the 2-hop path (deferred to a lateral-join follow-up)"]
async fn test_two_hop_per_hop_limit() {
    let (_r, engine, ids) = setup_two_hop_fixture().await;
    let limited = engine
        .batch_edge::<HubSpoke, Hub, Spoke>(&[ids.hub1])
        .then_edge::<SpokeLeaf, Leaf>()
        .with_edge_query(EdgeQuery::default().with_limit(1))
        .collect_with_target()
        .await
        .unwrap();
    let (_, s1_leaves) = limited
        .get(&ids.hub1)
        .unwrap()
        .iter()
        .find(|(s, _)| s.id() == ids.s1)
        .unwrap();
    assert_eq!(s1_leaves.len(), 1); // would require JOIN LATERAL (... LIMIT 1)
}

#[tokio::test]
async fn test_count_owned_objects_batch() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut owner1 = User::default();
    owner1.username = "coob_owner1".into();
    owner1.email = "coob_owner1@x.com".into();
    engine.create_object(&owner1).await.unwrap();

    let mut owner2 = User::default();
    owner2.username = "coob_owner2".into();
    owner2.email = "coob_owner2@x.com".into();
    engine.create_object(&owner2).await.unwrap();

    let mut owner3 = User::default();
    owner3.username = "coob_owner3".into();
    owner3.email = "coob_owner3@x.com".into();
    engine.create_object(&owner3).await.unwrap();

    for title in ["Count A", "Count B", "Count C"] {
        let mut p = Post::default();
        p.set_owner(owner1.id());
        p.title = title.into();
        engine.create_object(&p).await.unwrap();
    }
    let mut p = Post::default();
    p.set_owner(owner2.id());
    p.title = "Count D".into();
    engine.create_object(&p).await.unwrap();

    let absent_owner = uuid::Uuid::now_v7();

    let counts = engine
        .count_owned_objects_batch::<Post>(&[owner1.id(), owner2.id(), owner3.id(), absent_owner])
        .await
        .unwrap();

    assert_eq!(counts.get(&owner1.id()).copied(), Some(3));
    assert_eq!(counts.get(&owner2.id()).copied(), Some(1));
    // Zero objects → absent from the map entirely (GROUP BY produces no
    // row), matching count_reverse_edges_batch's convention.
    assert!(counts.get(&owner3.id()).is_none());
    assert!(counts.get(&absent_owner).is_none());
}
