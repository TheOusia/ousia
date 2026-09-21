use super::*;

// ============================================================
// Section 13: Geo — derive, schema, CRUD, queries
// ============================================================

#[test]
fn test_geo_derive_metadata() {
    use ousia::query::IndexKind;

    let kinds = Place::FIELDS.location.kinds;
    assert_eq!(kinds.len(), 1);
    match kinds[0] {
        IndexKind::Geo {
            lat_field,
            lon_field,
        } => {
            assert_eq!(lat_field, "lat");
            assert_eq!(lon_field, "lon");
        }
        other => panic!("expected IndexKind::Geo, got {:?}", other),
    }
    assert_eq!(Place::FIELDS.location.name, "location");
    assert!(Place::HAS_GEO_FIELDS);
    assert!(!User::HAS_GEO_FIELDS);
    assert!(Delivery::HAS_GEO_FIELDS);
}

#[tokio::test]
async fn test_geo_schema_and_crud() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    let postgis_enabled: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'postgis')")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(postgis_enabled);

    let engine = Engine::new(Box::new(adapter));

    let mut place = Place::default();
    place.name = "Eiffel".into();
    place.lat = 48.8584;
    place.lon = 2.2945;
    engine.create_object(&place).await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM object_geo WHERE object_id = $1")
        .bind(place.id())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);

    let (lon_val, lat_val): (f64, f64) = sqlx::query_as(
        "SELECT ST_X(location::geometry), ST_Y(location::geometry) \
         FROM object_geo WHERE object_id = $1",
    )
    .bind(place.id())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!((lon_val - 2.2945).abs() < 1e-6);
    assert!((lat_val - 48.8584).abs() < 1e-6);

    // Delete cascades to object_geo.
    let _: Option<Place> = engine
        .delete_object(place.id(), system_owner())
        .await
        .unwrap();
    let gone: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM object_geo WHERE object_id = $1")
        .bind(place.id())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(gone, 0);
}

#[tokio::test]
async fn test_geo_update_diff() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut place = Place::default();
    place.name = "Original".into();
    place.lat = 10.0;
    place.lon = 20.0;
    engine.create_object(&place).await.unwrap();

    let hash_before: String =
        sqlx::query_scalar("SELECT hash FROM object_geo WHERE object_id = $1")
            .bind(place.id())
            .fetch_one(&pool)
            .await
            .unwrap();

    // Scalar-only update: geo row should not change.
    place.name = "Renamed".into();
    engine.update_object(&mut place).await.unwrap();
    let hash_scalar: String =
        sqlx::query_scalar("SELECT hash FROM object_geo WHERE object_id = $1")
            .bind(place.id())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        hash_before, hash_scalar,
        "scalar update must not change geo row"
    );

    // Lat mutation: hash must change.
    place.lat = 11.0;
    engine.update_object(&mut place).await.unwrap();
    let hash_geo: String = sqlx::query_scalar("SELECT hash FROM object_geo WHERE object_id = $1")
        .bind(place.id())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_ne!(hash_before, hash_geo, "geo update must change hash");
}

#[tokio::test]
async fn test_geo_query_within() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    for (name, lat, lon) in [("near", 0.0, 0.0), ("mid", 0.01, 0.0), ("far", 1.0, 0.0)] {
        let mut p = Place::default();
        p.name = name.into();
        p.lat = lat;
        p.lon = lon;
        engine.create_object(&p).await.unwrap();
    }

    // Within 5 km — near + mid only.
    let hits: Vec<Place> = engine
        .query_objects::<Place>(Query::default().where_geo_within(
            &Place::FIELDS.location,
            0.0,
            0.0,
            5_000.0,
        ))
        .await
        .unwrap();
    let names: std::collections::HashSet<&str> = hits.iter().map(|p| p.name.as_str()).collect();
    assert!(names.contains("near"));
    assert!(names.contains("mid"));
    assert!(!names.contains("far"));

    let count = engine
        .count_objects::<Place>(Some(Query::default().where_geo_within(
            &Place::FIELDS.location,
            0.0,
            0.0,
            5_000.0,
        )))
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_geo_query_within_empty() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut p = Place::default();
    p.name = "somewhere".into();
    p.lat = 50.0;
    p.lon = 50.0;
    engine.create_object(&p).await.unwrap();

    // Radius at (0,0) — no match.
    let hits: Vec<Place> = engine
        .query_objects::<Place>(Query::default().where_geo_within(
            &Place::FIELDS.location,
            0.0,
            0.0,
            100.0,
        ))
        .await
        .unwrap();
    assert!(hits.is_empty());
}

fn make_place(name: &str, lat: f64, lon: f64) -> Place {
    let mut p = Place::default();
    p.name = name.to_string();
    p.lat = lat;
    p.lon = lon;
    p
}

fn haversine_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let r = 6_371_008.8_f64;
    let to_rad = std::f64::consts::PI / 180.0;
    let dphi = (lat2 - lat1) * to_rad;
    let dlam = (lon2 - lon1) * to_rad;
    let phi1 = lat1 * to_rad;
    let phi2 = lat2 * to_rad;
    let a = (dphi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (dlam / 2.0).sin().powi(2);
    2.0 * r * a.sqrt().asin()
}

#[tokio::test]
async fn test_geo_bbox_basic() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    for (i, lat) in [-1.0, 0.0, 1.0].iter().enumerate() {
        for (j, lon) in [-1.0, 0.0, 1.0].iter().enumerate() {
            engine
                .create_object(&make_place(&format!("p{}{}", i, j), *lat, *lon))
                .await
                .unwrap();
        }
    }

    let hits: Vec<Place> = engine
        .query_objects::<Place>(Query::default().where_geo_in_bbox(
            &Place::FIELDS.location,
            -0.5,
            -0.5,
            0.5,
            0.5,
        ))
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].lat, 0.0);
    assert_eq!(hits[0].lon, 0.0);

    // Corner bbox.
    let corner: Vec<Place> = engine
        .query_objects::<Place>(Query::default().where_geo_in_bbox(
            &Place::FIELDS.location,
            -1.5,
            -1.5,
            -0.5,
            -0.5,
        ))
        .await
        .unwrap();
    assert_eq!(corner.len(), 1);
    assert_eq!(corner[0].lat, -1.0);
    assert_eq!(corner[0].lon, -1.0);
}

#[tokio::test]
async fn test_geo_order_by_distance_asc_and_desc() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    for (n, lat, lon) in [
        ("a", 0.01, 0.01),
        ("b", 0.05, 0.05),
        ("c", 0.10, 0.10),
        ("d", 0.50, 0.50),
        ("e", 1.00, 1.00),
    ] {
        engine
            .create_object(&make_place(n, lat, lon))
            .await
            .unwrap();
    }

    let asc: Vec<Place> = engine
        .query_objects::<Place>(Query::default().order_by_distance(
            &Place::FIELDS.location,
            0.0,
            0.0,
            true,
        ))
        .await
        .unwrap();
    let asc_names: Vec<&str> = asc.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(asc_names, vec!["a", "b", "c", "d", "e"]);

    let desc: Vec<Place> = engine
        .query_objects::<Place>(
            Query::default()
                .order_by_distance(&Place::FIELDS.location, 0.0, 0.0, false)
                .with_limit(2),
        )
        .await
        .unwrap();
    let desc_names: Vec<&str> = desc.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(desc_names, vec!["e", "d"]);
}

#[tokio::test]
async fn test_geo_collect_with_distance() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    for (n, lat, lon) in [
        ("near", 0.001, 0.001),
        ("mid", 0.05, 0.05),
        ("far", 0.2, 0.2),
        ("farther", 1.0, 1.0),
    ] {
        engine
            .create_object(&make_place(n, lat, lon))
            .await
            .unwrap();
    }

    let results: Vec<(Place, f64)> = engine
        .query_objects_with_distance::<Place>(Query::default().order_by_distance(
            &Place::FIELDS.location,
            0.0,
            0.0,
            true,
        ))
        .await
        .unwrap();

    assert_eq!(results.len(), 4);
    for w in results.windows(2) {
        assert!(w[0].1 <= w[1].1, "distances must be non-decreasing");
    }
    for (p, d) in &results {
        let oracle = haversine_m(0.0, 0.0, p.lon, p.lat);
        let rel = (d - oracle).abs() / oracle.max(1.0);
        assert!(rel < 0.01, "distance {} too far from oracle {}", d, oracle);
    }
}

#[tokio::test]
async fn test_geo_collect_with_distance_requires_order() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let err = engine
        .query_objects_with_distance::<Place>(Query::default().where_geo_within(
            &Place::FIELDS.location,
            0.0,
            0.0,
            10_000.0,
        ))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidQuery(_)));
}

#[tokio::test]
async fn test_geo_multi_field_delivery() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut d = Delivery::default();
    d.pickup_lat = 6.5244;
    d.pickup_lon = 3.3792;
    d.dropoff_lat = 9.0820;
    d.dropoff_lon = 8.6753;
    engine.create_object(&d).await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM object_geo WHERE object_id = $1")
        .bind(d.id())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 2, "Delivery should write two object_geo rows");

    let near_pickup: Vec<Delivery> = engine
        .query_objects::<Delivery>(Query::default().where_geo_within(
            &Delivery::FIELDS.pickup,
            3.3792,
            6.5244,
            50_000.0,
        ))
        .await
        .unwrap();
    assert_eq!(near_pickup.len(), 1);

    let not_dropoff: Vec<Delivery> = engine
        .query_objects::<Delivery>(Query::default().where_geo_within(
            &Delivery::FIELDS.dropoff,
            3.3792,
            6.5244,
            50_000.0,
        ))
        .await
        .unwrap();
    assert_eq!(not_dropoff.len(), 0);
}

#[tokio::test]
async fn test_geo_multiple_within_filters_and() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut both = Delivery::default();
    both.pickup_lat = 0.001;
    both.pickup_lon = 0.001;
    both.dropoff_lat = 1.001;
    both.dropoff_lon = 1.001;
    engine.create_object(&both).await.unwrap();

    let mut pickup_only = Delivery::default();
    pickup_only.pickup_lat = 0.001;
    pickup_only.pickup_lon = 0.001;
    pickup_only.dropoff_lat = 5.0;
    pickup_only.dropoff_lon = 5.0;
    engine.create_object(&pickup_only).await.unwrap();

    let mut dropoff_only = Delivery::default();
    dropoff_only.pickup_lat = 5.0;
    dropoff_only.pickup_lon = 5.0;
    dropoff_only.dropoff_lat = 1.001;
    dropoff_only.dropoff_lon = 1.001;
    engine.create_object(&dropoff_only).await.unwrap();

    let hits: Vec<Delivery> = engine
        .query_objects::<Delivery>(
            Query::default()
                .where_geo_within(&Delivery::FIELDS.pickup, 0.0, 0.0, 50_000.0)
                .where_geo_within(&Delivery::FIELDS.dropoff, 1.0, 1.0, 50_000.0),
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id(), both.id());
}

#[tokio::test]
async fn test_geo_mixed_within_and_bbox() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    engine
        .create_object(&make_place("in_both", 0.001, 0.001))
        .await
        .unwrap();
    engine
        .create_object(&make_place("only_radius", 0.4, 0.0))
        .await
        .unwrap();
    engine
        .create_object(&make_place("outside", 5.0, 5.0))
        .await
        .unwrap();

    let hits: Vec<Place> = engine
        .query_objects::<Place>(
            Query::default()
                .where_geo_within(&Place::FIELDS.location, 0.0, 0.0, 50_000.0)
                .where_geo_in_bbox(&Place::FIELDS.location, -0.1, -0.1, 0.1, 0.1),
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].name, "in_both");
}

#[tokio::test]
async fn test_geo_order_on_different_field_than_filter() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut d1 = Delivery::default();
    d1.pickup_lat = 0.01;
    d1.pickup_lon = 0.01;
    d1.dropoff_lat = 9.0;
    d1.dropoff_lon = 9.0;
    engine.create_object(&d1).await.unwrap();

    let mut d2 = Delivery::default();
    d2.pickup_lat = 0.05;
    d2.pickup_lon = 0.05;
    d2.dropoff_lat = 9.9;
    d2.dropoff_lon = 9.9;
    engine.create_object(&d2).await.unwrap();

    let mut d3 = Delivery::default();
    d3.pickup_lat = 0.1;
    d3.pickup_lon = 0.1;
    d3.dropoff_lat = 9.5;
    d3.dropoff_lon = 9.5;
    engine.create_object(&d3).await.unwrap();

    // Filter on pickup, order by dropoff distance to (10,10).
    let results: Vec<Delivery> = engine
        .query_objects::<Delivery>(
            Query::default()
                .where_geo_within(&Delivery::FIELDS.pickup, 0.0, 0.0, 50_000.0)
                .order_by_distance(&Delivery::FIELDS.dropoff, 10.0, 10.0, true),
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].id(), d2.id()); // closest dropoff to (10,10)
    assert_eq!(results[2].id(), d1.id()); // farthest dropoff
}
