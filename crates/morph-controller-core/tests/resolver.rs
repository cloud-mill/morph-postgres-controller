//! Resolving references against a source that is not a database: a key
//! held as a number, a row that refers to itself, a reference into a
//! table the source does not map, and references that set another
//! attribute, as records of their own and within one stack.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use morph_controller_core::error::Error;
use morph_controller_core::references::{Provenance, Reference, References, Resolved, Resolver};
use morph_controller_core::source::Source;
use morph_controller_core::worker::Reconciler;
use morph_mock::MockCloud;
use serde_json::json;

/// Servers whose id is a number, and records that refer to them by it.
const NUMBERED: &str = r#"
[[mappings]]
table = "servers"
resource_type = "server"
exports = "ip_address"
[[mappings]]
table = "records"
resource_type = "dns_record"
[[mappings.references]]
column = "target"
table = "servers"
"#;

#[tokio::test]
async fn a_reference_held_as_a_number_names_the_row_by_its_text() {
    let source = FakeSource::new(NUMBERED, Vec::new());
    let cloud = MockCloud::new();
    let engine = engine(&cloud);
    let seven = source.upsert(
        "servers",
        json!({"id": 7, "name": "seven", "image": "linux-1", "network_id": "net-1"}),
    );
    assert_eq!(seven, id("server.7"));
    let row = source.row(&seven).await.unwrap().expect("the server's row");
    engine.reconcile(&seven, Some(row)).await.unwrap();
    let address = attribute(&cloud, "seven", "ip_address").expect("the server has an address");

    let references = Arc::new(source.references().await.unwrap());
    let resolver = Resolver::new(
        Arc::clone(&references),
        Arc::clone(&source) as Arc<dyn Source>,
        Arc::clone(&engine),
    );
    assert_eq!(resolver.references().len(), 1);
    let reconciler = Reconciler::new(
        Arc::clone(&source) as Arc<dyn Source>,
        Arc::clone(&engine),
        Duration::from_secs(10),
        Arc::clone(&references),
        Arc::new(MemoryReferences::default()),
    );
    assert_eq!(reconciler.references(), references.as_ref());

    source.upsert("records", json!({"id": "a", "name": "a", "target": 7}));
    let mut row = source.row(&id("dns_record.a")).await.unwrap().unwrap();
    assert!(resolver.refers(&row));
    let resolved = resolver
        .resolve(&id("dns_record.a"), &mut row)
        .await
        .unwrap();
    let Resolved::Ready(dependencies) = resolved else {
        panic!("{resolved:?}");
    };
    assert_eq!(dependencies.len(), 1);
    assert_eq!(dependencies[0].member, seven);
    assert_eq!(dependencies[0].unit, seven);
    assert_eq!(dependencies[0].attribute, "ip_address");
    assert_eq!(dependencies[0].value, address);
    assert_eq!(
        row.json["target"].to_string(),
        address,
        "the number was replaced by the address"
    );
    assert!(row.links.is_empty());
}

#[tokio::test]
async fn a_row_that_refers_to_its_own_resource_fails_rather_than_waiting_for_itself() {
    let source = FakeSource::new(RECORDS, Vec::new());
    let cloud = MockCloud::new();
    let references = Arc::new(source.references().await.unwrap());
    let resolver = Resolver::new(
        references,
        Arc::clone(&source) as Arc<dyn Source>,
        engine(&cloud),
    );
    source.upsert(
        "records",
        json!({"id": "a", "name": "a", "target": "10.0.0.1"}),
    );
    let mut row = source.row(&id("dns_record.a")).await.unwrap().unwrap();
    let error = resolver
        .resolve(&id("dns_record.a"), &mut row)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, Error::Reconcile { resource, reason } if *resource == id("dns_record.a") && reason.contains("column name refers to the row's own resource")),
        "{error}"
    );
    // A null in the column refers to nothing, and resolves to nothing.
    source.upsert(
        "records",
        json!({"id": "b", "name": null, "target": "10.0.0.1"}),
    );
    let mut row = source.row(&id("dns_record.b")).await.unwrap().unwrap();
    assert_eq!(
        resolver
            .resolve(&id("dns_record.b"), &mut row)
            .await
            .unwrap(),
        Resolved::Ready(Vec::new())
    );
}

#[tokio::test]
async fn a_reference_into_a_table_the_source_does_not_map_is_a_mapping_error() {
    let source = FakeSource::new(RECORDS, Vec::new());
    let cloud = MockCloud::new();
    let ghost = Reference {
        schema: "public".into(),
        table: "records".into(),
        column: "target".into(),
        target_schema: "public".into(),
        target_table: "ghosts".into(),
        attribute: "id".into(),
        sets: "target".into(),
        source: Provenance::Mapping,
    };
    assert_eq!(
        ghost.to_string(),
        "public.records.target -> public.ghosts (id), from the mapping"
    );
    let constrained = Reference {
        source: Provenance::Constraint("records_target_fkey".into()),
        ..ghost.clone()
    };
    assert_eq!(
        constrained.to_string(),
        "public.records.target -> public.ghosts (id), from the constraint records_target_fkey"
    );
    let references = Arc::new(References::new(vec![ghost]));
    let resolver = Resolver::new(
        references,
        Arc::clone(&source) as Arc<dyn Source>,
        engine(&cloud),
    );
    source.upsert("records", json!({"id": "a", "name": "a", "target": "x"}));
    let mut row = source.row(&id("dns_record.a")).await.unwrap().unwrap();
    let error = resolver
        .resolve(&id("dns_record.a"), &mut row)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, Error::Mapping(reason) if reason.contains("points at a table that is not mapped")),
        "{error}"
    );
}

/// Networks and servers whose `network_ref` column gives the network's
/// id, and whose `network_alt` column gives the network's name, to the
/// `network_id` attribute.
const SETTING: &str = r#"
[[mappings]]
table = "networks"
resource_type = "network"
[[mappings]]
table = "servers"
resource_type = "server"
[[mappings.references]]
column = "network_ref"
table = "networks"
sets = "network_id"
[[mappings.references]]
column = "network_alt"
table = "networks"
attribute = "name"
sets = "network_id"
"#;

/// The same, with every row in the stack `farm`.
const SETTING_FARM: &str = r#"
[[mappings]]
table = "networks"
resource_type = "network"
stack = "farm"
[[mappings]]
table = "servers"
resource_type = "server"
stack = "farm"
[[mappings.references]]
column = "network_ref"
table = "networks"
sets = "network_id"
[[mappings.references]]
column = "network_alt"
table = "networks"
attribute = "name"
sets = "network_id"
"#;

#[tokio::test]
async fn a_reference_that_sets_another_attribute_takes_its_column_off_the_row() {
    // Records of their own: the network's id lands under network_id, in
    // place of what the column held, and network_ref goes.
    let source = FakeSource::new(SETTING, Vec::new());
    let cloud = MockCloud::new();
    let engine = engine(&cloud);
    let main = source.upsert(
        "networks",
        json!({"id": "main", "name": "main", "cidr": "10.0.0.0/16"}),
    );
    let row = source.row(&main).await.unwrap().unwrap();
    engine.reconcile(&main, Some(row)).await.unwrap();
    let references = Arc::new(source.references().await.unwrap());
    let shown: Vec<String> = references.iter().map(ToString::to_string).collect();
    assert_eq!(
        shown,
        [
            "public.servers.network_alt -> public.networks (name) into network_id, from the mapping",
            "public.servers.network_ref -> public.networks (id) into network_id, from the mapping",
        ]
    );
    let resolver = Resolver::new(
        Arc::clone(&references),
        Arc::clone(&source) as Arc<dyn Source>,
        Arc::clone(&engine),
    );
    source.upsert(
        "servers",
        json!({"id": "web", "name": "web", "image": "linux-1", "network_ref": "main", "network_id": "stale"}),
    );
    let mut row = source.row(&id("server.web")).await.unwrap().unwrap();
    let Resolved::Ready(dependencies) =
        resolver.resolve(&id("server.web"), &mut row).await.unwrap()
    else {
        panic!("the network has a record");
    };
    assert_eq!(dependencies.len(), 1);
    assert_eq!(dependencies[0].attribute, "id");
    assert_eq!(
        row.json["network_id"].to_string(),
        attribute(&cloud, "main", "id").unwrap()
    );
    assert!(row.json.get("network_ref").is_none());
    assert!(row.links.is_empty());

    // The other column gives the network's name.
    source.upsert(
        "servers",
        json!({"id": "db", "name": "db", "image": "linux-1", "network_alt": "main"}),
    );
    let mut row = source.row(&id("server.db")).await.unwrap().unwrap();
    resolver.resolve(&id("server.db"), &mut row).await.unwrap();
    assert_eq!(row.json["network_id"], json!("main"));
    assert!(row.json.get("network_alt").is_none());

    // Both at once is a failure that names both columns.
    source.upsert(
        "servers",
        json!({"id": "odd", "name": "odd", "image": "linux-1", "network_ref": "main", "network_alt": "main"}),
    );
    let mut row = source.row(&id("server.odd")).await.unwrap().unwrap();
    let error = resolver
        .resolve(&id("server.odd"), &mut row)
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Reconcile { ref reason, .. } if reason == "columns network_alt and network_ref both give the attribute network_id"),
        "{error}"
    );

    // In one stack: a link under network_id, and network_ref goes, so
    // that the engine resolves it in its plan.
    let source = FakeSource::new(SETTING_FARM, Vec::new());
    let references = Arc::new(source.references().await.unwrap());
    let resolver = Resolver::new(
        Arc::clone(&references),
        Arc::clone(&source) as Arc<dyn Source>,
        common::engine(&MockCloud::new()),
    );
    source.upsert(
        "networks",
        json!({"id": "main", "name": "main", "cidr": "10.0.0.0/16"}),
    );
    source.upsert(
        "servers",
        json!({"id": "web", "name": "web", "image": "linux-1", "network_ref": "main"}),
    );
    let mut row = source.row(&id("server.web")).await.unwrap().unwrap();
    let resolved = resolver.resolve(&id("stack.farm"), &mut row).await.unwrap();
    assert!(
        matches!(resolved, Resolved::Ready(ref dependencies) if dependencies.is_empty()),
        "{resolved:?}"
    );
    assert_eq!(row.links.len(), 1);
    assert_eq!(row.links[0].column, "network_id");
    assert_eq!(row.links[0].target, id("network.main"));
    assert_eq!(row.links[0].attribute, "id");
    assert!(row.json.get("network_ref").is_none());
    source.upsert(
        "servers",
        json!({"id": "odd", "name": "odd", "image": "linux-1", "network_ref": "main", "network_alt": "main"}),
    );
    let mut row = source.row(&id("server.odd")).await.unwrap().unwrap();
    let error = resolver
        .resolve(&id("stack.farm"), &mut row)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("both give"), "{error}");
}
