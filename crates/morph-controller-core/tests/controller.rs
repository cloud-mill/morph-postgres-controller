//! The controller over a source that is not a database.
//!
//! The source keeps its rows in memory and sends the transactions the test
//! commits, so these tests need no server, and can make the stores fail at
//! will. They cover what the PostgreSQL tests cannot reach: a dirty store
//! or a store of references that cannot be written or read, a source whose
//! stream ends, a source that cannot list its rows, and the relay falling
//! behind the source's events.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use morph_controller_core::app::{Checkpoint, Controller, Event, Options};
use morph_controller_core::config::CheckpointMode;
use morph_controller_core::error::Error;
use morph_controller_core::observe::Metrics;
use morph_controller_core::scheduler::{Disposition, Origin};
use morph_controller_core::source::Position;
use morph_mock::{Fault, MockCloud, Op};
use serde_json::json;

/// Options without the full reconciliation at start, so that nothing is
/// reconciled but what the test causes.
fn quiet() -> Options {
    Options {
        full_run_at_start: false,
    }
}

#[tokio::test]
async fn a_source_other_than_postgres_drives_the_controller_end_to_end() {
    let log = Log::default();
    let _guard = log.install();
    let source = FakeSource::new(FLEET, fleet_keys());
    let cloud = MockCloud::new();
    let references = Arc::new(MemoryReferences::default());
    let parts = parts(&source, Checkpoint::Memory, &references, &cloud, "1h");
    assert!(matches!(parts.checkpoint.mode(), CheckpointMode::Memory));
    let shown = format!("{parts:?}");
    assert!(
        shown.contains("Parts") && shown.contains("FakeSource") && shown.contains("Memory"),
        "{shown}"
    );
    let controller = Controller::start(parts, quiet()).await.unwrap();
    assert!(controller.mock().is_some());
    assert!(format!("{controller:?}").contains("Controller"));
    let mut events = Events::of(&controller);
    events.connected().await;
    assert!(log.contains("a reference between rows"), "{}", log.text());
    assert!(log.contains("the controller is running"), "{}", log.text());

    // A network is created, and the transaction that described it is
    // confirmed once the network has been reconciled.
    assert_eq!(source.confirmed_position(), Position::START);
    let main = source.change(
        "networks",
        json!({"id": "main", "name": "main", "cidr": "10.0.0.0/16"}),
    );
    assert_eq!(main, id("network.main"));
    assert_eq!(events.transaction().await, (1, 1));
    let (ok, changed, origin, attempt, disposition) = events.reconciled("network.main").await;
    assert!(ok && changed, "{ok} {changed}");
    assert_eq!(
        (origin, attempt, disposition),
        (Origin::Change, 1, Disposition::Done)
    );
    assert_eq!(cloud.count(Op::Create), 1);
    assert_eq!(source.confirmed_position(), Position::from_u64(1));

    // A server that refers to the network is handed the network's id, and
    // the reference is on record.
    source.change(
        "servers",
        json!({"id": "web", "name": "web", "image": "linux-1", "network_id": "main"}),
    );
    let (ok, ..) = events.reconciled("server.web").await;
    assert!(ok);
    assert_eq!(
        attribute(&cloud, "web", "network_id"),
        attribute(&cloud, "main", "id"),
        "the server took the network's real id"
    );
    let dependencies = references.dependencies_of(&id("server.web"));
    assert_eq!(dependencies.len(), 1, "{dependencies:?}");
    assert_eq!(dependencies[0].member, id("network.main"));
    assert_eq!(dependencies[0].attribute, "id");
    assert_eq!(source.confirmed_position(), Position::from_u64(2));

    // A row that goes takes its resource with it, and what it referred to
    // is forgotten.
    source.remove("servers", "web");
    let (ok, changed, ..) = events.reconciled("server.web").await;
    assert!(ok && changed);
    assert_eq!(cloud.count(Op::Delete), 1);
    assert!(references.dependencies_of(&id("server.web")).is_empty());
    assert_eq!(source.confirmed_position(), Position::from_u64(3));

    let metrics = controller.metrics();
    assert_eq!(Metrics::get(&metrics.transactions), 3);
    assert_eq!(Metrics::get(&metrics.reconciliations_succeeded), 3);
    assert_eq!(Metrics::get(&metrics.marked), 3);
    assert!(controller.unhealthy().await.is_empty());
    assert!(controller.parked().await.is_empty());
    controller.stop(Duration::from_secs(10)).await;
    assert!(log.contains("stopped"), "{}", log.text());
}

#[tokio::test]
async fn what_was_dirty_when_the_controller_last_stopped_is_taken_up_first() {
    let log = Log::default();
    let _guard = log.install();
    let source = FakeSource::new(FLEET, fleet_keys());
    let cloud = MockCloud::new();
    let references = Arc::new(MemoryReferences::default());
    let dirty = MemoryDirty::holding(&[("network.old", 2), ("server.gone", 0)]);
    let parts = parts(
        &source,
        Checkpoint::Durable(Arc::clone(&dirty) as Arc<_>),
        &references,
        &cloud,
        "1h",
    );
    assert!(matches!(parts.checkpoint.mode(), CheckpointMode::Durable));
    let controller = Controller::start(parts, quiet()).await.unwrap();
    let mut events = Events::of(&controller);
    // Neither has a row any more, so each is reconciled to nothing, and
    // leaves the store.
    let (ok, changed, origin, attempt, _) = events.reconciled("network.old").await;
    assert!(ok && !changed);
    assert_eq!((origin, attempt), (Origin::Recovered, 1));
    let (ok, _, origin, ..) = events.reconciled("server.gone").await;
    assert!(ok);
    assert_eq!(origin, Origin::Recovered);
    assert!(dirty.entries().is_empty(), "{:?}", dirty.entries());
    assert!(
        log.contains("taking up what was waiting when the controller last stopped"),
        "{}",
        log.text()
    );
    assert_eq!(Metrics::get(&controller.metrics().marked), 2);
    controller.stop(Duration::from_secs(10)).await;
}

#[tokio::test]
async fn a_dirty_store_that_cannot_be_written_is_tried_again_and_the_transaction_waits() {
    let log = Log::default();
    let _guard = log.install();
    let source = FakeSource::new(FLEET, fleet_keys());
    let cloud = MockCloud::new();
    let references = Arc::new(MemoryReferences::default());
    let dirty = Arc::new(MemoryDirty::default());
    dirty.fail_next_marks(2);
    let controller = Controller::start(
        parts(
            &source,
            Checkpoint::Durable(Arc::clone(&dirty) as Arc<_>),
            &references,
            &cloud,
            "1h",
        ),
        quiet(),
    )
    .await
    .unwrap();
    let mut events = Events::of(&controller);
    events.connected().await;
    source.change(
        "networks",
        json!({"id": "main", "name": "main", "cidr": "10.0.0.0/16"}),
    );
    let (ok, ..) = events.reconciled("network.main").await;
    assert!(ok);
    assert_eq!(dirty.mark_calls(), 3, "two failures, then the write");
    assert_eq!(
        log.count("the dirty store could not be written; trying again"),
        2,
        "{}",
        log.text()
    );
    assert_eq!(source.confirmed_position(), Position::from_u64(1));
    assert!(dirty.entries().is_empty(), "the turn removed the entry");
    controller.stop(Duration::from_secs(10)).await;

    // A store that never takes the write holds the transaction back for
    // ever: it is neither confirmed nor reconciled, and stopping does not
    // wait for it.
    let source = FakeSource::new(FLEET, fleet_keys());
    let cloud = MockCloud::new();
    let dirty = Arc::new(MemoryDirty::default());
    dirty.fail_next_marks(usize::MAX);
    let controller = Controller::start(
        parts(
            &source,
            Checkpoint::Durable(Arc::clone(&dirty) as Arc<_>),
            &references,
            &cloud,
            "1h",
        ),
        quiet(),
    )
    .await
    .unwrap();
    let mut events = Events::of(&controller);
    events.connected().await;
    source.change(
        "networks",
        json!({"id": "main", "name": "main", "cidr": "10.0.0.0/16"}),
    );
    events.transaction().await;
    log.wait_for("the dirty store could not be written; trying again")
        .await;
    let stopping = std::time::Instant::now();
    controller.stop(Duration::from_secs(10)).await;
    assert!(
        stopping.elapsed() < Duration::from_secs(5),
        "stopping did not wait for the store"
    );
    assert_eq!(source.confirmed_position(), Position::START);
    assert_eq!(cloud.count(Op::Create), 0);
    assert!(dirty.entries().is_empty());
}

#[tokio::test]
async fn a_dirty_store_that_cannot_be_updated_leaves_the_outcome_of_a_turn_standing() {
    let log = Log::default();
    let _guard = log.install();
    let source = FakeSource::new(FLEET, fleet_keys());
    let cloud = MockCloud::new();
    let references = Arc::new(MemoryReferences::default());
    let dirty = Arc::new(MemoryDirty::default());
    dirty.fail_updates(true);
    let controller = Controller::start(
        parts(
            &source,
            Checkpoint::Durable(Arc::clone(&dirty) as Arc<_>),
            &references,
            &cloud,
            "1h",
        ),
        quiet(),
    )
    .await
    .unwrap();
    let mut events = Events::of(&controller);
    events.connected().await;

    // A success whose entry cannot be removed: the network exists, and the
    // entry stays for the next start to take up.
    source.change(
        "networks",
        json!({"id": "main", "name": "main", "cidr": "10.0.0.0/16"}),
    );
    let (ok, ..) = events.reconciled("network.main").await;
    assert!(ok);
    assert_eq!(cloud.count(Op::Create), 1);
    assert_eq!(dirty.entries(), vec![id("network.main")]);
    assert!(
        log.contains("the resource will be reconciled again after a restart"),
        "{}",
        log.text()
    );

    // A wait whose note cannot be written: the server still waits for its
    // network, which is reconciled because the server needs it, and the
    // server follows.
    source.upsert(
        "networks",
        json!({"id": "edge", "name": "edge", "cidr": "10.1.0.0/16"}),
    );
    source.change(
        "servers",
        json!({"id": "web", "name": "web", "image": "linux-1", "network_id": "edge"}),
    );
    let (on, reason) = events.waiting("server.web").await;
    assert_eq!(on, vec![id("network.edge")]);
    assert!(
        reason.contains("network.edge has not been reconciled yet"),
        "{reason}"
    );
    let (ok, _, origin, ..) = events.reconciled("network.edge").await;
    assert!(ok);
    assert_eq!(origin, Origin::Needed);
    let (ok, ..) = events.reconciled("server.web").await;
    assert!(ok);
    assert_eq!(
        attribute(&cloud, "web", "network_id"),
        attribute(&cloud, "edge", "id")
    );
    assert_eq!(
        dirty.note_of(&id("server.web")),
        None,
        "the note was refused"
    );

    // A failure whose attempt cannot be written: the resource is retried
    // and given up on all the same, and the entry still says no attempt
    // failed.
    cloud.inject(Fault::on(Op::Create, id("network.bad")).always());
    source.change(
        "networks",
        json!({"id": "bad", "name": "bad", "cidr": "10.2.0.0/16"}),
    );
    for attempt in 1..=3 {
        let (ok, _, _, counted, disposition) = events.reconciled("network.bad").await;
        assert!(!ok);
        assert_eq!(counted, attempt);
        if attempt < 3 {
            assert!(
                matches!(disposition, Disposition::Retrying(_)),
                "{disposition:?}"
            );
        } else {
            assert_eq!(disposition, Disposition::Unhealthy);
        }
    }
    assert_eq!(controller.unhealthy().await, vec![id("network.bad")]);
    assert_eq!(dirty.attempts_of(&id("network.bad")), Some(0));
    assert!(
        log.count("the dirty store could not be updated") >= 4,
        "the note and three failures were each refused:\n{}",
        log.text()
    );
    controller.stop(Duration::from_secs(10)).await;
}

#[tokio::test]
async fn a_circular_wait_is_a_failure_even_when_the_dirty_store_cannot_record_it() {
    let log = Log::default();
    let _guard = log.install();
    let source = FakeSource::new(RECORDS, Vec::new());
    let cloud = MockCloud::new();
    let references = Arc::new(MemoryReferences::default());
    let dirty = Arc::new(MemoryDirty::default());
    dirty.fail_updates(true);
    let controller = Controller::start(
        parts(
            &source,
            Checkpoint::Durable(Arc::clone(&dirty) as Arc<_>),
            &references,
            &cloud,
            "1h",
        ),
        quiet(),
    )
    .await
    .unwrap();
    let mut events = Events::of(&controller);
    events.connected().await;
    // Two records that each need the other's id first.
    let a = source.upsert(
        "records",
        json!({"id": "a", "name": "b", "target": "10.0.0.1"}),
    );
    let b = source.upsert(
        "records",
        json!({"id": "b", "name": "a", "target": "10.0.0.1"}),
    );
    source.commit(&[a.clone(), b.clone()]);
    assert_eq!(events.transaction().await, (1, 2));
    let event = events
        .wait_for(|event| {
            matches!(
                event,
                Event::Reconciled {
                    ok: false,
                    disposition: Disposition::Unhealthy,
                    ..
                }
            )
        })
        .await;
    let Event::Reconciled { resource, .. } = event else {
        unreachable!()
    };
    let parked = controller.parked().await;
    assert_eq!(parked.len(), 1, "{parked:?}");
    assert_ne!(parked[0].0, resource);
    assert_eq!(parked[0].1, vec![resource.clone()]);
    assert_eq!(cloud.count(Op::Create), 0, "neither record was made");
    assert_eq!(
        dirty.attempts_of(&resource),
        Some(0),
        "the failure could not be written"
    );
    assert!(
        log.contains("which waits for it, directly or through others"),
        "{}",
        log.text()
    );
    assert!(
        log.contains("the dirty store could not be updated"),
        "{}",
        log.text()
    );
    // The transaction is confirmed all the same: its resources were written
    // to the store, and the store is what remembers them.
    assert_eq!(source.confirmed_position(), Position::from_u64(1));
    controller.stop(Duration::from_secs(10)).await;
}

#[tokio::test]
async fn the_controller_says_when_a_part_of_it_stops_fails_or_panics() {
    let log = Log::default();
    let _guard = log.install();
    let references = Arc::new(MemoryReferences::default());
    for (ending, expected) in [
        (Ending::Quietly, "a part of the controller stopped"),
        (Ending::Failing, "the fake source's log is gone"),
        (Ending::Panicking, "a part of the controller panicked"),
    ] {
        let source = FakeSource::new(FLEET, fleet_keys());
        let cloud = MockCloud::new();
        let mut controller = Controller::start(
            parts(&source, Checkpoint::Memory, &references, &cloud, "1h"),
            quiet(),
        )
        .await
        .unwrap();
        // Nothing is reported while every part runs.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), controller.failed())
                .await
                .is_err(),
            "{ending:?}"
        );
        source.close(ending);
        let error = tokio::time::timeout(PATIENCE, controller.failed())
            .await
            .expect("the ending is reported");
        assert!(
            matches!(error, Error::Connection(ref reason) if reason.contains(expected)),
            "{ending:?}: {error}"
        );
        // The source took its channels with it, so the relay and the
        // intake end too, and are reported in turn.
        for _ in 0..2 {
            let error = tokio::time::timeout(PATIENCE, controller.failed())
                .await
                .expect("the next ending is reported");
            assert!(
                matches!(error, Error::Connection(ref reason) if reason.contains("a part of the controller stopped")),
                "{ending:?}: {error}"
            );
        }
        // The workers and the loops run on.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), controller.failed())
                .await
                .is_err(),
            "{ending:?}"
        );
        controller.stop(Duration::from_secs(10)).await;
    }

    // A part that ended before the controller is stopped is reported in
    // the log when stopping finds it.
    for (ending, expected) in [
        (Ending::Failing, "a part of the controller failed"),
        (Ending::Panicking, "a part of the controller panicked"),
    ] {
        let source = FakeSource::new(FLEET, fleet_keys());
        let cloud = MockCloud::new();
        let controller = Controller::start(
            parts(&source, Checkpoint::Memory, &references, &cloud, "1h"),
            quiet(),
        )
        .await
        .unwrap();
        source.close(ending);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        controller.stop(Duration::from_secs(10)).await;
        assert!(log.contains(expected), "{ending:?}:\n{}", log.text());
    }
}

#[tokio::test]
async fn events_the_relay_falls_behind_on_are_lost_and_the_rest_arrive() {
    let source = FakeSource::new(FLEET, fleet_keys());
    source.greet_with(500);
    let cloud = MockCloud::new();
    let references = Arc::new(MemoryReferences::default());
    let controller = Controller::start(
        parts(&source, Checkpoint::Memory, &references, &cloud, "1h"),
        quiet(),
    )
    .await
    .unwrap();
    let mut events = Events::of(&controller);
    source.change(
        "networks",
        json!({"id": "main", "name": "main", "cidr": "10.0.0.0/16"}),
    );
    assert_eq!(events.transaction().await, (1, 1));
    let (ok, ..) = events.reconciled("network.main").await;
    assert!(ok);
    let connected = events.kept(|event| matches!(event, Event::Connected));
    assert!(
        (1..500).contains(&connected),
        "the relay passed on what it could, {connected} of 500"
    );
    controller.stop(Duration::from_secs(10)).await;
}

#[tokio::test]
async fn a_full_pass_that_cannot_find_out_what_exists_tries_again_later() {
    let log = Log::default();
    let _guard = log.install();
    let source = FakeSource::new(FLEET, fleet_keys());
    source.upsert(
        "networks",
        json!({"id": "main", "name": "main", "cidr": "10.0.0.0/16"}),
    );
    source.set_down(true);
    let cloud = MockCloud::new();
    let references = Arc::new(MemoryReferences::default());
    let controller = Controller::start(
        parts(&source, Checkpoint::Memory, &references, &cloud, "100ms"),
        Options::default(),
    )
    .await
    .unwrap();
    let mut events = Events::of(&controller);
    log.wait_for("the full reconciliation could not find out what exists")
        .await;
    assert_eq!(Metrics::get(&controller.metrics().full_runs), 0);
    source.set_down(false);
    let event = events
        .wait_for(|event| matches!(event, Event::FullRun { .. }))
        .await;
    assert!(
        matches!(event, Event::FullRun { resources: 1 }),
        "{event:?}"
    );
    let (ok, _, origin, ..) = events.reconciled("network.main").await;
    assert!(ok);
    assert_eq!(origin, Origin::Periodic);
    assert_eq!(cloud.count(Op::Create), 1);
    assert!(Metrics::get(&controller.metrics().full_runs) >= 1);
    controller.stop(Duration::from_secs(10)).await;
}

#[tokio::test]
async fn references_that_cannot_be_looked_up_leave_propagation_to_the_full_pass() {
    let log = Log::default();
    let _guard = log.install();
    let source = FakeSource::new(FLEET, fleet_keys());
    let cloud = MockCloud::new();
    let references = Arc::new(MemoryReferences::default());
    references.fail_stale(true);
    let controller = Controller::start(
        parts(&source, Checkpoint::Memory, &references, &cloud, "1h"),
        quiet(),
    )
    .await
    .unwrap();
    let mut events = Events::of(&controller);
    events.connected().await;
    // Servers refer to networks, so a network exports its id, and
    // creating one changes what it exports.
    source.change(
        "networks",
        json!({"id": "main", "name": "main", "cidr": "10.0.0.0/16"}),
    );
    let (ok, ..) = events.reconciled("network.main").await;
    assert!(ok);
    log.wait_for("the resources that refer to it could not be found")
        .await;
    assert_eq!(Metrics::get(&controller.metrics().propagated), 0);
    controller.stop(Duration::from_secs(10)).await;
}

#[tokio::test]
async fn a_stack_waits_for_a_resource_outside_it_and_follows_once_it_is_there() {
    let source = FakeSource::new(FARM, fleet_keys());
    let cloud = MockCloud::new();
    let references = Arc::new(MemoryReferences::default());
    let controller = Controller::start(
        parts(&source, Checkpoint::Memory, &references, &cloud, "1h"),
        quiet(),
    )
    .await
    .unwrap();
    let mut events = Events::of(&controller);
    events.connected().await;
    source.upsert(
        "networks",
        json!({"id": "main", "name": "main", "cidr": "10.0.0.0/16"}),
    );
    let farm = source.change(
        "servers",
        json!({"id": "web", "name": "web", "image": "linux-1", "network_id": "main"}),
    );
    assert_eq!(farm, id("stack.farm"));
    let (on, reason) = events.waiting("stack.farm").await;
    assert_eq!(on, vec![id("network.main")]);
    assert!(
        reason.contains("network.main has not been reconciled yet"),
        "{reason}"
    );
    let (ok, _, origin, ..) = events.reconciled("network.main").await;
    assert!(ok);
    assert_eq!(origin, Origin::Needed);
    let (ok, changed, ..) = events.reconciled("stack.farm").await;
    assert!(ok && changed);
    assert_eq!(
        attribute(&cloud, "web", "network_id"),
        attribute(&cloud, "main", "id")
    );
    let dependencies = references.dependencies_of(&id("stack.farm"));
    assert_eq!(dependencies.len(), 1, "{dependencies:?}");
    assert_eq!(dependencies[0].unit, id("network.main"));
    // The stack follows its rows out.
    source.remove("servers", "web");
    let (ok, changed, ..) = events.reconciled("stack.farm").await;
    assert!(ok && changed);
    assert_eq!(cloud.count(Op::Delete), 1);
    controller.stop(Duration::from_secs(10)).await;
}

#[tokio::test]
async fn a_row_with_an_import_id_takes_charge_of_what_exists_instead_of_making_it() {
    let log = Log::default();
    let _guard = log.install();
    let source = FakeSource::new(ADOPTED, Vec::new());
    let cloud = MockCloud::new();
    let references = Arc::new(MemoryReferences::default());
    let parts = parts(&source, Checkpoint::Memory, &references, &cloud, "1h");
    let controller = Controller::start(parts, quiet()).await.unwrap();
    let mut events = Events::of(&controller);
    events.connected().await;

    // Something made outside the controller, as Terraform would have.
    let handle = cloud.plant("network", [("name", "legacy"), ("cidr", "10.9.0.0/16")]);
    source.change(
        "networks",
        json!({"id": "legacy", "name": "legacy", "cidr": "10.9.0.0/16", "object": handle.to_string()}),
    );
    let (ok, changed, ..) = events.reconciled("network.legacy").await;
    assert!(ok, "{}", log.text());
    assert!(!changed, "the row describes the object as it is");
    assert_eq!(cloud.count(Op::Import), 1, "{:?}", cloud.calls());
    assert_eq!(cloud.count(Op::Create), 0, "nothing was made again");
    assert!(
        log.contains("took charge of an existing object"),
        "{}",
        log.text()
    );

    // From then on the row drives the object, and the import id may stay.
    // The name updates in place; the cidr would replace the network.
    source.change(
        "networks",
        json!({"id": "legacy", "name": "legacy-renamed", "cidr": "10.9.0.0/16", "object": handle.to_string()}),
    );
    let (ok, changed, ..) = events.reconciled("network.legacy").await;
    assert!(ok && changed);
    assert_eq!(cloud.count(Op::Update), 1, "{:?}", cloud.calls());
    assert_eq!(cloud.count(Op::Create), 0);
    assert_eq!(
        cloud.count(Op::Import),
        1,
        "an object on record is not imported again"
    );
    assert_eq!(
        cloud
            .object(&handle)
            .unwrap()
            .attributes
            .get("name")
            .map(ToString::to_string),
        Some("\"legacy-renamed\"".into())
    );

    // An id that names nothing fails the row, and nothing is made in its
    // place.
    source.change(
        "networks",
        json!({"id": "ghost", "name": "ghost", "cidr": "10.11.0.0/16", "object": "network-999"}),
    );
    let (ok, ..) = events.reconciled("network.ghost").await;
    assert!(!ok);
    assert_eq!(cloud.count(Op::Create), 0);
    assert!(
        log.contains("could not take charge of network.ghost") && log.contains("no such object"),
        "{}",
        log.text()
    );

    // A row without an id is made as usual.
    source.change(
        "networks",
        json!({"id": "fresh", "name": "fresh", "cidr": "10.12.0.0/16", "object": null}),
    );
    let (ok, changed, ..) = events.reconciled("network.fresh").await;
    assert!(ok && changed);
    assert_eq!(cloud.count(Op::Create), 1);
    controller.stop(Duration::from_secs(10)).await;
}

#[tokio::test]
async fn a_reference_that_sets_another_attribute_gives_it_the_value_and_is_not_one_itself() {
    let log = Log::default();
    let _guard = log.install();
    let source = FakeSource::new(ADOPTED, Vec::new());
    let cloud = MockCloud::new();
    let references = Arc::new(MemoryReferences::default());
    let parts = parts(&source, Checkpoint::Memory, &references, &cloud, "1h");
    let controller = Controller::start(parts, quiet()).await.unwrap();
    let mut events = Events::of(&controller);
    events.connected().await;
    assert!(
        log.contains("public.servers.network_ref -> public.networks (id) into network_id"),
        "{}",
        log.text()
    );

    source.change(
        "networks",
        json!({"id": "main", "name": "main", "cidr": "10.0.0.0/16"}),
    );
    let (ok, ..) = events.reconciled("network.main").await;
    assert!(ok);

    // One server takes its network from the row it names; the other gives
    // the attribute as a plain value.
    source.change(
        "servers",
        json!({"id": "web", "name": "web", "image": "linux-1", "network_ref": "main", "network_id": null}),
    );
    let (ok, ..) = events.reconciled("server.web").await;
    assert!(ok, "{}", log.text());
    assert_eq!(
        attribute(&cloud, "web", "network_id"),
        attribute(&cloud, "main", "id"),
        "the server's network_id is the network's id"
    );
    assert_eq!(
        attribute(&cloud, "web", "network_ref"),
        None,
        "the reference column is not an attribute"
    );
    let dependencies = references.dependencies_of(&id("server.web"));
    assert_eq!(dependencies.len(), 1, "{dependencies:?}");
    assert_eq!(dependencies[0].attribute, "id");

    source.change(
        "servers",
        json!({"id": "db", "name": "db", "image": "linux-1", "network_ref": null, "network_id": "net-elsewhere"}),
    );
    let (ok, ..) = events.reconciled("server.db").await;
    assert!(ok, "{}", log.text());
    assert_eq!(
        attribute(&cloud, "db", "network_id").as_deref(),
        Some("\"net-elsewhere\"")
    );
    assert!(references.dependencies_of(&id("server.db")).is_empty());

    // A third takes the network's name through the other column.
    source.change(
        "servers",
        json!({"id": "app", "name": "app", "image": "linux-1", "network_alt": "main"}),
    );
    let (ok, ..) = events.reconciled("server.app").await;
    assert!(ok, "{}", log.text());
    assert_eq!(
        attribute(&cloud, "app", "network_id").as_deref(),
        Some("\"main\"")
    );

    // A row that fills both columns fails, naming them.
    source.change(
        "servers",
        json!({"id": "odd", "name": "odd", "image": "linux-1", "network_ref": "main", "network_alt": "main"}),
    );
    let (ok, ..) = events.reconciled("server.odd").await;
    assert!(!ok);
    assert!(
        log.contains("columns network_alt and network_ref both give the attribute network_id"),
        "{}",
        log.text()
    );
    controller.stop(Duration::from_secs(10)).await;
}
