//! The controller's remaining edges: marks that merge while a resource is
//! waiting or running and what they settle, rows holding values Morph
//! cannot keep, a mapping that names no kind, the TOML scalars a provider
//! is configured with, the Pulumi engine's name, and a record directory
//! that cannot be read.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use morph::ResourceId;
use morph_postgres_controller::config::{Filters, Mapping, MorphConfig, PulumiConfig, RetryConfig};
use morph_postgres_controller::engine::{Engine, PulumiEngine};
use morph_postgres_controller::error::Error;
use morph_postgres_controller::mapping::ResourceMapper;
use morph_postgres_controller::mapping::desired::{desired_from_row, spec_from_row};
use morph_postgres_controller::mapping::resource_mapper::TableMapper;
use morph_postgres_controller::postgres::pgoutput::{
    Column, Datum, Relation, ReplicaIdentity, Tuple,
};
use morph_postgres_controller::postgres::relation::{ChangeKind, RowChange};
use morph_postgres_controller::providers::from_toml;
use morph_postgres_controller::scheduler::{Disposition, Marked, Origin, Outcome, Scheduler};
use morph_postgres_controller::stores::StateStores;

fn id(text: &str) -> ResourceId {
    text.parse().unwrap()
}

fn scheduler(max_attempts: u32) -> (Arc<Scheduler>, Arc<Mutex<Vec<u64>>>) {
    let settled = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::clone(&settled);
    let retry: RetryConfig = toml::from_str(&format!(
        "max_attempts = {max_attempts}\ninitial = \"1ms\"\nmax = \"5ms\"\nmultiplier = 2.0"
    ))
    .unwrap();
    let scheduler = Scheduler::new(10, 2, retry, move |number| {
        recorder.lock().unwrap().push(number);
    });
    (Arc::new(scheduler), settled)
}

fn settled(numbers: &Arc<Mutex<Vec<u64>>>) -> Vec<u64> {
    let mut numbers = numbers.lock().unwrap().clone();
    numbers.sort_unstable();
    numbers
}

#[tokio::test]
async fn marks_that_merge_while_waiting_or_running_settle_every_transaction_they_covered() {
    let (scheduler, numbers) = scheduler(3);
    let resource = id("server.a");
    assert_eq!(
        scheduler
            .mark(resource.clone(), Some(1), Origin::Change)
            .await,
        Marked::New
    );
    // Waiting already: the second mark merges into it.
    assert_eq!(
        scheduler
            .mark(resource.clone(), Some(2), Origin::Change)
            .await,
        Marked::Coalesced
    );
    let lease = scheduler.next().await.unwrap();
    assert_eq!(lease.attempt, 1);
    // Running: a mark makes it run again afterwards, and a further mark
    // merges into that.
    assert_eq!(
        scheduler
            .mark(resource.clone(), Some(3), Origin::Change)
            .await,
        Marked::Coalesced
    );
    assert_eq!(
        scheduler
            .mark(resource.clone(), Some(4), Origin::Periodic)
            .await,
        Marked::Coalesced
    );
    assert_eq!(
        scheduler
            .mark(resource.clone(), None, Origin::Periodic)
            .await,
        Marked::Coalesced
    );
    let disposition = scheduler.finish(lease, Outcome::Succeeded).await;
    assert_eq!(disposition, Disposition::DoneAndMarkedAgain);
    assert_eq!(
        settled(&numbers),
        [1, 2],
        "the first turn settled what it covered"
    );
    assert_eq!(scheduler.waiting().await, 1);
    let again = scheduler.next().await.unwrap();
    assert_eq!(again.attempt, 1, "a fresh turn, not a retry");
    assert_eq!(
        scheduler.finish(again, Outcome::Succeeded).await,
        Disposition::Done
    );
    assert_eq!(settled(&numbers), [1, 2, 3, 4]);
    assert_eq!(scheduler.waiting().await, 0);
    assert_eq!(scheduler.running().await, 0);
}

#[tokio::test]
async fn a_mark_that_arrives_during_a_failed_turn_rides_along_with_the_retry() {
    let (scheduler, numbers) = scheduler(3);
    let resource = id("server.b");
    scheduler
        .mark(resource.clone(), Some(5), Origin::Change)
        .await;
    let lease = scheduler.next().await.unwrap();
    assert_eq!(
        scheduler
            .mark(resource.clone(), Some(6), Origin::Change)
            .await,
        Marked::Coalesced
    );
    match scheduler
        .finish(lease, Outcome::Failed("busy".into()))
        .await
    {
        Disposition::Retrying(wait) => assert!(wait <= Duration::from_millis(10), "{wait:?}"),
        other => panic!("{other:?}"),
    }
    assert!(
        settled(&numbers).is_empty(),
        "nothing settles until the retry succeeds"
    );
    let retry = tokio::time::timeout(Duration::from_secs(5), scheduler.next())
        .await
        .expect("the retry is due at once, because a mark arrived meanwhile")
        .unwrap();
    assert_eq!(retry.attempt, 2);
    assert_eq!(
        scheduler.finish(retry, Outcome::Succeeded).await,
        Disposition::Done
    );
    assert_eq!(settled(&numbers), [5, 6]);
    assert!(scheduler.unhealthy().await.is_empty());
}

#[tokio::test]
async fn a_resource_given_up_on_settles_what_it_covered_and_is_listed_as_unhealthy() {
    let (scheduler, numbers) = scheduler(2);
    let resource = id("server.c");
    scheduler
        .mark(resource.clone(), Some(7), Origin::Change)
        .await;
    let first = scheduler.next().await.unwrap();
    assert!(matches!(
        scheduler.finish(first, Outcome::Failed("no".into())).await,
        Disposition::Retrying(_)
    ));
    let second = tokio::time::timeout(Duration::from_secs(5), scheduler.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.attempt, 2);
    assert_eq!(
        scheduler
            .finish(second, Outcome::Failed("still no".into()))
            .await,
        Disposition::Unhealthy
    );
    assert_eq!(
        settled(&numbers),
        [7],
        "a lost cause does not hold the ledger back"
    );
    assert_eq!(scheduler.unhealthy().await, std::slice::from_ref(&resource));
    // A new mark gives it another chance, and clears the listing.
    assert_eq!(
        scheduler
            .mark(resource.clone(), Some(8), Origin::Periodic)
            .await,
        Marked::New
    );
    let third = scheduler.next().await.unwrap();
    assert_eq!(third.attempt, 1);
    assert_eq!(
        scheduler.finish(third, Outcome::Succeeded).await,
        Disposition::Done
    );
    assert!(scheduler.unhealthy().await.is_empty());
    assert_eq!(settled(&numbers), [7, 8]);
}

#[test]
fn a_value_morph_cannot_keep_names_its_column_or_attribute() {
    let mapping: Mapping = toml::from_str(
        "table = \"things\"\nresource_type = \"server\"\nattributes_column = \"spec\"",
    )
    .unwrap();
    let too_big = serde_json::json!({"id": "7", "size": 18446744073709551615u64});
    let error = spec_from_row(&mapping, id("server.7"), too_big).unwrap_err();
    assert!(matches!(error, Error::Mapping(_)), "{error}");
    assert!(
        error.to_string().contains("column size of public.things"),
        "{error}"
    );

    let nested = serde_json::json!({"id": "7", "spec": {"size": 18446744073709551615u64}});
    let error = desired_from_row(&mapping, id("server.7"), nested).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("attribute size of public.things"),
        "{error}"
    );
}

#[test]
fn a_mapping_that_names_no_kind_cannot_name_a_resource() {
    // The configuration's checks refuse such a mapping; the mapper is
    // guarded against it all the same.
    let mapping: Mapping = toml::from_str("table = \"things\"").unwrap();
    let mapper = TableMapper::new(vec![mapping], Filters::default());
    let relation = Arc::new(Relation {
        id: 1,
        schema: "public".into(),
        name: "things".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![Column {
            name: "id".into(),
            key: true,
            type_oid: 25,
            modifier: -1,
        }],
    });
    let change = RowChange {
        kind: ChangeKind::Insert,
        relation: relation.clone(),
        new: Some(Tuple(vec![Datum::Text("7".into())])),
        old: None,
    };
    let error = mapper.map_change(&relation, &change).unwrap_err();
    assert!(error.to_string().contains("names no kind"), "{error}");
}

#[test]
fn every_toml_scalar_becomes_the_matching_value() {
    let table: toml::Table = toml::from_str(
        "ratio = 1.5\non = true\nwhen = 1979-05-27T07:32:00Z\ncount = 3\nname = \"x\"",
    )
    .unwrap();
    let morph::Value::Map(values) = from_toml(toml::Value::Table(table)) else {
        panic!("a table becomes a map");
    };
    assert_eq!(values["ratio"], morph::Value::from(1.5));
    assert_eq!(values["on"], morph::Value::from(true));
    assert_eq!(values["count"], morph::Value::from(3));
    assert_eq!(values["name"], morph::Value::from("x"));
    assert_eq!(values["when"], morph::Value::from("1979-05-27T07:32:00Z"));
}

#[test]
fn the_pulumi_engine_knows_its_name() {
    let work = std::env::temp_dir().join(format!("mpc-engine-name-{}", std::process::id()));
    let settings: PulumiConfig =
        toml::from_str(&format!("work_dir = \"{}\"", work.display())).unwrap();
    let engine = Engine::Pulumi(PulumiEngine::new(&settings).unwrap());
    assert_eq!(engine.name(), "pulumi");
    assert!(format!("{engine:?}").contains("Pulumi"));
    let _ = std::fs::remove_dir_all(&work);
}

#[cfg(unix)]
#[tokio::test]
async fn a_record_directory_that_cannot_be_read_is_a_checkpoint_failure() {
    use std::os::unix::fs::PermissionsExt;
    if nix_is_root() {
        eprintln!("skipped: running as root, who can read anything");
        return;
    }
    let directory = std::env::temp_dir().join(format!("mpc-unreadable-{}", std::process::id()));
    let kind = directory.join("server");
    std::fs::create_dir_all(&kind).unwrap();
    std::fs::set_permissions(&kind, std::fs::Permissions::from_mode(0o000)).unwrap();
    let config: MorphConfig = toml::from_str(&format!(
        "state = {{ backend = \"file\", directory = \"{}\" }}",
        directory.display()
    ))
    .unwrap();
    let stores = StateStores::new(&config).unwrap();
    let error = stores.managed().await.unwrap_err();
    assert!(matches!(error, Error::Checkpoint(_)), "{error}");
    std::fs::set_permissions(&kind, std::fs::Permissions::from_mode(0o755)).unwrap();
    let _ = std::fs::remove_dir_all(&directory);
}

#[cfg(unix)]
fn nix_is_root() -> bool {
    std::process::Command::new("id")
        .arg("-u")
        .output()
        .map(|output| String::from_utf8_lossy(&output.stdout).trim() == "0")
        .unwrap_or(false)
}
