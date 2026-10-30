//! The controller's own tables when they are not what it made: a table of
//! another shape under the dirty table's name, a domain under it, a table of
//! another shape under the references table's name, and a row of the
//! references table that names no resource. Also Morph's records in the
//! database when the certificate file cannot be read.
//!
//! The database parts need a server named by `MPC_TEST_DATABASE_URL`, and
//! say that they were skipped without one.

use std::collections::BTreeMap;
use std::sync::Arc;

use morph::ResourceId;
use morph_postgres_controller::Error;
use morph_postgres_controller::checkpoint::{DirtyStore, DirtyTable};
use morph_postgres_controller::config::{MorphConfig, PostgresConfig};
use morph_postgres_controller::postgres::database::Database;
use morph_postgres_controller::references::{Dependency, ReferenceStore, ReferenceTable};
use morph_postgres_controller::stores::state_stores;
use serde_json::json;

fn id(text: &str) -> ResourceId {
    text.parse().unwrap()
}

/// The export `network.a.id` after a change to `value`.
fn changed(value: &str) -> BTreeMap<String, Option<serde_json::Value>> {
    BTreeMap::from([("network.a.id".to_owned(), Some(json!(value)))])
}

#[tokio::test]
async fn tables_that_are_not_the_controllers_are_checkpoint_failures_and_not_mistaken_for_its_own()
{
    let Ok(url) = std::env::var("MPC_TEST_DATABASE_URL") else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set");
        return;
    };
    let database = Arc::new(Database::new(&url, None).unwrap());
    let client = database.client().await.unwrap();
    let unique = format!("stores_{}", std::process::id());
    let dirty = format!("d_{unique}");
    let taken = format!("k_{unique}");
    let references = format!("r_{unique}");
    let proper = format!("x_{unique}");
    client
        .batch_execute(&format!(
            "CREATE TABLE {dirty} (wrong text); \
             CREATE DOMAIN {taken} AS int; \
             CREATE TABLE {references} (wrong text)"
        ))
        .await
        .unwrap();
    let a = id("network.a");

    // A table of another shape under the dirty table's name: making the
    // table does nothing, since one exists, and every use of it fails.
    let table = DirtyTable::new(Arc::clone(&database), &dirty);
    table.prepare().await.unwrap();
    let outcomes = [
        ("mark", table.mark(std::slice::from_ref(&a)).await),
        ("load", table.load().await.map(|_| ())),
        ("done", table.done(&a).await),
        ("note", table.note(&a, "waiting").await),
        ("failed", table.failed(&a, 1, "no").await),
    ];
    for (what, outcome) in outcomes {
        assert!(
            matches!(outcome, Err(Error::Checkpoint(ref reason)) if reason.contains("does not exist")),
            "{what} says what the server said: {outcome:?}"
        );
    }

    // A domain under the dirty table's name is in the way of making the
    // table, whose row type would need the name.
    let error = DirtyTable::new(Arc::clone(&database), &taken)
        .prepare()
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Checkpoint(ref reason) if reason.contains("already exists")),
        "{error}"
    );

    // A table of another shape under the references table's name: its
    // index cannot be made, and every use of it fails.
    let table = ReferenceTable::new(Arc::clone(&database), &references);
    let dependency = Dependency {
        member: a.clone(),
        unit: a.clone(),
        attribute: "id".into(),
        value: "\"x\"".into(),
    };
    let outcomes = [
        ("prepare", table.prepare().await),
        (
            "add",
            table
                .add(&id("server.b"), std::slice::from_ref(&dependency))
                .await,
        ),
        (
            "replace",
            table
                .replace(&id("server.b"), std::slice::from_ref(&dependency))
                .await,
        ),
        (
            "dependents_of",
            table.dependents_of(&a, &[]).await.map(|_| ()),
        ),
        (
            "stale_dependents",
            table.stale_dependents(&changed("y")).await.map(|_| ()),
        ),
    ];
    for (what, outcome) in outcomes {
        assert!(
            matches!(outcome, Err(Error::Checkpoint(ref reason)) if reason.contains("does not exist")),
            "{what} says what the server said: {outcome:?}"
        );
    }

    // A proper table with a row that names no resource: the row is passed
    // over, and the rest is read as usual.
    let table = ReferenceTable::new(Arc::clone(&database), &proper);
    table.prepare().await.unwrap();
    table
        .add(&id("server.b"), std::slice::from_ref(&dependency))
        .await
        .unwrap();
    client
        .execute(
            &format!(
                "INSERT INTO {proper} (dependent, dependency, dependency_unit, attribute, value) \
                 VALUES ('not a resource', 'network.a', 'network.a', 'id', '\"x\"')"
            ),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        table.dependents_of(&a, &[]).await.unwrap(),
        vec![id("server.b")]
    );
    assert!(
        table
            .dependents_of(&a, std::slice::from_ref(&a))
            .await
            .unwrap()
            .is_empty(),
        "a member that is kept holds nobody back"
    );
    assert_eq!(
        table.stale_dependents(&changed("y")).await.unwrap(),
        vec![id("server.b")]
    );
    assert!(
        table
            .stale_dependents(&changed("x"))
            .await
            .unwrap()
            .is_empty(),
        "the value it took is the current one"
    );

    client
        .batch_execute(&format!(
            "DROP TABLE {dirty}; DROP DOMAIN {taken}; DROP TABLE {references}; DROP TABLE {proper}"
        ))
        .await
        .unwrap();
}

#[test]
fn records_in_the_database_need_a_certificate_file_that_can_be_read() {
    let morph: MorphConfig = toml::from_str("state = { backend = \"postgres\" }").unwrap();
    let postgres: PostgresConfig = toml::from_str(
        "url = \"postgres://u@localhost/db\"\nca_certificate = \"/nonexistent/authority.pem\"",
    )
    .unwrap();
    let database = Arc::new(Database::new("postgres://u@localhost/db", None).unwrap());
    let error = state_stores(&morph, &postgres, database).unwrap_err();
    assert!(matches!(error, Error::Checkpoint(_)), "{error}");

    // Without a certificate the stores are made, and nothing is connected.
    let postgres: PostgresConfig = toml::from_str("url = \"postgres://u@localhost/db\"").unwrap();
    let database = Arc::new(Database::new("postgres://u@localhost/db", None).unwrap());
    let stores = state_stores(&morph, &postgres, database).unwrap();
    assert!(
        format!("{stores:?}").contains("PostgresRecordStores"),
        "{stores:?}"
    );
}
