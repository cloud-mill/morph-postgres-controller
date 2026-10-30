//! Taking charge of what exists, through a real provider written for
//! Terraform.
//!
//! Needs a server with `wal_level = logical`, named by
//! `MPC_TEST_DATABASE_URL`, and `hashicorp/random` fetched into
//! `../morph/target/providers`, as Morph's own examples leave it; says
//! that it was skipped without either. Nothing here reaches the network.
//!
//! The provider's `random_id` is the ideal object to adopt: it is nothing
//! but its identifier, so importing an identifier recreates the object
//! exactly, and `morph-terraform-migrate`'s own fixture holds one whose
//! values are known. The scenario is the migration's gate, end to end:
//! a row whose import id names the object as the row describes it
//! reconciles with no change; one whose identifier names nothing fails and
//! makes nothing; one whose identifier leaves out the prefix is imported,
//! differs from its row, and is kept from being replaced by
//! `prevent_destroy`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use morph_postgres_controller::config::Config;
use morph_postgres_controller::{Controller, Event};
use tokio::sync::broadcast;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// How long a test waits for an event. The provider starts from the cache.
const PATIENCE: Duration = Duration::from_secs(120);

/// The cache that Morph's examples fill, and the newest version of the
/// random provider in it.
fn cached_random() -> Option<(PathBuf, String)> {
    let cache = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../morph/target/providers");
    let versions = cache.join("registry.opentofu.org/hashicorp/random");
    let mut found: Vec<String> = std::fs::read_dir(&versions)
        .ok()?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().is_dir())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    found.sort();
    Some((cache.canonicalize().ok()?, found.pop()?))
}

struct Fixture {
    client: tokio_postgres::Client,
    url: String,
    table: String,
    slot: String,
    publication: String,
    dirty: String,
    references: String,
    prefix: String,
    cache: PathBuf,
    version: String,
}

impl Fixture {
    async fn new() -> Option<Self> {
        let url = std::env::var("MPC_TEST_DATABASE_URL").ok()?;
        let (cache, version) = cached_random()?;
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("the database is reachable");
        tokio::spawn(connection);
        let unique = format!(
            "adopt_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        );
        let fixture = Self {
            client,
            url,
            table: format!("t_{unique}"),
            slot: format!("s_{unique}"),
            publication: format!("p_{unique}"),
            dirty: format!("d_{unique}"),
            references: format!("r_{unique}"),
            prefix: unique,
            cache,
            version,
        };
        fixture
            .client
            .batch_execute(&format!(
                "CREATE TABLE {} (id text PRIMARY KEY, byte_length int, prefix text, object text)",
                fixture.table
            ))
            .await
            .unwrap();
        Some(fixture)
    }

    fn config(&self) -> Config {
        let text = format!(
            r#"
            [postgres]
            url = "{url}"
            [replication]
            slot = "{slot}"
            publication = "{publication}"
            create_publication = true
            status_interval = "300ms"
            [checkpoint]
            table = "{dirty}"
            [references]
            table = "{references}"
            [reconcile]
            workers = 2
            queue_size = 50
            full_interval = "1h"
            timeout = "2m"
            [reconcile.retry]
            max_attempts = 2
            initial = "100ms"
            max = "500ms"
            [morph]
            state = {{ backend = "postgres" }}
            state_prefix = "{prefix}/"
            [[mappings]]
            table = "{table}"
            resource_type = "random_id"
            import_column = "object"
            [mappings.lifecycle]
            prevent_destroy = true
            [[providers]]
            kind = "terraform"
            source = "hashicorp/random"
            version = "{version}"
            cache = "{cache}"
            [logging]
            metrics_interval = "1h"
            "#,
            url = self.url,
            slot = self.slot,
            publication = self.publication,
            dirty = self.dirty,
            references = self.references,
            prefix = self.prefix,
            table = self.table,
            version = self.version,
            cache = self.cache.display(),
        );
        Config::parse(&text).expect("a valid configuration")
    }

    async fn insert(&self, id: &str, byte_length: i32, prefix: &str, object: &str) {
        self.client
            .execute(
                &format!(
                    "INSERT INTO {} (id, byte_length, prefix, object) VALUES ($1, $2, $3, $4)",
                    self.table
                ),
                &[&id, &byte_length, &prefix, &object],
            )
            .await
            .unwrap();
    }

    /// Morph's record of a resource, as text, or `None` if there is none.
    async fn record(&self, resource: &str) -> Option<String> {
        self.client
            .query_opt(
                "SELECT state::text FROM morph_state WHERE name = $1",
                &[&format!("{}/{resource}", self.prefix)],
            )
            .await
            .unwrap()
            .map(|row| row.get(0))
    }

    async fn remove(&self) {
        let _ = self
            .client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {}; DROP TABLE IF EXISTS {}; DROP TABLE IF EXISTS {}; \
                 DROP PUBLICATION IF EXISTS {}; \
                 DELETE FROM morph_state WHERE name LIKE '{}/%'",
                self.table, self.dirty, self.references, self.publication, self.prefix
            ))
            .await;
        for _ in 0..50 {
            let dropped = self
                .client
                .batch_execute(&format!(
                    "SELECT pg_drop_replication_slot('{}') FROM pg_replication_slots WHERE slot_name = '{}'",
                    self.slot, self.slot
                ))
                .await;
            if dropped.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

struct Events {
    receiver: broadcast::Receiver<Event>,
    kept: std::collections::VecDeque<Event>,
}

impl Events {
    fn of(controller: &Controller) -> Self {
        Self {
            receiver: controller.events(),
            kept: std::collections::VecDeque::new(),
        }
    }

    async fn wait_for(&mut self, wanted: impl Fn(&Event) -> bool) -> Event {
        if let Some(index) = self.kept.iter().position(&wanted) {
            return self.kept.remove(index).expect("just found");
        }
        tokio::time::timeout(PATIENCE, async {
            loop {
                match self.receiver.recv().await {
                    Ok(event) if wanted(&event) => return event,
                    Ok(event) => self.kept.push_back(event),
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => {
                        panic!("the controller stopped sending events")
                    }
                }
            }
        })
        .await
        .expect("the event arrives in time")
    }

    /// Whether a reconciliation of `resource` succeeded, and whether it
    /// changed anything.
    async fn reconciled(&mut self, resource: &str) -> (bool, bool) {
        let wanted: morph::ResourceId = resource.parse().unwrap();
        let event = self
            .wait_for(
                |event| matches!(event, Event::Reconciled { resource, .. } if *resource == wanted),
            )
            .await;
        match event {
            Event::Reconciled { ok, changed, .. } => (ok, changed),
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn a_random_id_is_adopted_by_its_identifier_through_the_real_provider() {
    let Some(fixture) = Fixture::new().await else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set, or hashicorp/random is not cached");
        return;
    };
    let outcome = tokio::spawn(async move {
        let controller = Controller::start(fixture.config())
            .await
            .expect("the controller starts the cached provider");
        let mut events = Events::of(&controller);
        events
            .wait_for(|event| matches!(event, Event::Connected))
            .await;

        // The row the migration tool makes of the fixture's random_id.seed,
        // with the prefix put into the import id as the report says to.
        // The provider rebuilds the object from the identifier, and the
        // row describes it as it is: nothing changes, and the record holds
        // the hex Terraform's state held.
        fixture.insert("seed", 4, "demo", "demo,Ggg4Rw").await;
        let (ok, changed) = events.reconciled("random_id.seed").await;
        assert!(ok, "the import id names the object");
        assert!(!changed, "the first turn changes nothing");
        let record = fixture.record("random_id.seed").await.expect("a record");
        assert!(record.contains("demo1a083847"), "{record}");
        assert!(record.contains("Ggg4Rw"), "{record}");

        // An identifier that names nothing fails the row, and nothing is
        // made in its place.
        fixture.insert("bad", 4, "demo", "not base64!!").await;
        let (ok, changed) = events.reconciled("random_id.bad").await;
        assert!(!ok && !changed);
        let (ok, _) = events.reconciled("random_id.bad").await;
        assert!(!ok, "the retry fails the same way");
        assert!(fixture.record("random_id.bad").await.is_none());
        assert!(
            controller
                .unhealthy()
                .await
                .iter()
                .any(|resource| resource.to_string() == "random_id.bad")
        );

        // An identifier without the prefix is imported as an object without
        // one. The row differs, the difference calls for a replacement, and
        // prevent_destroy refuses it: the gate holds.
        fixture.insert("plain", 4, "demo", "Ggg4Rw").await;
        let (ok, changed) = events.reconciled("random_id.plain").await;
        assert!(!ok && !changed);
        let record = fixture
            .record("random_id.plain")
            .await
            .expect("imported all the same");
        assert!(record.contains("\"1a083847\""), "{record}");
        assert!(!record.contains("demo1a083847"), "{record}");

        controller.stop(Duration::from_secs(30)).await;
        fixture
    })
    .await;
    match outcome {
        Ok(fixture) => fixture.remove().await,
        Err(error) => std::panic::resume_unwind(error.into_panic()),
    }
}
