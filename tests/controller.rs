//! The controller against a real PostgreSQL, with Morph's in-memory cloud
//! as the provider.
//!
//! These need a server with `wal_level = logical`, named by
//! `MPC_TEST_DATABASE_URL`, and say that they were skipped without one:
//!
//! ```sh
//! docker run -d --rm -p 5432:5432 -e POSTGRES_PASSWORD=morph postgres:16-alpine -c wal_level=logical
//! MPC_TEST_DATABASE_URL=postgres://postgres:morph@localhost/postgres cargo test --test controller
//! ```
//!
//! Each test makes a table, a publication, a slot and a dirty table of its
//! own, and removes them at the end, with the records Morph kept for it.
//! The names are unique to the test and the process, so the tests can run
//! side by side against one server.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use morph::{ExecutorOptions, ProviderRegistry, ResourceId, Value};
use morph_mock::{Fault, MockCloud, Op};
use morph_postgres_controller::config::Config;
use morph_postgres_controller::providers::Providers;
use morph_postgres_controller::scheduler::{Disposition, Origin};
use morph_postgres_controller::{Controller, Error, Event, Options};
use tokio::sync::broadcast;

/// Tells the fixtures of one process apart.
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// How long a test waits for an event before it fails. Generous, because
/// a build with coverage instrumentation and a busy machine can slow the
/// thirteen controllers that run side by side several times over; a
/// passing run never waits this long.
const PATIENCE: Duration = Duration::from_secs(90);

/// Everything one test makes in the database.
struct Fixture {
    url: String,
    client: tokio_postgres::Client,
    table: String,
    slot: String,
    publication: String,
    dirty: String,
    references: String,
}

/// Runs a test body with a fixture, and removes what the fixture made even
/// if the body panics.
///
/// The body runs in a task of its own. A panic in it then comes back as an
/// error, the fixture is removed, and the panic is raised again. Without a
/// database the test says that it was skipped, and passes.
macro_rules! scenario {
    ($name:literal, |$fixture:ident, $cloud:ident| $body:block) => {
        let Some(fixture) = Fixture::new($name).await else {
            eprintln!("skipped: MPC_TEST_DATABASE_URL is not set");
            return;
        };
        let $fixture = Arc::new(fixture);
        let $cloud = MockCloud::new();
        let outcome = tokio::spawn({
            let $fixture = Arc::clone(&$fixture);
            let $cloud = $cloud.clone();
            async move { $body }
        })
        .await;
        $fixture.remove().await;
        if let Err(error) = outcome {
            std::panic::resume_unwind(error.into_panic());
        }
    };
}

fn id(text: &str) -> ResourceId {
    text.parse().unwrap()
}

impl Fixture {
    /// Connects and makes the test's table, or gives `None` if no database
    /// is named.
    ///
    /// The table's replica identity is full, so that a delete carries the
    /// `resource_type` column, which is where the mapping takes the kind
    /// from.
    async fn new(test: &str) -> Option<Self> {
        let url = std::env::var("MPC_TEST_DATABASE_URL").ok()?;
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("the database is reachable");
        tokio::spawn(connection);
        let unique = format!(
            "{}_{}_{}",
            test,
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        );
        let fixture = Self {
            url,
            client,
            table: format!("t_{unique}"),
            slot: format!("s_{unique}"),
            publication: format!("p_{unique}"),
            dirty: format!("d_{unique}"),
            references: format!("r_{unique}"),
        };
        fixture
            .client
            .batch_execute(&format!(
                "CREATE TABLE {} (id text PRIMARY KEY, resource_type text NOT NULL, name text, cidr text, region text); \
                 ALTER TABLE {} REPLICA IDENTITY FULL",
                fixture.table, fixture.table
            ))
            .await
            .unwrap();
        Some(fixture)
    }

    /// A configuration over what this fixture made, with `extra` appended.
    ///
    /// The waits are short, so that the tests are quick. The full
    /// reconciliation and the metrics line are an hour apart, so that
    /// neither happens unless a test asks for it.
    fn config(&self, extra: &str) -> Config {
        let text = format!(
            r#"
            [postgres]
            url = "{url}"
            [replication]
            slot = "{slot}"
            publication = "{publication}"
            create_publication = true
            status_interval = "300ms"
            [replication.reconnect]
            initial = "200ms"
            max = "2s"
            [checkpoint]
            table = "{dirty}"
            [references]
            table = "{references}"
            [reconcile]
            workers = 4
            queue_size = 100
            full_interval = "1h"
            [reconcile.retry]
            max_attempts = 3
            initial = "100ms"
            max = "500ms"
            [morph]
            state = {{ backend = "postgres" }}
            state_prefix = "{prefix}/"
            [[mappings]]
            table = "{table}"
            type_column = "resource_type"
            [[providers]]
            kind = "mock"
            [logging]
            metrics_interval = "1h"
            {extra}
            "#,
            url = self.url,
            slot = self.slot,
            publication = self.publication,
            dirty = self.dirty,
            references = self.references,
            table = self.table,
            prefix = self.dirty,
        );
        Config::parse(&text).expect("a valid configuration")
    }

    /// Starts a controller with `cloud` as its only provider.
    async fn start(&self, config: Config, cloud: &MockCloud, options: Options) -> Controller {
        let registry = ProviderRegistry::new()
            .with(Arc::new(cloud.clone()))
            .unwrap();
        let providers = Providers::ready(registry, ExecutorOptions::default(), Some(cloud.clone()));
        Controller::start_with(config, providers, options)
            .await
            .expect("the controller starts")
    }

    /// Options without the full reconciliation at start, so that nothing
    /// is reconciled but what the test causes.
    fn quiet() -> Options {
        Options {
            acknowledge: true,
            full_run_at_start: false,
        }
    }

    /// Inserts the row that describes the resource `network.<name>`.
    async fn insert(&self, name: &str, cidr: &str) {
        self.client
            .execute(
                &format!(
                    "INSERT INTO {} (id, resource_type, name, cidr) VALUES ($1, 'network', $2, $3)",
                    self.table
                ),
                &[&name, &name, &cidr],
            )
            .await
            .unwrap();
    }

    async fn finish(&self, controller: Controller) {
        controller.stop(Duration::from_secs(20)).await;
    }

    /// Removes everything the fixture and its controllers made: the tables,
    /// the publication, Morph's records, and the slot.
    async fn remove(&self) {
        let _ = self
            .client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {}; DROP TABLE IF EXISTS {}; DROP TABLE IF EXISTS {}; \
                 DROP PUBLICATION IF EXISTS {}; \
                 DELETE FROM morph_state WHERE name LIKE '{}/%'",
                self.table, self.dirty, self.references, self.publication, self.dirty
            ))
            .await;
        // A stream that was abandoned rather than stopped takes a moment to
        // let go of the slot.
        for _ in 0..50 {
            let dropped = self
                .client
                .batch_execute(&format!(
                    "SELECT pg_drop_replication_slot('{}') FROM pg_replication_slots WHERE slot_name = '{}'",
                    self.slot, self.slot
                ))
                .await;
            if dropped.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// The controller's events, kept until they are asked for, so that waiting
/// for one never loses another.
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

    /// Waits for an event that `wanted` accepts, keeping the others.
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

    /// Waits for the end of a reconciliation of `resource`.
    async fn reconciled(&mut self, resource: &str) -> (bool, bool, Origin, u32, Disposition) {
        let wanted = id(resource);
        let event = self
            .wait_for(
                |event| matches!(event, Event::Reconciled { resource, .. } if *resource == wanted),
            )
            .await;
        match event {
            Event::Reconciled {
                ok,
                changed,
                origin,
                attempt,
                disposition,
                ..
            } => (ok, changed, origin, attempt, disposition),
            _ => unreachable!(),
        }
    }

    async fn connected(&mut self) {
        self.wait_for(|event| matches!(event, Event::Connected))
            .await;
    }
}

/// The network objects the cloud has, by name.
fn networks(cloud: &MockCloud) -> Vec<String> {
    let mut names: Vec<String> = cloud
        .objects()
        .iter()
        .filter_map(|object| match object.attributes.get("name") {
            Some(Value::String(name)) => Some(name.clone()),
            _ => None,
        })
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn an_insert_an_update_and_a_delete_each_reconcile_through_morph() {
    scenario!("lifecycle", |fixture, cloud| {
        let controller = fixture
            .start(fixture.config(""), &cloud, Fixture::quiet())
            .await;
        let mut events = Events::of(&controller);
        events.connected().await;

        // A row appears: Morph makes the resource.
        fixture.insert("main", "10.0.0.0/16").await;
        let (ok, changed, origin, _, _) = events.reconciled("network.main").await;
        assert!(ok && changed);
        assert_eq!(origin, Origin::Change);
        assert_eq!(networks(&cloud), ["main"]);
        assert_eq!(
            cloud.mutations().last().map(|(op, _)| *op),
            Some(Op::Create)
        );

        // The row changes: Morph decides that this is an update in place. The
        // controller never looked at what changed.
        fixture
            .client
            .execute(
                &format!(
                    "UPDATE {} SET name = 'renamed' WHERE id = 'main'",
                    fixture.table
                ),
                &[],
            )
            .await
            .unwrap();
        let (ok, changed, ..) = events.reconciled("network.main").await;
        assert!(ok && changed);
        assert_eq!(networks(&cloud), ["renamed"]);
        assert_eq!(
            cloud.mutations().last().map(|(op, _)| *op),
            Some(Op::Update)
        );

        // The row goes: so does the resource.
        fixture
            .client
            .execute(
                &format!("DELETE FROM {} WHERE id = 'main'", fixture.table),
                &[],
            )
            .await
            .unwrap();
        let (ok, changed, ..) = events.reconciled("network.main").await;
        assert!(ok && changed);
        assert!(networks(&cloud).is_empty());
        assert_eq!(
            cloud.mutations().last().map(|(op, _)| *op),
            Some(Op::Delete)
        );

        fixture.finish(controller).await;
    });
}

#[tokio::test]
async fn many_changes_in_one_transaction_are_one_reconciliation() {
    scenario!("coalesce", |fixture, cloud| {
        let controller = fixture
            .start(fixture.config(""), &cloud, Fixture::quiet())
            .await;
        let mut events = Events::of(&controller);
        events.connected().await;
        fixture.insert("main", "10.0.0.0/16").await;
        events.reconciled("network.main").await;
        // The insert's own transaction event is taken off first, so that
        // the next one waited for is the one under test.
        events
            .wait_for(|event| matches!(event, Event::Transaction { .. }))
            .await;
        let started_before = morph_postgres_controller::observe::Metrics::get(
            &controller.metrics().reconciliations_started,
        );

        fixture
            .client
            .batch_execute(&format!(
                "BEGIN; \
                 UPDATE {t} SET name = 'one' WHERE id = 'main'; \
                 INSERT INTO {t} (id, resource_type, name, cidr) VALUES ('other', 'network', 'other', '10.1.0.0/16'); \
                 UPDATE {t} SET name = 'two' WHERE id = 'main'; \
                 UPDATE {t} SET name = 'three' WHERE id = 'main'; \
                 COMMIT",
                t = fixture.table
            ))
            .await
            .unwrap();

        let event = events
            .wait_for(|event| matches!(event, Event::Transaction { .. }))
            .await;
        assert!(
            matches!(event, Event::Transaction { resources: 2, .. }),
            "{event:?}"
        );
        events.reconciled("network.main").await;
        events.reconciled("network.other").await;
        // Nothing more for a while: three changes to one row were one turn.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let started = morph_postgres_controller::observe::Metrics::get(
            &controller.metrics().reconciliations_started,
        );
        assert_eq!(started - started_before, 2);
        assert_eq!(networks(&cloud), ["other", "three"]);

        fixture.finish(controller).await;
    });
}

#[tokio::test]
async fn what_happened_while_it_was_stopped_is_taken_up_after_a_restart() {
    scenario!("restart", |fixture, cloud| {
        let controller = fixture
            .start(fixture.config(""), &cloud, Fixture::quiet())
            .await;
        let mut events = Events::of(&controller);
        events.connected().await;
        fixture.insert("first", "10.0.0.0/16").await;
        events.reconciled("network.first").await;
        controller.stop(Duration::from_secs(20)).await;

        // While nothing is listening, the slot keeps the changes.
        fixture.insert("second", "10.1.0.0/16").await;
        fixture
            .client
            .execute(
                &format!(
                    "UPDATE {} SET name = 'first-renamed' WHERE id = 'first'",
                    fixture.table
                ),
                &[],
            )
            .await
            .unwrap();

        let controller = fixture
            .start(fixture.config(""), &cloud, Fixture::quiet())
            .await;
        let mut events = Events::of(&controller);
        events.reconciled("network.second").await;
        events.reconciled("network.first").await;
        assert_eq!(networks(&cloud), ["first-renamed", "second"]);

        fixture.finish(controller).await;
    });
}

#[tokio::test]
async fn a_lost_connection_is_made_again_and_nothing_is_missed() {
    scenario!("reconnect", |fixture, cloud| {
        let controller = fixture
            .start(fixture.config(""), &cloud, Fixture::quiet())
            .await;
        let mut events = Events::of(&controller);
        events.connected().await;

        // The server ends the stream behind the controller's back, once the
        // stream is there to end.
        let mut cut = 0;
        for _ in 0..100 {
            cut = fixture
                .client
                .query_one(
                    "SELECT count(pg_terminate_backend(active_pid)) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL",
                    &[&fixture.slot],
                )
                .await
                .unwrap()
                .get::<_, i64>(0);
            if cut == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(cut, 1, "the stream's connection was found and cut");
        events
            .wait_for(|event| matches!(event, Event::Disconnected { .. }))
            .await;
        events.connected().await;

        fixture.insert("after", "10.0.0.0/16").await;
        events.reconciled("network.after").await;
        assert_eq!(networks(&cloud), ["after"]);
        assert!(
            morph_postgres_controller::observe::Metrics::get(&controller.metrics().reconnects) >= 1
        );

        fixture.finish(controller).await;
    });
}

#[tokio::test]
async fn changes_delivered_twice_are_reconciled_twice_and_applied_once() {
    scenario!("duplicate", |fixture, cloud| {
        // A controller that never tells the server how far it got.
        let silent = Options {
            acknowledge: false,
            full_run_at_start: false,
        };
        let controller = fixture.start(fixture.config(""), &cloud, silent).await;
        let mut events = Events::of(&controller);
        events.connected().await;
        fixture.insert("main", "10.0.0.0/16").await;
        let (ok, changed, ..) = events.reconciled("network.main").await;
        assert!(ok && changed);
        controller.stop(Duration::from_secs(20)).await;

        // The server sends the same transaction again. Morph finds the resource
        // as it should be, and does nothing to it.
        let controller = fixture
            .start(fixture.config(""), &cloud, Fixture::quiet())
            .await;
        let mut events = Events::of(&controller);
        let (ok, changed, ..) = events.reconciled("network.main").await;
        assert!(ok && !changed, "the second time there is nothing to do");
        let creations = cloud
            .mutations()
            .iter()
            .filter(|(op, _)| *op == Op::Create)
            .count();
        assert_eq!(creations, 1);

        fixture.finish(controller).await;
    });
}

#[tokio::test]
async fn a_resource_that_cannot_be_reconciled_does_not_hold_up_the_others() {
    scenario!("unhealthy", |fixture, cloud| {
        // Every attempt to create `network.bad` fails.
        cloud.inject(
            Fault::on(Op::Create, id("network.bad"))
                .permanent()
                .always(),
        );
        let controller = fixture
            .start(fixture.config(""), &cloud, Fixture::quiet())
            .await;
        let mut events = Events::of(&controller);
        events.connected().await;

        fixture
            .client
            .batch_execute(&format!(
                "BEGIN; \
                 INSERT INTO {t} (id, resource_type, name, cidr) VALUES ('bad', 'network', 'bad', '10.0.0.0/16'); \
                 INSERT INTO {t} (id, resource_type, name, cidr) VALUES ('good', 'network', 'good', '10.1.0.0/16'); \
                 COMMIT",
                t = fixture.table
            ))
            .await
            .unwrap();

        let (ok, ..) = events.reconciled("network.good").await;
        assert!(ok);
        let mut dispositions = Vec::new();
        for attempt in 1..=3 {
            let (ok, _, _, numbered, disposition) = events.reconciled("network.bad").await;
            assert!(!ok);
            assert_eq!(numbered, attempt);
            dispositions.push(disposition);
        }
        assert!(matches!(dispositions[0], Disposition::Retrying(_)));
        assert_eq!(dispositions[2], Disposition::Unhealthy);
        assert_eq!(controller.unhealthy().await, [id("network.bad")]);
        assert_eq!(networks(&cloud), ["good"]);

        // The failure is on record for whoever looks.
        let row = fixture
            .client
            .query_one(
                &format!(
                    "SELECT attempts, last_error FROM {} WHERE resource = 'network.bad'",
                    fixture.dirty
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, i32>(0), 3);
        assert!(row.get::<_, String>(1).contains("injected fault"));

        fixture.finish(controller).await;
    });
}

#[tokio::test]
async fn the_full_reconciliation_catches_what_no_change_said() {
    scenario!("periodic", |fixture, cloud| {
        let config = fixture.config("");
        let mut config = config;
        // Short, so that the test need not wait long for the next full
        // reconciliation.
        config.reconcile.full_interval = Duration::from_secs(2);
        let controller = fixture.start(config, &cloud, Fixture::quiet()).await;
        let mut events = Events::of(&controller);
        events.connected().await;
        fixture.insert("main", "10.0.0.0/16").await;
        events.reconciled("network.main").await;

        // Somebody removes the resource behind everybody's back. No row changed,
        // so no change says so.
        let handle = cloud.objects()[0].handle.clone();
        assert!(cloud.destroy(&handle));

        events
            .wait_for(|event| matches!(event, Event::FullRun { .. }))
            .await;
        let (ok, changed, origin, ..) = events.reconciled("network.main").await;
        assert!(ok && changed, "it was made again");
        assert_eq!(origin, Origin::Periodic);
        assert_eq!(networks(&cloud), ["main"]);

        fixture.finish(controller).await;
    });
}

#[tokio::test]
async fn what_was_waiting_in_the_dirty_table_is_taken_up_at_start() {
    scenario!("recovered", |fixture, cloud| {
        fixture.insert("main", "10.0.0.0/16").await;
        // As if a controller had taken the change in, and stopped before it
        // could reconcile.
        fixture
            .client
            .batch_execute(&format!(
                "CREATE TABLE {d} (resource text PRIMARY KEY, marked_at timestamptz NOT NULL DEFAULT now(), attempts integer NOT NULL DEFAULT 0, last_error text, last_failed_at timestamptz); \
                 INSERT INTO {d} (resource) VALUES ('network.main')",
                d = fixture.dirty
            ))
            .await
            .unwrap();

        let controller = fixture
            .start(fixture.config(""), &cloud, Fixture::quiet())
            .await;
        let mut events = Events::of(&controller);
        let (ok, changed, origin, ..) = events.reconciled("network.main").await;
        assert!(ok && changed);
        assert_eq!(origin, Origin::Recovered);
        assert_eq!(networks(&cloud), ["main"]);
        let left: i64 = fixture
            .client
            .query_one(&format!("SELECT count(*) FROM {}", fixture.dirty), &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(left, 0, "done, and no longer waiting");

        fixture.finish(controller).await;
    });
}

#[tokio::test]
async fn retain_keeps_the_real_thing_and_prevent_destroy_refuses_to_let_it_go() {
    scenario!("lifecycle", |fixture, cloud| {
        // retain: the row goes, the network stays, and Morph lets go of it.
        let retaining = fixture.config("[mappings.lifecycle]\nretain = true");
        let controller = fixture.start(retaining, &cloud, Fixture::quiet()).await;
        let mut events = Events::of(&controller);
        events.connected().await;
        fixture.insert("kept", "10.1.0.0/16").await;
        assert!(events.reconciled("network.kept").await.0);
        assert_eq!(networks(&cloud), ["kept"]);
        fixture
            .client
            .execute(
                &format!("DELETE FROM {} WHERE id = 'kept'", fixture.table),
                &[],
            )
            .await
            .unwrap();
        let (ok, changed, ..) = events.reconciled("network.kept").await;
        assert!(ok && changed, "letting go is a step");
        assert_eq!(networks(&cloud), ["kept"], "the real thing is kept");
        fixture.finish(controller).await;

        // prevent_destroy: the row goes, and every attempt is refused.
        let protecting = fixture.config("[mappings.lifecycle]\nprevent_destroy = true");
        let controller = fixture.start(protecting, &cloud, Fixture::quiet()).await;
        let mut events = Events::of(&controller);
        events.connected().await;
        fixture.insert("safe", "10.2.0.0/16").await;
        assert!(events.reconciled("network.safe").await.0);
        fixture
            .client
            .execute(
                &format!("DELETE FROM {} WHERE id = 'safe'", fixture.table),
                &[],
            )
            .await
            .unwrap();
        let mut last = None;
        for _ in 0..3 {
            let (ok, _, _, attempt, disposition) = events.reconciled("network.safe").await;
            assert!(!ok, "attempt {attempt} must be refused");
            last = Some(disposition);
        }
        assert_eq!(last, Some(Disposition::Unhealthy));
        assert_eq!(controller.unhealthy().await, [id("network.safe")]);
        assert!(
            networks(&cloud).contains(&"safe".to_string()),
            "still there"
        );
        fixture.finish(controller).await;
    });
}

#[tokio::test]
async fn the_memory_checkpoint_mode_and_the_file_record_backend_work_end_to_end() {
    scenario!("memoryfile", |fixture, cloud| {
        let directory = std::env::temp_dir().join(format!("mpc-records-{}", fixture.dirty));
        let _ = std::fs::remove_dir_all(&directory);
        let mut config = fixture.config("");
        config.checkpoint.mode = morph_postgres_controller::config::CheckpointMode::Memory;
        config.morph.state = morph_postgres_controller::config::StateConfig::File {
            directory: directory.clone(),
        };
        // The watchers run often and warn at the slightest lag, so that
        // their paths are walked too.
        config.replication.lag_check_interval = Duration::from_millis(200);
        config.replication.lag_warn_bytes = 0;
        config.logging.metrics_interval = Duration::from_millis(200);
        let controller = fixture.start(config, &cloud, Fixture::quiet()).await;
        let mut events = Events::of(&controller);
        events.connected().await;

        fixture.insert("a", "10.3.0.0/16").await;
        assert!(events.reconciled("network.a").await.0);
        assert!(
            directory.join("network/a.json").is_file(),
            "one file per record"
        );
        fixture
            .client
            .execute(
                &format!(
                    "UPDATE {} SET cidr = '10.4.0.0/16' WHERE id = 'a'",
                    fixture.table
                ),
                &[],
            )
            .await
            .unwrap();
        let (ok, changed, ..) = events.reconciled("network.a").await;
        assert!(ok && changed);
        fixture
            .client
            .execute(
                &format!("DELETE FROM {} WHERE id = 'a'", fixture.table),
                &[],
            )
            .await
            .unwrap();
        assert!(events.reconciled("network.a").await.0);
        assert!(networks(&cloud).is_empty());

        let metrics = controller.metrics();
        assert!(morph_postgres_controller::observe::Metrics::get(&metrics.transactions) >= 3);
        assert!(
            morph_postgres_controller::observe::Metrics::get(&metrics.reconciliations_succeeded)
                >= 3
        );
        assert_eq!(
            morph_postgres_controller::observe::Metrics::get(&metrics.reconciliations_failed),
            0
        );
        assert_eq!(
            morph_postgres_controller::observe::Metrics::get(&metrics.connected),
            1
        );
        tokio::time::sleep(Duration::from_millis(600)).await;
        fixture.finish(controller).await;
        let _ = std::fs::remove_dir_all(&directory);
    });
}

#[tokio::test]
async fn a_reconciliation_that_takes_too_long_is_abandoned_and_retried() {
    scenario!("timeout", |fixture, cloud| {
        let _ = cloud.clone().with_latency(Duration::from_secs(2));
        let mut config = fixture.config("");
        config.reconcile.timeout = Duration::from_millis(300);
        let controller = fixture.start(config, &cloud, Fixture::quiet()).await;
        let mut events = Events::of(&controller);
        events.connected().await;
        assert!(controller.mock().is_some());
        fixture.insert("slow", "10.5.0.0/16").await;
        let mut last = None;
        for expected in 1..=3 {
            let (ok, _, _, attempt, disposition) = events.reconciled("network.slow").await;
            assert!(!ok);
            assert_eq!(attempt, expected);
            last = Some(disposition);
        }
        assert_eq!(last, Some(Disposition::Unhealthy));
        assert_eq!(controller.unhealthy().await, [id("network.slow")]);
        fixture.finish(controller).await;
    });
}

#[tokio::test]
async fn the_controller_starts_from_its_configuration_alone_and_refuses_what_it_may_not_make() {
    scenario!("startup", |fixture, _cloud| {
        // A row of the dirty table that names no resource is passed over.
        fixture
            .client
            .batch_execute(&format!(
                "CREATE TABLE {d} (resource text PRIMARY KEY, marked_at timestamptz NOT NULL DEFAULT now(), \
                 attempts integer NOT NULL DEFAULT 0, last_error text); \
                 INSERT INTO {d} (resource) VALUES ('not a resource')",
                d = fixture.dirty
            ))
            .await
            .unwrap();
        // The providers of the configuration: the mock cloud, started by the
        // controller itself, with the default options.
        let controller = Controller::start(fixture.config("")).await.unwrap();
        assert!(controller.mock().is_some());
        assert!(format!("{controller:?}").contains("Controller"));
        let mut events = Events::of(&controller);
        events.connected().await;
        events
            .wait_for(|event| matches!(event, Event::FullRun { .. }))
            .await;

        // A truncate is logged and left to the full pass; the stream goes on.
        fixture
            .client
            .batch_execute(&format!("TRUNCATE {}", fixture.table))
            .await
            .unwrap();
        events
            .wait_for(
                |event| matches!(event, Event::Transaction { resources, .. } if *resources == 0),
            )
            .await;
        fixture.insert("after", "10.9.0.0/16").await;
        assert!(events.reconciled("network.after").await.0);
        fixture.finish(controller).await;

        // A publication or a slot that is missing and may not be made.
        let mut config = fixture.config("");
        config.replication.publication = format!("{}_absent", fixture.publication);
        config.replication.create_publication = false;
        let error = Controller::start(config).await.unwrap_err();
        assert!(
            matches!(error, morph_postgres_controller::Error::Config(ref m) if m.contains("publication")),
            "{error}"
        );
        let mut config = fixture.config("");
        config.replication.slot = format!("{}_absent", fixture.slot);
        config.replication.create_slot = false;
        let error = Controller::start(config).await.unwrap_err();
        assert!(
            matches!(error, morph_postgres_controller::Error::Config(ref m) if m.contains("slot")),
            "{error}"
        );
    });
}

#[tokio::test]
async fn work_still_in_flight_when_the_grace_runs_out_is_abandoned() {
    scenario!("abandon", |fixture, cloud| {
        let _ = cloud.clone().with_latency(Duration::from_secs(3));
        let controller = fixture
            .start(fixture.config(""), &cloud, Fixture::quiet())
            .await;
        let mut events = Events::of(&controller);
        events.connected().await;
        fixture.insert("slow", "10.6.0.0/16").await;
        events
            .wait_for(|event| matches!(event, Event::Transaction { .. }))
            .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        // No grace to speak of: the reconciliation is abandoned, and the
        // controller still stops.
        controller.stop(Duration::from_millis(10)).await;
    });
}

/// `url` with a socket that is not there as its first host and the real
/// server as its second. The ordinary connection tries the hosts in turn
/// and reaches the server; the stream takes the first host alone, and
/// cannot use a socket.
fn socket_first(url: &str) -> String {
    let config: tokio_postgres::Config = url.parse().unwrap();
    let host = match &config.get_hosts()[0] {
        tokio_postgres::config::Host::Tcp(host) => host.clone(),
        other => panic!("the test database is not on a TCP host: {other:?}"),
    };
    let port = config.get_ports().first().copied().unwrap_or(5432);
    let user = config.get_user().unwrap_or("postgres");
    let password = config
        .get_password()
        .map(|password| format!(" password={}", String::from_utf8_lossy(password)))
        .unwrap_or_default();
    let database = config.get_dbname().unwrap_or(user);
    // The key-value form, because in the URL form a list of hosts cannot
    // hold a socket.
    format!(
        "host=/nonexistent/socket,{host} port=5432,{port} user={user}{password} dbname={database}"
    )
}

#[tokio::test]
async fn a_healthy_controller_reports_no_failure_and_a_stream_that_cannot_use_the_url_is_one() {
    scenario!("failed", |fixture, cloud| {
        let mut controller = fixture
            .start(fixture.config(""), &cloud, Fixture::quiet())
            .await;
        let mut events = Events::of(&controller);
        events.connected().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(300), controller.failed())
                .await
                .is_err(),
            "nothing has failed"
        );
        fixture.finish(controller).await;

        let providers = || {
            let registry = ProviderRegistry::new()
                .with(Arc::new(cloud.clone()))
                .unwrap();
            Providers::ready(registry, ExecutorOptions::default(), Some(cloud.clone()))
        };

        // A certificate file that is not there is refused before anything
        // is connected.
        let mut config = fixture.config("");
        config.postgres.ca_certificate = Some("/nonexistent/authority.pem".into());
        let error = Controller::start_with(config, providers(), Fixture::quiet())
            .await
            .expect_err("the certificate is refused");
        assert!(matches!(error, Error::Config(_)), "{error}");

        // A URL that only the stream cannot use: the controller starts,
        // the reader fails at once, and the failure is reported.
        let mut config = fixture.config("");
        config.postgres.url = socket_first(&fixture.url);
        let mut controller = Controller::start_with(config, providers(), Fixture::quiet())
            .await
            .expect("the ordinary connection moves on to the second host");
        let error = tokio::time::timeout(PATIENCE, controller.failed())
            .await
            .expect("the reader's failure is reported");
        assert!(
            matches!(error, Error::Config(ref reason) if reason.contains("TCP address")),
            "{error}"
        );
        fixture.finish(controller).await;
    });
}

#[tokio::test]
async fn a_table_with_a_column_of_a_user_defined_type_is_described_to_the_reader_and_reconciled() {
    scenario!("enum", |fixture, cloud| {
        // The server describes the type before the table, in a message
        // the reader has no use for and passes over.
        let kind = format!("mood_{}", fixture.table);
        let table = format!("{}_moods", fixture.table);
        fixture
            .client
            .batch_execute(&format!(
                "CREATE TYPE {kind} AS ENUM ('happy', 'grumpy'); \
                 CREATE TABLE {table} (id text PRIMARY KEY, name text, cidr text, mood {kind}); \
                 ALTER TABLE {table} REPLICA IDENTITY FULL"
            ))
            .await
            .unwrap();
        let config = fixture.config(&format!(
            "[[mappings]]\ntable = \"{table}\"\nresource_type = \"network\"\nexclude_columns = [\"mood\"]"
        ));
        let controller = fixture.start(config, &cloud, Fixture::quiet()).await;
        let mut events = Events::of(&controller);
        events.connected().await;
        fixture
            .client
            .execute(
                &format!(
                    "INSERT INTO {table} (id, name, cidr, mood) VALUES ('moody', 'moody', '10.7.0.0/16', 'happy')"
                ),
                &[],
            )
            .await
            .unwrap();
        let (ok, changed, ..) = events.reconciled("network.moody").await;
        assert!(ok && changed);
        assert_eq!(networks(&cloud), ["moody"]);
        assert!(
            !cloud.objects()[0].attributes.contains_key("mood"),
            "the excluded column is not an attribute"
        );
        let metrics = controller.metrics();
        assert_eq!(
            morph_postgres_controller::observe::Metrics::get(&metrics.mapping_errors),
            0,
            "a message the reader has no use for is not an error"
        );
        fixture.finish(controller).await;
        fixture
            .client
            .batch_execute(&format!("DROP TABLE {table}; DROP TYPE {kind}"))
            .await
            .unwrap();
    });
}

#[tokio::test]
async fn a_row_with_an_import_id_takes_charge_of_an_object_that_exists() {
    scenario!("adopt", |fixture, cloud| {
        let table = format!("{}_adopt", fixture.table);
        fixture
            .client
            .batch_execute(&format!(
                "CREATE TABLE {table} (id text PRIMARY KEY, name text, cidr text, object text)"
            ))
            .await
            .unwrap();
        let config = fixture.config(&format!(
            "[[mappings]]\ntable = \"{table}\"\nresource_type = \"network\"\nimport_column = \"object\""
        ));
        let controller = fixture.start(config, &cloud, Fixture::quiet()).await;
        let mut events = Events::of(&controller);
        events.connected().await;

        // Something made outside the controller, as Terraform would have.
        let handle = cloud.plant("network", [("name", "legacy"), ("cidr", "10.9.0.0/16")]);
        fixture
            .client
            .execute(
                &format!(
                    "INSERT INTO {table} (id, name, cidr, object) VALUES ('legacy', 'legacy', '10.9.0.0/16', $1)"
                ),
                &[&handle.to_string()],
            )
            .await
            .unwrap();
        let (ok, changed, ..) = events.reconciled("network.legacy").await;
        assert!(ok);
        assert!(!changed, "the row describes the object as it is");
        assert_eq!(cloud.count(Op::Import), 1);
        assert_eq!(cloud.count(Op::Create), 0, "nothing was made again");

        // The name updates in place; the cidr would replace the network.
        fixture
            .client
            .execute(
                &format!("UPDATE {table} SET name = 'legacy-renamed' WHERE id = 'legacy'"),
                &[],
            )
            .await
            .unwrap();
        let (ok, changed, ..) = events.reconciled("network.legacy").await;
        assert!(ok && changed);
        assert_eq!(cloud.count(Op::Update), 1, "{:?}", cloud.calls());
        assert_eq!(cloud.count(Op::Create), 0);
        assert!(
            !cloud
                .object(&handle)
                .unwrap()
                .attributes
                .contains_key("object"),
            "the import column is not an attribute"
        );
        fixture.finish(controller).await;
        fixture
            .client
            .batch_execute(&format!("DROP TABLE {table}"))
            .await
            .unwrap();
    });
}
