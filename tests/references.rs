//! Rows that refer to other rows, end to end: the controller against a
//! real PostgreSQL, with Morph and its in-memory cloud as the engine.
//!
//! These need a server with `wal_level = logical`, named by
//! `MPC_TEST_DATABASE_URL`, and say that they were skipped without one:
//!
//! ```sh
//! docker run -d --rm -p 5432:5432 -e POSTGRES_PASSWORD=morph postgres:16-alpine -c wal_level=logical
//! MPC_TEST_DATABASE_URL=postgres://postgres:morph@localhost/postgres cargo test --test references
//! ```
//!
//! The mock cloud's `server` needs the `id` of a `network`, and replaces
//! the server when it changes; its `dns_record` points at an address. That
//! is the shape of a topic and its subscriptions, or a network and its
//! machines, without a cloud.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use morph::{ExecutorOptions, ProviderRegistry, ResourceId, Value};
use morph_mock::{MockCloud, Op};
use morph_postgres_controller::config::Config;
use morph_postgres_controller::providers::Providers;
use morph_postgres_controller::scheduler::{Disposition, Origin};
use morph_postgres_controller::{Controller, Event, Options};
use tokio::sync::broadcast;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// How long a test waits for an event before it fails.
const PATIENCE: Duration = Duration::from_secs(90);

/// Everything one test makes in the database: a table of networks, a
/// table of servers whose `network_id` is a foreign key into it, a table
/// of DNS records with no constraint at all, and the controller's tables.
struct Fixture {
    url: String,
    client: tokio_postgres::Client,
    networks: String,
    servers: String,
    records: String,
    slot: String,
    publication: String,
    dirty: String,
    references: String,
}

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
            networks: format!("n_{unique}"),
            servers: format!("v_{unique}"),
            records: format!("r_{unique}"),
            slot: format!("s_{unique}"),
            publication: format!("p_{unique}"),
            dirty: format!("d_{unique}"),
            references: format!("x_{unique}"),
        };
        fixture
            .client
            .batch_execute(&format!(
                "CREATE TABLE {n} (id text PRIMARY KEY, name text NOT NULL, cidr text NOT NULL); \
                 CREATE TABLE {v} (id text PRIMARY KEY, name text NOT NULL, image text NOT NULL, \
                                   network_id text NOT NULL REFERENCES {n}(id) ON DELETE CASCADE, size text); \
                 CREATE TABLE {r} (id text PRIMARY KEY, name text NOT NULL, target text NOT NULL); \
                 ALTER TABLE {n} REPLICA IDENTITY FULL; \
                 ALTER TABLE {v} REPLICA IDENTITY FULL; \
                 ALTER TABLE {r} REPLICA IDENTITY FULL",
                n = fixture.networks,
                v = fixture.servers,
                r = fixture.records
            ))
            .await
            .unwrap();
        Some(fixture)
    }

    /// A configuration over the three tables. The DNS records refer to the
    /// servers by a declared reference, taking the server's address, and
    /// `records_extra` is appended to that mapping.
    fn config(&self, records_extra: &str) -> Config {
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
            workers = 4
            queue_size = 100
            full_interval = "1h"
            [reconcile.retry]
            max_attempts = 3
            initial = "100ms"
            max = "500ms"
            [morph]
            state = {{ backend = "postgres" }}
            state_prefix = "{dirty}/"
            [[mappings]]
            table = "{networks}"
            resource_type = "network"
            [[mappings]]
            table = "{servers}"
            resource_type = "server"
            exports = "ip_address"
            [[mappings]]
            table = "{records}"
            resource_type = "dns_record"
            [[mappings.references]]
            column = "target"
            table = "{servers}"
            {records_extra}
            [[providers]]
            kind = "mock"
            [logging]
            metrics_interval = "1h"
            "#,
            url = self.url,
            slot = self.slot,
            publication = self.publication,
            dirty = self.dirty,
            references = self.references,
            networks = self.networks,
            servers = self.servers,
            records = self.records,
        );
        Config::parse(&text).expect("a valid configuration")
    }

    async fn start(&self, config: Config, cloud: &MockCloud) -> Controller {
        let registry = ProviderRegistry::new()
            .with(Arc::new(cloud.clone()))
            .unwrap();
        let providers = Providers::ready(registry, ExecutorOptions::default(), Some(cloud.clone()));
        let options = Options {
            acknowledge: true,
            full_run_at_start: false,
        };
        Controller::start_with(config, providers, options)
            .await
            .expect("the controller starts")
    }

    async fn sql(&self, statements: &str) {
        self.client.batch_execute(statements).await.unwrap();
    }

    /// The rows of the references table, as `dependent -> dependency`.
    async fn recorded_references(&self) -> Vec<String> {
        self.client
            .query(
                &format!(
                    "SELECT dependent, dependency, dependency_unit FROM {} ORDER BY 1, 2",
                    self.references
                ),
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|row| {
                format!(
                    "{} -> {} ({})",
                    row.get::<_, String>(0),
                    row.get::<_, String>(1),
                    row.get::<_, String>(2)
                )
            })
            .collect()
    }

    async fn remove(&self) {
        let _ = self
            .client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {}; DROP TABLE IF EXISTS {}; DROP TABLE IF EXISTS {}; \
                 DROP TABLE IF EXISTS {}; DROP TABLE IF EXISTS {}; DROP PUBLICATION IF EXISTS {}; \
                 DELETE FROM morph_state WHERE name LIKE '{}/%'",
                self.servers,
                self.records,
                self.networks,
                self.dirty,
                self.references,
                self.publication,
                self.dirty
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
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// The controller's events, kept until they are asked for.
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

    /// Waits for the end of a reconciliation of `resource` that was not a
    /// wait: whether it succeeded, whether it changed anything, its origin
    /// and its disposition.
    async fn reconciled(&mut self, resource: &str) -> (bool, bool, Origin, Disposition) {
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
                disposition,
                ..
            } => (ok, changed, origin, disposition),
            _ => unreachable!(),
        }
    }

    /// Waits for `resource` to be parked waiting, and says for what.
    async fn waited(&mut self, resource: &str) -> (Vec<ResourceId>, String) {
        let wanted = id(resource);
        let event = self
            .wait_for(
                |event| matches!(event, Event::Waiting { resource, .. } if *resource == wanted),
            )
            .await;
        match event {
            Event::Waiting { on, reason, .. } => (on, reason),
            _ => unreachable!(),
        }
    }

    /// Whether a wait of `resource` has been seen among the events kept
    /// so far, without waiting for one.
    fn has_waited(&self, resource: &str) -> bool {
        let wanted = id(resource);
        self.kept
            .iter()
            .any(|event| matches!(event, Event::Waiting { resource, .. } if *resource == wanted))
    }

    async fn connected(&mut self) {
        self.wait_for(|event| matches!(event, Event::Connected))
            .await;
    }
}

/// The attribute `name` of the cloud object called `called`, as text.
fn attribute(cloud: &MockCloud, called: &str, name: &str) -> Option<String> {
    cloud.objects().iter().find_map(|object| {
        match (object.attributes.get("name"), object.attributes.get(name)) {
            (Some(Value::String(found)), Some(value)) if found == called => Some(value.to_string()),
            _ => None,
        }
    })
}

/// The operations the cloud saw, in order, as `op kind.name`.
fn mutations(cloud: &MockCloud) -> Vec<String> {
    cloud
        .mutations()
        .iter()
        .map(|(op, resource)| format!("{op:?} {resource}"))
        .collect()
}

#[tokio::test]
async fn a_server_waits_for_its_network_and_is_handed_the_network_id() {
    scenario!("waits", |fixture, cloud| {
        let controller = fixture.start(fixture.config(""), &cloud).await;
        let mut events = Events::of(&controller);
        events.connected().await;

        // One transaction: the network and a server on it. The server's
        // column holds the network's row id.
        fixture
            .sql(&format!(
                "BEGIN; \
                 INSERT INTO {n} VALUES ('main', 'main', '10.0.0.0/16'); \
                 INSERT INTO {v} VALUES ('web', 'web', 'linux-1', 'main', 'small'); \
                 COMMIT",
                n = fixture.networks,
                v = fixture.servers
            ))
            .await;
        let (ok, changed, ..) = events.reconciled("network.main").await;
        assert!(ok && changed);
        let (ok, changed, ..) = events.reconciled("server.web").await;
        assert!(ok && changed);

        // The cloud's server points at the cloud's network, not at the row.
        let network_id = attribute(&cloud, "main", "id").expect("the network exists");
        assert_eq!(
            attribute(&cloud, "web", "network_id").as_deref(),
            Some(network_id.as_str())
        );
        assert_ne!(network_id, "\"main\"");
        // Nothing failed on the way: the server either waited for the
        // network, or found it there.
        assert_eq!(
            morph_postgres_controller::observe::Metrics::get(
                &controller.metrics().reconciliations_failed
            ),
            0
        );
        assert_eq!(
            fixture.recorded_references().await,
            ["server.web -> network.main (network.main)".to_string()]
        );

        // A second server, with the network long there, is handed the id
        // at once.
        fixture
            .sql(&format!(
                "INSERT INTO {v} VALUES ('db', 'db', 'linux-1', 'main', 'large')",
                v = fixture.servers
            ))
            .await;
        let (ok, changed, origin, _) = events.reconciled("server.db").await;
        assert!(ok && changed);
        assert_eq!(origin, Origin::Change);
        assert!(!events.has_waited("server.db"));
        assert_eq!(
            attribute(&cloud, "db", "network_id").as_deref(),
            Some(network_id.as_str())
        );

        // A change to a server that leaves the reference alone is an
        // update in place, and the reference stays recorded.
        fixture
            .sql(&format!(
                "UPDATE {v} SET size = 'medium' WHERE id = 'db'",
                v = fixture.servers
            ))
            .await;
        let (ok, changed, ..) = events.reconciled("server.db").await;
        assert!(ok && changed);
        assert_eq!(
            cloud.mutations().last().map(|(op, _)| *op),
            Some(Op::Update)
        );
        assert_eq!(fixture.recorded_references().await.len(), 2);

        controller.stop(Duration::from_secs(20)).await;
    });
}

#[tokio::test]
async fn a_server_inserted_before_its_network_exists_waits_and_is_woken() {
    scenario!("parked", |fixture, cloud| {
        let controller = fixture.start(fixture.config(""), &cloud).await;
        let mut events = Events::of(&controller);
        events.connected().await;

        // The network row exists but is not reconciled: the controller was
        // not watching when it was inserted. The server, inserted now,
        // finds no record of the network and waits; the scheduler marks
        // the network itself.
        fixture
            .sql(&format!(
                "INSERT INTO {n} VALUES ('main', 'main', '10.0.0.0/16')",
                n = fixture.networks
            ))
            .await;
        events.reconciled("network.main").await;
        fixture
            .sql(&format!(
                "DELETE FROM morph_state WHERE name = '{}/network.main'",
                fixture.dirty
            ))
            .await;
        fixture
            .sql(&format!(
                "INSERT INTO {v} VALUES ('web', 'web', 'linux-1', 'main', 'small')",
                v = fixture.servers
            ))
            .await;
        let (on, reason) = events.waited("server.web").await;
        assert_eq!(on, [id("network.main")]);
        assert!(
            reason.contains("network.main has not been reconciled"),
            "{reason}"
        );
        assert_eq!(
            controller.parked().await,
            [(id("server.web"), vec![id("network.main")])]
        );
        let row = fixture
            .client
            .query_one(
                &format!(
                    "SELECT attempts, last_error FROM {} WHERE resource = 'server.web'",
                    fixture.dirty
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, i32>(0), 0, "waiting is not failing");
        assert!(
            row.get::<_, String>(1)
                .starts_with("waiting for network.main"),
            "{}",
            row.get::<_, String>(1)
        );

        let (ok, _, origin, _) = events.reconciled("network.main").await;
        assert!(ok);
        assert_eq!(origin, Origin::Needed);
        let (ok, changed, origin, _) = events.reconciled("server.web").await;
        assert!(ok && changed);
        assert_eq!(origin, Origin::Change, "the server's own mark, resumed");
        assert!(controller.parked().await.is_empty());
        let network_id = attribute(&cloud, "main", "id").expect("the network exists");
        assert_eq!(
            attribute(&cloud, "web", "network_id").as_deref(),
            Some(network_id.as_str())
        );
        assert_eq!(
            morph_postgres_controller::observe::Metrics::get(&controller.metrics().waits),
            1
        );

        controller.stop(Duration::from_secs(20)).await;
    });
}

#[tokio::test]
async fn a_new_network_id_reaches_the_servers_that_took_the_old_one() {
    scenario!("propagate", |fixture, cloud| {
        let controller = fixture.start(fixture.config(""), &cloud).await;
        let mut events = Events::of(&controller);
        events.connected().await;
        fixture
            .sql(&format!(
                "BEGIN; \
                 INSERT INTO {n} VALUES ('main', 'main', '10.0.0.0/16'); \
                 INSERT INTO {v} VALUES ('web', 'web', 'linux-1', 'main', 'small'); \
                 INSERT INTO {v} VALUES ('db', 'db', 'linux-1', 'main', 'small'); \
                 INSERT INTO {r} VALUES ('www', 'www', 'web'); \
                 COMMIT",
                n = fixture.networks,
                v = fixture.servers,
                r = fixture.records
            ))
            .await;
        for resource in ["network.main", "server.web", "server.db", "dns_record.www"] {
            let (ok, ..) = events.reconciled(resource).await;
            assert!(ok, "{resource}");
        }
        let old_network = attribute(&cloud, "main", "id").unwrap();
        let old_address = attribute(&cloud, "web", "ip_address").unwrap();
        assert_eq!(
            attribute(&cloud, "www", "target").as_deref(),
            Some(old_address.as_str()),
            "the record took the server's address, as the mapping exports it"
        );
        let mut recorded = fixture.recorded_references().await;
        recorded.sort();
        assert_eq!(
            recorded,
            [
                "dns_record.www -> server.web (server.web)",
                "server.db -> network.main (network.main)",
                "server.web -> network.main (network.main)",
            ]
        );

        // The network's range changes, which replaces it and gives it a
        // new id. Both servers are marked, replaced in their turn, and the
        // record follows the web server's new address.
        fixture
            .sql(&format!(
                "UPDATE {n} SET cidr = '10.9.0.0/16' WHERE id = 'main'",
                n = fixture.networks
            ))
            .await;
        let (ok, changed, ..) = events.reconciled("network.main").await;
        assert!(ok && changed);
        let new_network = attribute(&cloud, "main", "id").unwrap();
        assert_ne!(new_network, old_network);
        for server in ["server.web", "server.db"] {
            let (ok, changed, origin, _) = events.reconciled(server).await;
            assert!(ok && changed, "{server}");
            assert_eq!(origin, Origin::Propagated, "{server}");
        }
        for server in ["web", "db"] {
            assert_eq!(
                attribute(&cloud, server, "network_id").as_deref(),
                Some(new_network.as_str()),
                "{server}"
            );
        }
        let (ok, changed, origin, _) = events.reconciled("dns_record.www").await;
        assert!(ok && changed);
        assert_eq!(origin, Origin::Propagated);
        let new_address = attribute(&cloud, "web", "ip_address").unwrap();
        assert_ne!(
            new_address, old_address,
            "a replaced server has a new address"
        );
        assert_eq!(
            attribute(&cloud, "www", "target").as_deref(),
            Some(new_address.as_str())
        );
        assert_eq!(
            morph_postgres_controller::observe::Metrics::get(&controller.metrics().propagated),
            3
        );

        // A change to the network that leaves its id alone reaches nobody.
        fixture
            .sql(&format!(
                "UPDATE {n} SET name = 'renamed' WHERE id = 'main'",
                n = fixture.networks
            ))
            .await;
        let (ok, changed, ..) = events.reconciled("network.main").await;
        assert!(ok && changed);
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            morph_postgres_controller::observe::Metrics::get(&controller.metrics().propagated),
            3,
            "nothing more was marked"
        );

        controller.stop(Duration::from_secs(20)).await;
    });
}

#[tokio::test]
async fn a_network_goes_after_the_servers_that_refer_to_it() {
    scenario!("order", |fixture, cloud| {
        let controller = fixture.start(fixture.config(""), &cloud).await;
        let mut events = Events::of(&controller);
        events.connected().await;
        fixture
            .sql(&format!(
                "BEGIN; \
                 INSERT INTO {n} VALUES ('main', 'main', '10.0.0.0/16'); \
                 INSERT INTO {v} VALUES ('web', 'web', 'linux-1', 'main', 'small'); \
                 INSERT INTO {v} VALUES ('db', 'db', 'linux-1', 'main', 'small'); \
                 COMMIT",
                n = fixture.networks,
                v = fixture.servers
            ))
            .await;
        for resource in ["network.main", "server.web", "server.db"] {
            let (ok, ..) = events.reconciled(resource).await;
            assert!(ok, "{resource}");
        }

        // The database refuses to let the network row go while the servers
        // point at it, unless the delete cascades, as this table's does:
        // one statement removes all three rows, and the stream carries all
        // three deletes in one transaction.
        fixture
            .sql(&format!(
                "DELETE FROM {n} WHERE id = 'main'",
                n = fixture.networks
            ))
            .await;
        for resource in ["server.web", "server.db", "network.main"] {
            let (ok, changed, ..) = events.reconciled(resource).await;
            assert!(ok && changed, "{resource}");
        }
        assert!(cloud.objects().is_empty(), "{:?}", cloud.objects());
        let order = mutations(&cloud);
        let position = |what: &str| {
            order
                .iter()
                .position(|entry| entry == what)
                .unwrap_or_else(|| panic!("{what} not in {order:?}"))
        };
        assert!(
            position("Delete server.web") < position("Delete network.main"),
            "{order:?}"
        );
        assert!(
            position("Delete server.db") < position("Delete network.main"),
            "{order:?}"
        );
        assert!(
            fixture.recorded_references().await.is_empty(),
            "removed resources refer to nothing"
        );
        assert_eq!(
            morph_postgres_controller::observe::Metrics::get(
                &controller.metrics().reconciliations_failed
            ),
            0,
            "nothing was refused by the cloud"
        );

        controller.stop(Duration::from_secs(20)).await;
    });
}

#[tokio::test]
async fn a_removal_waits_across_a_restart_because_the_references_are_on_record() {
    scenario!("restart", |fixture, cloud| {
        let controller = fixture.start(fixture.config(""), &cloud).await;
        let mut events = Events::of(&controller);
        events.connected().await;
        fixture
            .sql(&format!(
                "BEGIN; \
                 INSERT INTO {n} VALUES ('main', 'main', '10.0.0.0/16'); \
                 INSERT INTO {v} VALUES ('web', 'web', 'linux-1', 'main', 'small'); \
                 COMMIT",
                n = fixture.networks,
                v = fixture.servers
            ))
            .await;
        for resource in ["network.main", "server.web"] {
            let (ok, ..) = events.reconciled(resource).await;
            assert!(ok, "{resource}");
        }
        controller.stop(Duration::from_secs(20)).await;

        // While nothing watches, both rows go. The new controller takes
        // the deletes up from the stream, and the network still waits for
        // the server, because the reference survived the restart.
        fixture
            .sql(&format!(
                "DELETE FROM {n} WHERE id = 'main'",
                n = fixture.networks
            ))
            .await;
        let controller = fixture.start(fixture.config(""), &cloud).await;
        let mut events = Events::of(&controller);
        for resource in ["server.web", "network.main"] {
            let (ok, changed, ..) = events.reconciled(resource).await;
            assert!(ok && changed, "{resource}");
        }
        assert!(cloud.objects().is_empty());
        let order = mutations(&cloud);
        let position = |what: &str| order.iter().position(|entry| entry == what).unwrap();
        assert!(
            position("Delete server.web") < position("Delete network.main"),
            "{order:?}"
        );
        controller.stop(Duration::from_secs(20)).await;
    });
}

#[tokio::test]
async fn a_reference_to_nothing_or_to_an_attribute_that_is_not_there_fails_with_a_reason() {
    scenario!("dangling", |fixture, cloud| {
        let controller = fixture.start(fixture.config(""), &cloud).await;
        let mut events = Events::of(&controller);
        events.connected().await;

        // No constraint guards the records table, so a record may name a
        // server that has no row. That is a failure, not a wait.
        fixture
            .sql(&format!(
                "INSERT INTO {r} VALUES ('www', 'www', 'nowhere')",
                r = fixture.records
            ))
            .await;
        let (ok, _, _, disposition) = events.reconciled("dns_record.www").await;
        assert!(!ok);
        assert!(matches!(disposition, Disposition::Retrying(_)));
        let row = fixture
            .client
            .query_one(
                &format!(
                    "SELECT last_error FROM {} WHERE resource = 'dns_record.www'",
                    fixture.dirty
                ),
                &[],
            )
            .await
            .unwrap();
        let reason: String = row.get(0);
        assert!(
            reason.contains("refers to the row \"nowhere\"") && reason.contains("does not exist"),
            "{reason}"
        );
        assert!(cloud.objects().is_empty());

        // Pointed at a server that exists, the retry goes through.
        fixture
            .sql(&format!(
                "BEGIN; \
                 INSERT INTO {n} VALUES ('main', 'main', '10.0.0.0/16'); \
                 INSERT INTO {v} VALUES ('web', 'web', 'linux-1', 'main', 'small'); \
                 UPDATE {r} SET target = 'web' WHERE id = 'www'; \
                 COMMIT",
                n = fixture.networks,
                v = fixture.servers,
                r = fixture.records
            ))
            .await;
        loop {
            let (ok, ..) = events.reconciled("dns_record.www").await;
            if ok {
                break;
            }
        }
        let address = attribute(&cloud, "web", "ip_address").unwrap();
        assert_eq!(
            attribute(&cloud, "www", "target").as_deref(),
            Some(address.as_str())
        );
        controller.stop(Duration::from_secs(20)).await;

        // A reference that asks for an attribute the record does not have
        // fails too, naming the attribute.
        let controller = fixture
            .start(fixture.config("attribute = \"no_such_thing\""), &cloud)
            .await;
        let mut events = Events::of(&controller);
        events.connected().await;
        fixture
            .sql(&format!(
                "INSERT INTO {r} VALUES ('api', 'api', 'web')",
                r = fixture.records
            ))
            .await;
        let (ok, ..) = events.reconciled("dns_record.api").await;
        assert!(!ok);
        let row = fixture
            .client
            .query_one(
                &format!(
                    "SELECT last_error FROM {} WHERE resource = 'dns_record.api'",
                    fixture.dirty
                ),
                &[],
            )
            .await
            .unwrap();
        let reason: String = row.get(0);
        assert!(
            reason.contains("server.web has no attribute no_such_thing"),
            "{reason}"
        );
        controller.stop(Duration::from_secs(20)).await;
    });
}

#[tokio::test]
async fn rows_that_wait_for_each_other_are_a_failure_not_a_deadlock() {
    scenario!("cycle", |fixture, cloud| {
        // Two records that refer to each other, through a reference that
        // no constraint checks: each needs the other's id first.
        let config = fixture.config(&format!(
            "[[mappings.references]]\ncolumn = \"name\"\ntable = \"{}\"\nattribute = \"id\"",
            fixture.records
        ));
        let controller = fixture.start(config, &cloud).await;
        let mut events = Events::of(&controller);
        events.connected().await;
        fixture
            .sql(&format!(
                "BEGIN; \
                 INSERT INTO {n} VALUES ('main', 'main', '10.0.0.0/16'); \
                 INSERT INTO {v} VALUES ('web', 'web', 'linux-1', 'main', 'small'); \
                 INSERT INTO {r} VALUES ('a', 'b', 'web'); \
                 INSERT INTO {r} VALUES ('b', 'a', 'web'); \
                 COMMIT",
                n = fixture.networks,
                v = fixture.servers,
                r = fixture.records
            ))
            .await;
        // One of the two is parked waiting for the other; the other's wait
        // would close the circle, so its turn fails, is retried, and is
        // given up on. Neither is created.
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
        assert!(resource.kind().as_str() == "dns_record", "{resource}");
        let parked = controller.parked().await;
        assert_eq!(parked.len(), 1, "{parked:?}");
        assert_ne!(parked[0].0, resource);
        assert_eq!(parked[0].1, std::slice::from_ref(&resource));
        let row = fixture
            .client
            .query_one(
                &format!(
                    "SELECT last_error FROM {} WHERE resource = $1",
                    fixture.dirty
                ),
                &[&resource.to_string()],
            )
            .await
            .unwrap();
        let reason: String = row.get(0);
        assert!(reason.contains("which waits for it"), "{reason}");
        assert!(
            !cloud
                .objects()
                .iter()
                .any(|object| object.attributes.contains_key("target")),
            "no record was created"
        );
        controller.stop(Duration::from_secs(20)).await;
    });
}

#[tokio::test]
async fn rows_of_a_stack_are_one_morph_plan_and_outsiders_read_its_state() {
    scenario!("stack", |fixture, cloud| {
        // The networks and the servers share one stack: one Morph state,
        // one plan. The records stay records of their own.
        let config = fixture.config("");
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
            workers = 4
            queue_size = 100
            full_interval = "1h"
            [reconcile.retry]
            max_attempts = 3
            initial = "100ms"
            max = "500ms"
            [morph]
            state = {{ backend = "postgres" }}
            state_prefix = "{dirty}/"
            [[mappings]]
            table = "{networks}"
            resource_type = "network"
            stack = "fleet"
            [[mappings]]
            table = "{servers}"
            resource_type = "server"
            exports = "ip_address"
            stack = "fleet"
            [[mappings]]
            table = "{records}"
            resource_type = "dns_record"
            [[mappings.references]]
            column = "target"
            table = "{servers}"
            [[providers]]
            kind = "mock"
            [logging]
            metrics_interval = "1h"
            "#,
            url = fixture.url,
            slot = fixture.slot,
            publication = fixture.publication,
            dirty = fixture.dirty,
            references = fixture.references,
            networks = fixture.networks,
            servers = fixture.servers,
            records = fixture.records,
        );
        drop(config);
        let config = Config::parse(&text).expect("a valid configuration");
        let controller = fixture.start(config, &cloud).await;
        let mut events = Events::of(&controller);
        events.connected().await;
        let started = || {
            morph_postgres_controller::observe::Metrics::get(
                &controller.metrics().reconciliations_started,
            )
        };

        // One transaction, one reconciliation of the stack: Morph makes the
        // network and then the server, with the network's id, in one plan.
        fixture
            .sql(&format!(
                "BEGIN; \
                 INSERT INTO {n} VALUES ('main', 'main', '10.0.0.0/16'); \
                 INSERT INTO {v} VALUES ('web', 'web', 'linux-1', 'main', 'small'); \
                 COMMIT",
                n = fixture.networks,
                v = fixture.servers
            ))
            .await;
        let (ok, changed, ..) = events.reconciled("stack.fleet").await;
        assert!(ok && changed);
        assert_eq!(started(), 1, "one turn for both rows");
        assert_eq!(
            mutations(&cloud),
            ["Create network.main", "Create server.web"]
        );
        let network_id = attribute(&cloud, "main", "id").unwrap();
        assert_eq!(
            attribute(&cloud, "web", "network_id").as_deref(),
            Some(network_id.as_str())
        );
        assert!(
            fixture.recorded_references().await.is_empty(),
            "a reference within the stack is Morph's, not the controller's"
        );

        // A record of its own refers into the stack, and reads the
        // server's address from the stack's state.
        fixture
            .sql(&format!(
                "INSERT INTO {r} VALUES ('www', 'www', 'web')",
                r = fixture.records
            ))
            .await;
        let (ok, changed, ..) = events.reconciled("dns_record.www").await;
        assert!(ok && changed);
        let address = attribute(&cloud, "web", "ip_address").unwrap();
        assert_eq!(
            attribute(&cloud, "www", "target").as_deref(),
            Some(address.as_str())
        );
        assert_eq!(
            fixture.recorded_references().await,
            ["dns_record.www -> server.web (stack.fleet)".to_string()]
        );

        // The network's range changes: one plan replaces the network and,
        // because its id changed, the server too. The record outside the
        // stack follows the server's new address in a turn of its own.
        cloud.reset_observations();
        fixture
            .sql(&format!(
                "UPDATE {n} SET cidr = '10.9.0.0/16' WHERE id = 'main'",
                n = fixture.networks
            ))
            .await;
        let (ok, changed, ..) = events.reconciled("stack.fleet").await;
        assert!(ok && changed);
        let order = mutations(&cloud);
        assert!(
            order.contains(&"Create network.main".to_owned())
                && order.contains(&"Create server.web".to_owned()),
            "{order:?}"
        );
        let new_network_id = attribute(&cloud, "main", "id").unwrap();
        assert_ne!(new_network_id, network_id);
        assert_eq!(
            attribute(&cloud, "web", "network_id").as_deref(),
            Some(new_network_id.as_str())
        );
        let (ok, changed, origin, _) = events.reconciled("dns_record.www").await;
        assert!(ok && changed);
        assert_eq!(origin, Origin::Propagated);
        let new_address = attribute(&cloud, "web", "ip_address").unwrap();
        assert_ne!(new_address, address);
        assert_eq!(
            attribute(&cloud, "www", "target").as_deref(),
            Some(new_address.as_str())
        );

        // The server row and the record row go together. The stack may
        // only drop the server once the record is gone.
        fixture
            .sql(&format!(
                "BEGIN; DELETE FROM {r} WHERE id = 'www'; DELETE FROM {v} WHERE id = 'web'; COMMIT",
                r = fixture.records,
                v = fixture.servers
            ))
            .await;
        let (ok, changed, ..) = events.reconciled("dns_record.www").await;
        assert!(ok && changed);
        let (ok, changed, ..) = events.reconciled("stack.fleet").await;
        assert!(ok && changed);
        assert_eq!(networks_and_servers(&cloud), ["network main"]);
        assert!(fixture.recorded_references().await.is_empty());

        // The last row goes, and the stack's state empties.
        fixture
            .sql(&format!(
                "DELETE FROM {n} WHERE id = 'main'",
                n = fixture.networks
            ))
            .await;
        let (ok, changed, ..) = events.reconciled("stack.fleet").await;
        assert!(ok && changed);
        assert!(cloud.objects().is_empty());
        assert_eq!(
            morph_postgres_controller::observe::Metrics::get(
                &controller.metrics().reconciliations_failed
            ),
            0
        );
        controller.stop(Duration::from_secs(20)).await;
    });
}

/// The networks and servers the cloud has, as `kind name`, sorted.
fn networks_and_servers(cloud: &MockCloud) -> Vec<String> {
    let mut found: Vec<String> = cloud
        .objects()
        .iter()
        .filter_map(|object| {
            let name = object.attributes.get("name")?.as_str()?.to_owned();
            let kind = if object.attributes.contains_key("cidr") {
                "network"
            } else if object.attributes.contains_key("image") {
                "server"
            } else {
                return None;
            };
            Some(format!("{kind} {name}"))
        })
        .collect();
    found.sort();
    found
}
