//! The PostgreSQL source on its own: how its stream ends, how it carries on
//! when its server cannot be reached, and what it says about the slot's
//! lag.
//!
//! Most of these need a server with `wal_level = logical`, named by
//! `MPC_TEST_DATABASE_URL`, and say that they were skipped without one.

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use morph::ResourceId;
use morph_controller_core::source::{Confirmed, Position, Source, SourceEvent};
use morph_postgres_controller::PostgresSource;
use morph_postgres_controller::app::prepare_database;
use morph_postgres_controller::config::Config;
use morph_postgres_controller::observe::Metrics;
use morph_postgres_controller::postgres::database::Database;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

static COUNTER: AtomicU64 = AtomicU64::new(0);

const PATIENCE: Duration = Duration::from_secs(60);

fn id(text: &str) -> ResourceId {
    text.parse().unwrap()
}

/// Everything written to the log while a guard from `install` lives.
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<u8>>>);

impl Log {
    fn install(&self) -> tracing::subscriber::DefaultGuard {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .with_writer(self.clone())
            .finish();
        tracing::subscriber::set_default(subscriber)
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }

    /// Waits until the log says one of `needles`, and gives which.
    async fn wait_for_any(&self, needles: &[&str]) -> String {
        let found = tokio::time::timeout(PATIENCE, async {
            loop {
                let text = self.text();
                if let Some(needle) = needles.iter().find(|needle| text.contains(**needle)) {
                    return (*needle).to_owned();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        match found {
            Ok(needle) => needle,
            Err(_) => panic!("the log never said any of {needles:?}:\n{}", self.text()),
        }
    }

    async fn wait_for(&self, needle: &str) {
        self.wait_for_any(&[needle]).await;
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Log {
    type Writer = Log;

    fn make_writer(&'a self) -> Log {
        self.clone()
    }
}

impl Write for Log {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A table of networks, with a slot and a publication of its own.
struct Fixture {
    url: String,
    client: tokio_postgres::Client,
    table: String,
    slot: String,
    publication: String,
}

impl Fixture {
    async fn new(test: &str) -> Option<Self> {
        let url = std::env::var("MPC_TEST_DATABASE_URL").ok()?;
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("the database is reachable");
        tokio::spawn(connection);
        let unique = format!(
            "{test}_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        );
        let fixture = Self {
            url,
            client,
            table: format!("t_{unique}"),
            slot: format!("s_{unique}"),
            publication: format!("p_{unique}"),
        };
        fixture
            .client
            .batch_execute(&format!(
                "CREATE TABLE {t} (id text PRIMARY KEY, name text, cidr text); \
                 ALTER TABLE {t} REPLICA IDENTITY FULL",
                t = fixture.table
            ))
            .await
            .unwrap();
        Some(fixture)
    }

    /// A configuration over the table that checks the lag every tenth of
    /// a second and warns above `warn_above` bytes.
    fn config(&self, warn_above: u64) -> Config {
        Config::parse(&format!(
            r#"
            [postgres]
            url = "{url}"
            [replication]
            slot = "{slot}"
            publication = "{publication}"
            create_publication = true
            status_interval = "200ms"
            lag_check_interval = "100ms"
            lag_warn_bytes = {warn_above}
            [replication.reconnect]
            initial = "50ms"
            max = "200ms"
            [[mappings]]
            table = "{table}"
            resource_type = "network"
            [[providers]]
            kind = "mock"
            "#,
            url = self.url,
            slot = self.slot,
            publication = self.publication,
            table = self.table,
        ))
        .expect("a valid configuration")
    }

    async fn insert(&self, name: &str, cidr: &str) {
        self.client
            .execute(
                &format!(
                    "INSERT INTO {} (id, name, cidr) VALUES ($1, $1, $2)",
                    self.table
                ),
                &[&name, &cidr],
            )
            .await
            .unwrap();
    }

    async fn remove(&self) {
        let _ = self
            .client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {}; DROP PUBLICATION IF EXISTS {}",
                self.table, self.publication
            ))
            .await;
        for _ in 0..50 {
            let dropped = self
                .client
                .batch_execute(&format!(
                    "SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots WHERE slot_name = '{s}'",
                    s = self.slot
                ))
                .await;
            if dropped.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// Starts watching, and gives the task, the events and the metrics.
fn watch(
    source: &Arc<PostgresSource>,
    outbox: mpsc::Sender<morph_controller_core::source::Transaction>,
    start: Position,
    shutdown: &CancellationToken,
) -> (
    tokio::task::JoinHandle<morph_postgres_controller::Result<()>>,
    broadcast::Receiver<SourceEvent>,
    Arc<Metrics>,
) {
    let (events, listening) = broadcast::channel(64);
    let metrics = Arc::new(Metrics::default());
    let task = tokio::spawn({
        let source = Arc::clone(source);
        let metrics = Arc::clone(&metrics);
        let shutdown = shutdown.clone();
        async move {
            source
                .watch(
                    outbox,
                    Arc::new(Confirmed::new(start)),
                    metrics,
                    events,
                    shutdown,
                )
                .await
        }
    });
    (task, listening, metrics)
}

async fn connected(listening: &mut broadcast::Receiver<SourceEvent>) {
    tokio::time::timeout(PATIENCE, async {
        loop {
            if let Ok(SourceEvent::Connected) = listening.recv().await {
                return;
            }
        }
    })
    .await
    .expect("the stream connects");
}

#[tokio::test]
async fn the_stream_ends_when_nobody_takes_its_transactions_and_a_cancelled_one_reads_nothing() {
    let Some(fixture) = Fixture::new("ends").await else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set");
        return;
    };
    let config = fixture.config(1_000_000_000);
    let database = Arc::new(Database::new(&config.postgres.url, None).unwrap());
    prepare_database(&config, &database).await.unwrap();
    let source = Arc::new(PostgresSource::new(&config, Arc::clone(&database), true));
    assert_eq!(source.name(), "postgres");
    assert_eq!(source.mappings().len(), 1);
    assert!(source.mapping_of("public", &fixture.table).is_some());
    assert!(source.mapping_of("public", "elsewhere").is_none());
    assert!(source.database().location().starts_with("postgres://"));
    assert_eq!(source.rows().mappings().len(), 1);
    assert!(format!("{source:?}").contains("PostgresSource"));
    let start = source.confirmed().await.unwrap();
    assert_ne!(start, Position::START, "the slot has a position on record");
    assert!(source.references().await.unwrap().is_empty());
    assert!(source.all_resources().await.unwrap().is_empty());

    // Cancelled before it starts: nothing is connected, and nothing read.
    let (outbox, _inbox) = mpsc::channel(8);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let (task, _, metrics) = watch(&source, outbox, start.clone(), &cancelled);
    tokio::time::timeout(PATIENCE, task)
        .await
        .expect("a cancelled source returns at once")
        .unwrap()
        .unwrap();
    assert_eq!(Metrics::get(&metrics.connected), 0);

    // Nobody takes the transactions: the stream ends with the first.
    let (outbox, inbox) = mpsc::channel(1);
    drop(inbox);
    let shutdown = CancellationToken::new();
    let (task, mut listening, metrics) = watch(&source, outbox, start, &shutdown);
    connected(&mut listening).await;
    fixture.insert("main", "10.0.0.0/16").await;
    let ended = tokio::time::timeout(PATIENCE, task)
        .await
        .expect("the stream ends")
        .unwrap();
    assert!(ended.is_ok(), "{ended:?}");
    assert!(!shutdown.is_cancelled(), "it ended on its own");
    assert_eq!(Metrics::get(&metrics.transactions), 1);

    // The row is read as usual all the same.
    let row = source
        .row(&id("network.main"))
        .await
        .unwrap()
        .expect("the row exists");
    assert_eq!(row.json["cidr"], "10.0.0.0/16");
    assert_eq!(
        source.all_resources().await.unwrap(),
        vec![id("network.main")]
    );
    assert!(source.group_rows("nothing").await.unwrap().is_empty());
    fixture.remove().await;
}

#[tokio::test]
async fn a_source_that_cannot_reach_its_server_keeps_trying_and_says_so() {
    let log = Log::default();
    let _guard = log.install();
    let config = Config::parse(
        r#"
        [postgres]
        url = "postgres://nobody@127.0.0.1:1/nothing?connect_timeout=1"
        [replication]
        slot = "s"
        publication = "p"
        lag_check_interval = "100ms"
        [replication.reconnect]
        initial = "50ms"
        max = "200ms"
        [[mappings]]
        table = "t"
        resource_type = "network"
        [[providers]]
        kind = "mock"
        "#,
    )
    .unwrap();
    let database = Arc::new(Database::new(&config.postgres.url, None).unwrap());
    let source = Arc::new(PostgresSource::new(&config, database, true));
    let (outbox, _inbox) = mpsc::channel(8);
    let shutdown = CancellationToken::new();
    let (task, _, _) = watch(&source, outbox, Position::START, &shutdown);
    let said = log
        .wait_for_any(&[
            "the stream of changes could not be connected",
            "the stream of changes was lost; connecting again",
        ])
        .await;
    assert!(said.starts_with("the stream of changes"), "{said}");
    log.wait_for("the slot's lag could not be read").await;
    assert!(!task.is_finished(), "it keeps trying");
    shutdown.cancel();
    let ended = tokio::time::timeout(PATIENCE, task)
        .await
        .expect("a cancelled source stops trying")
        .unwrap();
    assert!(ended.is_ok(), "{ended:?}");
}

#[tokio::test]
async fn the_slots_lag_is_watched_and_a_large_one_is_a_warning() {
    let Some(fixture) = Fixture::new("lag").await else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set");
        return;
    };
    let log = Log::default();
    let _guard = log.install();

    // A lag below the threshold is noted at debug level.
    let config = fixture.config(1_000_000_000);
    let database = Arc::new(Database::new(&config.postgres.url, None).unwrap());
    prepare_database(&config, &database).await.unwrap();
    let source = Arc::new(PostgresSource::new(&config, Arc::clone(&database), false));
    let start = source.confirmed().await.unwrap();
    let (outbox, _inbox) = mpsc::channel(8);
    let shutdown = CancellationToken::new();
    let (task, mut listening, _) = watch(&source, outbox, start.clone(), &shutdown);
    connected(&mut listening).await;
    log.wait_for("slot lag").await;
    assert!(
        !log.text().contains("holding back a lot of log"),
        "{}",
        log.text()
    );
    shutdown.cancel();
    tokio::time::timeout(PATIENCE, task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    // Above the threshold it is a warning. Nothing is acknowledged, so the
    // lag only grows as rows are written.
    let config = fixture.config(0);
    let source = Arc::new(PostgresSource::new(&config, Arc::clone(&database), false));
    let (outbox, _inbox) = mpsc::channel(8);
    let shutdown = CancellationToken::new();
    let (task, mut listening, metrics) = watch(&source, outbox, start, &shutdown);
    connected(&mut listening).await;
    fixture.insert("a", "10.0.0.0/16").await;
    fixture.insert("b", "10.1.0.0/16").await;
    log.wait_for("the slot is holding back a lot of log").await;
    assert!(Metrics::get(&metrics.lag) > 0);
    assert!(Metrics::get(&metrics.confirmed_position) > 0);
    shutdown.cancel();
    tokio::time::timeout(PATIENCE, task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    fixture.remove().await;
}
