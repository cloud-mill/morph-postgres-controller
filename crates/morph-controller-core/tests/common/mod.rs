//! What the core's tests share: a source that keeps its rows in memory and
//! sends what it is told to, stores that fail on demand, an engine over the
//! in-memory cloud, and a log that the tests can read.
//!
//! The source stands in for a database. It is a second implementation of
//! [`Source`], and the controller cannot tell it from the first, which is
//! what the trait is for.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use morph::{ExecutorOptions, ProviderRegistry, ResourceId, Value};
use morph_controller_core::app::{Checkpoint, Controller, Event, Parts};
use morph_controller_core::checkpoint::{DirtyRow, DirtyStore};
use morph_controller_core::config::{
    Backoff, Mapping, MorphConfig, ReconcileConfig, STACK_KIND, StateConfig,
};
use morph_controller_core::engine::{Engine, MorphEngine, export_parts};
use morph_controller_core::error::{Error, Result};
use morph_controller_core::mapping::Row;
use morph_controller_core::observe::Metrics;
use morph_controller_core::references::{Dependency, ForeignKey, ReferenceStore, References};
use morph_controller_core::scheduler::{Disposition, Origin};
use morph_controller_core::source::{
    Committed, Confirmed, Position, Source, SourceEvent, Transaction,
};
use morph_controller_core::stores::StateStores;
use morph_mock::MockCloud;
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

/// How long a test waits for something to happen before it fails.
pub const PATIENCE: Duration = Duration::from_secs(60);

pub fn id(text: &str) -> ResourceId {
    text.parse().unwrap()
}

/// Networks, servers with a foreign key into the networks, and DNS records
/// with a declared reference to the servers: the kinds the mock cloud
/// offers, mapped as the PostgreSQL tests map them.
pub const FLEET: &str = r#"
[[mappings]]
table = "networks"
resource_type = "network"
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

/// The foreign key of the fleet: `servers.network_id` into `networks.id`.
pub fn fleet_keys() -> Vec<ForeignKey> {
    vec![ForeignKey {
        constraint: "servers_network_id_fkey".into(),
        schema: "public".into(),
        table: "servers".into(),
        column: "network_id".into(),
        target_schema: "public".into(),
        target_table: "networks".into(),
        target_column: "id".into(),
        columns: 1,
    }]
}

/// The fleet with every server in the stack `farm`.
pub const FARM: &str = r#"
[[mappings]]
table = "networks"
resource_type = "network"
[[mappings]]
table = "servers"
resource_type = "server"
stack = "farm"
"#;

/// Networks whose rows may name an object that exists already, and servers
/// whose `network_ref` column gives the network's id, or whose
/// `network_alt` column gives the network's name, to the `network_id`
/// attribute: for taking charge of what exists, and for references that
/// set another attribute.
pub const ADOPTED: &str = r#"
[[mappings]]
table = "networks"
resource_type = "network"
import_column = "object"
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

/// DNS records whose `name` column refers to another record: a table that
/// can refer to itself.
pub const RECORDS: &str = r#"
[[mappings]]
table = "records"
resource_type = "dns_record"
[[mappings.references]]
column = "name"
table = "records"
attribute = "id"
"#;

#[derive(Deserialize)]
struct Mappings {
    mappings: Vec<Mapping>,
}

/// How the fake source's stream ends once [`FakeSource::close`] has been
/// called.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ending {
    /// `watch` returns `Ok`, as if it had been told to stop.
    Quietly,
    /// `watch` returns a connection error.
    Failing,
    /// The task panics.
    Panicking,
}

/// A source with its rows in memory, that sends the transactions a test
/// commits.
///
/// Positions are the transaction ids, which count up from one.
pub struct FakeSource {
    mappings: Vec<Mapping>,
    keys: Vec<ForeignKey>,
    rows: Mutex<BTreeMap<String, BTreeMap<String, serde_json::Value>>>,
    feed: Mutex<Option<mpsc::UnboundedSender<Transaction>>>,
    stream: tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<Transaction>>>,
    next: AtomicU64,
    ending: Mutex<Ending>,
    down: AtomicBool,
    greetings: AtomicUsize,
    confirmed: Mutex<Option<Arc<Confirmed>>>,
}

impl std::fmt::Debug for FakeSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeSource")
            .field("tables", &self.mappings.len())
            .finish_non_exhaustive()
    }
}

impl FakeSource {
    /// A source over `mappings`, given as the `[[mappings]]` of a
    /// configuration, whose catalogue holds `keys`.
    pub fn new(mappings: &str, keys: Vec<ForeignKey>) -> Arc<Self> {
        let parsed: Mappings = toml::from_str(mappings).expect("valid mappings");
        let (feed, stream) = mpsc::unbounded_channel();
        Arc::new(Self {
            mappings: parsed.mappings,
            keys,
            rows: Mutex::new(BTreeMap::new()),
            feed: Mutex::new(Some(feed)),
            stream: tokio::sync::Mutex::new(Some(stream)),
            next: AtomicU64::new(1),
            ending: Mutex::new(Ending::Quietly),
            down: AtomicBool::new(false),
            greetings: AtomicUsize::new(1),
            confirmed: Mutex::new(None),
        })
    }

    fn mapping(&self, table: &str) -> &Mapping {
        self.mappings
            .iter()
            .find(|mapping| mapping.table == table)
            .unwrap_or_else(|| panic!("{table} is not mapped"))
    }

    /// Puts a row in `table`, in place of one with the same id, without
    /// committing anything, and gives the resource that reconciles it: its
    /// own, or its stack.
    pub fn upsert(&self, table: &str, json: serde_json::Value) -> ResourceId {
        let mapping = self.mapping(table);
        let row = Row::from_json(mapping, json.clone()).expect("a row that names its resource");
        let unit = row.unit().expect("a row that names its unit");
        let key = text(&json, &mapping.id_column);
        self.rows
            .lock()
            .unwrap()
            .entry(table.to_owned())
            .or_default()
            .insert(key, json);
        unit
    }

    /// Takes the row with id `key` out of `table`, without committing
    /// anything, and gives the resource that reconciles it.
    pub fn delete(&self, table: &str, key: &str) -> ResourceId {
        let mapping = self.mapping(table);
        let json = self
            .rows
            .lock()
            .unwrap()
            .get_mut(table)
            .and_then(|rows| rows.remove(key))
            .unwrap_or_else(|| panic!("{table} has no row {key}"));
        Row::from_json(mapping, json)
            .expect("a row that names its resource")
            .unit()
            .expect("a row that names its unit")
    }

    /// Sends a committed transaction that touched `touched`, and gives its
    /// id, which is also its position.
    pub fn commit(&self, touched: &[ResourceId]) -> u64 {
        let id = self.next.fetch_add(1, Ordering::SeqCst);
        let transaction = Committed {
            position: Position::from_u64(id),
            id,
            touched: touched.iter().cloned().collect(),
            changes: touched.len(),
        };
        self.feed
            .lock()
            .unwrap()
            .as_ref()
            .expect("the stream is open")
            .send(transaction)
            .expect("the stream is being watched");
        id
    }

    /// Puts a row and commits the change, giving the resource it marks.
    pub fn change(&self, table: &str, json: serde_json::Value) -> ResourceId {
        let unit = self.upsert(table, json);
        self.commit(std::slice::from_ref(&unit));
        unit
    }

    /// Takes a row out and commits the change, giving the resource it
    /// marks.
    pub fn remove(&self, table: &str, key: &str) -> ResourceId {
        let unit = self.delete(table, key);
        self.commit(std::slice::from_ref(&unit));
        unit
    }

    /// Ends the stream, which `watch` then leaves as `ending` says.
    pub fn close(&self, ending: Ending) {
        *self.ending.lock().unwrap() = ending;
        self.feed.lock().unwrap().take();
    }

    /// Whether reading rows fails from now on.
    pub fn set_down(&self, down: bool) {
        self.down.store(down, Ordering::SeqCst);
    }

    /// How many `Connected` events `watch` sends at once when it starts.
    pub fn greet_with(&self, count: usize) {
        self.greetings.store(count, Ordering::SeqCst);
    }

    /// The position the controller has made safe to confirm.
    pub fn confirmed_position(&self) -> Position {
        self.confirmed
            .lock()
            .unwrap()
            .as_ref()
            .expect("the source has been watched")
            .get()
    }

    fn check_up(&self) -> Result<()> {
        if self.down.load(Ordering::SeqCst) {
            return Err(Error::Connection("the fake source is down".into()));
        }
        Ok(())
    }
}

fn text(json: &serde_json::Value, column: &str) -> String {
    match json.get(column) {
        Some(serde_json::Value::String(text)) => text.clone(),
        Some(other) => other.to_string(),
        None => panic!("the row has no {column}"),
    }
}

#[async_trait]
impl Source for FakeSource {
    fn name(&self) -> &'static str {
        "fake"
    }

    fn mappings(&self) -> &[Mapping] {
        &self.mappings
    }

    async fn confirmed(&self) -> Result<Position> {
        Ok(Position::START)
    }

    async fn references(&self) -> Result<References> {
        References::assemble(&self.mappings, &self.keys)
    }

    async fn watch(
        &self,
        outbox: mpsc::Sender<Transaction>,
        confirmed: Arc<Confirmed>,
        metrics: Arc<Metrics>,
        events: broadcast::Sender<SourceEvent>,
        shutdown: CancellationToken,
    ) -> Result<()> {
        *self.confirmed.lock().unwrap() = Some(Arc::clone(&confirmed));
        for _ in 0..self.greetings.load(Ordering::SeqCst) {
            let _ = events.send(SourceEvent::Connected);
        }
        Metrics::bump(&metrics.connected);
        let mut stream = self.stream.lock().await.take().expect("watched once");
        loop {
            let next = tokio::select! {
                next = stream.recv() => next,
                () = shutdown.cancelled() => return Ok(()),
            };
            match next {
                Some(transaction) => {
                    Metrics::bump(&metrics.transactions);
                    Metrics::set(&metrics.received_position, transaction.id);
                    let _ = events.send(SourceEvent::Committed {
                        id: transaction.id,
                        resources: transaction.touched.len(),
                    });
                    if outbox.send(transaction).await.is_err() {
                        return Ok(());
                    }
                }
                None => {
                    let ending = *self.ending.lock().unwrap();
                    return match ending {
                        Ending::Quietly => Ok(()),
                        Ending::Failing => {
                            Err(Error::Connection("the fake source's log is gone".into()))
                        }
                        Ending::Panicking => panic!("the fake source panicked"),
                    };
                }
            }
        }
    }

    async fn row(&self, resource: &ResourceId) -> Result<Option<Row>> {
        self.check_up()?;
        let rows = self.rows.lock().unwrap();
        for mapping in &self.mappings {
            let fits = match &mapping.resource_type {
                Some(kind) => kind == resource.kind().as_str(),
                None => mapping.type_column.is_some(),
            };
            if !fits {
                continue;
            }
            if let Some(json) = rows
                .get(&mapping.table)
                .and_then(|rows| rows.get(resource.name()))
            {
                let row = Row::from_json(mapping, json.clone())?;
                if row.resource == *resource {
                    return Ok(Some(row));
                }
            }
        }
        Ok(None)
    }

    async fn row_in(&self, mapping: &Mapping, name: &str) -> Result<Option<Row>> {
        self.check_up()?;
        let rows = self.rows.lock().unwrap();
        rows.get(&mapping.table)
            .and_then(|rows| rows.get(name))
            .map(|json| Row::from_json(mapping, json.clone()))
            .transpose()
    }

    async fn group_rows(&self, group: &str) -> Result<Vec<Row>> {
        self.check_up()?;
        let stack = ResourceId::new(STACK_KIND, group)
            .map_err(|error| Error::Mapping(error.to_string()))?;
        let rows = self.rows.lock().unwrap();
        let mut found = Vec::new();
        for mapping in &self.mappings {
            for json in rows
                .get(&mapping.table)
                .into_iter()
                .flat_map(|rows| rows.values())
            {
                let row = Row::from_json(mapping, json.clone())?;
                if row.unit()? == stack {
                    found.push(row);
                }
            }
        }
        found.sort_by(|a, b| a.resource.cmp(&b.resource));
        Ok(found)
    }

    async fn all_resources(&self) -> Result<Vec<ResourceId>> {
        self.check_up()?;
        let rows = self.rows.lock().unwrap();
        let mut found = Vec::new();
        for mapping in &self.mappings {
            for json in rows
                .get(&mapping.table)
                .into_iter()
                .flat_map(|rows| rows.values())
            {
                found.push(Row::from_json(mapping, json.clone())?.unit()?);
            }
        }
        found.sort();
        found.dedup();
        Ok(found)
    }
}

/// A dirty store in memory that can be told to fail.
#[derive(Debug, Default)]
pub struct MemoryDirty {
    entries: Mutex<BTreeMap<ResourceId, (u32, Option<String>)>>,
    marks_to_fail: AtomicUsize,
    updates_fail: AtomicBool,
    mark_calls: AtomicUsize,
}

impl MemoryDirty {
    /// A store that already holds `resources`, each with a count of failed
    /// attempts, as one left by a controller that stopped.
    pub fn holding(resources: &[(&str, u32)]) -> Arc<Self> {
        let store = Self::default();
        for (resource, attempts) in resources {
            store
                .entries
                .lock()
                .unwrap()
                .insert(id(resource), (*attempts, None));
        }
        Arc::new(store)
    }

    /// Makes the next `count` calls to `mark` fail. `usize::MAX` is for
    /// ever.
    pub fn fail_next_marks(&self, count: usize) {
        self.marks_to_fail.store(count, Ordering::SeqCst);
    }

    /// Whether `done`, `note` and `failed` fail from now on.
    pub fn fail_updates(&self, fail: bool) {
        self.updates_fail.store(fail, Ordering::SeqCst);
    }

    pub fn entries(&self) -> Vec<ResourceId> {
        self.entries.lock().unwrap().keys().cloned().collect()
    }

    pub fn attempts_of(&self, resource: &ResourceId) -> Option<u32> {
        self.entries
            .lock()
            .unwrap()
            .get(resource)
            .map(|entry| entry.0)
    }

    pub fn note_of(&self, resource: &ResourceId) -> Option<String> {
        self.entries
            .lock()
            .unwrap()
            .get(resource)
            .and_then(|entry| entry.1.clone())
    }

    pub fn mark_calls(&self) -> usize {
        self.mark_calls.load(Ordering::SeqCst)
    }

    fn down() -> Error {
        Error::Checkpoint("the dirty store is down".into())
    }

    fn check_updates(&self) -> Result<()> {
        if self.updates_fail.load(Ordering::SeqCst) {
            return Err(Self::down());
        }
        Ok(())
    }
}

#[async_trait]
impl DirtyStore for MemoryDirty {
    async fn prepare(&self) -> Result<()> {
        Ok(())
    }

    async fn mark(&self, resources: &[ResourceId]) -> Result<()> {
        self.mark_calls.fetch_add(1, Ordering::SeqCst);
        let remaining = self.marks_to_fail.load(Ordering::SeqCst);
        if remaining > 0 {
            if remaining != usize::MAX {
                self.marks_to_fail.store(remaining - 1, Ordering::SeqCst);
            }
            return Err(Self::down());
        }
        let mut entries = self.entries.lock().unwrap();
        for resource in resources {
            entries.entry(resource.clone()).or_insert((0, None));
        }
        Ok(())
    }

    async fn load(&self) -> Result<Vec<DirtyRow>> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|(resource, (attempts, _))| DirtyRow {
                resource: resource.clone(),
                attempts: *attempts,
            })
            .collect())
    }

    async fn done(&self, resource: &ResourceId) -> Result<()> {
        self.check_updates()?;
        self.entries.lock().unwrap().remove(resource);
        Ok(())
    }

    async fn note(&self, resource: &ResourceId, text: &str) -> Result<()> {
        self.check_updates()?;
        if let Some(entry) = self.entries.lock().unwrap().get_mut(resource) {
            entry.1 = Some(text.to_owned());
        }
        Ok(())
    }

    async fn failed(&self, resource: &ResourceId, attempts: u32, reason: &str) -> Result<()> {
        self.check_updates()?;
        if let Some(entry) = self.entries.lock().unwrap().get_mut(resource) {
            *entry = (attempts, Some(reason.to_owned()));
        }
        Ok(())
    }
}

/// A store of references in memory that can be told to fail.
#[derive(Debug, Default)]
pub struct MemoryReferences {
    entries: Mutex<BTreeMap<ResourceId, Vec<Dependency>>>,
    stale_fails: AtomicBool,
}

impl MemoryReferences {
    /// Whether `stale_dependents` fails from now on.
    pub fn fail_stale(&self, fail: bool) {
        self.stale_fails.store(fail, Ordering::SeqCst);
    }

    /// What `dependent` is recorded as referring to.
    pub fn dependencies_of(&self, dependent: &ResourceId) -> Vec<Dependency> {
        self.entries
            .lock()
            .unwrap()
            .get(dependent)
            .cloned()
            .unwrap_or_default()
    }
}

#[async_trait]
impl ReferenceStore for MemoryReferences {
    async fn prepare(&self) -> Result<()> {
        Ok(())
    }

    async fn add(&self, dependent: &ResourceId, dependencies: &[Dependency]) -> Result<()> {
        if dependencies.is_empty() {
            return Ok(());
        }
        let mut entries = self.entries.lock().unwrap();
        let recorded = entries.entry(dependent.clone()).or_default();
        for dependency in dependencies {
            recorded.retain(|known| {
                !(known.member == dependency.member && known.attribute == dependency.attribute)
            });
            recorded.push(dependency.clone());
        }
        recorded.sort();
        Ok(())
    }

    async fn replace(&self, dependent: &ResourceId, dependencies: &[Dependency]) -> Result<()> {
        let mut entries = self.entries.lock().unwrap();
        if dependencies.is_empty() {
            entries.remove(dependent);
        } else {
            let mut recorded = dependencies.to_vec();
            recorded.sort();
            recorded.dedup();
            entries.insert(dependent.clone(), recorded);
        }
        Ok(())
    }

    async fn dependents_of(
        &self,
        unit: &ResourceId,
        kept: &[ResourceId],
    ) -> Result<Vec<ResourceId>> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, dependencies)| {
                dependencies.iter().any(|dependency| {
                    dependency.unit == *unit && !kept.contains(&dependency.member)
                })
            })
            .map(|(dependent, _)| dependent.clone())
            .collect())
    }

    async fn stale_dependents(
        &self,
        changed: &BTreeMap<String, Option<serde_json::Value>>,
    ) -> Result<Vec<ResourceId>> {
        if self.stale_fails.load(Ordering::SeqCst) {
            return Err(Error::Checkpoint("the store of references is down".into()));
        }
        let entries = self.entries.lock().unwrap();
        let mut found = BTreeSet::new();
        for (name, value) in changed {
            let Some((member, attribute)) = export_parts(name) else {
                continue;
            };
            let current = value.as_ref().map(ToString::to_string);
            for (dependent, dependencies) in entries.iter() {
                if dependencies.iter().any(|dependency| {
                    dependency.member == member
                        && dependency.attribute == attribute
                        && Some(&dependency.value) != current.as_ref()
                }) {
                    found.insert(dependent.clone());
                }
            }
        }
        Ok(found.into_iter().collect())
    }
}

/// Morph as the engine, with `cloud` as the only provider and the records
/// in memory.
pub fn engine(cloud: &MockCloud) -> Arc<Engine> {
    let registry = ProviderRegistry::new()
        .with(Arc::new(cloud.clone()))
        .expect("the mock registers");
    let stores = StateStores::new(&MorphConfig {
        state: StateConfig::Memory,
        ..MorphConfig::default()
    })
    .expect("memory stores");
    Arc::new(Engine::Morph(MorphEngine::new(
        stores,
        registry,
        ExecutorOptions::default(),
    )))
}

/// Settings with short waits, two workers, and a full reconciliation every
/// `full_interval`.
pub fn reconcile(full_interval: &str) -> ReconcileConfig {
    toml::from_str(&format!(
        "workers = 2\nqueue_size = 50\nfull_interval = \"{full_interval}\"\ntimeout = \"10s\"\n\
         [retry]\nmax_attempts = 3\ninitial = \"20ms\"\nmax = \"100ms\"\nmultiplier = 2.0"
    ))
    .expect("valid settings")
}

/// A short wait between tries at writing the dirty store.
pub fn write_retry() -> Backoff {
    toml::from_str("initial = \"10ms\"\nmax = \"50ms\"\nmultiplier = 2.0").expect("valid backoff")
}

/// The parts of a controller over `source`, with Morph over `cloud` as the
/// engine, and the metrics line an hour away.
pub fn parts(
    source: &Arc<FakeSource>,
    checkpoint: Checkpoint,
    references: &Arc<MemoryReferences>,
    cloud: &MockCloud,
    full_interval: &str,
) -> Parts {
    Parts {
        source: Arc::clone(source) as Arc<dyn Source>,
        engine: engine(cloud),
        checkpoint,
        references: Arc::clone(references) as Arc<dyn ReferenceStore>,
        reconcile: reconcile(full_interval),
        write_retry: write_retry(),
        metrics_interval: Duration::from_secs(3600),
        mock: Some(cloud.clone()),
    }
}

/// The attribute `name` of the cloud's object called `called`, as text of
/// its value.
pub fn attribute(cloud: &MockCloud, called: &str, name: &str) -> Option<String> {
    cloud.objects().iter().find_map(|object| {
        match (object.attributes.get("name"), object.attributes.get(name)) {
            (Some(Value::String(found)), Some(value)) if found == called => Some(value.to_string()),
            _ => None,
        }
    })
}

/// Everything written to the log while a [`Log::install`] guard lives.
///
/// The guard sets the subscriber for the current thread, which under the
/// current-thread runtime of `tokio::test` is where every task runs.
#[derive(Clone, Default)]
pub struct Log(Arc<Mutex<Vec<u8>>>);

impl Log {
    /// Starts capturing everything at every level, until the guard is
    /// dropped.
    pub fn install(&self) -> tracing::subscriber::DefaultGuard {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .with_writer(self.clone())
            .finish();
        tracing::subscriber::set_default(subscriber)
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }

    pub fn contains(&self, needle: &str) -> bool {
        self.text().contains(needle)
    }

    pub fn count(&self, needle: &str) -> usize {
        self.text().matches(needle).count()
    }

    /// Waits until the log says `needle`.
    pub async fn wait_for(&self, needle: &str) {
        let waited = tokio::time::timeout(PATIENCE, async {
            while !self.contains(needle) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            waited.is_ok(),
            "the log never said {needle:?}:\n{}",
            self.text()
        );
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

/// The controller's events, kept until they are asked for, so that waiting
/// for one never loses another.
pub struct Events {
    receiver: broadcast::Receiver<Event>,
    kept: VecDeque<Event>,
}

impl Events {
    pub fn of(controller: &Controller) -> Self {
        Self {
            receiver: controller.events(),
            kept: VecDeque::new(),
        }
    }

    /// Waits for an event that `wanted` accepts, keeping the others.
    pub async fn wait_for(&mut self, wanted: impl Fn(&Event) -> bool) -> Event {
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

    /// How many of the events kept so far `wanted` accepts.
    pub fn kept(&self, wanted: impl Fn(&Event) -> bool) -> usize {
        self.kept.iter().filter(|event| wanted(event)).count()
    }

    /// Waits for the stream to be connected.
    pub async fn connected(&mut self) {
        self.wait_for(|event| matches!(event, Event::Connected))
            .await;
    }

    /// Waits for the next transaction, and gives its id and how many
    /// resources it touched.
    pub async fn transaction(&mut self) -> (u64, usize) {
        match self
            .wait_for(|event| matches!(event, Event::Transaction { .. }))
            .await
        {
            Event::Transaction { id, resources } => (id, resources),
            _ => unreachable!(),
        }
    }

    /// Waits for the end of a turn at `resource`.
    pub async fn reconciled(&mut self, resource: &str) -> (bool, bool, Origin, u32, Disposition) {
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

    /// Waits for `resource` to be parked waiting for others, and gives what
    /// it waits for and why.
    pub async fn waiting(&mut self, resource: &str) -> (Vec<ResourceId>, String) {
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
}
