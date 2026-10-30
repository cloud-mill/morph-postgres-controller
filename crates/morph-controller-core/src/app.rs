//! The controller, wired together and running.
//!
//! ```text
//! source --transactions--> intake --marks--> scheduler --turns--> workers --> engine
//!                            |                  ^
//!                            v                  |
//!                       dirty store        periodic loop
//!
//! source <--safe position-- ledger <-- the intake opens, the scheduler settles
//! ```
//!
//! The first line is the way in. A committed change names a resource, the
//! resource is marked, and a worker asks the engine to reconcile it. The
//! last line is the way back. Once a transaction has been dealt with, its
//! position is safe to confirm, and the source tells its server so.
//!
//! [`Controller::start`] builds the parts and spawns them as tasks, and
//! [`Controller::stop`] takes them down in order. The parts themselves are
//! in the other modules, and the source and the stores are given ready-made
//! in [`Parts`] by the front-end. This module has the loops that join them.
//!
//! # The tasks
//!
//! * The **source** watches its stream of changes, as [`Source::watch`]
//!   describes. It sends each committed transaction, reduced to the
//!   resources it touched, into a channel that holds 64. It ends when the
//!   shutdown token is cancelled, or when the intake has gone.
//! * The **relay** passes the source's events on as [`Event`]s. It ends
//!   when the source has gone.
//! * The **intake** has the other end of the channel. It records each
//!   transaction and marks its resources. It ends when the source has gone
//!   and the channel is empty, or when the token is cancelled while a write
//!   to the dirty store is waiting to be tried again.
//! * The **workers**, `reconcile.workers` of them, have a [`Reconciler`]
//!   each. A worker takes a turn from the scheduler, reconciles the
//!   resource, and finishes the turn. In the durable mode it also takes the
//!   resource out of the dirty store, or notes the failure or the wait in
//!   it. When what the resource exports has changed, it marks the resources
//!   that refer to it. It ends when the scheduler is closed.
//! * The **periodic loop** marks every resource that has a row or a record:
//!   at start, unless [`Options::full_run_at_start`] is off, and then every
//!   `reconcile.full_interval`.
//! * The **metrics logger** writes the counters to the log every
//!   `logging.metrics_interval`.
//!
//! The last two end when the token is cancelled. The tasks share the
//! scheduler, the ledger, the metrics, the source and the stores. None of
//! them should end while the controller runs, and [`Controller::failed`]
//! waits for the first that does.
//!
//! # Confirming positions
//!
//! The source is told that a transaction has been dealt with by confirming
//! the position that follows its commit. The [`Ledger`] keeps the position
//! that is safe to confirm, starting from where the source stood, and the
//! source reads it through a shared [`Confirmed`](crate::source::Confirmed)
//! whenever it reports.
//!
//! * In the durable mode the intake writes the transaction's resources to
//!   the dirty store, and the position is safe as soon as that write has
//!   succeeded. A write that fails is tried again, paced by
//!   [`Parts::write_retry`], until it succeeds or the controller is told
//!   to stop. Nothing else is taken in meanwhile, so no position is
//!   confirmed, and no resource is marked, for a transaction that is not in
//!   the store.
//! * In the memory mode the transaction is opened in the ledger with one
//!   piece of work outstanding for each of its resources. The scheduler
//!   settles a piece when the turn that covers it succeeds, or when the
//!   resource is given up on. The position is safe once the transaction and
//!   every one before it have nothing outstanding.
//!
//! The ledger is behind a standard mutex. It is locked for the length of
//! one call and never across an `.await`.
//!
//! # Backpressure
//!
//! The scheduler's set is bounded. When it is full the intake waits in
//! [`Scheduler::mark`], the channel from the source fills, and the source
//! waits to send and takes no more from the stream. Nothing is dropped, and
//! the log is read no faster than resources are reconciled. The periodic
//! loop waits for room in the same way.
//!
//! # Shutdown
//!
//! [`Controller::stop`] cancels the shutdown token, closes the scheduler,
//! and waits for the tasks. The token stops the source, the periodic loop
//! and the metrics logger. The relay and the intake follow the source. The
//! workers do not look at the token: a closed scheduler hands out no more
//! turns, so each worker finishes the turn it has and ends. A task that is
//! still running when the grace period is over is aborted.

use std::sync::Arc;
use std::time::Duration;

use morph::ResourceId;
use morph_mock::MockCloud;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::checkpoint::{DirtyStore, Ledger};
use crate::config::{Backoff, CheckpointMode, ReconcileConfig};
use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::observe::Metrics;
use crate::references::ReferenceStore;
use crate::scheduler::{Disposition, Origin, Outcome, Scheduler};
use crate::source::{Source, SourceEvent, Transaction};
use crate::worker::{Reconciler, Turn};

/// Something that happened, for whoever is watching.
///
/// Events are sent whether or not anyone listens. [`Controller::events`]
/// subscribes to them.
#[derive(Clone, Debug)]
pub enum Event {
    /// The stream of changes is connected. Sent at every connection, the
    /// first included.
    Connected,
    /// A connected stream was lost. The source connects again by itself.
    Disconnected {
        /// Why.
        reason: String,
    },
    /// The source read the commit of a transaction.
    ///
    /// This is sent before the intake has the transaction, so nothing has
    /// been recorded or marked for it yet.
    Transaction {
        /// Its id, as the source numbers them.
        id: u64,
        /// How many resources it touched.
        resources: usize,
    },
    /// A turn at a resource ended, and the scheduler has been told.
    Reconciled {
        /// The resource.
        resource: ResourceId,
        /// Why it was reconciled.
        origin: Origin,
        /// Which attempt it was, starting at one.
        attempt: u32,
        /// Whether it succeeded.
        ok: bool,
        /// Whether the engine changed anything. Always `false` for a
        /// reconciliation that failed, whatever it did before it failed.
        changed: bool,
        /// What became of the resource.
        disposition: Disposition,
    },
    /// A turn at a resource ended with the resource waiting for others,
    /// and the scheduler has parked it.
    Waiting {
        /// The resource.
        resource: ResourceId,
        /// What it waits for.
        on: Vec<ResourceId>,
        /// Why.
        reason: String,
    },
    /// A full reconciliation found out what exists, and is about to mark
    /// it.
    FullRun {
        /// How many resources it will mark.
        resources: usize,
    },
}

/// Where the resources still to be reconciled are kept, as the
/// configuration's `checkpoint.mode` chose.
#[derive(Clone, Debug)]
pub enum Checkpoint {
    /// In a store the front-end keeps: [`CheckpointMode::Durable`].
    Durable(Arc<dyn DirtyStore>),
    /// Nowhere but the ledger: [`CheckpointMode::Memory`].
    Memory,
}

impl Checkpoint {
    /// The mode this is.
    pub fn mode(&self) -> CheckpointMode {
        match self {
            Checkpoint::Durable(_) => CheckpointMode::Durable,
            Checkpoint::Memory => CheckpointMode::Memory,
        }
    }

    /// The dirty store, in the durable mode.
    fn store(&self) -> Option<&Arc<dyn DirtyStore>> {
        match self {
            Checkpoint::Durable(store) => Some(store),
            Checkpoint::Memory => None,
        }
    }
}

/// What a controller is made of, as a front-end puts it together.
///
/// The front-end reads its configuration, connects its source, chooses the
/// engine and makes the stores. The core takes it from there.
pub struct Parts {
    /// Where rows and their changes come from.
    pub source: Arc<dyn Source>,
    /// What reconciles a resource.
    pub engine: Arc<Engine>,
    /// Where the resources still to be reconciled are kept.
    pub checkpoint: Checkpoint,
    /// Where which resource refers to which is kept.
    pub references: Arc<dyn ReferenceStore>,
    /// How reconciliations are run and retried.
    pub reconcile: ReconcileConfig,
    /// How tries at writing the dirty store are paced when a write fails.
    pub write_retry: Backoff,
    /// How often the counters are written to the log.
    pub metrics_interval: Duration,
    /// The in-memory cloud among the providers, if there is one, so that
    /// tests can look at what it holds.
    pub mock: Option<MockCloud>,
}

impl std::fmt::Debug for Parts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Parts")
            .field("source", &self.source)
            .field("engine", &self.engine)
            .field("checkpoint", &self.checkpoint)
            .finish_non_exhaustive()
    }
}

/// A running controller: its tasks, and the means to watch and stop them.
///
/// [`start`](Self::start) makes one, and [`stop`](Self::stop) shuts it
/// down in order. A controller that is dropped instead has its tasks
/// aborted where they stand.
pub struct Controller {
    shutdown: CancellationToken,
    tasks: JoinSet<Result<()>>,
    events: broadcast::Sender<Event>,
    metrics: Arc<Metrics>,
    scheduler: Arc<Scheduler>,
    mock: Option<MockCloud>,
}

impl std::fmt::Debug for Controller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Controller").finish_non_exhaustive()
    }
}

/// How the controller is started, beyond what the parts say.
///
/// The defaults are what a service wants. The fields are there for tests.
#[derive(Clone, Debug)]
pub struct Options {
    /// Whether everything is reconciled once at start, as well as every
    /// `reconcile.full_interval`. `true` by default.
    pub full_run_at_start: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            full_run_at_start: true,
        }
    }
}

impl Controller {
    /// Starts the controller from its parts.
    ///
    /// The source is asked for the references between the mapped tables
    /// and for where it stands, the store of references is made if it is
    /// missing, and in the durable mode the dirty store is made if it is
    /// missing and every resource in it is marked. Last, the tasks are
    /// spawned.
    ///
    /// The tasks are running when this returns. The stream may not be
    /// connected yet: [`Event::Connected`] says when it is, to whoever has
    /// subscribed by then.
    ///
    /// # Errors
    ///
    /// * Whatever [`Source::references`] or [`Source::confirmed`] fails
    ///   with.
    /// * [`Error::Checkpoint`] if the dirty store or the store of
    ///   references cannot be made or read.
    pub async fn start(parts: Parts, options: Options) -> Result<Self> {
        let Parts {
            source,
            engine,
            checkpoint,
            references: reference_store,
            reconcile,
            write_retry,
            metrics_interval,
            mock,
        } = parts;
        let references = Arc::new(source.references().await?);
        for reference in references.iter() {
            tracing::info!(%reference, "a reference between rows");
        }
        reference_store.prepare().await?;

        let metrics = Arc::new(Metrics::default());
        let (events, _) = broadcast::channel(1024);
        let shutdown = CancellationToken::new();
        let mut tasks = JoinSet::new();

        // Where the source stands is where confirmation starts from, so
        // that it is never told less than it already knows.
        let start = source.confirmed().await?;
        tracing::info!(position = %start, "confirming from where the source stands");
        let ledger = Arc::new(std::sync::Mutex::new(Ledger::new(start)));
        let confirmed = ledger.lock().expect("fresh").watch();

        let scheduler = Arc::new(Scheduler::new(
            reconcile.queue_size,
            reconcile.workers,
            reconcile.retry.clone(),
            {
                let ledger = Arc::clone(&ledger);
                move |number| {
                    if let Ok(mut ledger) = ledger.lock() {
                        ledger.settle(number);
                    }
                }
            },
        ));

        let reconciler = Reconciler::new(
            Arc::clone(&source),
            Arc::clone(&engine),
            reconcile.timeout,
            references,
            Arc::clone(&reference_store),
        );

        // What was waiting when the controller last stopped is taken up
        // first: it is marked before any task runs, so it waits ahead of
        // everything that the stream or the periodic loop marks.
        if let Some(store) = checkpoint.store() {
            store.prepare().await?;
            let recovered = store.load().await?;
            if !recovered.is_empty() {
                tracing::info!(
                    resources = recovered.len(),
                    "taking up what was waiting when the controller last stopped"
                );
            }
            for row in recovered {
                scheduler.mark(row.resource, None, Origin::Recovered).await;
                Metrics::bump(&metrics.marked);
            }
        }

        // The source, and the channel that carries its transactions to the
        // intake. The channel is bounded, so an intake that waits holds the
        // source back. The relay ends when the source lets go of its end of
        // `source_events`.
        let (outbox, inbox) = mpsc::channel::<Transaction>(64);
        let (source_events, _) = broadcast::channel::<SourceEvent>(64);
        tasks.spawn({
            let source = Arc::clone(&source);
            let confirmed = Arc::clone(&confirmed);
            let metrics = Arc::clone(&metrics);
            let source_events = source_events.clone();
            let shutdown = shutdown.clone();
            async move {
                source
                    .watch(outbox, confirmed, metrics, source_events, shutdown)
                    .await
            }
        });
        tasks.spawn(relay(source_events.subscribe(), events.clone()));

        tasks.spawn(intake(
            inbox,
            Arc::clone(&scheduler),
            Arc::clone(&ledger),
            checkpoint.clone(),
            write_retry,
            Arc::clone(&metrics),
            shutdown.clone(),
        ));

        // The workers. They are given no shutdown token: closing the
        // scheduler is what ends them, after the turn each has in hand.
        for _ in 0..reconcile.workers {
            tasks.spawn(work(
                Arc::clone(&scheduler),
                reconciler.clone(),
                checkpoint.clone(),
                Arc::clone(&reference_store),
                Arc::clone(&metrics),
                events.clone(),
            ));
        }

        // The safety path, and the watcher.
        tasks.spawn(periodic(
            Arc::clone(&scheduler),
            Arc::clone(&source),
            Arc::clone(&engine),
            reconcile.full_interval,
            options.full_run_at_start,
            Arc::clone(&metrics),
            events.clone(),
            shutdown.clone(),
        ));
        tasks.spawn(log_metrics(
            Arc::clone(&metrics),
            Arc::clone(&scheduler),
            metrics_interval,
            shutdown.clone(),
        ));

        tracing::info!(
            source = source.name(),
            engine = engine.name(),
            workers = reconcile.workers,
            checkpoint = ?checkpoint.mode(),
            "the controller is running"
        );
        Ok(Self {
            shutdown,
            tasks,
            events,
            metrics,
            scheduler,
            mock,
        })
    }

    /// Subscribes to what happens, as it happens.
    ///
    /// A receiver gets the events sent after it subscribed. The channel
    /// keeps 1024 of them. A receiver that falls further behind loses the
    /// oldest, and is told so by a `Lagged` error.
    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    /// The counters, shared with the running tasks.
    pub fn metrics(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    /// The in-memory cloud among the providers, if there is one.
    pub fn mock(&self) -> Option<&MockCloud> {
        self.mock.as_ref()
    }

    /// The resources that were given up on and have not been reconciled
    /// since.
    pub async fn unhealthy(&self) -> Vec<ResourceId> {
        self.scheduler.unhealthy().await
    }

    /// The resources that wait for others to be reconciled, each with what
    /// it waits for.
    pub async fn parked(&self) -> Vec<(ResourceId, Vec<ResourceId>)> {
        self.scheduler.parked_on().await
    }

    /// Stops reading changes, lets the reconciliations in flight finish,
    /// and returns once every task has ended or been aborted.
    ///
    /// In this order:
    ///
    /// 1. The shutdown token is cancelled. The source stops reading, hands
    ///    its server the position that is safe to confirm at that moment,
    ///    and closes its connection. The intake takes in what the source
    ///    had already passed on, and ends. The periodic loop and the
    ///    metrics logger end.
    /// 2. The scheduler is closed. No further turn is handed out, and each
    ///    worker ends once the turn it has is finished.
    /// 3. The tasks are waited for, for `grace` at the longest. One that
    ///    failed or panicked is logged.
    /// 4. If `grace` runs out, the tasks still running are aborted. A
    ///    reconciliation among them is abandoned where it stands.
    /// 5. The counters are written to the log a last time.
    ///
    /// Resources that are still waiting are not reconciled. In the durable
    /// mode the next start takes up whatever the dirty store holds. In the
    /// memory mode the transactions that marked them were never confirmed,
    /// and the source sends those again. A resource that only the periodic
    /// loop marked is in neither, and is marked again by the next full
    /// reconciliation.
    pub async fn stop(mut self, grace: Duration) {
        tracing::info!("stopping");
        self.shutdown.cancel();
        self.scheduler.close().await;
        let drained = tokio::time::timeout(grace, async {
            while let Some(finished) = self.tasks.join_next().await {
                match finished {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => tracing::error!(%error, "a part of the controller failed"),
                    Err(error) => tracing::error!(%error, "a part of the controller panicked"),
                }
            }
        })
        .await;
        if drained.is_err() {
            tracing::warn!(grace = ?grace, "some work did not finish in time and was abandoned");
            self.tasks.abort_all();
        }
        self.metrics.log(
            self.scheduler.waiting().await,
            self.scheduler.running().await,
            self.scheduler.parked().await,
            self.scheduler.unhealthy().await.len(),
        );
        tracing::info!("stopped");
    }

    /// Waits until a part of the controller that should run for ever
    /// stops on its own, which is a failure.
    ///
    /// This waits for the first task to end, whichever it is, and returns:
    ///
    /// * the task's own error, if it ended with one;
    /// * [`Error::Connection`], saying so, if it ended without an error,
    ///   if it panicked, or if no task is left.
    ///
    /// A task that has ended is no longer among those that
    /// [`stop`](Self::stop) waits for.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. A future that is dropped has taken no
    /// task's ending with it, and a later call still sees it.
    pub async fn failed(&mut self) -> Error {
        loop {
            match self.tasks.join_next().await {
                Some(Ok(Ok(()))) if self.shutdown.is_cancelled() => {}
                Some(Ok(Ok(()))) => {
                    return Error::Connection("a part of the controller stopped".into());
                }
                Some(Ok(Err(error))) => return error,
                Some(Err(error)) => {
                    return Error::Connection(format!(
                        "a part of the controller panicked: {error}"
                    ));
                }
                None => {
                    return Error::Connection("every part of the controller has stopped".into());
                }
            }
        }
    }
}

/// Passes the source's events on as [`Event`]s, until the source has gone.
///
/// Events that the relay falls too far behind to receive are lost.
async fn relay(
    mut from: broadcast::Receiver<SourceEvent>,
    to: broadcast::Sender<Event>,
) -> Result<()> {
    loop {
        match from.recv().await {
            Ok(SourceEvent::Connected) => {
                let _ = to.send(Event::Connected);
            }
            Ok(SourceEvent::Disconnected { reason }) => {
                let _ = to.send(Event::Disconnected { reason });
            }
            Ok(SourceEvent::Committed { id, resources }) => {
                let _ = to.send(Event::Transaction { id, resources });
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {}
            Err(broadcast::error::RecvError::Closed) => return Ok(()),
        }
    }
}

/// Takes committed transactions in: records their resources, and marks them
/// for reconciliation.
///
/// Transactions are taken one at a time, in the order they committed. The
/// next is not taken until every resource of this one is marked, so a full
/// scheduler holds the intake, and behind it the source.
///
/// In the durable mode the resources are written to the dirty store first.
/// A write that fails is tried again after a wait from `backoff`, without
/// end. If the controller is stopped during such a wait the intake returns,
/// with the transaction neither recorded nor confirmed, so the source sends
/// it again. Once the write has succeeded the transaction is opened in the
/// ledger with nothing outstanding, which makes its position safe, and its
/// resources are marked.
///
/// In the memory mode the transaction is opened in the ledger with one
/// piece outstanding for each resource, and each mark carries the number
/// the ledger gave, for the scheduler to settle.
///
/// The ledger's lock is let go before anything is awaited.
async fn intake(
    mut inbox: mpsc::Receiver<Transaction>,
    scheduler: Arc<Scheduler>,
    ledger: Arc<std::sync::Mutex<Ledger>>,
    checkpoint: Checkpoint,
    backoff: Backoff,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> Result<()> {
    while let Some(transaction) = inbox.recv().await {
        let resources: Vec<ResourceId> = transaction.touched.iter().cloned().collect();
        match &checkpoint {
            Checkpoint::Durable(store) => {
                // Once the resources are in the store, the transaction is
                // dealt with as far as the source is concerned: a restart
                // reads the store.
                let mut failures = 0;
                loop {
                    match store.mark(&resources).await {
                        Ok(()) => break,
                        Err(error) => {
                            failures += 1;
                            let wait = backoff.delay(failures);
                            tracing::warn!(%error, wait = ?wait, "the dirty store could not be written; trying again");
                            tokio::select! {
                                () = tokio::time::sleep(wait) => {}
                                () = shutdown.cancelled() => return Ok(()),
                            }
                        }
                    }
                }
                if let Ok(mut ledger) = ledger.lock() {
                    ledger.open(transaction.position, 0);
                }
                for resource in resources {
                    note_mark(
                        &metrics,
                        scheduler.mark(resource, None, Origin::Change).await,
                    );
                }
            }
            Checkpoint::Memory => {
                let number = ledger
                    .lock()
                    .map(|mut ledger| ledger.open(transaction.position, resources.len()))
                    .unwrap_or_default();
                for resource in resources {
                    note_mark(
                        &metrics,
                        scheduler.mark(resource, Some(number), Origin::Change).await,
                    );
                }
            }
        }
    }
    Ok(())
}

/// Counts a mark, and counts it again as coalesced if it was merged with
/// another.
fn note_mark(metrics: &Metrics, marked: crate::scheduler::Marked) {
    Metrics::bump(&metrics.marked);
    if marked == crate::scheduler::Marked::Coalesced {
        Metrics::bump(&metrics.coalesced);
    }
}

/// Reconciles whatever the scheduler hands out, until it is closed.
///
/// For each turn: take a lease, call the reconciler, record the outcome in
/// the dirty store in the durable mode, finish the lease, and send
/// [`Event::Reconciled`] or [`Event::Waiting`]. There is no shutdown token
/// here. A turn that has begun runs to its end, and the loop ends when the
/// scheduler is closed.
///
/// In the durable mode a turn that succeeds removes the resource from the
/// dirty store, also when the resource was marked again during the turn. A
/// turn that fails writes the attempt and the error to the entry, if there
/// is one: a resource that only the periodic loop marked has none. A turn
/// that waits notes what it waits for in the entry. If the store cannot be
/// updated, that is logged and the turn's outcome stands. An entry that
/// could not be removed is reconciled again after a restart.
///
/// A turn that succeeds and changes what the resource exports is followed
/// by a mark, under [`Origin::Propagated`], of every resource that the
/// store of references says took a value other than the new one. A
/// resource that took the new value already, as one reconciled while the
/// change was under way may have, is left alone. If the store cannot be
/// read, that is logged, and the next full reconciliation reaches them.
async fn work(
    scheduler: Arc<Scheduler>,
    reconciler: Reconciler,
    checkpoint: Checkpoint,
    references: Arc<dyn ReferenceStore>,
    metrics: Arc<Metrics>,
    events: broadcast::Sender<Event>,
) -> Result<()> {
    while let Some(lease) = scheduler.next().await {
        let resource = lease.resource.clone();
        let (origin, attempt) = (lease.origin, lease.attempt);
        Metrics::bump(&metrics.reconciliations_started);
        let started = tokio::time::Instant::now();
        let outcome = reconciler.reconcile(&resource).await;
        metrics.took(started.elapsed());
        let dirty = checkpoint.store();
        let (result, changed, reason, exported) = match outcome {
            Ok(Turn::Reconciled(reconciled)) => {
                Metrics::bump(&metrics.reconciliations_succeeded);
                tracing::info!(%resource, ?origin, attempt, changed = reconciled.changed, elapsed = ?reconciled.elapsed, summary = %reconciled.summary, "reconciled");
                if let Some(store) = dirty
                    && let Err(error) = store.done(&resource).await
                {
                    tracing::warn!(%resource, %error, "the dirty store could not be updated; the resource will be reconciled again after a restart");
                }
                (
                    Outcome::Succeeded,
                    reconciled.changed,
                    String::new(),
                    reconciled.exports.changed(),
                )
            }
            Ok(Turn::Waiting { on, reason }) => {
                Metrics::bump(&metrics.waits);
                let names: Vec<String> = on.iter().map(ToString::to_string).collect();
                tracing::info!(%resource, ?origin, attempt, on = %names.join(", "), %reason, "waiting for other resources");
                if let Some(store) = dirty
                    && let Err(error) = store
                        .note(
                            &resource,
                            &format!("waiting for {}: {reason}", names.join(", ")),
                        )
                        .await
                {
                    tracing::warn!(%resource, %error, "the dirty store could not be updated");
                }
                (Outcome::Waiting(on), false, reason, Default::default())
            }
            Err(error) => {
                Metrics::bump(&metrics.reconciliations_failed);
                if let Some(store) = dirty
                    && let Err(error) = store.failed(&resource, attempt, &error.to_string()).await
                {
                    tracing::warn!(%resource, %error, "the dirty store could not be updated");
                }
                (
                    Outcome::Failed(error.to_string()),
                    false,
                    String::new(),
                    Default::default(),
                )
            }
        };
        let failure = match &result {
            Outcome::Failed(reason) => Some(reason.clone()),
            Outcome::Succeeded | Outcome::Waiting(_) => None,
        };
        let waited_on = match &result {
            Outcome::Waiting(on) => Some(on.clone()),
            Outcome::Succeeded | Outcome::Failed(_) => None,
        };
        let disposition = scheduler.finish(lease, result).await;
        let mut failed = failure;
        if let (Some(on), Disposition::Retrying(_) | Disposition::Unhealthy) =
            (&waited_on, &disposition)
        {
            // The scheduler found that the wait would go round in a circle,
            // and counted the turn as failed.
            let names: Vec<String> = on.iter().map(ToString::to_string).collect();
            let circular = format!(
                "waits for {}, which waits for it, directly or through others",
                names.join(", ")
            );
            Metrics::bump(&metrics.reconciliations_failed);
            if let Some(store) = dirty
                && let Err(error) = store.failed(&resource, attempt, &circular).await
            {
                tracing::warn!(%resource, %error, "the dirty store could not be updated");
            }
            failed = Some(circular);
        }
        match (&failed, &disposition) {
            (Some(reason), Disposition::Unhealthy) => {
                tracing::error!(%resource, attempt, %reason, "given up on until the next full reconciliation");
            }
            (Some(reason), Disposition::Retrying(wait)) => {
                tracing::warn!(%resource, attempt, %reason, wait = ?wait, "reconciliation failed; trying again");
            }
            _ => {}
        }
        if !exported.is_empty() {
            match references.stale_dependents(&exported).await {
                Ok(dependents) => {
                    for dependent in dependents
                        .into_iter()
                        .filter(|dependent| *dependent != resource)
                    {
                        tracing::info!(%resource, %dependent, "what the resource exports changed; the resource that refers to it is marked");
                        Metrics::bump(&metrics.propagated);
                        note_mark(
                            &metrics,
                            scheduler.mark(dependent, None, Origin::Propagated).await,
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(%resource, %error, "the resources that refer to it could not be found; the next full reconciliation reaches them");
                }
            }
        }
        let _ = match (waited_on, &disposition) {
            (Some(on), Disposition::Waiting) => events.send(Event::Waiting {
                resource,
                on,
                reason,
            }),
            _ => events.send(Event::Reconciled {
                resource,
                origin,
                attempt,
                ok: failed.is_none(),
                changed,
                disposition,
            }),
        };
    }
    Ok(())
}

/// Marks everything, every so often: the safety path.
///
/// A pass lists the resources that a row describes and the resources that
/// have a record, and marks each of them once. The second list is what
/// finds a resource whose row has gone, and with it the deletes that the
/// stream could not name. If either list cannot be made, the pass is given
/// up, and the next is made after the usual interval.
///
/// The first pass is made at once if `at_start` is set. After that the loop
/// sleeps for `interval` between the end of one pass and the start of the
/// next. A mark waits for room in the scheduler like any other, and is
/// abandoned if the controller is stopped meanwhile, which is safe because
/// [`Scheduler::mark`] is cancel safe.
#[allow(clippy::too_many_arguments)]
async fn periodic(
    scheduler: Arc<Scheduler>,
    source: Arc<dyn Source>,
    engine: Arc<Engine>,
    interval: Duration,
    at_start: bool,
    metrics: Arc<Metrics>,
    events: broadcast::Sender<Event>,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut first = true;
    loop {
        if !(first && at_start) {
            tokio::select! {
                () = tokio::time::sleep(interval) => {}
                () = shutdown.cancelled() => return Ok(()),
            }
        }
        first = false;
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let described = source.all_resources().await;
        let recorded = engine.managed().await;
        let mut resources = match (described, recorded) {
            (Ok(described), Ok(recorded)) => [described, recorded].concat(),
            (Err(error), _) | (_, Err(error)) => {
                tracing::warn!(%error, "the full reconciliation could not find out what exists; it will try again");
                continue;
            }
        };
        resources.sort();
        resources.dedup();
        Metrics::bump(&metrics.full_runs);
        let _ = events.send(Event::FullRun {
            resources: resources.len(),
        });
        tracing::info!(resources = resources.len(), "full reconciliation");
        for resource in resources {
            tokio::select! {
                marked = scheduler.mark(resource, None, Origin::Periodic) => note_mark(&metrics, marked),
                () = shutdown.cancelled() => return Ok(()),
            }
        }
    }
}

/// Writes the counters to the log every `interval`, with how many resources
/// are waiting, in a turn, and unhealthy.
async fn log_metrics(
    metrics: Arc<Metrics>,
    scheduler: Arc<Scheduler>,
    interval: Duration,
    shutdown: CancellationToken,
) -> Result<()> {
    loop {
        tokio::select! {
            () = tokio::time::sleep(interval) => {}
            () = shutdown.cancelled() => return Ok(()),
        }
        metrics.log(
            scheduler.waiting().await,
            scheduler.running().await,
            scheduler.parked().await,
            scheduler.unhealthy().await.len(),
        );
    }
}
