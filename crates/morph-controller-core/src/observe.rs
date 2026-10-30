//! What the controller counts, for whoever is watching.
//!
//! One [`Metrics`] is shared by every task. The source, the intake, the
//! workers and the periodic loop each write the numbers that are theirs,
//! and the metrics logger writes them all to the log every
//! `logging.metrics_interval`. A program that embeds the controller reads
//! the same numbers through
//! [`Controller::metrics`](crate::app::Controller::metrics).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Counters and gauges, kept since the controller started.
///
/// Every field is an atomic, read and written with relaxed ordering. Each
/// number is independent of the others, so several read one after another
/// are not a picture of a single moment.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Whether the stream of changes is connected: one or nought.
    pub connected: AtomicU64,
    /// How many times the stream was connected again after its first
    /// connection.
    pub reconnects: AtomicU64,
    /// The latest position the source sent, as a number, for a source
    /// whose positions are numbers. PostgreSQL's is the log sequence
    /// number.
    pub received_position: AtomicU64,
    /// The position the source holds as confirmed, as its server last
    /// reported it, as a number in the same way.
    pub confirmed_position: AtomicU64,
    /// How far the source is behind that position, in the source's own
    /// unit: bytes of log for PostgreSQL. Nought until the first reading.
    pub lag: AtomicU64,
    /// Committed transactions read from the stream.
    pub transactions: AtomicU64,
    /// Inserts, updates and deletes read from the stream, whether or not
    /// their table is mapped.
    pub row_changes: AtomicU64,
    /// Row changes that should have named a resource and could not, and
    /// messages that could not be decoded.
    pub mapping_errors: AtomicU64,
    /// Marks made, counting those that were merged with another.
    pub marked: AtomicU64,
    /// Marks that were merged with another, because the resource was
    /// already waiting or in a turn.
    pub coalesced: AtomicU64,
    /// Reconciliations started. Every attempt counts.
    pub reconciliations_started: AtomicU64,
    /// Reconciliations that succeeded.
    pub reconciliations_succeeded: AtomicU64,
    /// Reconciliations that failed. Every failed attempt counts.
    pub reconciliations_failed: AtomicU64,
    /// Turns that ended waiting for other resources, because a row refers
    /// to a resource with no record yet, or a resource to be removed is
    /// still referred to.
    pub waits: AtomicU64,
    /// Marks made because what a resource exports changed, one for each
    /// resource that had taken the old value.
    pub propagated: AtomicU64,
    /// Time spent reconciling, in microseconds.
    pub reconciliation_micros: AtomicU64,
    /// Full reconciliations that found out what exists and began to mark
    /// it.
    pub full_runs: AtomicU64,
}

impl Metrics {
    /// Adds one to a counter.
    pub fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Adds `amount` to a counter.
    pub fn add(counter: &AtomicU64, amount: u64) {
        counter.fetch_add(amount, Ordering::Relaxed);
    }

    /// Sets a gauge to `value`.
    pub fn set(gauge: &AtomicU64, value: u64) {
        gauge.store(value, Ordering::Relaxed);
    }

    /// Adds `elapsed` to the time spent reconciling.
    pub fn took(&self, elapsed: Duration) {
        Self::add(
            &self.reconciliation_micros,
            u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
        );
    }

    /// Reads a counter or a gauge.
    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    /// Writes every number to the log as one `info` event, with the four
    /// that only the scheduler knows.
    ///
    /// `connected` is written as a boolean, the positions as plain numbers,
    /// and the time spent reconciling in whole seconds.
    pub fn log(&self, waiting: usize, running: usize, parked: usize, unhealthy: usize) {
        let get = Self::get;
        tracing::info!(
            connected = get(&self.connected) == 1,
            reconnects = get(&self.reconnects),
            received_position = get(&self.received_position),
            confirmed_position = get(&self.confirmed_position),
            lag = get(&self.lag),
            transactions = get(&self.transactions),
            row_changes = get(&self.row_changes),
            mapping_errors = get(&self.mapping_errors),
            marked = get(&self.marked),
            coalesced = get(&self.coalesced),
            started = get(&self.reconciliations_started),
            succeeded = get(&self.reconciliations_succeeded),
            failed = get(&self.reconciliations_failed),
            waits = get(&self.waits),
            propagated = get(&self.propagated),
            reconciling_seconds = get(&self.reconciliation_micros) / 1_000_000,
            full_runs = get(&self.full_runs),
            waiting,
            running,
            parked,
            unhealthy,
            "metrics"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_count_gauges_hold_and_time_adds_up() {
        let metrics = Metrics::default();
        Metrics::bump(&metrics.transactions);
        Metrics::bump(&metrics.transactions);
        Metrics::add(&metrics.row_changes, 5);
        Metrics::set(&metrics.lag, 42);
        Metrics::set(&metrics.lag, 7);
        metrics.took(Duration::from_millis(1500));
        metrics.took(Duration::from_micros(250));
        assert_eq!(Metrics::get(&metrics.transactions), 2);
        assert_eq!(Metrics::get(&metrics.row_changes), 5);
        assert_eq!(Metrics::get(&metrics.lag), 7);
        assert_eq!(Metrics::get(&metrics.reconciliation_micros), 1_500_250);
        assert_eq!(Metrics::get(&metrics.full_runs), 0);
        assert_eq!(Metrics::get(&metrics.waits), 0);
        metrics.log(3, 1, 0, 0);
    }
}
