//! PostgreSQL as the source of rows and changes.
//!
//! [`PostgresSource`] is the core's [`Source`] over one database: the
//! stream of changes comes from logical replication, read by
//! [`replication`], and rows are read back
//! over the ordinary connection by [`RowReader`]. Beside the stream, the
//! source watches how much log the slot holds back, and warns when it is
//! more than `replication.lag_warn_bytes`.
//!
//! Where the source stands as confirmed is the slot's `confirmed_flush_lsn`,
//! which is what the controller begins confirming from, so that the server
//! is never told less than it already knows.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use morph::ResourceId;
use morph_controller_core::config::Mapping;
use morph_controller_core::mapping::Row;
use morph_controller_core::observe::Metrics;
use morph_controller_core::references::References;
use morph_controller_core::source::{Confirmed, Position, Source, SourceEvent, Transaction};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::error::Result;
use crate::mapping::{ResourceMapper, TableMapper};
use crate::postgres::Lsn;
use crate::postgres::database::Database;
use crate::postgres::replication::{self, StreamSettings};
use crate::postgres::rows::RowReader;
use crate::references::discover;

/// One PostgreSQL database as the source.
///
/// Shared behind an [`Arc`] by every task of the controller.
#[derive(Debug)]
pub struct PostgresSource {
    database: Arc<Database>,
    rows: RowReader,
    mapper: Arc<TableMapper>,
    stream: StreamSettings,
    lag_check_interval: Duration,
    lag_warn_bytes: u64,
}

impl PostgresSource {
    /// A source over `database`, set up as `config` says.
    ///
    /// `acknowledge` is whether the server is told how far the controller
    /// has got, which only tests turn off. Nothing is connected yet: the
    /// stream is connected by [`watch`](Source::watch), and rows are read
    /// over `database` when they are asked for.
    pub fn new(config: &Config, database: Arc<Database>, acknowledge: bool) -> Self {
        Self {
            rows: RowReader::new(Arc::clone(&database), config.mappings.clone()),
            mapper: Arc::new(TableMapper::new(
                config.mappings.clone(),
                config.filters.clone(),
            )),
            stream: StreamSettings {
                url: config.postgres.url.clone(),
                ca_certificate: config.postgres.ca_certificate.clone(),
                slot: config.replication.slot.clone(),
                publication: config.replication.publication.clone(),
                status_interval: config.replication.status_interval,
                reconnect: config.replication.reconnect.clone(),
                acknowledge,
            },
            lag_check_interval: config.replication.lag_check_interval,
            lag_warn_bytes: config.replication.lag_warn_bytes,
            database,
        }
    }

    /// The ordinary connection the source reads rows over.
    pub fn database(&self) -> &Arc<Database> {
        &self.database
    }

    /// The reader of rows.
    pub fn rows(&self) -> &RowReader {
        &self.rows
    }
}

#[async_trait]
impl Source for PostgresSource {
    fn name(&self) -> &'static str {
        "postgres"
    }

    fn mappings(&self) -> &[Mapping] {
        self.rows.mappings()
    }

    /// The slot's `confirmed_flush_lsn`.
    ///
    /// # Errors
    ///
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the slot does
    /// not exist, [`Error::Connection`](crate::Error::Connection) if the
    /// database cannot be reached, or [`Error::Query`](crate::Error::Query)
    /// if it refuses the statement.
    async fn confirmed(&self) -> Result<Position> {
        let lag = self.database.slot_lag(&self.stream.slot).await?;
        Ok(lag.confirmed_flush_lsn.into())
    }

    /// The foreign keys of the mapped tables, read from the catalogue, and
    /// the mappings' own declarations, as [`discover`] assembles them.
    async fn references(&self) -> Result<References> {
        discover(&self.database, self.rows.mappings()).await
    }

    /// Reads the stream, as [`replication::run`] describes, and watches the
    /// slot's lag meanwhile. Ends when the reader ends.
    async fn watch(
        &self,
        outbox: mpsc::Sender<Transaction>,
        confirmed: Arc<Confirmed>,
        metrics: Arc<Metrics>,
        events: broadcast::Sender<SourceEvent>,
        shutdown: CancellationToken,
    ) -> Result<()> {
        let mapper: Arc<dyn ResourceMapper> = Arc::clone(&self.mapper) as Arc<dyn ResourceMapper>;
        let reader = replication::run(
            self.stream.clone(),
            mapper,
            outbox,
            Arc::clone(&confirmed),
            Arc::clone(&metrics),
            events,
            shutdown.clone(),
        );
        let lag = watch_lag(
            Arc::clone(&self.database),
            self.stream.slot.clone(),
            self.lag_check_interval,
            self.lag_warn_bytes,
            metrics,
            confirmed,
            shutdown,
        );
        // The lag watcher ends only when the controller stops. The reader
        // may end sooner, when whoever takes its transactions has gone, and
        // that ends the source, so that the controller notices.
        tokio::select! {
            ended = reader => ended,
            ended = lag => ended,
        }
    }

    async fn row(&self, resource: &ResourceId) -> Result<Option<Row>> {
        self.rows.row(resource).await
    }

    async fn row_in(&self, mapping: &Mapping, name: &str) -> Result<Option<Row>> {
        self.rows.row_in(mapping, name).await
    }

    async fn group_rows(&self, group: &str) -> Result<Vec<Row>> {
        self.rows.group_rows(group).await
    }

    async fn all_resources(&self) -> Result<Vec<ResourceId>> {
        self.rows.all_resources().await
    }
}

/// Keeps an eye on how much log the slot holds back.
///
/// Every `interval` the slot's lag is read, the `lag` and
/// `confirmed_position` gauges are set from it, and a warning is logged if
/// it is more than `warn_above` bytes. Nothing is done about the lag beyond
/// that. A reading that fails is logged, and the next is made as usual.
/// Ends when `shutdown` is cancelled.
async fn watch_lag(
    database: Arc<Database>,
    slot: String,
    interval: Duration,
    warn_above: u64,
    metrics: Arc<Metrics>,
    confirmed: Arc<Confirmed>,
    shutdown: CancellationToken,
) -> Result<()> {
    loop {
        tokio::select! {
            () = tokio::time::sleep(interval) => {}
            () = shutdown.cancelled() => return Ok(()),
        }
        match database.slot_lag(&slot).await {
            Ok(lag) => {
                Metrics::set(&metrics.lag, lag.bytes());
                Metrics::set(&metrics.confirmed_position, lag.confirmed_flush_lsn.0);
                let safe = Lsn::from(&confirmed.get());
                if lag.bytes() > warn_above {
                    tracing::warn!(
                        %slot,
                        lag_bytes = lag.bytes(),
                        restart_lsn = %lag.restart_lsn,
                        confirmed_flush_lsn = %lag.confirmed_flush_lsn,
                        current_lsn = %lag.current_lsn,
                        safe_to_confirm = %safe,
                        "the slot is holding back a lot of log"
                    );
                } else {
                    tracing::debug!(%slot, lag_bytes = lag.bytes(), confirmed_flush_lsn = %lag.confirmed_flush_lsn, safe_to_confirm = %safe, "slot lag");
                }
            }
            Err(error) => tracing::warn!(%slot, %error, "the slot's lag could not be read"),
        }
    }
}
