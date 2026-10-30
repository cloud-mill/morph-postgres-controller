//! The controller, built from the configuration and running.
//!
//! The core's [`Controller`](morph_controller_core::Controller) runs the
//! scheduler, the workers, the periodic loop and the metrics logger, and
//! takes its source and its stores ready-made in [`Parts`]. This module
//! makes them from a [`Config`]: the ordinary database connection, the
//! [`PostgresSource`] over the replication stream, the engine, the dirty
//! table and the table of references, and Morph's stores in the database
//! when the configuration asks for them. [`Controller`] here wraps the
//! core's, so that a program sees one type.
//!
//! Before anything starts, [`prepare_database`] checks the server and makes
//! the publication and the slot if asked to.

use std::sync::Arc;
use std::time::Duration;

use morph::ResourceId;
use morph_controller_core::engine::{Engine, MorphEngine, PulumiEngine};
use morph_controller_core::observe::Metrics;
use morph_controller_core::providers::Providers;
use morph_controller_core::{Checkpoint, Event, Parts};
use tokio::sync::broadcast;

use crate::checkpoint::DirtyTable;
use crate::config::{CheckpointMode, Config, EngineConfig};
use crate::error::{Error, Result};
use crate::postgres::database::Database;
use crate::postgres::failed;
use crate::references::ReferenceTable;
use crate::source::PostgresSource;
use crate::stores::state_stores;

/// How the controller is started, beyond what the configuration says.
///
/// The defaults are what a service wants. The fields are there for tests.
#[derive(Clone, Debug)]
pub struct Options {
    /// Whether the server is told how far the controller has got. `true`
    /// by default.
    ///
    /// Turned off only in tests of what happens when it is not told.
    pub acknowledge: bool,
    /// Whether everything is reconciled once at start, as well as every
    /// `reconcile.full_interval`. `true` by default.
    pub full_run_at_start: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            acknowledge: true,
            full_run_at_start: true,
        }
    }
}

/// A running controller: its tasks, and the means to watch and stop them.
///
/// [`start`](Self::start) and [`start_with`](Self::start_with) make one,
/// and [`stop`](Self::stop) shuts it down in order. A controller that is
/// dropped instead has its tasks aborted where they stand.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
/// use std::time::Duration;
///
/// use morph_postgres_controller::config::Config;
/// use morph_postgres_controller::{Controller, Event};
///
/// # async fn example() -> morph_postgres_controller::Result<()> {
/// let config = Config::load(Path::new("controller.toml"))?;
/// let controller = Controller::start(config).await?;
///
/// // Watch the reconciliations until one fails, then stop.
/// let mut events = controller.events();
/// while let Ok(event) = events.recv().await {
///     if let Event::Reconciled { resource, ok, changed, .. } = event {
///         println!("{resource}: ok={ok} changed={changed}");
///         if !ok {
///             break;
///         }
///     }
/// }
/// controller.stop(Duration::from_secs(60)).await;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Controller {
    inner: morph_controller_core::Controller,
}

impl Controller {
    /// Starts the providers the configuration names, and then the
    /// controller, with the default [`Options`].
    ///
    /// # Errors
    ///
    /// [`Error::Provider`] if a provider cannot be started, as
    /// [`Providers::start`] describes. After that, whatever
    /// [`start_with`](Self::start_with) fails with.
    pub async fn start(config: Config) -> Result<Self> {
        let providers = match &config.engine {
            EngineConfig::Morph => Providers::start(&config.providers, &config.morph).await?,
            EngineConfig::Pulumi(_) => Providers::ready(
                morph::ProviderRegistry::new(),
                morph::ExecutorOptions::default(),
                None,
            ),
        };
        Self::start_with(config, providers, Options::default()).await
    }

    /// Starts the controller with providers given ready-made.
    ///
    /// The providers are Morph's. With the Pulumi engine they are not used,
    /// and any may be given.
    ///
    /// The configuration is checked first. Then the server is checked, and
    /// the publication and the slot are made if they are missing and the
    /// configuration says to make them. The foreign keys of the mapped
    /// tables are read, and the table of references is made if it is
    /// missing. In the durable mode the dirty table is made if it is
    /// missing, and every resource in it is marked. Last, the tasks are
    /// spawned.
    ///
    /// The tasks are running when this returns. The stream may not be
    /// connected yet: [`Event::Connected`] says when it is, to whoever has
    /// subscribed by then.
    ///
    /// # Errors
    ///
    /// * [`Error::Config`] if the configuration fails [`Config::validate`],
    ///   if the database URL or the certificate file cannot be read, if the
    ///   server's `wal_level` is not `logical`, or if the publication or
    ///   the slot is missing and is not to be made.
    /// * [`Error::Connection`] if the database cannot be reached, and
    ///   [`Error::Query`] if it refuses a statement, such as the one that
    ///   makes the publication or the slot.
    /// * [`Error::Checkpoint`] if the dirty table or the table of
    ///   references cannot be made or read, if the slot's position cannot
    ///   be read, or if the stores for Morph's records cannot be set up.
    /// * [`Error::Engine`] if the Pulumi engine cannot set up its work
    ///   directory, read its passphrase file, or run `pulumi version`.
    ///
    /// A URL that only the replication connection cannot use is not found
    /// here. The reader fails with [`Error::Config`] once it runs, and
    /// [`failed`](Self::failed) returns that.
    pub async fn start_with(
        config: Config,
        providers: Providers,
        options: Options,
    ) -> Result<Self> {
        config.validate()?;
        let database = Arc::new(Database::new(
            &config.postgres.url,
            config.postgres.ca_certificate.as_deref(),
        )?);
        prepare_database(&config, &database).await?;

        let engine = Arc::new(match &config.engine {
            EngineConfig::Morph => Engine::Morph(MorphEngine::new(
                state_stores(&config.morph, &config.postgres, Arc::clone(&database))?,
                providers.registry.clone(),
                providers.executor,
            )),
            EngineConfig::Pulumi(settings) => {
                let pulumi = PulumiEngine::new(settings)?;
                let version = pulumi.version().await?;
                tracing::info!(%version, backend = %pulumi.backend(), project = pulumi.project(), "pulumi is the engine");
                Engine::Pulumi(pulumi)
            }
        });
        let source = Arc::new(PostgresSource::new(
            &config,
            Arc::clone(&database),
            options.acknowledge,
        ));
        let checkpoint = match config.checkpoint.mode {
            CheckpointMode::Durable => Checkpoint::Durable(Arc::new(DirtyTable::new(
                Arc::clone(&database),
                &config.checkpoint.table,
            ))),
            CheckpointMode::Memory => Checkpoint::Memory,
        };
        let references = Arc::new(ReferenceTable::new(
            Arc::clone(&database),
            &config.references.table,
        ));
        tracing::info!(
            database = database.location(),
            slot = %config.replication.slot,
            publication = %config.replication.publication,
            "PostgreSQL is the source"
        );
        let inner = morph_controller_core::Controller::start(
            Parts {
                source,
                engine,
                checkpoint,
                references,
                reconcile: config.reconcile.clone(),
                write_retry: config.replication.reconnect.clone(),
                metrics_interval: config.logging.metrics_interval,
                mock: providers.mock,
            },
            morph_controller_core::Options {
                full_run_at_start: options.full_run_at_start,
            },
        )
        .await?;
        Ok(Self { inner })
    }

    /// Subscribes to what happens, as it happens.
    ///
    /// A receiver gets the events sent after it subscribed. The channel
    /// keeps 1024 of them. A receiver that falls further behind loses the
    /// oldest, and is told so by a `Lagged` error.
    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.inner.events()
    }

    /// The counters, shared with the running tasks.
    pub fn metrics(&self) -> Arc<Metrics> {
        self.inner.metrics()
    }

    /// The in-memory cloud among the providers, if there is one.
    pub fn mock(&self) -> Option<&morph_mock::MockCloud> {
        self.inner.mock()
    }

    /// The resources that were given up on and have not been reconciled
    /// since.
    pub async fn unhealthy(&self) -> Vec<ResourceId> {
        self.inner.unhealthy().await
    }

    /// The resources that wait for others to be reconciled, each with what
    /// it waits for.
    pub async fn parked(&self) -> Vec<(ResourceId, Vec<ResourceId>)> {
        self.inner.parked().await
    }

    /// Stops reading changes, lets the reconciliations in flight finish,
    /// and returns once every task has ended or been aborted, as
    /// [`Controller::stop`](morph_controller_core::Controller::stop)
    /// describes.
    ///
    /// Resources that are still waiting are not reconciled. In the durable
    /// mode the next start takes up whatever the dirty table holds. In the
    /// memory mode the transactions that marked them were never confirmed,
    /// and the server sends those again. A resource that only the periodic
    /// loop marked is in neither, and is marked again by the next full
    /// reconciliation.
    pub async fn stop(self, grace: Duration) {
        self.inner.stop(grace).await;
    }

    /// Waits until a part of the controller that should run for ever
    /// stops on its own, which is a failure, as
    /// [`Controller::failed`](morph_controller_core::Controller::failed)
    /// describes.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. A future that is dropped has taken no
    /// task's ending with it, and a later call still sees it.
    pub async fn failed(&mut self) -> Error {
        self.inner.failed().await
    }
}

/// Checks the server, and makes the publication and the slot if asked to.
///
/// The publication is made for the mapped tables. One that exists is used
/// as it is, and so is a slot that exists.
///
/// # Errors
///
/// [`Error::Config`] if `wal_level` is not `logical`, or if the
/// publication or the slot is missing and the configuration does not say
/// to make it. [`Error::Connection`] if the database cannot be reached,
/// and [`Error::Query`] if it refuses a statement.
pub async fn prepare_database(config: &Config, database: &Database) -> Result<()> {
    let client = database.client().await?;
    let level: String = client
        .query_one("SHOW wal_level", &[])
        .await
        .map_err(failed)?
        .get(0);
    if level != "logical" {
        return Err(Error::Config(format!(
            "wal_level is {level}; logical replication needs it to be logical"
        )));
    }
    let publication = &config.replication.publication;
    if !database.publication_exists(publication).await? {
        if !config.replication.create_publication {
            return Err(Error::Config(format!(
                "the publication {publication} does not exist; set replication.create_publication to make it"
            )));
        }
        let tables: Vec<(String, String)> = config
            .mappings
            .iter()
            .map(|mapping| (mapping.schema.clone(), mapping.table.clone()))
            .collect();
        database.create_publication(publication, &tables).await?;
        tracing::info!(%publication, tables = tables.len(), "made the publication");
    }
    let slot = &config.replication.slot;
    if !database.slot_exists(slot).await? {
        if !config.replication.create_slot {
            return Err(Error::Config(format!(
                "the slot {slot} does not exist; set replication.create_slot to make it"
            )));
        }
        let from = database.create_slot(slot).await?;
        tracing::info!(%slot, %from, "made the replication slot");
    }
    Ok(())
}
