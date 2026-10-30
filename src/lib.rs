//! A controller that reconciles external resources whenever PostgreSQL
//! says that their desired state changed, through Morph or through Pulumi.
//!
//! PostgreSQL says *when* and *which*. The engine decides *what*: given the
//! row that describes a resource, it reads the actual state, works out the
//! difference, and applies it. Morph does this as a library in this
//! process; Pulumi does it as a command with a stack per resource. Nothing
//! in this crate turns a row change into an action on a resource directly.
//!
//! # Two crates
//!
//! Everything that does not depend on PostgreSQL is in
//! [`morph_controller_core`]: the scheduler, the workers, the engines, the
//! references between rows, the stacks, and the controller that wires them
//! together. This crate is the PostgreSQL front-end on top of it:
//!
//! * [`source`] implements the core's [`Source`](morph_controller_core::Source)
//!   over logical replication. The reader ([`postgres::replication`])
//!   holds the replication connection and reduces each committed
//!   transaction to the resources it touched, which [`mapping`] names, and
//!   [`postgres::rows`] reads rows back when a resource is reconciled.
//! * [`checkpoint`] and [`references`] keep the controller's own records,
//!   the dirty table and the table of references, in the same database.
//! * [`stores`] keeps Morph's records in Morph's own table of the database,
//!   when the configuration asks for that.
//! * [`config`] is the whole configuration file, of which the PostgreSQL
//!   tables are this crate's and the rest the core's.
//! * [`app`] builds the parts from the configuration and starts the core's
//!   controller with them.
//!
//! The core's modules that a program is likely to need are re-exported
//! here under the same names: [`engine`], [`error`], [`observe`],
//! [`providers`], [`scheduler`] and [`worker`].
//!
//! # Examples
//!
//! Running a controller until Ctrl-C, or until a part of it fails:
//!
//! ```no_run
//! use std::path::Path;
//! use std::time::Duration;
//!
//! use morph_postgres_controller::Controller;
//! use morph_postgres_controller::config::Config;
//!
//! # async fn example() -> morph_postgres_controller::Result<()> {
//! let config = Config::load(Path::new("controller.toml"))?;
//! let mut controller = Controller::start(config).await?;
//! tokio::select! {
//!     _ = tokio::signal::ctrl_c() => {}
//!     error = controller.failed() => eprintln!("{error}"),
//! }
//! controller.stop(Duration::from_secs(60)).await;
//! # Ok(())
//! # }
//! ```

pub mod app;
pub mod checkpoint;
pub mod config;
pub mod mapping;
pub mod postgres;
pub mod references;
pub mod source;
pub mod stores;

pub use morph_controller_core::{
    Error, Event, Result, engine, error, observe, providers, scheduler, worker,
};

pub use app::{Controller, Options};
pub use source::PostgresSource;
