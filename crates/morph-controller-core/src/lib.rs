//! The core of a controller that reconciles external resources whenever a
//! source of rows says that their desired state changed, through Morph or
//! through Pulumi.
//!
//! The source says *when* and *which*. The engine decides *what*: given the
//! row that describes a resource, it reads the actual state, works out the
//! difference, and applies it. Morph does this as a library in this
//! process; Pulumi does it as a command with a stack per resource. Nothing
//! in this crate turns a row change into an action on a resource directly.
//!
//! This crate knows nothing of any particular database. A front-end such as
//! `morph-postgres-controller` adds a [`Source`], stores for the
//! controller's own records, a configuration file and a program, and the
//! rest is here.
//!
//! # The flow
//!
//! * The [`source`] holds the stream of changes and reduces each committed
//!   transaction to the resources it touched.
//! * The intake records those resources as [`checkpoint`] says and marks
//!   them in the scheduler.
//! * The [`scheduler`] is a bounded set of resources to look at. It hands
//!   out turns, and never two at the same resource at once.
//! * The workers take the turns and call the [`engine`] ([`worker`]):
//!   Morph with a record per resource ([`stores`]) and the configured
//!   [`providers`], or Pulumi with a stack per resource. A row that refers
//!   to another row is handed what the referenced resource exports, and
//!   waits for it if it is not there yet ([`references`]).
//! * The periodic loop marks every resource on a timer.
//!
//! [`app`] wires these together as tasks and has the picture. [`config`] is
//! the part of the configuration that every front-end shares, [`observe`]
//! is what the controller counts, and [`error`] is what can go wrong.
//! [`mapping`] turns a row into a desired state.
//!
//! # Writing a source
//!
//! A new front-end implements [`Source`] for its database, and
//! [`DirtyStore`](checkpoint::DirtyStore) and
//! [`ReferenceStore`](references::ReferenceStore) for wherever it keeps
//! the controller's own records. It then builds [`Parts`] and starts a
//! [`Controller`] with them. Everything else, the scheduler, the workers,
//! the engines, the references and the stacks, is the same for every
//! source.

pub mod app;
pub mod checkpoint;
pub mod config;
pub mod engine;
pub mod error;
pub mod mapping;
pub mod observe;
pub mod providers;
pub mod references;
pub mod scheduler;
pub mod source;
pub mod stores;
pub mod worker;

pub use app::{Checkpoint, Controller, Event, Options, Parts};
pub use error::{Error, Result};
pub use source::Source;
