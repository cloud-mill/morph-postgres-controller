//! Reconciling one resource through the engine.
//!
//! A worker takes a turn from the scheduler, calls
//! [`Reconciler::reconcile`], and tells the scheduler how it went: done,
//! failed, or waiting for other resources. The loop that does so is in
//! [`crate::app`]. What happens within a turn is in [`reconcile`].

pub mod reconcile;

pub use reconcile::{Reconciled, Reconciler, Turn};
