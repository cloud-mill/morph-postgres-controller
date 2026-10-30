//! Which resource is reconciled next.
//!
//! The scheduler stands between whatever marks resources (the intake, the
//! periodic loop, and the taking up of the dirty table at start) and the
//! workers that reconcile them. [`Scheduler`] is the whole of it, and
//! [`dirty_set`] has its rules.

pub mod dirty_set;

pub use dirty_set::{Disposition, Lease, Marked, Origin, Outcome, Scheduler};
