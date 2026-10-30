//! From rows to resources.
//!
//! A source knows tables and rows. Morph knows resources, each with a kind
//! and a name. This is where the one becomes the other: a row, read back
//! when its resource is reconciled, becomes the resource's desired state.
//!
//! The other direction, naming the resource that a row change is about, is
//! the source's own work, because it depends on how the source reports a
//! change. A source gives a resource id for each change and nothing else.
//! No value from a change reaches an engine: [`desired`] reads the row as
//! it is at the moment of the reconciliation.

pub mod desired;

pub use desired::{Link, Row};
