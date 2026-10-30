//! From rows to resources.
//!
//! PostgreSQL knows tables and rows. Morph knows resources, each with a kind
//! and a name. This is where the one becomes the other, in two directions:
//! a row change names the resource to reconcile, and a row, read back when
//! the resource is reconciled, becomes the resource's desired state.
//!
//! The two are kept apart on purpose. [`resource_mapper`] is used by the
//! reader, on every row change, and gives a resource id and nothing else.
//! [`desired`], which is the core's, is used by the workers, and reads the
//! row as it is at that moment. No value from a change reaches an engine.

pub mod resource_mapper;

pub use morph_controller_core::mapping::desired;
pub use morph_controller_core::mapping::{Link, Row};
pub use resource_mapper::{ResourceMapper, TableMapper};
