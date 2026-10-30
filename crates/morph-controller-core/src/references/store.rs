//! Which resource refers to which, kept where a restart finds it.
//!
//! When a resource is reconciled with a reference resolved, the controller
//! writes down that it refers to the referenced resource, and what it took
//! from it. Two things are read back from that:
//!
//! * Before a resource is removed, whether anything still refers to it.
//!   If so, the removal waits until those have been reconciled, which is
//!   when their rows have gone and they have been removed themselves. The
//!   rows are gone by then, so the user's tables cannot answer this; the
//!   store can.
//! * When what a resource exports changes, which resources took another
//!   value, so that they can be reconciled with the new one.
//!
//! An entry is a dependent, a dependency and an attribute, with the unit
//! that reconciles the dependency and the value the dependent took. A
//! dependent, a dependency and an attribute make one entry, however many
//! columns take that attribute. References within one stack are not
//! written: the engine orders those itself.

use std::collections::BTreeMap;
use std::fmt;

use async_trait::async_trait;
use morph::ResourceId;

use super::Dependency;
use crate::error::Result;

/// The store of references between resources.
///
/// A store lives where the front-end that made it decided: the PostgreSQL
/// controller keeps it as a table in the same database as the rows.
#[async_trait]
pub trait ReferenceStore: Send + Sync + fmt::Debug {
    /// Makes whatever holds the store, if it does not exist.
    ///
    /// # Errors
    ///
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if it cannot be
    /// made, or a connection failure.
    async fn prepare(&self) -> Result<()>;

    /// Records that `dependent` refers to each of `dependencies`, with the
    /// values it took, besides whatever it was recorded as referring to
    /// before.
    ///
    /// With no dependencies nothing is done.
    ///
    /// # Errors
    ///
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the write fails,
    /// or a connection failure.
    async fn add(&self, dependent: &ResourceId, dependencies: &[Dependency]) -> Result<()>;

    /// Records that `dependent` refers to exactly `dependencies`: what it
    /// referred to before and is not among them is forgotten.
    ///
    /// With no dependencies, everything it referred to is forgotten.
    ///
    /// # Errors
    ///
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if a write fails,
    /// or a connection failure.
    async fn replace(&self, dependent: &ResourceId, dependencies: &[Dependency]) -> Result<()>;

    /// The resources that refer to a member of `unit` other than those in
    /// `kept`, sorted.
    ///
    /// With `kept` empty, everything that refers to any member of `unit`.
    /// This is what has to be reconciled before `unit` may remove those
    /// members.
    ///
    /// # Errors
    ///
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the read fails,
    /// or a connection failure.
    async fn dependents_of(
        &self,
        unit: &ResourceId,
        kept: &[ResourceId],
    ) -> Result<Vec<ResourceId>>;

    /// The resources that took a value of one of `changed` exports other
    /// than its current one, sorted.
    ///
    /// `changed` maps export names, `kind.name.attribute`, to the value
    /// after the change, or `None` for an export that is gone. A dependent
    /// whose recorded value is the current one has nothing to do, and is
    /// left out; one whose value differs, or that recorded none, has to be
    /// reconciled again. A name that is not of the shape of an export is
    /// passed over.
    ///
    /// # Errors
    ///
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if a read fails, or
    /// a connection failure.
    async fn stale_dependents(
        &self,
        changed: &BTreeMap<String, Option<serde_json::Value>>,
    ) -> Result<Vec<ResourceId>>;
}
