//! Recording how far the controller has got, so that nothing is lost and
//! nothing is confirmed too early.
//!
//! This is the last step for a committed transaction. Once its resources
//! are accounted for, its position may be confirmed to the source, which
//! then stops keeping the transaction for the controller.
//!
//! The configuration chooses between two ways of accounting. In the durable
//! mode the resources are written to a [`DirtyStore`], and the transaction
//! is accounted for as soon as they are there. In the memory mode nothing
//! is written, and the transaction is accounted for once each of its
//! resources has been reconciled or given up on.
//!
//! In both modes the [`Ledger`] works out the position that is safe to
//! confirm.

pub mod ledger;

use std::fmt;

use async_trait::async_trait;
use morph::ResourceId;

pub use ledger::Ledger;

use crate::error::Result;

/// One entry of a dirty store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirtyRow {
    /// The resource.
    pub resource: ResourceId,
    /// How many attempts at it had failed when a failure was last recorded.
    ///
    /// Zero if none has been.
    pub attempts: u32,
}

/// What is still to be reconciled, kept where a restart finds it.
///
/// In the durable checkpoint mode the resources of a committed transaction
/// are written here before the transaction's position is confirmed. From
/// then on it is the store, and no longer the stream, that remembers they
/// need a look. At start the controller reads the store and takes up
/// whatever it holds. The store holds resource ids and how they fared. It
/// holds no positions in the log and nothing of any row.
///
/// For each resource the store keeps: when it was last marked, how many
/// attempts had failed when a failure was last recorded, and why the last
/// recorded attempt failed, or what the resource is waiting for.
///
/// A store lives where the front-end that made it decided: the PostgreSQL
/// controller keeps it as a table in the same database as the rows.
#[async_trait]
pub trait DirtyStore: Send + Sync + fmt::Debug {
    /// Makes whatever holds the store, if it does not exist.
    ///
    /// # Errors
    ///
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if it cannot be
    /// made, or a connection failure.
    async fn prepare(&self) -> Result<()>;

    /// Records that these resources are waiting.
    ///
    /// One that is already waiting stays so, with its count of failures and
    /// its last error; only the time of its mark moves to now. Either every
    /// one of the resources is recorded or none is. With no resources
    /// nothing is done.
    ///
    /// # Errors
    ///
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the write fails,
    /// or a connection failure.
    async fn mark(&self, resources: &[ResourceId]) -> Result<()>;

    /// Everything that is waiting, oldest mark first.
    ///
    /// An entry whose text is not a resource id is passed over with a
    /// warning.
    ///
    /// # Errors
    ///
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the read fails,
    /// or a connection failure.
    async fn load(&self) -> Result<Vec<DirtyRow>>;

    /// Records that a resource is reconciled, by forgetting it.
    ///
    /// The entry goes whenever it was marked, so a mark that was written
    /// while the resource was being reconciled goes with it. A resource
    /// with no entry is not an error.
    ///
    /// # Errors
    ///
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the write fails,
    /// or a connection failure.
    async fn done(&self, resource: &ResourceId) -> Result<()>;

    /// Writes `text` as the resource's last error, leaving the attempts and
    /// the time of the last failure alone.
    ///
    /// This is how a resource that waits for others says so, without
    /// counting the wait as a failure. A resource with no entry gets none.
    ///
    /// # Errors
    ///
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the write fails,
    /// or a connection failure.
    async fn note(&self, resource: &ResourceId, text: &str) -> Result<()>;

    /// Records that reconciling a resource failed, and why.
    ///
    /// `attempts` is written as given, in place of what was there, and the
    /// time of the mark is left alone. A resource with no entry gets none:
    /// only [`mark`](Self::mark) adds entries.
    ///
    /// # Errors
    ///
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the write fails,
    /// or a connection failure.
    async fn failed(&self, resource: &ResourceId, attempts: u32, reason: &str) -> Result<()>;
}
