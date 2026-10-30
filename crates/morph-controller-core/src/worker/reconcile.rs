//! The one place the engine is called.
//!
//! The controller never decides what should happen to a resource. For each
//! turn it reads the resource's row as it is now and hands the engine
//! either that row or the news that there is none. The engine does the
//! rest, as [`Engine`] describes.
//!
//! Two things happen on the way. A row's reference columns are resolved:
//! each is replaced by what the referenced resource exports, as
//! [`Resolver`] describes, and a row whose referenced resource has not
//! been reconciled yet makes the turn end in [`Turn::Waiting`] rather than
//! fail. And before a resource is removed, the store of references is
//! asked whether anything still refers to it; if so the turn waits for
//! those, so that a resource goes after the resources that use it, however
//! the rows were deleted.
//!
//! # What counts as failure
//!
//! * The row cannot be read. The engine is not called.
//! * A reference cannot be resolved: the referenced row does not exist, or
//!   its resource has a record without the attribute. The engine is not
//!   called.
//! * The engine fails: the row does not make sense for it, its record
//!   cannot be had, or the reconciliation itself fails.
//! * The store of references cannot be read or written.
//! * All of this together takes longer than `reconcile.timeout`.
//!
//! Nothing is tried again here. A failure goes back to the worker, which
//! tells the scheduler, and the scheduler decides when the next attempt is.
//! Waiting is not failing: the scheduler parks the resource and gives it
//! its next turn when what it waits for has been reconciled.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use morph::ResourceId;
use tokio::time::Instant;

use crate::config::STACK_KIND;
use crate::engine::{Engine, Exports};
use crate::error::{Error, Result};
use crate::references::{Dependency, ReferenceStore, References, Resolved, Resolver};
use crate::source::Source;

/// What a reconciliation that succeeded came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reconciled {
    /// The engine's one-line summary of what it did.
    pub summary: String,
    /// Whether the engine changed anything, as opposed to finding the
    /// resource as it should be.
    pub changed: bool,
    /// Whether a row described the resource, or, for a stack that groups
    /// rows, whether it had any. If not, the engine was told that nothing
    /// is desired.
    pub desired: bool,
    /// How long the reconciliation took, from before the row was read
    /// until the engine's outcome was in.
    pub elapsed: Duration,
    /// What the resource, or the stack's resources, export: before and
    /// after. The worker finds from this which other resources took a
    /// value that changed.
    pub exports: Exports,
}

/// How a turn at a resource ended, short of failing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Turn {
    /// The engine was called, and succeeded.
    Reconciled(Reconciled),
    /// The engine was not called, because the resource has to wait for
    /// these resources to be reconciled first: those its row refers to
    /// and that have no record yet, or, for a resource that is to be
    /// removed, those that still refer to it.
    Waiting {
        /// What to wait for, each once.
        on: Vec<ResourceId>,
        /// Why, for the log.
        reason: String,
    },
}

/// Reconciles resources through the engine, one call for one resource.
///
/// Clones share the source, the engine, the references and the store of
/// references, and each worker has one. Nothing here keeps two calls for
/// the same resource apart. That is the scheduler's work.
#[derive(Clone, Debug)]
pub struct Reconciler {
    source: Arc<dyn Source>,
    engine: Arc<Engine>,
    timeout: Duration,
    resolver: Resolver,
    store: Arc<dyn ReferenceStore>,
}

impl Reconciler {
    /// A reconciler that reads rows from `source`, resolves their
    /// `references`, and hands them to `engine`. Which resource refers to
    /// which is kept in `store`. A reconciliation that takes longer than
    /// `timeout` is abandoned.
    pub fn new(
        source: Arc<dyn Source>,
        engine: Arc<Engine>,
        timeout: Duration,
        references: Arc<References>,
        store: Arc<dyn ReferenceStore>,
    ) -> Self {
        Self {
            resolver: Resolver::new(references, Arc::clone(&source), Arc::clone(&engine)),
            source,
            engine,
            timeout,
            store,
        }
    }

    /// Brings `resource` in line with its row, or with the absence of one.
    ///
    /// The row is read now. It is not taken from the change that marked the
    /// resource, so the engine always works from the latest row. A resource
    /// of the kind [`STACK_KIND`] is a stack that groups rows: every
    /// row of the group is read, and the engine is handed them all.
    ///
    /// With a row, or rows: the references are resolved first. If any
    /// referenced resource has not been reconciled yet, the turn is
    /// [`Turn::Waiting`] for it, and the engine is not called. Otherwise
    /// what the resource now refers to is written to the store of
    /// references, the engine is called, and the store is brought exactly
    /// in line once the engine has succeeded.
    ///
    /// Without a row, or with rows missing from a group: if the store of
    /// references says that another resource still refers to what is to be
    /// removed, the turn is [`Turn::Waiting`] for that resource. Its own
    /// reconciliation removes it and forgets what it referred to, which
    /// ends the wait. Otherwise the engine is told that nothing is desired.
    ///
    /// # Errors
    ///
    /// * [`Error::Connection`] or [`Error::Query`] if a row cannot be
    ///   read.
    /// * [`Error::Checkpoint`] if the store of references cannot be read
    ///   or written.
    /// * [`Error::Reconcile`] if a reference names a row that does not
    ///   exist or an attribute the referenced resource lacks, as
    ///   [`Resolver::resolve`] describes.
    /// * Whatever [`Engine::reconcile`] fails with: [`Error::Mapping`],
    ///   [`Error::Checkpoint`] or [`Error::Reconcile`].
    /// * [`Error::Reconcile`] if the whole call takes longer than the
    ///   timeout. The work is then dropped where it stands, and what the
    ///   engine had done by then is not undone.
    pub async fn reconcile(&self, resource: &ResourceId) -> Result<Turn> {
        let started = Instant::now();
        let work = async {
            if resource.kind().as_str() == STACK_KIND {
                self.reconcile_group(resource, started).await
            } else {
                self.reconcile_one(resource, started).await
            }
        };
        match tokio::time::timeout(self.timeout, work).await {
            Ok(outcome) => outcome,
            Err(_) => Err(Error::Reconcile {
                resource: resource.clone(),
                reason: format!("took longer than {:?}", self.timeout),
            }),
        }
    }

    /// The references, as they were discovered.
    pub fn references(&self) -> &References {
        self.resolver.references()
    }

    /// A turn at a resource that is a row of its own.
    async fn reconcile_one(&self, resource: &ResourceId, started: Instant) -> Result<Turn> {
        let table = &self.store;
        let Some(mut row) = self.source.row(resource).await? else {
            let dependents = table.dependents_of(resource, &[]).await?;
            if !dependents.is_empty() {
                return Ok(Turn::Waiting {
                    reason: still_referred_to(&dependents),
                    on: dependents,
                });
            }
            let outcome = self.engine.reconcile(resource, None).await?;
            table.replace(resource, &[]).await?;
            return Ok(Turn::Reconciled(Reconciled {
                summary: outcome.summary,
                changed: outcome.changed,
                desired: false,
                elapsed: started.elapsed(),
                exports: outcome.exports,
            }));
        };
        let refers = self.resolver.refers(&row);
        let dependencies = match self.resolver.resolve(resource, &mut row).await? {
            Resolved::Ready(dependencies) => dependencies,
            Resolved::Waiting { on, reason } => return Ok(Turn::Waiting { on, reason }),
        };
        if refers {
            table.add(resource, &dependencies).await?;
        }
        let outcome = self.engine.reconcile(resource, Some(row)).await?;
        if refers {
            table.replace(resource, &dependencies).await?;
        }
        Ok(Turn::Reconciled(Reconciled {
            summary: outcome.summary,
            changed: outcome.changed,
            desired: true,
            elapsed: started.elapsed(),
            exports: outcome.exports,
        }))
    }

    /// A turn at a stack that groups rows.
    async fn reconcile_group(&self, resource: &ResourceId, started: Instant) -> Result<Turn> {
        let table = &self.store;
        let mut rows = self.source.group_rows(resource.name()).await?;
        let kept: Vec<ResourceId> = rows.iter().map(|row| row.resource.clone()).collect();
        let dependents = table.dependents_of(resource, &kept).await?;
        if !dependents.is_empty() {
            return Ok(Turn::Waiting {
                reason: still_referred_to(&dependents),
                on: dependents,
            });
        }
        let mut dependencies: Vec<Dependency> = Vec::new();
        let mut waiting: BTreeSet<ResourceId> = BTreeSet::new();
        let mut reasons = Vec::new();
        let mut refers = false;
        for row in &mut rows {
            refers |= self.resolver.refers(row);
            match self.resolver.resolve(resource, row).await? {
                Resolved::Ready(found) => dependencies.extend(found),
                Resolved::Waiting { on, reason } => {
                    waiting.extend(on);
                    reasons.push(reason);
                }
            }
        }
        if !waiting.is_empty() {
            return Ok(Turn::Waiting {
                on: waiting.into_iter().collect(),
                reason: reasons.join("; "),
            });
        }
        dependencies.sort();
        dependencies.dedup();
        let desired = !rows.is_empty();
        if refers {
            table.add(resource, &dependencies).await?;
        }
        let outcome = self.engine.reconcile_group(resource.name(), rows).await?;
        if refers || !desired {
            table.replace(resource, &dependencies).await?;
        }
        Ok(Turn::Reconciled(Reconciled {
            summary: outcome.summary,
            changed: outcome.changed,
            desired,
            elapsed: started.elapsed(),
            exports: outcome.exports,
        }))
    }
}

/// Why a removal waits: `still referred to by a, b`.
fn still_referred_to(dependents: &[ResourceId]) -> String {
    let names: Vec<String> = dependents.iter().map(ToString::to_string).collect();
    format!("still referred to by {}", names.join(", "))
}
