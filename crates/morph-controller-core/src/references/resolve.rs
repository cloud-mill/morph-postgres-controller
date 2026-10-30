//! Replacing the ids in a row's reference columns with what the referenced
//! resources export.
//!
//! A [`Resolver`] takes a row on its way to the engine and, for each of
//! its reference columns, finds the referenced row, works out which
//! resource is reconciled for it, and asks the engine what that resource
//! exports. The value in the column is replaced by the answer. A row whose
//! referenced resource has nothing to give yet is not a failure. It is
//! [`Resolved::Waiting`], and the scheduler holds the row's resource until
//! the referenced one has been reconciled.
//!
//! A row grouped into a stack may refer to a row of the same stack.
//! Nothing is replaced then: the engine is told of the [`Link`], and
//! resolves it itself within the one reconciliation, Morph as a reference
//! in its plan and Pulumi as an expression in its program, so that the
//! engine orders the two.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use morph::ResourceId;

use super::References;
use crate::engine::{Engine, Exported};
use crate::error::{Error, Result};
use crate::mapping::desired::{Link, Row};
use crate::source::Source;

/// A resource that another refers to, with the resource that is
/// reconciled on its behalf, and what was taken from it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Dependency {
    /// The resource referred to.
    pub member: ResourceId,
    /// The resource whose reconciliation reconciles the member: the member
    /// itself, or the stack that groups its row.
    pub unit: ResourceId,
    /// The attribute taken from the member.
    pub attribute: String,
    /// Its value at the time, as text of its JSON, which is what tells a
    /// dependent that took the current value from one that has to be
    /// reconciled again when the value changes.
    pub value: String,
}

/// What resolving a row's references came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resolved {
    /// Every reference has its value. The row depends on these, one entry
    /// per reference column that held a value.
    Ready(Vec<Dependency>),
    /// At least one referenced resource has not been reconciled yet. The
    /// row's resource has to wait for these units, and the reason says
    /// which members are missing.
    Waiting {
        /// The units to wait for, each once.
        on: Vec<ResourceId>,
        /// Why, for the log.
        reason: String,
    },
}

/// Resolves the references of rows against the engine's records.
///
/// Clones share the references, the source of rows and the engine.
#[derive(Clone, Debug)]
pub struct Resolver {
    references: Arc<References>,
    source: Arc<dyn Source>,
    engine: Arc<Engine>,
}

impl Resolver {
    /// A resolver over these references, reading rows from `source` and
    /// exports from `engine`.
    pub fn new(references: Arc<References>, source: Arc<dyn Source>, engine: Arc<Engine>) -> Self {
        Self {
            references,
            source,
            engine,
        }
    }

    /// The references, as they were discovered.
    pub fn references(&self) -> &References {
        &self.references
    }

    /// Whether the rows of `row`'s table hold references at all.
    pub fn refers(&self, row: &Row) -> bool {
        self.references.of(&row.mapping).next().is_some()
    }

    /// Resolves the references of `row`, which is reconciled as part of
    /// `unit`: the row's own resource, or the stack that groups it.
    ///
    /// For each reference column that holds a value:
    ///
    /// * The referenced row is read. Without one, the reconciliation
    ///   fails: the row names something that does not exist, which a
    ///   foreign key constraint would have refused.
    /// * If the referenced row is reconciled as part of `unit` itself, the
    ///   column becomes a [`Link`] on the row, and the value is left to
    ///   the engine. A row that refers to itself fails.
    /// * Otherwise the engine is asked what the referenced resource
    ///   exports. A value replaces the column's. Nothing yet makes the row
    ///   wait. A record that exists but lacks the attribute fails the
    ///   reconciliation, naming the attribute, because waiting would not
    ///   bring it.
    /// * A reference that `sets` another attribute gives the value, or the
    ///   link, to that attribute, and the column itself is taken off the
    ///   row, so that the engine never sees it. A row that fills two
    ///   columns which set the same attribute fails, naming both.
    ///
    /// The row's `exports` are set to what other tables take from its
    /// own resources, so that the engine exports them.
    ///
    /// # Errors
    ///
    /// * [`Error::Reconcile`] for `unit` if a referenced row does not
    ///   exist, a row refers to itself, two filled columns set the same
    ///   attribute, or a referenced record lacks the attribute.
    /// * [`Error::Connection`], [`Error::Query`] or [`Error::Mapping`] if
    ///   a referenced row cannot be read or names no valid resource.
    /// * Whatever [`Engine::exported`] fails with.
    pub async fn resolve(&self, unit: &ResourceId, row: &mut Row) -> Result<Resolved> {
        let failed = |reason: String| Error::Reconcile {
            resource: unit.clone(),
            reason,
        };
        row.exports = self
            .references
            .exported_by(&row.mapping.schema, &row.mapping.table);
        let mut dependencies = Vec::new();
        let mut waiting: BTreeSet<ResourceId> = BTreeSet::new();
        let mut missing = Vec::new();
        let mut given: BTreeMap<String, String> = BTreeMap::new();
        let references: Vec<_> = self.references.of(&row.mapping).cloned().collect();
        for reference in references {
            let key = match row.json.get(&reference.column) {
                None | Some(serde_json::Value::Null) => continue,
                Some(serde_json::Value::String(text)) => text.clone(),
                Some(other) => other.to_string(),
            };
            if let Some(other) = given.insert(reference.sets.clone(), reference.column.clone()) {
                return Err(failed(format!(
                    "columns {other} and {} both give the attribute {}",
                    reference.column, reference.sets
                )));
            }
            let target_mapping = self
                .source
                .mapping_of(&reference.target_schema, &reference.target_table)
                .ok_or_else(|| {
                    Error::Mapping(format!(
                        "the reference {reference} points at a table that is not mapped"
                    ))
                })?
                .clone();
            let target = self
                .source
                .row_in(&target_mapping, &key)
                .await?
                .ok_or_else(|| {
                    failed(format!(
                        "column {} refers to the row {key:?} of {}, which does not exist",
                        reference.column,
                        reference.qualified_target()
                    ))
                })?;
            let member = target.resource.clone();
            let target_unit = target.unit()?;
            if target_unit == *unit {
                if member == row.resource {
                    return Err(failed(format!(
                        "column {} refers to the row's own resource",
                        reference.column
                    )));
                }
                row.links.push(Link {
                    column: reference.sets.clone(),
                    target: member,
                    attribute: reference.attribute.clone(),
                });
                if reference.sets != reference.column
                    && let serde_json::Value::Object(columns) = &mut row.json
                {
                    columns.remove(&reference.column);
                }
                continue;
            }
            match self
                .engine
                .exported(&target_unit, &member, &reference.attribute)
                .await?
            {
                Exported::Value(value) => {
                    dependencies.push(Dependency {
                        member,
                        unit: target_unit,
                        attribute: reference.attribute.clone(),
                        value: value.to_string(),
                    });
                    if let serde_json::Value::Object(columns) = &mut row.json {
                        if reference.sets != reference.column {
                            columns.remove(&reference.column);
                        }
                        columns.insert(reference.sets.clone(), value);
                    }
                }
                Exported::NotYet => {
                    waiting.insert(target_unit.clone());
                    missing.push(member.to_string());
                }
                Exported::Missing => {
                    return Err(failed(format!(
                        "{member} has no attribute {} to give column {}",
                        reference.attribute, reference.column
                    )));
                }
            }
        }
        if waiting.is_empty() {
            dependencies.sort();
            dependencies.dedup();
            Ok(Resolved::Ready(dependencies))
        } else {
            Ok(Resolved::Waiting {
                on: waiting.into_iter().collect(),
                reason: format!("{} has not been reconciled yet", missing.join(", ")),
            })
        }
    }
}
