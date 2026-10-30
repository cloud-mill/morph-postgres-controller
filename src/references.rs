//! Rows that refer to other rows, as PostgreSQL knows them.
//!
//! The model of a reference, the resolving of row ids into what the
//! referenced resource exports, and the rules are the core's, in
//! [`morph_controller_core::references`], and are re-exported here. This
//! module adds what needs the database: [`foreign_keys`] reads the
//! constraints from the catalogue, [`discover`] assembles the references
//! from them and from the mappings, and [`ReferenceTable`] is the core's
//! [`ReferenceStore`] as a table of the same database as the rows.
//!
//! | Column | What it holds |
//! |--------|---------------|
//! | `dependent` | The resource that refers: the resource of the row, or the stack that groups it. |
//! | `dependency` | The resource referred to, as `kind.name`. |
//! | `dependency_unit` | The resource that is reconciled to reconcile the dependency: itself, or the stack that groups it. |
//! | `attribute` | The attribute of the dependency that the dependent took. |
//! | `value` | The value it took, as JSON. |
//!
//! A dependent, a dependency and an attribute make one row, however many
//! columns take that attribute. The value is what tells a dependent that
//! has the current value, and need not be looked at, from one that took a
//! value since changed. References within one stack are not written: the
//! engine orders those itself.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use morph::ResourceId;
use morph_controller_core::engine::export_parts;

pub use morph_controller_core::references::{
    Dependency, ForeignKey, Provenance, Reference, ReferenceStore, References, Resolved, Resolver,
};

use crate::config::{Mapping, quote_identifier};
use crate::error::Result;
use crate::postgres::checkpoint_failed;
use crate::postgres::database::Database;
use crate::postgres::failed;

/// Reads the foreign keys from `database` and assembles the references of
/// `mappings` from them and from the mappings' own declarations, as
/// [`References::assemble`] does.
///
/// # Errors
///
/// [`Error::Connection`](crate::Error::Connection) or [`Error::Query`](crate::Error::Query) if the catalogue cannot be
/// read, and whatever [`References::assemble`] fails with.
pub async fn discover(database: &Database, mappings: &[Mapping]) -> Result<References> {
    let keys = foreign_keys(database).await?;
    References::assemble(mappings, &keys)
}

/// Every foreign key in the database, as the catalogue describes them,
/// sorted by constraint name.
///
/// Keys of every table are read, mapped or not, in one statement. A key is
/// given by its first column; `columns` says how many it has.
///
/// # Errors
///
/// [`Error::Connection`](crate::Error::Connection) if the database cannot be reached, or
/// [`Error::Query`](crate::Error::Query) if the server refuses the statement.
pub async fn foreign_keys(database: &Database) -> Result<Vec<ForeignKey>> {
    let rows = database
        .client()
        .await?
        .query(
            "SELECT c.conname, ns.nspname, cl.relname, a.attname, \
                    fns.nspname, fcl.relname, fa.attname, array_length(c.conkey, 1) \
             FROM pg_constraint c \
             JOIN pg_class cl ON cl.oid = c.conrelid \
             JOIN pg_namespace ns ON ns.oid = cl.relnamespace \
             JOIN pg_class fcl ON fcl.oid = c.confrelid \
             JOIN pg_namespace fns ON fns.oid = fcl.relnamespace \
             JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1] \
             JOIN pg_attribute fa ON fa.attrelid = c.confrelid AND fa.attnum = c.confkey[1] \
             WHERE c.contype = 'f' \
             ORDER BY c.conname",
            &[],
        )
        .await
        .map_err(failed)?;
    Ok(rows
        .iter()
        .map(|row| ForeignKey {
            constraint: row.get(0),
            schema: row.get(1),
            table: row.get(2),
            column: row.get(3),
            target_schema: row.get(4),
            target_table: row.get(5),
            target_column: row.get(6),
            columns: usize::try_from(row.get::<_, Option<i32>>(7).unwrap_or(1)).unwrap_or(1),
        })
        .collect())
}

/// The table of references between resources.
///
/// A handle on the table. It shares the [`Database`] and holds nothing else
/// but the table's name.
#[derive(Debug)]
pub struct ReferenceTable {
    database: Arc<Database>,
    table: String,
    index: String,
}

impl ReferenceTable {
    /// The table called `table` in `database`.
    ///
    /// The name is quoted, and so is taken exactly as written. Nothing is
    /// read or written yet.
    pub fn new(database: Arc<Database>, table: &str) -> Self {
        Self {
            database,
            table: quote_identifier(table),
            index: quote_identifier(&format!("{table}_unit")),
        }
    }

    /// The SQL that makes the table and its index.
    ///
    /// The statements do nothing if a table and an index of those names
    /// exist, whatever their columns.
    pub fn definition(&self) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {table} (\n    \
                 dependent        text NOT NULL,\n    \
                 dependency       text NOT NULL,\n    \
                 dependency_unit  text NOT NULL,\n    \
                 attribute        text NOT NULL,\n    \
                 value            jsonb,\n    \
                 PRIMARY KEY (dependent, dependency, attribute)\n\
             );\n\
             CREATE INDEX IF NOT EXISTS {index} ON {table} (dependency_unit)",
            table = self.table,
            index = self.index
        )
    }
}

#[async_trait]
impl ReferenceStore for ReferenceTable {
    /// Makes the table and its index if they do not exist.
    ///
    /// Two controllers that start at once may both find the table missing
    /// and both try to make it, and PostgreSQL refuses the second with a
    /// duplicate key. The statements are therefore tried twice: the second
    /// try finds the table there and does nothing.
    ///
    /// # Errors
    ///
    /// [`Error::Connection`](crate::Error::Connection) if the database cannot be reached, or
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the statements fail twice, as when the user
    /// may not create tables.
    async fn prepare(&self) -> Result<()> {
        let client = self.database.client().await?;
        if client.batch_execute(&self.definition()).await.is_ok() {
            return Ok(());
        }
        client
            .batch_execute(&self.definition())
            .await
            .map_err(checkpoint_failed)
    }

    /// Records the dependencies in one statement, over what was there.
    ///
    /// # Errors
    ///
    /// [`Error::Connection`](crate::Error::Connection) if the database cannot be reached, or
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the statement fails, as when a value is
    /// not JSON.
    async fn add(&self, dependent: &ResourceId, dependencies: &[Dependency]) -> Result<()> {
        if dependencies.is_empty() {
            return Ok(());
        }
        let members: Vec<String> = dependencies
            .iter()
            .map(|dependency| dependency.member.to_string())
            .collect();
        let units: Vec<String> = dependencies
            .iter()
            .map(|dependency| dependency.unit.to_string())
            .collect();
        let attributes: Vec<&str> = dependencies
            .iter()
            .map(|dependency| dependency.attribute.as_str())
            .collect();
        let values: Vec<&str> = dependencies
            .iter()
            .map(|dependency| dependency.value.as_str())
            .collect();
        let sql = format!(
            "INSERT INTO {} (dependent, dependency, dependency_unit, attribute, value) \
             SELECT $1, member, unit, attribute, value::jsonb \
             FROM unnest($2::text[], $3::text[], $4::text[], $5::text[]) AS taken(member, unit, attribute, value) \
             ON CONFLICT (dependent, dependency, attribute) \
             DO UPDATE SET dependency_unit = EXCLUDED.dependency_unit, value = EXCLUDED.value",
            self.table
        );
        self.database
            .client()
            .await?
            .execute(
                &sql,
                &[
                    &dependent.to_string(),
                    &members,
                    &units,
                    &attributes,
                    &values,
                ],
            )
            .await
            .map_err(checkpoint_failed)?;
        Ok(())
    }

    /// Deletes what is not among the dependencies, then adds them.
    ///
    /// # Errors
    ///
    /// [`Error::Connection`](crate::Error::Connection) if the database cannot be reached, or
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if a statement fails.
    async fn replace(&self, dependent: &ResourceId, dependencies: &[Dependency]) -> Result<()> {
        let members: Vec<String> = dependencies
            .iter()
            .map(|dependency| dependency.member.to_string())
            .collect();
        let attributes: Vec<&str> = dependencies
            .iter()
            .map(|dependency| dependency.attribute.as_str())
            .collect();
        let sql = format!(
            "DELETE FROM {table} t WHERE t.dependent = $1 AND NOT EXISTS (\
                 SELECT 1 FROM unnest($2::text[], $3::text[]) AS kept(member, attribute) \
                 WHERE kept.member = t.dependency AND kept.attribute = t.attribute)",
            table = self.table
        );
        self.database
            .client()
            .await?
            .execute(&sql, &[&dependent.to_string(), &members, &attributes])
            .await
            .map_err(checkpoint_failed)?;
        self.add(dependent, dependencies).await
    }

    /// `SELECT DISTINCT dependent ... WHERE dependency_unit = $1 AND NOT
    /// (dependency = ANY($2))`, sorted.
    ///
    /// # Errors
    ///
    /// [`Error::Connection`](crate::Error::Connection) if the database cannot be reached, or
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the statement fails.
    async fn dependents_of(
        &self,
        unit: &ResourceId,
        kept: &[ResourceId],
    ) -> Result<Vec<ResourceId>> {
        let kept: Vec<String> = kept.iter().map(ToString::to_string).collect();
        let sql = format!(
            "SELECT DISTINCT dependent FROM {} \
             WHERE dependency_unit = $1 AND NOT (dependency = ANY($2::text[])) \
             ORDER BY dependent",
            self.table
        );
        let rows = self
            .database
            .client()
            .await?
            .query(&sql, &[&unit.to_string(), &kept])
            .await
            .map_err(checkpoint_failed)?;
        Ok(resources(rows))
    }

    /// One statement per changed export: the dependents whose recorded
    /// `value IS DISTINCT FROM` the new one.
    ///
    /// # Errors
    ///
    /// [`Error::Connection`](crate::Error::Connection) if the database cannot be reached, or
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if a statement fails.
    async fn stale_dependents(
        &self,
        changed: &BTreeMap<String, Option<serde_json::Value>>,
    ) -> Result<Vec<ResourceId>> {
        let mut found = Vec::new();
        let sql = format!(
            "SELECT DISTINCT dependent FROM {} \
             WHERE dependency = $1 AND attribute = $2 AND value IS DISTINCT FROM $3::jsonb",
            self.table
        );
        for (name, value) in changed {
            let Some((member, attribute)) = export_parts(name) else {
                continue;
            };
            let rows = self
                .database
                .client()
                .await?
                .query(&sql, &[&member.to_string(), &attribute, value])
                .await
                .map_err(checkpoint_failed)?;
            found.extend(resources(rows));
        }
        found.sort();
        found.dedup();
        Ok(found)
    }
}

/// The first column of each row as a resource id, passing over any that
/// is not one with a warning.
fn resources(rows: Vec<tokio_postgres::Row>) -> Vec<ResourceId> {
    rows.iter()
        .filter_map(|row| {
            let text: String = row.get(0);
            match text.parse() {
                Ok(resource) => Some(resource),
                Err(error) => {
                    tracing::warn!(resource = %text, %error, "a row of the references table names no resource");
                    None
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_definition_names_the_table_and_its_index() {
        let database = Arc::new(Database::new("postgres://morph@localhost:1/morph", None).unwrap());
        let table = ReferenceTable::new(database, "refs");
        let definition = table.definition();
        assert!(definition.contains("CREATE TABLE IF NOT EXISTS \"refs\""));
        assert!(definition.contains("CREATE INDEX IF NOT EXISTS \"refs_unit\" ON \"refs\""));
        assert!(definition.contains("PRIMARY KEY (dependent, dependency, attribute)"));
        assert!(format!("{table:?}").contains("ReferenceTable"));
    }
}
