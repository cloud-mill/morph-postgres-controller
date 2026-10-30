//! What is still to be reconciled, kept in the database so that a restart
//! takes it up again.
//!
//! [`DirtyTable`] is the core's [`DirtyStore`] as a table of the same
//! database as the rows. In the durable checkpoint mode the resources of a
//! committed transaction are written to it before the transaction's
//! position is confirmed. From then on it is the table, and no longer the
//! stream, that remembers they need a look. At start the controller reads
//! the table and takes up whatever it holds. The table holds resource ids
//! and how they fared. It holds no positions in the log and nothing of any
//! row.
//!
//! | Column | What it holds |
//! |--------|---------------|
//! | `resource` | The resource, as `kind.name`. The primary key, so a resource has one row however often it is marked. |
//! | `marked_at` | When it was last marked. |
//! | `attempts` | How many attempts had failed when a failure was last recorded. Zero until then. |
//! | `last_error` | Why the last recorded attempt failed, or what the resource is waiting for. Null until one has, or it has. |
//! | `last_failed_at` | When it failed. Null until one has. |
//!
//! [`DirtyStore::mark`] adds a row, or moves `marked_at` on a row that is
//! there. [`DirtyStore::done`] deletes the row. [`DirtyStore::failed`]
//! writes the last three columns of a row that is there, and
//! [`DirtyStore::note`] the `last_error` alone. The [`Ledger`], which works
//! out the position that is safe to confirm, is the core's.

use std::sync::Arc;

use async_trait::async_trait;
use morph::ResourceId;

pub use morph_controller_core::checkpoint::{DirtyRow, DirtyStore, Ledger};

use crate::config::quote_identifier;
use crate::error::Result;
use crate::postgres::checkpoint_failed;
use crate::postgres::database::Database;

/// The table of resources that are waiting, or that could not be
/// reconciled.
///
/// A handle on the table. It shares the [`Database`] and holds nothing else
/// but the table's name.
#[derive(Debug)]
pub struct DirtyTable {
    database: Arc<Database>,
    table: String,
}

impl DirtyTable {
    /// The table called `table` in `database`.
    ///
    /// The name is quoted, and so is taken exactly as written. Nothing is
    /// read or written yet.
    pub fn new(database: Arc<Database>, table: &str) -> Self {
        Self {
            database,
            table: quote_identifier(table),
        }
    }

    /// The SQL that makes the table.
    ///
    /// The statement does nothing if a table of that name exists, whatever
    /// its columns.
    pub fn definition(&self) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {} (\n    \
                 resource        text PRIMARY KEY,\n    \
                 marked_at       timestamptz NOT NULL DEFAULT now(),\n    \
                 attempts        integer NOT NULL DEFAULT 0,\n    \
                 last_error      text,\n    \
                 last_failed_at  timestamptz\n\
             )",
            self.table
        )
    }
}

#[async_trait]
impl DirtyStore for DirtyTable {
    /// Makes the table if it does not exist.
    ///
    /// # Errors
    ///
    /// [`Error::Connection`](crate::Error::Connection) if the database cannot be reached, or
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the statement fails, as when the user may
    /// not create tables.
    async fn prepare(&self) -> Result<()> {
        self.database
            .client()
            .await?
            .batch_execute(&self.definition())
            .await
            .map_err(checkpoint_failed)
    }

    /// Records that these resources are waiting, in one statement, so that
    /// either every one of them is recorded or none is.
    ///
    /// # Errors
    ///
    /// [`Error::Connection`](crate::Error::Connection) if the database cannot be reached, or
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the statement fails. It fails if
    /// `resources` names a resource twice, which PostgreSQL does not allow
    /// in one statement of this kind.
    async fn mark(&self, resources: &[ResourceId]) -> Result<()> {
        if resources.is_empty() {
            return Ok(());
        }
        let names: Vec<String> = resources.iter().map(ToString::to_string).collect();
        let sql = format!(
            "INSERT INTO {} (resource) SELECT unnest($1::text[]) \
             ON CONFLICT (resource) DO UPDATE SET marked_at = now()",
            self.table
        );
        self.database
            .client()
            .await?
            .execute(&sql, &[&names])
            .await
            .map_err(checkpoint_failed)?;
        Ok(())
    }

    /// Everything that is waiting, oldest mark first.
    ///
    /// # Errors
    ///
    /// [`Error::Connection`](crate::Error::Connection) if the database cannot be reached, or
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the statement fails, as when the table does
    /// not exist.
    async fn load(&self) -> Result<Vec<DirtyRow>> {
        let sql = format!(
            "SELECT resource, attempts FROM {} ORDER BY marked_at",
            self.table
        );
        let rows = self
            .database
            .client()
            .await?
            .query(&sql, &[])
            .await
            .map_err(checkpoint_failed)?;
        let mut loaded = Vec::with_capacity(rows.len());
        for row in rows {
            let name: String = row.get(0);
            match name.parse() {
                Ok(resource) => loaded.push(DirtyRow {
                    resource,
                    attempts: u32::try_from(row.get::<_, i32>(1)).unwrap_or_default(),
                }),
                Err(error) => {
                    tracing::warn!(resource = %name, %error, "a row of the dirty table names no resource")
                }
            }
        }
        Ok(loaded)
    }

    /// Records that a resource is reconciled, by deleting its row.
    ///
    /// # Errors
    ///
    /// [`Error::Connection`](crate::Error::Connection) if the database cannot be reached, or
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the statement fails.
    async fn done(&self, resource: &ResourceId) -> Result<()> {
        let sql = format!("DELETE FROM {} WHERE resource = $1", self.table);
        self.database
            .client()
            .await?
            .execute(&sql, &[&resource.to_string()])
            .await
            .map_err(checkpoint_failed)?;
        Ok(())
    }

    /// Writes `text` as the resource's `last_error`, leaving the attempts
    /// and the time of the last failure alone.
    ///
    /// # Errors
    ///
    /// [`Error::Connection`](crate::Error::Connection) if the database cannot be reached, or
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the statement fails.
    async fn note(&self, resource: &ResourceId, text: &str) -> Result<()> {
        let sql = format!(
            "UPDATE {} SET last_error = $2 WHERE resource = $1",
            self.table
        );
        self.database
            .client()
            .await?
            .execute(&sql, &[&resource.to_string(), &text])
            .await
            .map_err(checkpoint_failed)?;
        Ok(())
    }

    /// Records that reconciling a resource failed, and why.
    ///
    /// # Errors
    ///
    /// [`Error::Connection`](crate::Error::Connection) if the database cannot be reached, or
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the statement fails.
    async fn failed(&self, resource: &ResourceId, attempts: u32, reason: &str) -> Result<()> {
        let sql = format!(
            "UPDATE {} SET attempts = $2, last_error = $3, last_failed_at = now() WHERE resource = $1",
            self.table
        );
        let attempts = i32::try_from(attempts).unwrap_or(i32::MAX);
        self.database
            .client()
            .await?
            .execute(&sql, &[&resource.to_string(), &attempts, &reason])
            .await
            .map_err(checkpoint_failed)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_definition_names_the_table() {
        let database = Arc::new(Database::new("postgres://morph@localhost:1/morph", None).unwrap());
        let table = DirtyTable::new(database, "dirty");
        let definition = table.definition();
        assert!(definition.contains("CREATE TABLE IF NOT EXISTS \"dirty\""));
        assert!(definition.contains("resource        text PRIMARY KEY"));
        assert!(format!("{table:?}").contains("DirtyTable"));
    }
}
