//! Reading rows back as desired state.
//!
//! When a worker reconciles a resource, the source reads the row that
//! describes it, as it is at that moment, and the core makes of it the
//! desired state the engine is given. Nothing here looks at the stream. The
//! stream said which resource to look at, and that is all it is trusted
//! with.
//!
//! Every row is read as `to_jsonb(row)`, one JSON entry per column, which
//! is the shape the core's [`Row`] holds.

use std::sync::Arc;

use morph::{DesiredState, ResourceId};
use morph_controller_core::mapping::desired::desired_from_row;

use crate::config::{Mapping, STACK_KIND, quote_identifier};
use crate::error::Result;
use crate::mapping::Row;
use crate::postgres::database::Database;
use crate::postgres::failed;

/// Reads rows of the mapped tables over the ordinary connection.
///
/// The filters of the configuration play no part here. They decide which
/// tables are listened to. Here every mapped table is read.
#[derive(Clone, Debug)]
pub struct RowReader {
    database: Arc<Database>,
    mappings: Arc<[Mapping]>,
}

impl RowReader {
    /// A reader over these mappings, through `database`.
    pub fn new(database: Arc<Database>, mappings: Vec<Mapping>) -> Self {
        Self {
            database,
            mappings: mappings.into(),
        }
    }

    /// Every mapping, in the order of the configuration.
    pub fn mappings(&self) -> &[Mapping] {
        &self.mappings
    }

    /// The mappings that can hold a resource of `kind`.
    ///
    /// Those are the mappings fixed to that kind, and every mapping that
    /// reads the kind from a column.
    fn mappings_for(&self, kind: &str) -> impl Iterator<Item = &Mapping> {
        self.mappings
            .iter()
            .filter(move |mapping| match &mapping.resource_type {
                Some(fixed) => fixed == kind,
                None => mapping.type_column.is_some(),
            })
    }

    /// The desired state of `resource`: one resource if a row describes
    /// it, and nothing if no row does.
    ///
    /// The row is found as [`row`](Self::row) finds it. An empty desired
    /// state is not a failure. It is how Morph is told that the row has
    /// gone.
    ///
    /// # Errors
    ///
    /// Whatever [`row`](Self::row) fails with, and
    /// [`Error::Mapping`](crate::Error::Mapping) if the row cannot be made
    /// into a desired state, for the reasons [`desired_from_row`] gives.
    pub async fn load(&self, resource: &ResourceId) -> Result<DesiredState> {
        match self.row(resource).await? {
            Some(row) => desired_from_row(&row.mapping, resource.clone(), row.json),
            None => Ok(DesiredState::new()),
        }
    }

    /// The row that describes `resource`, if any does.
    ///
    /// The mappings that could hold the kind are tried in the order of the
    /// configuration, and the first row found is used. A row is found by
    /// the text of its id column, and of its type column where the mapping
    /// has one.
    ///
    /// # Errors
    ///
    /// [`Error::Connection`](crate::Error::Connection) if the database
    /// cannot be reached. [`Error::Query`](crate::Error::Query) if the
    /// server refuses the statement, as when a mapped table or column does
    /// not exist.
    pub async fn row(&self, resource: &ResourceId) -> Result<Option<Row>> {
        let client = self.database.client().await?;
        let kind = resource.kind().as_str();
        for mapping in self.mappings_for(kind) {
            let table = format!(
                "{}.{}",
                quote_identifier(&mapping.schema),
                quote_identifier(&mapping.table)
            );
            let row = match &mapping.type_column {
                Some(column) => {
                    let sql = format!(
                        "SELECT to_jsonb(t) FROM {table} t WHERE ({})::text = $1 AND ({})::text = $2 LIMIT 1",
                        quote_identifier(&mapping.id_column),
                        quote_identifier(column)
                    );
                    client
                        .query_opt(&sql, &[&resource.name(), &kind])
                        .await
                        .map_err(failed)?
                }
                None => {
                    let sql = format!(
                        "SELECT to_jsonb(t) FROM {table} t WHERE ({})::text = $1 LIMIT 1",
                        quote_identifier(&mapping.id_column)
                    );
                    client
                        .query_opt(&sql, &[&resource.name()])
                        .await
                        .map_err(failed)?
                }
            };
            if let Some(row) = row {
                let json: serde_json::Value = row.get(0);
                return Ok(Some(Row::new(resource.clone(), mapping.clone(), json)));
            }
        }
        Ok(None)
    }

    /// The row of `mapping`'s table whose id column reads `name`, if there
    /// is one, with the resource it describes.
    ///
    /// This is how a referenced row is found: by the table the reference
    /// points at and the text in the referring column. The row's kind is
    /// the mapping's, or is read from its type column.
    ///
    /// # Errors
    ///
    /// As [`row`](Self::row) fails, and
    /// [`Error::Mapping`](crate::Error::Mapping) if the row has no kind, or
    /// its kind and name do not make a valid resource id, as
    /// [`Row::from_json`] says.
    pub async fn row_in(&self, mapping: &Mapping, name: &str) -> Result<Option<Row>> {
        let client = self.database.client().await?;
        let sql = format!(
            "SELECT to_jsonb(t) FROM {}.{} t WHERE ({})::text = $1 LIMIT 1",
            quote_identifier(&mapping.schema),
            quote_identifier(&mapping.table),
            quote_identifier(&mapping.id_column)
        );
        let Some(row) = client.query_opt(&sql, &[&name]).await.map_err(failed)? else {
            return Ok(None);
        };
        let json: serde_json::Value = row.get(0);
        Row::from_json(mapping, json).map(Some)
    }

    /// Every row of the stack `group`, across the tables grouped into it,
    /// each with the resource it describes.
    ///
    /// A table with `stack = group` gives all its rows; a table with a
    /// `stack_column` gives the rows whose column reads `group`. The rows
    /// are sorted by resource, so that what is made from them is the same
    /// whatever order the database gives them in.
    ///
    /// # Errors
    ///
    /// As [`row`](Self::row) fails, and
    /// [`Error::Mapping`](crate::Error::Mapping) if a row has no id or
    /// kind, or they do not make a valid resource id.
    pub async fn group_rows(&self, group: &str) -> Result<Vec<Row>> {
        let client = self.database.client().await?;
        let mut rows = Vec::new();
        for mapping in self.mappings.iter() {
            let table = format!(
                "{}.{}",
                quote_identifier(&mapping.schema),
                quote_identifier(&mapping.table)
            );
            let found = match (&mapping.stack, &mapping.stack_column) {
                (Some(stack), _) if stack == group => client
                    .query(&format!("SELECT to_jsonb(t) FROM {table} t"), &[])
                    .await
                    .map_err(failed)?,
                (None, Some(column)) => {
                    let sql = format!(
                        "SELECT to_jsonb(t) FROM {table} t WHERE ({})::text = $1",
                        quote_identifier(column)
                    );
                    client.query(&sql, &[&group]).await.map_err(failed)?
                }
                _ => continue,
            };
            for row in found {
                let json: serde_json::Value = row.get(0);
                rows.push(Row::from_json(mapping, json)?);
            }
        }
        rows.sort_by(|a, b| a.resource.cmp(&b.resource));
        Ok(rows)
    }

    /// Every resource that a row describes, across the mapped tables.
    ///
    /// Each table is read whole, for its id and kind alone. The result is
    /// sorted and holds each resource once. Rows that name no valid
    /// resource are passed over with a warning. A table grouped into
    /// stacks gives its stacks instead, `stack.<name>`, one for its
    /// fixed `stack` or one per distinct value of its `stack_column`.
    ///
    /// # Errors
    ///
    /// As [`row`](Self::row) fails.
    pub async fn all_resources(&self) -> Result<Vec<ResourceId>> {
        let client = self.database.client().await?;
        let mut resources = Vec::new();
        for mapping in self.mappings.iter() {
            let table = format!(
                "{}.{}",
                quote_identifier(&mapping.schema),
                quote_identifier(&mapping.table)
            );
            if let Some(stack) = &mapping.stack {
                if let Ok(resource) = ResourceId::new(STACK_KIND, stack.as_str()) {
                    resources.push(resource);
                }
                continue;
            }
            if let Some(column) = &mapping.stack_column {
                let sql = format!(
                    "SELECT DISTINCT ({})::text FROM {table}",
                    quote_identifier(column)
                );
                for row in client.query(&sql, &[]).await.map_err(failed)? {
                    let stack: Option<String> = row.get(0);
                    match stack.map(|stack| ResourceId::new(STACK_KIND, stack)) {
                        Some(Ok(resource)) => resources.push(resource),
                        Some(Err(error)) => {
                            tracing::warn!(table = %mapping.qualified_table(), %error, "a row names no valid stack")
                        }
                        None => {
                            tracing::warn!(table = %mapping.qualified_table(), "a row has no stack")
                        }
                    }
                }
                continue;
            }
            let id = quote_identifier(&mapping.id_column);
            let rows = match &mapping.type_column {
                Some(column) => {
                    let sql = format!(
                        "SELECT ({id})::text, ({})::text FROM {table}",
                        quote_identifier(column)
                    );
                    client.query(&sql, &[]).await.map_err(failed)?
                }
                None => {
                    let sql = format!("SELECT ({id})::text, $1::text FROM {table}");
                    client
                        .query(&sql, &[&mapping.resource_type])
                        .await
                        .map_err(failed)?
                }
            };
            for row in rows {
                let (name, kind): (Option<String>, Option<String>) = (row.get(0), row.get(1));
                match (name, kind) {
                    (Some(name), Some(kind)) => match ResourceId::new(kind, name) {
                        Ok(resource) => resources.push(resource),
                        Err(error) => {
                            tracing::warn!(table = %mapping.qualified_table(), %error, "a row names no valid resource")
                        }
                    },
                    _ => {
                        tracing::warn!(table = %mapping.qualified_table(), "a row has no id or no kind")
                    }
                }
            }
        }
        resources.sort();
        resources.dedup();
        Ok(resources)
    }
}
