//! Morph's records in the database.
//!
//! The core keeps Morph's records in a directory of files or in memory,
//! and takes any other backend through its [`RecordStores`] trait.
//! [`PostgresRecordStores`] is that backend for the `postgres` setting of
//! `[morph] state`: Morph's own table in the same database as the rows,
//! through the `morph-postgres` crate. [`state_stores`] chooses between the
//! three as the configuration says.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use morph::{ResourceId, StateStore};
use morph_postgres::PostgresStateStore;
use tokio::sync::Mutex;

pub use morph_controller_core::stores::{RecordStores, StateStores};

use crate::config::{MorphConfig, PostgresConfig, StateConfig};
use crate::error::Result;
use crate::postgres::database::Database;
use crate::postgres::failed;

/// Morph's records in Morph's own table of the database.
///
/// The record of a resource is the state whose name is the prefix followed
/// by the resource id, as in `controller/aws_sqs_queue.orders`. A store is
/// connected when its resource is first reconciled, and is kept from then
/// on. Each store has a connection of its own, so the connections held
/// grow with the number of resources that have been reconciled.
pub struct PostgresRecordStores {
    /// How to connect.
    options: Box<morph_postgres::Options>,
    /// What each record's name starts with.
    prefix: String,
    /// The stores connected so far, by resource. Nothing is ever taken
    /// out.
    connected: Mutex<HashMap<ResourceId, PostgresStateStore>>,
    /// The ordinary connection, for listing the records.
    database: Arc<Database>,
}

impl std::fmt::Debug for PostgresRecordStores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresRecordStores")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl PostgresRecordStores {
    /// Stores in the database at `url`, trusting the authorities in `ca`
    /// besides the public ones, with records named under `prefix`.
    ///
    /// `database` is the ordinary connection, used to list the records.
    /// Nothing is connected, read or written yet.
    ///
    /// # Errors
    ///
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the URL cannot be
    /// read, or the certificate file cannot be read or holds no usable
    /// certificate.
    pub fn new(
        url: &str,
        ca: Option<&Path>,
        prefix: &str,
        database: Arc<Database>,
    ) -> Result<Self> {
        let mut options = morph_postgres::Options::new(url)?;
        if let Some(ca) = ca {
            options = options.trusting(ca)?;
        }
        Ok(Self {
            options: Box::new(options),
            prefix: prefix.to_owned(),
            connected: Mutex::new(HashMap::new()),
            database,
        })
    }
}

#[async_trait]
impl RecordStores for PostgresRecordStores {
    /// The store connected for the resource before, or one connected now
    /// and kept.
    ///
    /// Connecting also makes Morph's table if it is not there. The lock on
    /// the kept stores is held while a new one connects, so other calls
    /// wait for it, whatever their resource.
    ///
    /// # Errors
    ///
    /// [`Error::Checkpoint`](crate::Error::Checkpoint) if the database
    /// cannot be reached, or Morph's table cannot be made.
    async fn store_for(&self, resource: &ResourceId) -> Result<Arc<dyn StateStore>> {
        let mut connected = self.connected.lock().await;
        if let Some(store) = connected.get(resource) {
            return Ok(Arc::new(store.clone()));
        }
        let store = (*self.options)
            .clone()
            .connect(&format!("{}{resource}", self.prefix))
            .await?;
        connected.insert(resource.clone(), store.clone());
        Ok(Arc::new(store))
    }

    /// The names in Morph's table that start with the prefix, with the
    /// prefix taken off, sorted.
    ///
    /// A name whose remainder is not a resource id is passed over. A table
    /// that does not exist yet means no records.
    ///
    /// # Errors
    ///
    /// [`Error::Connection`](crate::Error::Connection) or
    /// [`Error::Query`](crate::Error::Query) if the table cannot be read,
    /// for any reason but its absence.
    async fn managed(&self) -> Result<Vec<ResourceId>> {
        // A `%` or `_` in the prefix is escaped, so that it matches only
        // itself.
        let pattern = format!("{}%", self.prefix.replace('%', "\\%").replace('_', "\\_"));
        let sql = format!(
            "SELECT name FROM \"{}\" WHERE name LIKE $1",
            morph_postgres::DEFAULT_TABLE
        );
        let rows = match self.database.client().await?.query(&sql, &[&pattern]).await {
            Ok(rows) => rows,
            // Nothing has been recorded yet, so there is no table.
            Err(error)
                if error.code() == Some(&tokio_postgres::error::SqlState::UNDEFINED_TABLE) =>
            {
                Vec::new()
            }
            Err(error) => return Err(failed(error)),
        };
        let mut resources: Vec<ResourceId> = rows
            .iter()
            .filter_map(|row| {
                row.get::<_, String>(0)
                    .strip_prefix(self.prefix.as_str())
                    .map(str::to_owned)
            })
            .filter_map(|name| name.parse().ok())
            .collect();
        resources.sort();
        Ok(resources)
    }
}

/// The stores the configuration asks for: Morph's table in the database
/// for `postgres`, and the core's own for `file` and `memory`.
///
/// Nothing is connected, read or written yet.
///
/// # Errors
///
/// [`Error::Checkpoint`](crate::Error::Checkpoint) if the backend is
/// `postgres` and the URL cannot be read, or the certificate file cannot be
/// read or holds no usable certificate.
pub fn state_stores(
    config: &MorphConfig,
    postgres: &PostgresConfig,
    database: Arc<Database>,
) -> Result<StateStores> {
    match &config.state {
        StateConfig::Postgres => Ok(StateStores::named(Arc::new(PostgresRecordStores::new(
            &postgres.url,
            postgres.ca_certificate.as_deref(),
            &config.state_prefix,
            database,
        )?))),
        StateConfig::File { .. } | StateConfig::Memory => StateStores::new(config),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    fn database() -> Arc<Database> {
        Arc::new(Database::new("postgres://morph@localhost:1/morph", None).unwrap())
    }

    #[test]
    fn the_postgres_backend_checks_its_url_and_certificate_before_connecting() {
        let stores =
            PostgresRecordStores::new("postgres://morph@localhost/morph", None, "c/", database())
                .unwrap();
        assert!(format!("{stores:?}").contains("PostgresRecordStores"));
        let bad_url = PostgresRecordStores::new("not a url", None, "c/", database());
        assert!(matches!(bad_url.unwrap_err(), Error::Checkpoint(_)));
        let missing_ca = PostgresRecordStores::new(
            "postgres://morph@localhost/morph",
            Some(Path::new("/nonexistent/ca.pem")),
            "c/",
            database(),
        );
        assert!(matches!(missing_ca.unwrap_err(), Error::Checkpoint(_)));
    }

    #[test]
    fn the_configuration_chooses_the_backend() {
        let postgres: PostgresConfig =
            toml::from_str("url = \"postgres://morph@localhost/morph\"").unwrap();
        let chosen = state_stores(&MorphConfig::default(), &postgres, database()).unwrap();
        assert!(format!("{chosen:?}").contains("PostgresRecordStores"));
        let memory = MorphConfig {
            state: StateConfig::Memory,
            ..MorphConfig::default()
        };
        let chosen = state_stores(&memory, &postgres, database()).unwrap();
        assert!(format!("{chosen:?}").contains("Memory"));
    }
}
