//! The ordinary connection: the queries the controller makes on its own
//! behalf, as opposed to the stream of changes.
//!
//! The reader in [`super::replication`] has a connection of its own for the
//! stream. Every other statement goes through one shared [`Database`]:
//! looking at and making the slot and the publication, reading how far the
//! slot has got as a [`SlotLag`], reading rows as desired state, and keeping
//! the dirty table.

use std::path::Path;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio_postgres::Client;

use super::{Lsn, failed};
use crate::config::quote_identifier;
use crate::error::{Error, Result};

/// A connection that is made when first needed, and made again when it is
/// found to have gone.
///
/// One client is shared by everyone who asks for it. Statements from
/// different tasks go down the same connection, and no lock is held while
/// a statement runs. Whether the connection has closed is looked at each
/// time [`client`](Self::client) is called, and not in between. A statement
/// that was under way when the connection went fails, and is not tried
/// again here: that is left to the caller.
///
/// # Examples
///
/// ```no_run
/// # async fn example() -> morph_postgres_controller::Result<()> {
/// use morph_postgres_controller::postgres::database::Database;
///
/// let database = Database::new("postgres://morph:secret@localhost/morph", None)?;
/// // Nothing is connected until the first statement.
/// let lag = database.slot_lag("morph_controller").await?;
/// println!("{} bytes behind", lag.bytes());
/// # Ok(())
/// # }
/// ```
pub struct Database {
    config: tokio_postgres::Config,
    /// `None` when the URL says `sslmode=disable`.
    tls: Option<Arc<rustls::ClientConfig>>,
    /// Whether the URL leaves TLS to preference, which is the default: TLS
    /// is tried first, and a connection in the clear is made if TLS cannot
    /// be set up.
    prefer: bool,
    location: String,
    /// The client in use, if one was ever made.
    ///
    /// The lock is held while the client is looked at and while a new
    /// connection is made, so that tasks which find the connection gone
    /// wait for one new connection between them. It is never held while a
    /// statement runs.
    client: Mutex<Option<Arc<Client>>>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("location", &self.location)
            .finish_non_exhaustive()
    }
}

/// Where a replication slot stands, and where the log ends.
///
/// All four values come from one statement, so they belong to one moment. A
/// position that the server gives as null is read as `0/0`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotLag {
    /// The oldest position the slot still holds back.
    ///
    /// The server keeps all of the log from here on for the slot's sake.
    pub restart_lsn: Lsn,
    /// The position the controller last confirmed.
    ///
    /// A connection to the slot is sent the transactions that committed
    /// after it.
    pub confirmed_flush_lsn: Lsn,
    /// Where the log currently ends.
    pub current_lsn: Lsn,
    /// Whether something is streaming from the slot now.
    pub active: bool,
}

impl SlotLag {
    /// How much log has been written since the confirmed position.
    ///
    /// This is the lag that is reported and warned about. Zero if the
    /// confirmed position is not behind the end of the log.
    pub fn bytes(&self) -> u64 {
        self.confirmed_flush_lsn.bytes_until(self.current_lsn)
    }
}

impl Database {
    /// A database at `url`, trusting the authorities in `ca` besides the
    /// public ones.
    ///
    /// Nothing is connected yet. The URL is the one `psql` takes. If it
    /// names no application, the connection calls itself
    /// `morph-postgres-controller`. With `sslmode=disable` the connection is
    /// in the clear. With `sslmode=require` it uses TLS or fails. Otherwise,
    /// which is the default `prefer`, TLS is tried first, and if it cannot
    /// be set up, as when the server's certificate cannot be verified, the
    /// connection is made in the clear and a warning says so. Whenever TLS
    /// is used, the server's certificate is verified.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] if the URL cannot be parsed, or if `ca` cannot be
    /// read as a file of certificates.
    pub fn new(url: &str, ca: Option<&Path>) -> Result<Self> {
        let mut config: tokio_postgres::Config = url
            .parse()
            .map_err(|error: tokio_postgres::Error| Error::Config(error.to_string()))?;
        if config.get_application_name().is_none() {
            config.application_name("morph-postgres-controller");
        }
        let tls = match config.get_ssl_mode() {
            tokio_postgres::config::SslMode::Disable => None,
            _ => Some(super::tls::client_config(ca)?),
        };
        let prefer = matches!(
            config.get_ssl_mode(),
            tokio_postgres::config::SslMode::Prefer
        );
        let location = describe(&config);
        Ok(Self {
            config,
            tls,
            prefer,
            location,
            client: Mutex::new(None),
        })
    }

    /// Where the database is, as `postgres://user@host:port/database`.
    ///
    /// The password is left out, so this is safe to log.
    pub fn location(&self) -> &str {
        &self.location
    }

    /// The shared client, connecting first if there is none or the last
    /// one has closed.
    ///
    /// Callers that arrive while a connection is being made wait for it and
    /// are given the same client. A failed attempt changes nothing, and the
    /// next call tries again. Dropping the future while it is connecting
    /// abandons the attempt and lets the next caller in.
    ///
    /// # Errors
    ///
    /// [`Error::Connection`], naming the database, if the connection cannot
    /// be made. That includes a server that answers and refuses, as for a
    /// wrong password.
    pub async fn client(&self) -> Result<Arc<Client>> {
        let mut slot = self.client.lock().await;
        if let Some(client) = slot.as_ref()
            && !client.is_closed()
        {
            return Ok(Arc::clone(client));
        }
        let client = Arc::new(self.connect().await?);
        *slot = Some(Arc::clone(&client));
        Ok(client)
    }

    /// Opens a connection, and spawns the task that drives it.
    ///
    /// The task ends when the connection does, and says why at debug level
    /// if that was an error. Nothing else is told: the next call to
    /// [`client`](Self::client) finds the client closed.
    async fn connect(&self) -> Result<Client> {
        let failed =
            |error: tokio_postgres::Error| Error::Connection(format!("{}: {error}", self.location));
        let Some(tls) = &self.tls else {
            return self.connect_in_the_clear().await.map_err(failed);
        };
        match self.connect_with_tls(tls).await {
            Ok(client) => Ok(client),
            // Under `prefer`, a failure that the server did not report, such
            // as a certificate that cannot be verified, means TLS cannot be
            // had, and the connection is made in the clear, as libpq would.
            // A failure the server reported, such as a refused password,
            // would only be repeated.
            Err(error) if self.prefer && error.as_db_error().is_none() => {
                tracing::warn!(
                    location = %self.location,
                    %error,
                    "TLS could not be set up; connecting in the clear, as sslmode=prefer allows. Set sslmode=require to insist on TLS"
                );
                self.connect_in_the_clear().await.map_err(failed)
            }
            Err(error) => Err(failed(error)),
        }
    }

    /// Connects over TLS, and lets the connection run on its own task.
    async fn connect_with_tls(
        &self,
        tls: &Arc<rustls::ClientConfig>,
    ) -> std::result::Result<Client, tokio_postgres::Error> {
        let connector = tokio_postgres_rustls::MakeRustlsConnect::new((**tls).clone());
        let (client, connection) = self.config.connect(connector).await?;
        let location = self.location.clone();
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%location, %error, "the database connection ended");
            }
        });
        Ok(client)
    }

    /// Connects without TLS, and lets the connection run on its own task.
    async fn connect_in_the_clear(&self) -> std::result::Result<Client, tokio_postgres::Error> {
        let (client, connection) = self.config.connect(tokio_postgres::NoTls).await?;
        let location = self.location.clone();
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%location, %error, "the database connection ended");
            }
        });
        Ok(client)
    }

    /// Whether a replication slot called `slot` exists.
    ///
    /// Any slot on the server counts, whatever its kind and its database.
    ///
    /// # Errors
    ///
    /// [`Error::Connection`] if the database cannot be reached, or
    /// [`Error::Query`] if the server refuses the statement.
    pub async fn slot_exists(&self, slot: &str) -> Result<bool> {
        let row = self
            .client()
            .await?
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .map_err(failed)?;
        Ok(row.get::<_, i64>(0) > 0)
    }

    /// Makes a logical replication slot for `pgoutput`, and gives the
    /// position it starts from.
    ///
    /// The slot is a lasting one. From now on the server keeps the log that
    /// the slot has not confirmed, until the slot is dropped.
    ///
    /// # Errors
    ///
    /// [`Error::Query`] if the server refuses, as when the name is taken,
    /// no slot is free, or `wal_level` is not `logical`.
    /// [`Error::Connection`] if the database cannot be reached.
    /// [`Error::Protocol`] if the position in the answer cannot be read.
    pub async fn create_slot(&self, slot: &str) -> Result<Lsn> {
        let row = self
            .client()
            .await?
            .query_one(
                "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot],
            )
            .await
            .map_err(failed)?;
        row.get::<_, String>(0).parse().map_err(Error::Protocol)
    }

    /// Whether a publication called `publication` exists in this database.
    ///
    /// # Errors
    ///
    /// [`Error::Connection`] if the database cannot be reached, or
    /// [`Error::Query`] if the server refuses the statement.
    pub async fn publication_exists(&self, publication: &str) -> Result<bool> {
        let row = self
            .client()
            .await?
            .query_one(
                "SELECT count(*) FROM pg_publication WHERE pubname = $1",
                &[&publication],
            )
            .await
            .map_err(failed)?;
        Ok(row.get::<_, i64>(0) > 0)
    }

    /// Makes a publication of these tables, each given as schema and table.
    ///
    /// The names are quoted, and so are taken exactly as written.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] if `tables` is empty. [`Error::Query`] if the
    /// server refuses, as when the publication exists already, a table does
    /// not, or the user may not publish it. [`Error::Connection`] if the
    /// database cannot be reached.
    pub async fn create_publication(
        &self,
        publication: &str,
        tables: &[(String, String)],
    ) -> Result<()> {
        if tables.is_empty() {
            return Err(Error::Config(
                "a publication needs at least one table".into(),
            ));
        }
        let listed: Vec<String> = tables
            .iter()
            .map(|(schema, table)| {
                format!("{}.{}", quote_identifier(schema), quote_identifier(table))
            })
            .collect();
        let sql = format!(
            "CREATE PUBLICATION {} FOR TABLE {}",
            quote_identifier(publication),
            listed.join(", ")
        );
        self.client()
            .await?
            .batch_execute(&sql)
            .await
            .map_err(failed)?;
        Ok(())
    }

    /// Where the slot called `slot` stands, and where the log ends.
    ///
    /// # Errors
    ///
    /// [`Error::Checkpoint`] if there is no such slot.
    /// [`Error::Connection`] if the database cannot be reached, or
    /// [`Error::Query`] if the server refuses the statement.
    /// [`Error::Protocol`] if a position in the answer cannot be read.
    pub async fn slot_lag(&self, slot: &str) -> Result<SlotLag> {
        let row = self
            .client()
            .await?
            .query_opt(
                "SELECT restart_lsn::text, confirmed_flush_lsn::text, pg_current_wal_lsn()::text, active \
                 FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .map_err(failed)?
            .ok_or_else(|| Error::Checkpoint(format!("the slot {slot} does not exist")))?;
        let lsn = |index: usize| -> Result<Lsn> {
            row.get::<_, Option<String>>(index)
                .unwrap_or_else(|| "0/0".to_owned())
                .parse()
                .map_err(Error::Protocol)
        };
        Ok(SlotLag {
            restart_lsn: lsn(0)?,
            confirmed_flush_lsn: lsn(1)?,
            current_lsn: lsn(2)?,
            active: row.get(3),
        })
    }
}

/// Where a configuration points, without the password.
///
/// Every host is listed. A host with no port of its own takes the first
/// port given, or 5432.
fn describe(config: &tokio_postgres::Config) -> String {
    let ports = config.get_ports();
    let hosts: Vec<String> = config
        .get_hosts()
        .iter()
        .enumerate()
        .map(|(index, host)| {
            let port = ports
                .get(index)
                .or_else(|| ports.first())
                .copied()
                .unwrap_or(5432);
            match host {
                tokio_postgres::config::Host::Tcp(host) => format!("{host}:{port}"),
                #[cfg(unix)]
                tokio_postgres::config::Host::Unix(path) => path.display().to_string(),
            }
        })
        .collect();
    format!(
        "postgres://{}@{}/{}",
        config.get_user().unwrap_or("?"),
        hosts.join(","),
        config.get_dbname().unwrap_or("?")
    )
}
