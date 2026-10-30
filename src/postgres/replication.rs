//! Reading the stream of changes, and turning it into committed
//! transactions that name resources.
//!
//! The wire protocol is spoken by the `pgwire-replication` crate. What
//! arrives from it is decoded here with [`super::pgoutput`], related to
//! tables with [`super::relation`], gathered into transactions with the
//! core's [`TransactionBuffer`], and named with the mapper. Nothing leaves
//! this module until its transaction has committed. This is the
//! PostgreSQL half of [`Source::watch`](morph_controller_core::Source::watch).
//!
//! # What leaves
//!
//! A [`Transaction`] for every `COMMIT`: the resources the transaction
//! touched, each once, and the position to confirm for it. No row contents
//! leave, and nothing says what changed. A transaction that touched nothing
//! of interest leaves too, so that its position can be confirmed in turn.
//!
//! It leaves at the `COMMIT` and not before, because the `COMMIT` is what
//! gives the position to confirm, and because a transaction that is cut off
//! before it is sent again whole.
//!
//! # Confirmation
//!
//! The reader does not decide what has been dealt with. [`run`] is given a
//! shared `confirmed` position that someone else moves, and passes it on to
//! the server: as the place to start from on each connection, and as the
//! position reported while connected. The server keeps every transaction
//! that committed after the confirmed position, and sends those again on
//! the next connection.
//!
//! # Reconnection
//!
//! When the stream is lost, [`run`] closes the client, waits, and connects
//! again from the confirmed position. Each connection starts with no table
//! descriptions and no open transaction: a description is good for one
//! session only, and a transaction that was cut off is sent again whole.
//! Transactions that went to the outbox and were not yet confirmed are sent
//! to it a second time. Delivery is at least once.

use std::sync::Arc;
use std::time::Duration;

use morph::ResourceId;
use morph_controller_core::observe::Metrics;
use morph_controller_core::source::{
    Confirmed, Position, SourceEvent, Transaction, TransactionBuffer,
};
use pgwire_replication::client::ReplicationEvent;
use pgwire_replication::config::{SslMode, TlsConfig};
use pgwire_replication::{ReplicationClient, ReplicationConfig};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use super::Lsn;
use super::pgoutput::{self, Message};
use super::relation::RelationCache;
use crate::config::Backoff;
use crate::error::{Error, Result};
use crate::mapping::ResourceMapper;

/// Where and how to read the stream.
#[derive(Clone, Debug)]
pub struct StreamSettings {
    /// The database URL, as for `psql`.
    ///
    /// The stream goes to the first host and port of the URL, which must
    /// be a TCP address, as the user the URL names. Its `sslmode` decides
    /// the encryption. With `disable` there is none. With `require` the
    /// stream uses TLS and verifies the server's certificate and host
    /// name. With neither, the stream uses TLS if the server offers it, and
    /// in that mode the replication client does not verify the certificate.
    pub url: String,
    /// A PEM file of authorities to trust.
    ///
    /// On the stream these are trusted in place of the public authorities,
    /// not besides them as on the ordinary connection. That is how the
    /// replication client treats the file it is given.
    pub ca_certificate: Option<std::path::PathBuf>,
    /// The slot to read from, which must exist.
    pub slot: String,
    /// The publication whose tables the server sends changes for.
    pub publication: String,
    /// How often to tell the server how far the controller has got.
    ///
    /// The confirmed position is handed to the replication client this
    /// often, and the client reports to the server this often, whether or
    /// not anything arrives.
    pub status_interval: Duration,
    /// How reconnecting is paced.
    pub reconnect: Backoff,
    /// Whether to tell the server how far the controller has got at all.
    ///
    /// Off only in tests of what happens when it is not told. With it off,
    /// each connection still starts from the confirmed position, and the
    /// replication client reports that starting position by itself.
    pub acknowledge: bool,
}

/// The parts of a URL that the replication client is given one by one.
struct Parts {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    tls: TlsConfig,
}

/// Takes the database URL apart for the replication client.
///
/// Only the first host and the first port are used. A URL with no host
/// means `localhost`, one with no port means 5432, and one with no database
/// means the database called after the user.
///
/// The `sslmode` of the URL becomes the client's TLS mode: `disable` is no
/// TLS, and `require` is the client's full verification of chain and host
/// name. A URL that says `prefer`, or says nothing, gets the client's
/// `Prefer` mode, which falls back to the clear if the server has no TLS
/// and, as of `pgwire-replication` 0.4.1, does not verify the certificate
/// when it has.
fn parts(settings: &StreamSettings) -> Result<Parts> {
    let parsed: tokio_postgres::Config = settings
        .url
        .parse()
        .map_err(|error: tokio_postgres::Error| Error::Config(error.to_string()))?;
    let host = match parsed.get_hosts().first() {
        Some(tokio_postgres::config::Host::Tcp(host)) => host.clone(),
        #[cfg(unix)]
        Some(tokio_postgres::config::Host::Unix(_)) => {
            return Err(Error::Config(
                "the stream of changes needs a TCP address, not a socket".into(),
            ));
        }
        None => "localhost".to_owned(),
    };
    let user = parsed
        .get_user()
        .map(str::to_owned)
        .ok_or_else(|| Error::Config("the database URL names no user".into()))?;
    let mut tls = match parsed.get_ssl_mode() {
        tokio_postgres::config::SslMode::Disable => TlsConfig::disabled(),
        tokio_postgres::config::SslMode::Require => {
            TlsConfig::verify_full(settings.ca_certificate.clone())
        }
        _ => TlsConfig::verify_full(settings.ca_certificate.clone()),
    };
    if matches!(
        parsed.get_ssl_mode(),
        tokio_postgres::config::SslMode::Prefer
    ) {
        tls.mode = SslMode::Prefer;
    }
    Ok(Parts {
        host,
        port: parsed.get_ports().first().copied().unwrap_or(5432),
        database: parsed
            .get_dbname()
            .map(str::to_owned)
            .unwrap_or_else(|| user.clone()),
        password: parsed
            .get_password()
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
            .unwrap_or_default(),
        user,
        tls,
    })
}

/// Reads the stream until told to stop, connecting again whenever it is
/// lost.
///
/// Committed transactions go to `outbox`, in the order they committed. When
/// `outbox` is full the reader waits for room and reads nothing more, which
/// is how a slow consumer slows the reading of the log. The replication
/// client goes on reporting to the server from a task of its own meanwhile,
/// so the server does not give the connection up as dead.
///
/// The position in `confirmed` is what the server is told the controller
/// has dealt with. The reader never moves it. It reads it when it connects,
/// as the position to start from. Unless `settings.acknowledge` is off, it
/// also reads it every `status_interval`, to hand it to the replication
/// client, which reports it in its status updates, and once more when a
/// connection ends. The core's ledger moves it, and the core speaks of it
/// as a [`Position`]; here it is read back as an [`Lsn`].
///
/// When the stream is lost, the reader waits before it tries again. The
/// wait is what `settings.reconnect` gives for the number of failures since
/// a client was last started.
///
/// Cancelling `shutdown` stops the reader while it waits for a connection,
/// for the next message, for room in `outbox`, or between attempts. If a
/// connection is open, the client is closed before `run` returns. A
/// transaction that is only partly received then, or that committed and
/// has not yet gone to `outbox`, is dropped. It was never confirmed, so the
/// server sends it again next time. The reader also stops when the
/// receiving end of `outbox` has gone.
///
/// # Errors
///
/// [`Error::Config`] if the URL in `settings` cannot be parsed, names a
/// socket in place of a TCP host, or names no user. This is checked once,
/// before the first connection. No later failure is returned: a lost
/// stream is logged, announced as [`SourceEvent::Disconnected`], and
/// connected again.
pub async fn run(
    settings: StreamSettings,
    mapper: Arc<dyn ResourceMapper>,
    outbox: mpsc::Sender<Transaction>,
    confirmed: Arc<Confirmed>,
    metrics: Arc<Metrics>,
    events: broadcast::Sender<SourceEvent>,
    shutdown: CancellationToken,
) -> Result<()> {
    let parts = parts(&settings)?;
    let mut failures: u32 = 0;
    let mut connected_before = false;
    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let config = ReplicationConfig::new(
            parts.host.clone(),
            parts.user.clone(),
            parts.password.clone(),
            parts.database.clone(),
            settings.slot.clone(),
            settings.publication.clone(),
        )
        .with_port(parts.port)
        .with_tls(parts.tls.clone())
        // The server starts at this position or at the one the slot has on
        // record as confirmed, whichever is later.
        .with_start_lsn(pgwire_replication::Lsn::from_u64(
            Lsn::from(&confirmed.get()).0,
        ))
        .with_status_interval(settings.status_interval)
        .with_wakeup_interval(settings.status_interval);
        // As of `pgwire-replication` 0.4.1, `connect` starts a task that
        // makes the connection and returns without waiting for it. A server
        // that cannot be reached, or that refuses the slot or the
        // publication, shows as the first `recv` in `stream` failing, not
        // as an error here.
        let connecting = tokio::select! {
            connecting = ReplicationClient::connect(config) => connecting,
            () = shutdown.cancelled() => return Ok(()),
        };
        let mut client = match connecting {
            Ok(client) => client,
            Err(error) => {
                failures += 1;
                let wait = settings.reconnect.delay(failures);
                tracing::warn!(slot = %settings.slot, %error, attempt = failures, wait = ?wait, "the stream of changes could not be connected");
                tokio::select! {
                    () = tokio::time::sleep(wait) => continue,
                    () = shutdown.cancelled() => return Ok(()),
                }
            }
        };
        if connected_before {
            Metrics::bump(&metrics.reconnects);
        }
        connected_before = true;
        // Because `connect` returns before the server has answered, the
        // count starts again on every attempt.
        failures = 0;
        Metrics::set(&metrics.connected, 1);
        let _ = events.send(SourceEvent::Connected);
        tracing::info!(slot = %settings.slot, publication = %settings.publication, "the stream of changes is connected");

        let ended = stream(
            &settings,
            &mut client,
            mapper.as_ref(),
            &outbox,
            &confirmed,
            &metrics,
            &events,
            &shutdown,
        )
        .await;
        Metrics::set(&metrics.connected, 0);
        // The latest confirmed position is handed over before the client is
        // closed. As of `pgwire-replication` 0.4.1 the client ends the
        // stream without a last status update, so the server learns of this
        // position only if an update goes out before the client sees the
        // request to stop. Whatever the server was not told, it sends again.
        if settings.acknowledge {
            client.update_applied_lsn(pgwire_replication::Lsn::from_u64(
                Lsn::from(&confirmed.get()).0,
            ));
        }
        // Asks the client to end the stream, and waits for its task. The
        // outcome is of no interest: either the controller is stopping or
        // the connection is about to be replaced.
        let _ = client.shutdown().await;
        match ended {
            Ok(()) => return Ok(()),
            Err(error) => {
                let _ = events.send(SourceEvent::Disconnected {
                    reason: error.to_string(),
                });
                failures += 1;
                let wait = settings.reconnect.delay(failures);
                tracing::warn!(slot = %settings.slot, %error, wait = ?wait, "the stream of changes was lost; connecting again");
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    () = shutdown.cancelled() => return Ok(()),
                }
            }
        }
    }
}

/// Reads one connection's worth of the stream.
///
/// `Ok` means the controller is stopping; `Err` means the connection is
/// gone. The table descriptions and the open transaction belong to the
/// connection, and are dropped with it.
#[allow(clippy::too_many_arguments)]
async fn stream(
    settings: &StreamSettings,
    client: &mut ReplicationClient,
    mapper: &dyn ResourceMapper,
    outbox: &mpsc::Sender<Transaction>,
    confirmed: &Confirmed,
    metrics: &Metrics,
    events: &broadcast::Sender<SourceEvent>,
    shutdown: &CancellationToken,
) -> Result<()> {
    // Both start empty on every connection. A description from an earlier
    // session may be out of date, and a transaction that was cut off is
    // sent again from its `BEGIN`.
    let mut relations = RelationCache::new();
    let mut buffer: TransactionBuffer<ResourceId> = TransactionBuffer::new();
    let mut status = tokio::time::interval(settings.status_interval);
    // Ticks that were missed while waiting for room in the outbox are not
    // made up for in a burst.
    status.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let event = tokio::select! {
            event = client.recv() => event,
            _ = status.tick() => {
                // The client only takes note of the position here. It sends
                // it to the server in its own status updates, and never
                // moves it backwards.
                if settings.acknowledge {
                    client.update_applied_lsn(pgwire_replication::Lsn::from_u64(Lsn::from(&confirmed.get()).0));
                }
                continue;
            }
            () = shutdown.cancelled() => return Ok(()),
        };
        let event = match event {
            Ok(Some(event)) => event,
            // The client ends the stream cleanly only when it is asked to
            // stop, which nothing does while reading.
            Ok(None) => return Err(Error::Connection("the server ended the stream".into())),
            Err(error) => return Err(Error::Connection(error.to_string())),
        };
        match event {
            // The client picks `BEGIN` and `COMMIT` out of the stream and
            // gives them as events of their own. Every other `pgoutput`
            // message but a logical message arrives as `XLogData`.
            ReplicationEvent::Begin { xid, .. } => buffer.begin(u64::from(xid)),
            ReplicationEvent::XLogData { wal_end, data, .. } => {
                // Zero is not a position, and may come with messages in
                // the middle of a transaction.
                if !wal_end.is_zero() {
                    Metrics::set(&metrics.received_position, wal_end.as_u64());
                }
                match pgoutput::decode(&data) {
                    Ok(Message::Relation(relation)) => {
                        tracing::debug!(table = %relation.qualified_name(), columns = relation.columns.len(), "a table was described");
                        relations.insert(relation);
                    }
                    Ok(
                        message @ (Message::Insert { .. }
                        | Message::Update { .. }
                        | Message::Delete { .. }),
                    ) => {
                        Metrics::bump(&metrics.row_changes);
                        let named =
                            relations
                                .row_change(&message)
                                .and_then(|change| match change {
                                    Some(change) => mapper.map_change(&change.relation, &change),
                                    None => Ok(None),
                                });
                        // A change that cannot be named must not stop the
                        // stream. It is counted as a change that touched
                        // nothing, and the resource is left to the next
                        // full reconciliation.
                        let touched = match named {
                            Ok(touched) => touched,
                            Err(error) => {
                                Metrics::bump(&metrics.mapping_errors);
                                tracing::warn!(%error, "a row change names no resource; the next full reconciliation will catch up");
                                None
                            }
                        };
                        buffer.change(touched)?;
                    }
                    // A truncation does not say which rows went, so it
                    // can name no resource.
                    Ok(Message::Truncate {
                        relations: truncated,
                    }) => {
                        tracing::warn!(
                            ?truncated,
                            "tables were truncated; the next full reconciliation will catch up"
                        );
                        buffer.change(None)?;
                    }
                    Ok(Message::Begin { .. } | Message::Commit { .. } | Message::Other(_)) => {}
                    // The message is dropped and the stream carries on. If
                    // it described a table, changes to that table cannot be
                    // named until the server describes it again.
                    Err(error) => {
                        Metrics::bump(&metrics.mapping_errors);
                        tracing::error!(%error, "a message could not be decoded and was passed over");
                    }
                }
            }
            ReplicationEvent::Commit { end_lsn, .. } => {
                // Only now does anything leave the reader. A transaction
                // that touched nothing is sent on as well, so that its
                // position can be confirmed.
                let committed = buffer.commit(Position::from(Lsn(end_lsn.as_u64())))?;
                Metrics::bump(&metrics.transactions);
                let _ = events.send(SourceEvent::Committed {
                    id: committed.id,
                    resources: committed.touched.len(),
                });
                // Waiting here is the backpressure: nothing more is read
                // until the outbox has room. A closed outbox means that
                // whoever takes the transactions has gone, which is treated
                // as stopping.
                tokio::select! {
                    sent = outbox.send(committed) => {
                        if sent.is_err() {
                            return Ok(());
                        }
                    }
                    () = shutdown.cancelled() => return Ok(()),
                }
            }
            ReplicationEvent::KeepAlive { wal_end, .. } => {
                Metrics::set(&metrics.received_position, wal_end.as_u64());
            }
            // A logical message, which the controller has no use for.
            ReplicationEvent::Message { .. } => {}
            // Sent when a position to stop at was asked for. None is.
            ReplicationEvent::StoppedAt { .. } => {
                return Err(Error::Connection("the stream stopped".into()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(url: &str) -> StreamSettings {
        StreamSettings {
            url: url.to_owned(),
            ca_certificate: None,
            slot: "s".to_owned(),
            publication: "p".to_owned(),
            status_interval: Duration::from_secs(10),
            reconnect: Backoff::default(),
            acknowledge: true,
        }
    }

    #[test]
    fn the_url_becomes_the_parts_of_a_connection() {
        let full = parts(&settings(
            "postgres://u:pw@db.example.test:5433/d?sslmode=disable",
        ))
        .unwrap();
        assert_eq!(full.host, "db.example.test");
        assert_eq!(full.port, 5433);
        assert_eq!(full.user, "u");
        assert_eq!(full.password, "pw");
        assert_eq!(full.database, "d");
        assert!(matches!(full.tls.mode, SslMode::Disable));

        let bare = parts(&settings("user=u")).unwrap();
        assert_eq!(bare.host, "localhost", "no host means the local machine");
        assert_eq!(bare.port, 5432);
        assert_eq!(bare.database, "u", "no database means the user's");
        assert_eq!(bare.password, "");
        assert!(matches!(bare.tls.mode, SslMode::Prefer));

        let required = parts(&settings("postgres://u@h/d?sslmode=require")).unwrap();
        assert!(!matches!(
            required.tls.mode,
            SslMode::Prefer | SslMode::Disable
        ));

        let no_user = parts(&settings("postgres://localhost/d"))
            .err()
            .expect("refused");
        assert!(
            matches!(no_user, Error::Config(ref m) if m.contains("no user")),
            "{no_user}"
        );
        let not_a_url = parts(&settings("::")).err().expect("refused");
        assert!(matches!(not_a_url, Error::Config(_)));
        #[cfg(unix)]
        {
            let socket = parts(&settings("user=u host=/var/run/postgresql"))
                .err()
                .expect("refused");
            assert!(
                matches!(socket, Error::Config(ref m) if m.contains("TCP address")),
                "{socket}"
            );
        }
    }
}
