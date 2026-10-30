//! Everything that speaks to PostgreSQL: the replication connection, the
//! messages on it, and the ordinary queries the controller makes.
//!
//! The stream of changes passes through four steps on its way out:
//!
//! 1. [`replication`] holds the connection, receives what the server sends,
//!    and connects again when the stream is lost.
//! 2. [`pgoutput`] decodes each message.
//! 3. [`relation`] relates a row change to its table, so that its values
//!    can be read by column name. The mapper in [`crate::mapping`] then
//!    names the resource the change is about.
//! 4. The core's [`TransactionBuffer`](morph_controller_core::source::TransactionBuffer)
//!    gathers what the changes touched, until the `COMMIT`.
//!
//! What comes out is a committed transaction and the resources it touched.
//! How far the server may be told the controller has got is worked out by
//! the core's ledger, in [`crate::checkpoint`].
//!
//! Beside the stream, [`database`] is the ordinary connection for the
//! controller's own queries, [`rows`] reads rows as desired state over it,
//! [`lsn`] has the type for positions in the log, and [`tls`] sets up
//! encryption.

pub mod database;
pub mod lsn;
pub mod pgoutput;
pub mod relation;
pub mod replication;
pub mod rows;
pub mod tls;

pub use lsn::Lsn;

use crate::error::Error;

/// Sorts a client error by who reported it.
///
/// An error that the server reported becomes [`Error::Query`], with the
/// server's message. Every other failure of the client library becomes
/// [`Error::Connection`]. Most of those are a connection that was refused
/// or lost, but a failure inside the client, such as an answer with an
/// unexpected number of rows, lands there too.
pub fn failed(error: tokio_postgres::Error) -> Error {
    match error.as_db_error() {
        Some(reported) => Error::Query(reported.to_string()),
        None => Error::Connection(error.to_string()),
    }
}

/// A client error met while keeping the controller's own records, as
/// [`Error::Checkpoint`].
///
/// The text is the server's message when the server reported the error,
/// such as a table of another shape under the record's name, and the
/// client's account of it otherwise. The client library's own text for a
/// reported error is only `db error`, with the message behind it, which is
/// why the two are told apart here.
pub fn checkpoint_failed(error: tokio_postgres::Error) -> Error {
    match error.as_db_error() {
        Some(reported) => Error::Checkpoint(reported.to_string()),
        None => Error::Checkpoint(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_client_failure_that_the_server_did_not_report_is_a_connection_error() {
        // Port 1 answers nothing, so the client fails before any server
        // error could be reported.
        let error = match tokio_postgres::connect(
            "host=127.0.0.1 port=1 user=x dbname=x connect_timeout=1",
            tokio_postgres::NoTls,
        )
        .await
        {
            Ok(_) => panic!("nothing listens on port 1"),
            Err(error) => failed(error),
        };
        assert!(matches!(error, Error::Connection(_)), "{error}");
    }
}
