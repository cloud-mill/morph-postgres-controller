//! What can go wrong, by layer.
//!
//! [`Error`] is the crate's one error type, and [`Result`] the result that
//! carries it. The variant says which layer failed, and the text inside says
//! what happened. The text is for people and for the log. Code that has to
//! tell failures apart matches on the variant.

use morph::ResourceId;

/// A failure somewhere in the controller.
///
/// The variant says which layer raised it. Every variant but
/// [`Reconcile`](Error::Reconcile) carries one message, which is displayed
/// after the name of the layer.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The configuration cannot be used as given.
    ///
    /// Raised when the configuration file cannot be read or parsed, or does
    /// not pass its checks, and when a setting such as a database URL or a
    /// certificate file cannot be used. A source raises it at start when its
    /// server is not set up as the configuration needs.
    #[error("configuration: {0}")]
    Config(String),
    /// The source could not be reached, or a connection to it was lost.
    ///
    /// Raised by a source's ordinary connection when it cannot be opened,
    /// and for a failed query that the server did not report itself, which
    /// most often means the connection has gone. A source uses it for a
    /// stream that ended, and connects again instead of passing it on. The
    /// controller also uses it to report that one of its own parts stopped.
    #[error("connection: {0}")]
    Connection(String),
    /// The source answered, and not as its protocol allows.
    ///
    /// Raised when a row change or a commit arrives outside any
    /// transaction, which ends that connection, and when a position in a
    /// server's answer cannot be read.
    #[error("protocol: {0}")]
    Protocol(String),
    /// A message from the source could not be understood.
    ///
    /// Raised by a decoder for a message that is cut short, malformed or of
    /// an unknown kind, and for a row change to a table that was not
    /// described first. A source logs and counts it, and carries on with
    /// the next message.
    #[error("decoding: {0}")]
    Decode(String),
    /// A row could not be related to a Morph resource.
    ///
    /// Raised by the mapper when a row change to a mapped table lacks the
    /// column that names the resource or the one that says its kind, or
    /// holds text that cannot be part of a resource id. Raised when desired
    /// state is read and a row cannot be made into a specification.
    #[error("resource mapping: {0}")]
    Mapping(String),
    /// Progress could not be recorded or read back.
    ///
    /// Raised for a failed read or write of the dirty store or of the
    /// references store, for a source whose record of where it stands is
    /// missing, and for a failure of the store that keeps Morph's records.
    #[error("checkpoint: {0}")]
    Checkpoint(String),
    /// The source refused a statement made on the controller's behalf.
    ///
    /// Raised when the server answers a statement with an error of its own,
    /// such as a missing table or column or a lack of permission. Statements
    /// on the controller's own stores are the exception: their failures are
    /// [`Checkpoint`](Error::Checkpoint).
    #[error("query: {0}")]
    Query(String),
    /// The engine could not reconcile a resource.
    ///
    /// Raised by the worker when the engine fails outright, when its report
    /// is not complete, when the reconciliation takes longer than its
    /// timeout, or when a row refers to a row that does not exist or to an
    /// attribute that the referenced resource does not have.
    #[error("reconciliation of {resource}: {reason}")]
    Reconcile {
        /// The resource that was being reconciled.
        resource: ResourceId,
        /// What went wrong, in Morph's words or the worker's.
        reason: String,
    },
    /// A provider could not be set up.
    ///
    /// Raised at start when a provider named in the configuration cannot be
    /// found, fetched, launched or registered with Morph.
    #[error("provider: {0}")]
    Provider(String),
    /// The engine could not be set up or asked something.
    ///
    /// Raised at start when the Pulumi command line cannot be run, or when
    /// the engine cannot say which resources it holds a record of.
    #[error("engine: {0}")]
    Engine(String),
}

/// A result of the controller's own kind.
pub type Result<T> = std::result::Result<T, Error>;

/// Treats an I/O failure as a lost connection.
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::Connection(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_error_says_which_layer_it_came_from() {
        let resource: ResourceId = "vm.a".parse().unwrap();
        let cases: Vec<(Error, &str)> = vec![
            (Error::Config("x".into()), "configuration: x"),
            (Error::Connection("x".into()), "connection: x"),
            (Error::Protocol("x".into()), "protocol: x"),
            (Error::Decode("x".into()), "decoding: x"),
            (Error::Mapping("x".into()), "resource mapping: x"),
            (Error::Checkpoint("x".into()), "checkpoint: x"),
            (Error::Query("x".into()), "query: x"),
            (
                Error::Reconcile {
                    resource,
                    reason: "x".into(),
                },
                "reconciliation of vm.a: x",
            ),
            (Error::Provider("x".into()), "provider: x"),
            (Error::Engine("x".into()), "engine: x"),
        ];
        for (error, text) in cases {
            assert_eq!(error.to_string(), text);
            assert!(!format!("{error:?}").is_empty());
        }
    }

    #[test]
    fn io_failures_count_as_lost_connections() {
        let error: Error = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe").into();
        assert!(
            matches!(error, Error::Connection(ref m) if m == "pipe"),
            "{error}"
        );
    }
}
