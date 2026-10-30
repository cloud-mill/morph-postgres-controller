//! Where rows and their changes come from.
//!
//! The controller does not know what a database is. It knows a [`Source`]:
//! something that says which resources changed, as committed transactions
//! in the order they committed, and that reads a row back when a resource
//! is reconciled. PostgreSQL, read over logical replication, is one source.
//! Another database with a change log of its own, or with triggers that
//! fill a table of changes, is another, and nothing else in the controller
//! has to know which it is.
//!
//! # What a source promises
//!
//! * Every committed change to a mapped row is reported, as part of a
//!   [`Transaction`] that names the resources the transaction touched,
//!   each once, and gives the [`Position`] to confirm for it. Transactions
//!   arrive in the order they committed. A transaction may arrive twice,
//!   as after a lost connection; it never goes missing.
//! * A transaction is reported only once it has committed. Nothing of a
//!   transaction that was cut off is reported.
//! * Positions sort in the order of the source's log. The controller
//!   confirms the latest position of the transactions it has dealt with,
//!   through [`Confirmed`], and after a restart the source sends again
//!   whatever committed after the confirmed position.
//! * A row is read as it is at the moment it is asked for, never from the
//!   change that reported it.
//!
//! [`transaction`] has a helper for sources whose log gives changes one
//! row at a time between a begin and a commit.

pub mod transaction;

use std::fmt;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use morph::ResourceId;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

pub use transaction::{Committed, TransactionBuffer};

use crate::config::Mapping;
use crate::error::Result;
use crate::mapping::desired::Row;
use crate::observe::Metrics;
use crate::references::References;

/// A place in a source's log.
///
/// The controller never looks inside a position. It keeps the positions of
/// the transactions it has been given, compares them, and hands the latest
/// one that is safe back to the source. A source encodes its positions as
/// bytes that sort in the order of its log: PostgreSQL's log sequence
/// number as eight big-endian bytes, for instance, which is what
/// [`from_u64`](Self::from_u64) makes. Positions of one source must sort
/// in log order as byte strings, which fixed-length encodings do.
///
/// # Examples
///
/// ```
/// use morph_controller_core::source::Position;
///
/// let early = Position::from_u64(0x16B3748);
/// let late = Position::from_u64(0x16B3780);
/// assert!(early < late);
/// assert!(Position::START < early);
/// assert_eq!(late.as_u64(), Some(0x16B3780));
/// assert_eq!(late.to_string(), "00000000016b3780");
/// assert_eq!(Position::START.to_string(), "start");
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position(Vec<u8>);

impl Position {
    /// The position before any other: no bytes at all.
    ///
    /// As a place to start from, it leaves the choice to the source.
    pub const START: Position = Position(Vec::new());

    /// A position from the bytes a source encodes it as.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// A position from a number, as eight big-endian bytes, so that
    /// positions made this way sort as the numbers do.
    pub fn from_u64(number: u64) -> Self {
        Self(number.to_be_bytes().to_vec())
    }

    /// The number a position made by [`from_u64`](Self::from_u64) holds,
    /// or `None` for a position of any other length.
    pub fn as_u64(&self) -> Option<u64> {
        let bytes: [u8; 8] = self.0.as_slice().try_into().ok()?;
        Some(u64::from_be_bytes(bytes))
    }

    /// The bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for Position {
    /// The bytes in lower-case hex, or `start` for the position before any
    /// other.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return f.write_str("start");
        }
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// The position that is safe to confirm, shared between the ledger that
/// moves it and the source that reports it.
///
/// It only ever moves forward. The ledger raises it as transactions are
/// dealt with, and the source reads it whenever it tells its server how
/// far the controller has got.
///
/// # Examples
///
/// ```
/// use morph_controller_core::source::{Confirmed, Position};
///
/// let confirmed = Confirmed::new(Position::from_u64(10));
/// assert!(confirmed.raise(Position::from_u64(20)));
/// assert!(!confirmed.raise(Position::from_u64(15)), "never backwards");
/// assert_eq!(confirmed.get(), Position::from_u64(20));
/// ```
#[derive(Debug)]
pub struct Confirmed(RwLock<Position>);

impl Confirmed {
    /// A safe position that starts at `position`.
    pub fn new(position: Position) -> Self {
        Self(RwLock::new(position))
    }

    /// The position that is safe to confirm now.
    pub fn get(&self) -> Position {
        self.0.read().map(|safe| safe.clone()).unwrap_or_default()
    }

    /// Moves the safe position to `position` if that is later than where it
    /// stands, and says whether it moved.
    pub fn raise(&self, position: Position) -> bool {
        let Ok(mut safe) = self.0.write() else {
            return false;
        };
        if position > *safe {
            *safe = position;
            true
        } else {
            false
        }
    }
}

/// A transaction that committed, and the resources it touched.
///
/// This is all that leaves a source about a change: resource ids, a count
/// and a position. What each row held plays no part in what is done about
/// it.
pub type Transaction = Committed<ResourceId>;

/// Something a source tells whoever is watching.
///
/// Events are sent as they happen and not kept: one that nobody is
/// listening for is dropped.
#[derive(Clone, Debug)]
pub enum SourceEvent {
    /// A connection for the stream of changes was started.
    ///
    /// A source may connect in the background, so the server may not have
    /// answered yet. One that cannot be reached shows as a
    /// [`Disconnected`](SourceEvent::Disconnected) soon after.
    Connected,
    /// The stream was lost, and will be connected again.
    Disconnected {
        /// Why.
        reason: String,
    },
    /// A transaction committed.
    ///
    /// Sent before the transaction is handed on, so nothing has been
    /// recorded or marked for it yet.
    Committed {
        /// Its id, as the source numbers them.
        id: u64,
        /// How many resources it touched.
        resources: usize,
    },
}

/// Where rows and their changes come from.
///
/// One source serves a whole controller, shared behind an [`Arc`]. Its
/// methods may be called from several tasks at once.
#[async_trait]
pub trait Source: Send + Sync + fmt::Debug {
    /// The source's name, as the logs give it: `postgres`, say.
    fn name(&self) -> &'static str;

    /// Every mapping, in the order of the configuration.
    fn mappings(&self) -> &[Mapping];

    /// The mapping of the table `schema.table`, if it is mapped.
    fn mapping_of(&self, schema: &str, table: &str) -> Option<&Mapping> {
        self.mappings()
            .iter()
            .find(|mapping| mapping.schema == schema && mapping.table == table)
    }

    /// Where the source stands as confirmed when the controller starts.
    ///
    /// The controller begins confirming from here, so that the source is
    /// never told less than it already knows.
    ///
    /// # Errors
    ///
    /// Whatever reading the source's record fails with: [`Error::Checkpoint`](crate::Error::Checkpoint)
    /// if the record is missing, or a connection or query failure.
    async fn confirmed(&self) -> Result<Position>;

    /// The references between the mapped tables, as the source knows them:
    /// from its constraints and from the mappings' own declarations.
    ///
    /// Asked once, at start.
    ///
    /// # Errors
    ///
    /// Whatever reading the source's catalogue fails with, and
    /// [`Error::Config`](crate::Error::Config) if a declared reference
    /// points at a table that is not mapped.
    async fn references(&self) -> Result<References>;

    /// Reads changes until `shutdown` is cancelled, connecting again
    /// whenever the stream is lost.
    ///
    /// Each committed transaction goes to `outbox`, in the order it
    /// committed. When `outbox` is full the source waits for room and reads
    /// nothing more, which is how a slow consumer slows the reading of the
    /// log. The source also stops when the receiving end of `outbox` has
    /// gone.
    ///
    /// `confirmed` is what the source tells its server the controller has
    /// dealt with. The source never moves it. It reads it when it connects,
    /// as the position to start from, and whenever it reports.
    ///
    /// The source counts what it sees in `metrics`: `connected`,
    /// `reconnects`, `transactions`, `row_changes`, `mapping_errors`, and
    /// the position gauges. It tells `events` when it connects, loses the
    /// stream, and reads a commit.
    ///
    /// # Errors
    ///
    /// Only for what cannot be put right by connecting again, such as
    /// settings that cannot be used. A lost stream is logged, announced
    /// as [`SourceEvent::Disconnected`], and connected again.
    async fn watch(
        &self,
        outbox: mpsc::Sender<Transaction>,
        confirmed: Arc<Confirmed>,
        metrics: Arc<Metrics>,
        events: broadcast::Sender<SourceEvent>,
        shutdown: CancellationToken,
    ) -> Result<()>;

    /// The row that describes `resource`, if any does, as it is now.
    ///
    /// The mappings that could hold the kind are tried in the order of the
    /// configuration, and the first row found is used.
    ///
    /// # Errors
    ///
    /// Whatever reading fails with: a connection or query failure, or
    /// [`Error::Mapping`](crate::Error::Mapping) if a row names no valid
    /// resource.
    async fn row(&self, resource: &ResourceId) -> Result<Option<Row>>;

    /// The row of `mapping`'s table whose id column reads `name`, if there
    /// is one, with the resource it describes.
    ///
    /// This is how a referenced row is found: by the table the reference
    /// points at and the text in the referring column.
    ///
    /// # Errors
    ///
    /// As [`row`](Self::row) fails, and [`Error::Mapping`](crate::Error::Mapping)
    /// if the row has no kind.
    async fn row_in(&self, mapping: &Mapping, name: &str) -> Result<Option<Row>>;

    /// Every row of the stack `group`, across the tables grouped into it,
    /// each with the resource it describes, sorted by resource.
    ///
    /// # Errors
    ///
    /// As [`row`](Self::row) fails, and [`Error::Mapping`](crate::Error::Mapping)
    /// if a row has no id or no kind.
    async fn group_rows(&self, group: &str) -> Result<Vec<Row>>;

    /// Every resource that a row describes, across the mapped tables,
    /// sorted and each once.
    ///
    /// A table grouped into stacks gives its stacks instead, as
    /// `stack.<name>`. Rows that name no valid resource are passed over
    /// with a warning.
    ///
    /// # Errors
    ///
    /// A connection or query failure.
    async fn all_resources(&self) -> Result<Vec<ResourceId>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions_sort_as_their_numbers_do_and_say_what_they_hold() {
        let positions: Vec<Position> = [0, 1, 255, 256, u64::MAX]
            .into_iter()
            .map(Position::from_u64)
            .collect();
        for pair in positions.windows(2) {
            assert!(pair[0] < pair[1], "{} < {}", pair[0], pair[1]);
        }
        assert_eq!(positions[2].as_u64(), Some(255));
        assert_eq!(positions[2].to_string(), "00000000000000ff");
        assert_eq!(Position::new(vec![1, 2]).as_u64(), None);
        assert_eq!(Position::new(vec![1, 2]).to_string(), "0102");
        assert_eq!(Position::new(vec![1, 2]).as_bytes(), &[1, 2]);
        assert_eq!(Position::default(), Position::START);
        assert!(Position::START < Position::new(vec![0]));
    }

    #[test]
    fn the_confirmed_position_only_moves_forward() {
        let confirmed = Confirmed::new(Position::START);
        assert_eq!(confirmed.get(), Position::START);
        assert!(confirmed.raise(Position::from_u64(5)));
        assert!(!confirmed.raise(Position::from_u64(5)));
        assert!(!confirmed.raise(Position::from_u64(4)));
        assert!(confirmed.raise(Position::from_u64(6)));
        assert_eq!(confirmed.get(), Position::from_u64(6));
        assert!(format!("{confirmed:?}").contains("Confirmed"));
    }

    #[test]
    fn the_start_has_a_name_and_a_poisoned_handle_neither_moves_nor_panics() {
        assert_eq!(Position::START.to_string(), "start");
        assert_eq!(Position::default(), Position::START);
        let confirmed = Confirmed::new(Position::from_u64(3));
        // A thread that panics while it holds the lock poisons it.
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _held = confirmed.0.write().unwrap();
            panic!("while holding the lock");
        }));
        assert!(poisoned.is_err());
        assert!(!confirmed.raise(Position::from_u64(9)), "nothing moves");
        assert_eq!(confirmed.get(), Position::START, "nothing can be read");
    }
}
