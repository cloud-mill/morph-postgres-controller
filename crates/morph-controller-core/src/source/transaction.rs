//! Transaction boundaries.
//!
//! In a log of changes, the changes arrive one row at a time, between a
//! begin and a commit. Nothing is acted on until the commit, and within a
//! transaction a resource that changed three times is a resource that
//! changed.
//!
//! The [`TransactionBuffer`] gathers what the changes of one transaction
//! touched and gives it up as a [`Committed`] when the commit arrives.
//! Holding everything until then is what lets a transaction that is cut off
//! half way be dropped without trace: its commit was never seen, so its
//! position is never confirmed, and the source sends all of it again on the
//! next connection.
//!
//! The buffer does not know what a resource is. A source fills it with the
//! resource ids its mapper gives.

use std::collections::BTreeSet;

use super::Position;
use crate::error::{Error, Result};

/// A transaction that committed, reduced to what it touched.
///
/// The changes themselves are not kept: what each row held plays no part in
/// what is done about it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Committed<T> {
    /// The position straight after the commit, which is what to confirm
    /// once the transaction has been dealt with.
    ///
    /// Confirming it tells the source that this transaction need not be
    /// sent again.
    pub position: Position,
    /// Its id, as the source numbers them. Nought if it does not.
    pub id: u64,
    /// Everything it touched, each once, in order.
    pub touched: BTreeSet<T>,
    /// How many row changes it had, before they were reduced.
    ///
    /// Changes that touched nothing of interest are counted too.
    pub changes: usize,
}

/// Gathers what a transaction touches, until it commits.
///
/// At most one transaction is open at a time, which is all that a log that
/// sends each committed transaction whole, one after another, needs.
///
/// # Examples
///
/// ```
/// use morph_controller_core::source::{Position, TransactionBuffer};
///
/// let mut buffer = TransactionBuffer::new();
/// buffer.begin(742);
/// buffer.change(Some("queue.orders"))?;
/// buffer.change(Some("queue.orders"))?;
/// buffer.change(None)?; // a row of a table that is not mapped
/// let committed = buffer.commit(Position::from_u64(0x16B3780))?;
///
/// assert_eq!(committed.id, 742);
/// assert_eq!(committed.changes, 3);
/// assert_eq!(committed.touched.len(), 1);
/// assert!(committed.touched.contains("queue.orders"));
/// assert!(!buffer.is_open());
///
/// // Outside a transaction there is nothing to add to.
/// assert!(buffer.change(Some("queue.orders")).is_err());
/// # Ok::<(), morph_controller_core::Error>(())
/// ```
#[derive(Debug)]
pub struct TransactionBuffer<T: Ord> {
    open: Option<Open<T>>,
}

/// The transaction being received.
#[derive(Debug)]
struct Open<T> {
    id: u64,
    touched: BTreeSet<T>,
    changes: usize,
}

impl<T: Ord> Default for TransactionBuffer<T> {
    fn default() -> Self {
        Self { open: None }
    }
}

impl<T: Ord> TransactionBuffer<T> {
    /// A buffer with no transaction open.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether a transaction has begun and not yet committed.
    pub fn is_open(&self) -> bool {
        self.open.is_some()
    }

    /// Starts a transaction.
    ///
    /// One that was open and never committed is dropped, with a warning,
    /// and nothing of it is given out. A source sends a transaction whole,
    /// so a begin while another transaction is open means that the open one
    /// was cut short, as by a lost connection, and that the source has
    /// started sending again from the last confirmed position.
    ///
    /// A source need not rely on this. It can start each connection with a
    /// new buffer.
    pub fn begin(&mut self, id: u64) {
        if let Some(open) = &self.open {
            tracing::warn!(
                id = open.id,
                "a transaction began before the last one committed; the last one is dropped"
            );
        }
        self.open = Some(Open {
            id,
            touched: BTreeSet::new(),
            changes: 0,
        });
    }

    /// Notes one row change, and what it touched, if anything of interest.
    ///
    /// The change is counted either way. What it touched is kept once,
    /// however many changes touch it.
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`] if no transaction is open.
    pub fn change(&mut self, touched: Option<T>) -> Result<()> {
        let open = self.open.as_mut().ok_or_else(|| {
            Error::Protocol("a row change arrived outside any transaction".into())
        })?;
        open.changes += 1;
        if let Some(touched) = touched {
            open.touched.insert(touched);
        }
        Ok(())
    }

    /// Ends the transaction, and gives what it touched.
    ///
    /// `position` is the position straight after the commit, as the commit
    /// message gives it. Afterwards no transaction is open.
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`] if no transaction is open.
    pub fn commit(&mut self, position: Position) -> Result<Committed<T>> {
        let open = self
            .open
            .take()
            .ok_or_else(|| Error::Protocol("a commit arrived outside any transaction".into()))?;
        Ok(Committed {
            position,
            id: open.id,
            touched: open.touched,
            changes: open.changes,
        })
    }

    /// Forgets whatever is open, as when the connection is lost.
    ///
    /// A transaction that was only partly received must not be given out.
    /// The source sends it again, from its begin, on the next connection.
    pub fn reset(&mut self) {
        self.open = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(number: u64) -> Position {
        Position::from_u64(number)
    }

    #[test]
    fn a_resource_changed_three_times_is_a_resource_that_changed() {
        let mut buffer = TransactionBuffer::new();
        buffer.begin(42);
        buffer.change(Some("a")).unwrap();
        buffer.change(Some("b")).unwrap();
        buffer.change(Some("a")).unwrap();
        buffer.change(None).unwrap();
        let committed = buffer.commit(at(100)).unwrap();
        assert_eq!(committed.touched, BTreeSet::from(["a", "b"]));
        assert_eq!(committed.changes, 4);
        assert_eq!(committed.id, 42);
        assert_eq!(committed.position, at(100));
        assert!(!buffer.is_open());
    }

    #[test]
    fn changes_outside_a_transaction_are_refused() {
        let mut buffer: TransactionBuffer<&str> = TransactionBuffer::new();
        assert!(buffer.change(Some("a")).is_err());
        assert!(buffer.commit(at(1)).is_err());
        buffer.begin(1);
        buffer.change(Some("a")).unwrap();
        buffer.reset();
        assert!(
            buffer.commit(at(1)).is_err(),
            "nothing is open after a reset"
        );
    }

    #[test]
    fn a_new_beginning_drops_what_never_committed() {
        let mut buffer = TransactionBuffer::new();
        buffer.begin(1);
        buffer.change(Some("stale")).unwrap();
        buffer.begin(2);
        buffer.change(Some("fresh")).unwrap();
        let committed = buffer.commit(at(5)).unwrap();
        assert_eq!(committed.id, 2);
        assert_eq!(committed.touched, BTreeSet::from(["fresh"]));
    }

    #[test]
    fn the_buffer_opens_closes_and_resets_as_its_contract_says() {
        let mut buffer: TransactionBuffer<String> = TransactionBuffer::default();
        assert!(!buffer.is_open());
        assert!(buffer.commit(Position::START).is_err(), "nothing to commit");
        buffer.begin(1);
        assert!(buffer.is_open());
        buffer.change(None).unwrap();
        buffer.change(Some("b".into())).unwrap();
        buffer.change(Some("a".into())).unwrap();
        let committed = buffer.commit(at(10)).unwrap();
        assert_eq!(committed.id, 1);
        assert_eq!(committed.position, at(10));
        assert_eq!(
            committed.touched.iter().cloned().collect::<Vec<String>>(),
            ["a".to_string(), "b".to_string()],
            "sorted, once each"
        );
        assert_eq!(
            committed.changes, 3,
            "a change that named nothing still counts"
        );
        assert!(!buffer.is_open());

        buffer.begin(2);
        buffer.change(Some("x".into())).unwrap();
        buffer.reset();
        assert!(!buffer.is_open());
        assert!(buffer.change(Some("y".into())).is_err());
    }
}
