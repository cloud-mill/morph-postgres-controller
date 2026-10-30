//! Which position in the log it is safe to confirm.
//!
//! Transactions arrive in order, and each is dealt with by some number of
//! reconciliations that finish in no particular order. The position at the
//! end of a transaction may be confirmed once that transaction and every
//! one before it have been dealt with. The ledger keeps that straight.
//!
//! The rule follows from what confirming means. The source sends again only
//! what committed after the confirmed position. A position confirmed while
//! an earlier transaction still had work outstanding would let a restart
//! lose that work.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::source::{Confirmed, Position};

/// A transaction whose position is not yet safe to confirm.
#[derive(Debug)]
struct Open {
    position: Position,
    outstanding: usize,
}

/// Keeps track of which transactions are dealt with, and so of the position
/// that is safe to confirm.
///
/// Transactions are opened in the order they committed, each with a count
/// of pieces of work outstanding. The safe position moves to the end of a
/// transaction only when its count has come down to zero and every
/// transaction opened before it has gone the same way. It never moves
/// backwards.
///
/// # Examples
///
/// ```
/// use morph_controller_core::checkpoint::Ledger;
/// use morph_controller_core::source::Position;
///
/// let mut ledger = Ledger::new(Position::from_u64(10));
/// let first = ledger.open(Position::from_u64(20), 2); // two resources to reconcile
/// let second = ledger.open(Position::from_u64(30), 1);
///
/// // The second transaction is dealt with, but the first is not.
/// ledger.settle(second);
/// assert_eq!(ledger.safe(), Position::from_u64(10));
///
/// ledger.settle(first);
/// ledger.settle(first);
/// assert_eq!(ledger.safe(), Position::from_u64(30));
/// assert_eq!(ledger.open_count(), 0);
/// ```
#[derive(Debug)]
pub struct Ledger {
    next: u64,
    open: BTreeMap<u64, Open>,
    safe: Arc<Confirmed>,
}

impl Ledger {
    /// A ledger that considers `safe` already confirmed.
    ///
    /// The controller starts it at the position the source has on record,
    /// so that the source is never told less than it already knows.
    pub fn new(safe: Position) -> Self {
        Self {
            next: 1,
            open: BTreeMap::new(),
            safe: Arc::new(Confirmed::new(safe)),
        }
    }

    /// Notes a transaction that ends at `position` and that `outstanding`
    /// pieces of work must deal with.
    ///
    /// Gives the number to settle those pieces under. Numbers start at one
    /// and rise with each transaction, and the order of opening is taken to
    /// be the order of the log. A transaction with nothing outstanding is
    /// dealt with at once, though its position is safe only when every
    /// earlier transaction is dealt with too.
    pub fn open(&mut self, position: Position, outstanding: usize) -> u64 {
        let number = self.next;
        self.next += 1;
        self.open.insert(
            number,
            Open {
                position,
                outstanding,
            },
        );
        self.advance();
        number
    }

    /// Notes that one piece of work for transaction `number` is dealt with.
    ///
    /// The ledger does not ask how the piece ended. The scheduler settles
    /// one when its resource is reconciled, and also when the resource is
    /// given up on. Settling a number that is unknown, or more often than
    /// there were pieces, changes nothing.
    pub fn settle(&mut self, number: u64) {
        if let Some(open) = self.open.get_mut(&number) {
            open.outstanding = open.outstanding.saturating_sub(1);
        }
        self.advance();
    }

    /// Moves the safe position past every transaction at the front that
    /// has nothing outstanding, and stops at the first that has.
    fn advance(&mut self) {
        while let Some(entry) = self.open.first_entry() {
            if entry.get().outstanding > 0 {
                break;
            }
            let position = entry.remove().position;
            // A transaction that the source sent a second time is opened a
            // second time, and may end before a position already passed.
            // The safe position never moves backwards for it.
            self.safe.raise(position);
        }
    }

    /// The position that is safe to confirm.
    ///
    /// It starts at the position the ledger was made with, and only moves
    /// forward.
    pub fn safe(&self) -> Position {
        self.safe.get()
    }

    /// A handle on the safe position that can be read without the ledger.
    ///
    /// The controller gives this to the source, which reports what it holds
    /// to its server.
    pub fn watch(&self) -> Arc<Confirmed> {
        Arc::clone(&self.safe)
    }

    /// How many transactions the safe position has not yet moved past.
    ///
    /// A transaction that is dealt with still counts while an earlier one
    /// holds it back.
    pub fn open_count(&self) -> usize {
        self.open.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(number: u64) -> Position {
        Position::from_u64(number)
    }

    #[test]
    fn a_position_is_confirmed_only_once_everything_before_it_is_dealt_with() {
        let mut ledger = Ledger::new(at(10));
        let first = ledger.open(at(20), 2);
        let second = ledger.open(at(30), 1);
        let third = ledger.open(at(40), 0);
        assert_eq!(ledger.safe(), at(10), "nothing is dealt with yet");
        assert_eq!(ledger.open_count(), 3);

        ledger.settle(second);
        assert_eq!(ledger.safe(), at(10), "the first is still open");
        ledger.settle(first);
        assert_eq!(ledger.safe(), at(10), "one of two pieces");
        ledger.settle(first);
        assert_eq!(ledger.safe(), at(40), "all three at once");
        assert_eq!(ledger.open_count(), 0);
        let _ = third;
    }

    #[test]
    fn a_transaction_with_nothing_to_do_is_confirmed_at_once() {
        let mut ledger = Ledger::new(Position::START);
        ledger.open(at(5), 0);
        assert_eq!(ledger.safe(), at(5));
        assert_eq!(ledger.watch().get(), at(5));
    }

    #[test]
    fn settling_more_than_was_outstanding_or_an_unknown_number_is_harmless() {
        let mut ledger = Ledger::new(Position::START);
        let number = ledger.open(at(5), 1);
        ledger.settle(99);
        ledger.settle(number);
        ledger.settle(number);
        assert_eq!(ledger.safe(), at(5));
    }

    #[test]
    fn a_transaction_sent_again_never_moves_the_safe_position_back() {
        let mut ledger = Ledger::new(at(40));
        let first = ledger.open(at(50), 1);
        let second = ledger.open(at(60), 0);
        // The stream is lost and both are sent again.
        let first_again = ledger.open(at(50), 1);
        let second_again = ledger.open(at(60), 0);
        ledger.settle(first);
        assert_eq!(ledger.safe(), at(60));
        ledger.settle(first_again);
        assert_eq!(ledger.safe(), at(60), "the second copies end no later");
        assert_eq!(ledger.open_count(), 0);
        let _ = (second, second_again);
    }
}
