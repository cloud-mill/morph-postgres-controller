//! Positions in the write-ahead log.
//!
//! Everything in this crate that speaks of progress does so in [`Lsn`]s:
//! where a transaction ends, how far the server has sent, and how far the
//! controller has confirmed. The core speaks of a
//! [`Position`] instead, and an `Lsn` becomes one as eight big-endian
//! bytes, so that positions sort as the log is written.

use std::fmt;
use std::str::FromStr;

use morph_controller_core::source::Position;

/// A position in PostgreSQL's write-ahead log, written as `X/Y` in hex.
///
/// The number is a byte offset into the log, so positions are ordered as the
/// log is written, and the difference between two of them is an amount of
/// log. In text, `X` is the upper 32 bits and `Y` the lower 32, each in
/// hexadecimal, which is the form of PostgreSQL's `pg_lsn` type.
///
/// # Examples
///
/// ```
/// use morph_postgres_controller::postgres::Lsn;
///
/// let lsn: Lsn = "1/A0".parse().unwrap();
/// assert_eq!(lsn, Lsn(0x1_0000_00A0));
/// assert_eq!(lsn.to_string(), "1/A0");
/// assert!(Lsn(0x16B3748) < lsn);
///
/// // Both halves are needed, and each has to fit in 32 bits.
/// assert!("16B3748".parse::<Lsn>().is_err());
/// assert!("100000000/0".parse::<Lsn>().is_err());
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub u64);

impl Lsn {
    /// The position before any other.
    ///
    /// As a place to start reading from, it leaves the choice to the slot:
    /// PostgreSQL starts a logical slot at the requested position or at the
    /// one last confirmed for it, whichever is later.
    pub const ZERO: Lsn = Lsn(0);

    /// How many bytes of log lie between `self` and `later`.
    ///
    /// Zero if `later` is not later.
    ///
    /// # Examples
    ///
    /// ```
    /// use morph_postgres_controller::postgres::Lsn;
    ///
    /// assert_eq!(Lsn(10).bytes_until(Lsn(25)), 15);
    /// assert_eq!(Lsn(25).bytes_until(Lsn(10)), 0);
    /// ```
    pub fn bytes_until(self, later: Lsn) -> u64 {
        later.0.saturating_sub(self.0)
    }
}

/// Writes the position as `X/Y`, in upper-case hex without leading zeros.
impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

/// Reads the `X/Y` form, in hex of either case.
impl FromStr for Lsn {
    /// A message that quotes the text that could not be read.
    type Err = String;

    /// Reads a position from text such as `0/16B3748`.
    ///
    /// # Errors
    ///
    /// A message that quotes the text, if it has no `/`, if either half is
    /// not a hexadecimal number, or if either half does not fit in 32 bits.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (high, low) = text
            .split_once('/')
            .ok_or_else(|| format!("{text:?} is not an LSN"))?;
        let high = u64::from_str_radix(high, 16).map_err(|_| format!("{text:?} is not an LSN"))?;
        let low = u64::from_str_radix(low, 16).map_err(|_| format!("{text:?} is not an LSN"))?;
        if high > u64::from(u32::MAX) || low > u64::from(u32::MAX) {
            return Err(format!("{text:?} is not an LSN"));
        }
        Ok(Lsn((high << 32) | low))
    }
}

impl From<u64> for Lsn {
    fn from(value: u64) -> Self {
        Lsn(value)
    }
}

/// An LSN as the core's position: eight big-endian bytes.
impl From<Lsn> for Position {
    fn from(lsn: Lsn) -> Self {
        Position::from_u64(lsn.0)
    }
}

/// A position made from an LSN, back. A position of any other shape, such
/// as the one before any other, reads as `0/0`.
impl From<&Position> for Lsn {
    fn from(position: &Position) -> Self {
        Lsn(position.as_u64().unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsns_round_trip_through_text() {
        for text in ["0/0", "0/16B3748", "1/A0", "FFFFFFFF/FFFFFFFF"] {
            let lsn: Lsn = text.parse().unwrap();
            assert_eq!(lsn.to_string(), text);
        }
        assert_eq!("0/16B3748".parse::<Lsn>().unwrap(), Lsn(0x16B3748));
        assert_eq!("1/A0".parse::<Lsn>().unwrap(), Lsn(0x1_0000_00A0));
        for bad in ["", "1", "x/y", "100000000/0", "1/2/3"] {
            assert!(bad.parse::<Lsn>().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn text_that_is_not_a_position_is_refused_with_the_text_quoted() {
        for text in [
            "",
            "16B3748",
            "0/",
            "x/1",
            "1/zz",
            "100000000/0",
            "0/100000000",
        ] {
            let error = text.parse::<Lsn>().unwrap_err();
            assert!(error.contains(&format!("{text:?}")), "{text:?}: {error}");
        }
        assert_eq!(Lsn::from(0).to_string(), "0/0");
        assert_eq!(Lsn::from(u64::MAX).to_string(), "FFFFFFFF/FFFFFFFF");
        assert_eq!("FFFFFFFF/FFFFFFFF".parse::<Lsn>().unwrap(), Lsn(u64::MAX));
    }

    #[test]
    fn distance_never_goes_negative() {
        assert_eq!(Lsn(10).bytes_until(Lsn(25)), 15);
        assert_eq!(Lsn(25).bytes_until(Lsn(10)), 0);
    }

    #[test]
    fn lsns_become_positions_that_sort_the_same_way_and_come_back() {
        let early = Position::from(Lsn(0x16B3748));
        let late = Position::from(Lsn(0x1_0000_00A0));
        assert!(early < late);
        assert_eq!(Lsn::from(&late), Lsn(0x1_0000_00A0));
        assert_eq!(Lsn::from(&Position::START), Lsn::ZERO);
    }
}
