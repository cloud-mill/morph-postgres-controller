//! Decoding of the `pgoutput` logical decoding plugin's messages.
//!
//! This is the protocol, and nothing else: no knowledge of tables, of
//! resources, or of what any change means. Each message is decoded on its
//! own, as the reader in [`super::replication`] takes it off the stream.
//! Keeping track of relations across messages is the job of
//! [`super::relation`], and of transactions the job of
//! the core's [`TransactionBuffer`](morph_controller_core::source::TransactionBuffer).
//!
//! The layout is that of protocol version 1, which is the version the
//! replication client asks for. In it the server sends a transaction whole,
//! once it has committed: a `BEGIN`, the changes, a `COMMIT`. Only the
//! `BEGIN` carries the transaction id.
//!
//! | Tag | Message | What it carries |
//! |-----|---------|-----------------|
//! | `B` | [`Message::Begin`] | The position and time of the commit to come, and the transaction id. |
//! | `C` | [`Message::Commit`] | The position of the commit, the position after it, and its time. |
//! | `R` | [`Message::Relation`] | A table's id, name, replica identity and columns. |
//! | `I` | [`Message::Insert`] | The new row. |
//! | `U` | [`Message::Update`] | The new row, with the old key or the old row when the replica identity gives one. |
//! | `D` | [`Message::Delete`] | The old key, or the old row. |
//! | `T` | [`Message::Truncate`] | The tables that were emptied. |
//! | `Y`, `O`, `M` | [`Message::Other`] | A type, an origin or a logical message. Passed over unread. |
//!
//! Any other tag is refused. That includes the messages of later protocol
//! versions, for transactions that are streamed in progress or prepared in
//! two phases, which are not asked for.

use std::fmt;

use super::lsn::Lsn;
use crate::error::{Error, Result};

/// How a table identifies its rows to logical decoding.
///
/// The setting decides what an update or a delete carries about the row as
/// it was. For a delete that is all there is to read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicaIdentity {
    /// The primary key, if the table has one.
    ///
    /// A delete carries the old key, and so does an update that changed it.
    Default,
    /// Nothing.
    ///
    /// No old row is logged at all. PostgreSQL refuses updates and deletes
    /// on such a table for as long as a publication publishes them.
    Nothing,
    /// The whole old row.
    ///
    /// Every update and every delete carries the row as it was, and every
    /// column counts as part of the key.
    Full,
    /// The columns of a chosen unique index.
    ///
    /// As with a primary key, a delete carries the old key, and so does an
    /// update that changed it.
    Index,
}

/// One column of a relation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    /// Its name.
    pub name: String,
    /// Whether it is part of the replica identity.
    ///
    /// Under `REPLICA IDENTITY FULL` every column is.
    pub key: bool,
    /// The OID of its type.
    pub type_oid: u32,
    /// The type modifier, such as a length, or `-1` if the type has none.
    pub modifier: i32,
}

/// A table, as logical decoding describes it before it describes rows of
/// it.
///
/// The server sends a description before the first change to a table on a
/// connection, and again after the table's definition changes. Row changes
/// then refer to the table by [`id`](Self::id) alone, and their tuples hold
/// one value for each column listed here, in this order.
///
/// A description is good for the replication session it arrived in and no
/// longer. On a new connection the server describes each table again before
/// the first change to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Relation {
    /// The table's OID, which is how row changes refer to it.
    ///
    /// It is the key to the descriptions of one session and nothing more.
    /// It must never be used to name a resource: nobody chose it, and a
    /// table that is dropped and made again comes back with another.
    pub id: u32,
    /// The schema.
    pub schema: String,
    /// The table.
    pub name: String,
    /// How rows are identified.
    pub replica_identity: ReplicaIdentity,
    /// The columns, in the order their values take in a [`Tuple`].
    pub columns: Vec<Column>,
}

impl Relation {
    /// The position of the column called `name`.
    ///
    /// This is also the position of its value in a [`Tuple`] of this table.
    /// `None` if the table has no such column.
    pub fn column(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|column| column.name == name)
    }

    /// The table as `schema.table`, without quoting.
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }
}

/// One value in a row, as decoding sends it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Datum {
    /// SQL null.
    ///
    /// In a key tuple the columns outside the replica identity are sent as
    /// null too, whatever the row held.
    Null,
    /// A large value that did not change, which decoding leaves out.
    ///
    /// PostgreSQL keeps large values apart from their row (TOAST), and an
    /// update that leaves such a value alone does not log it again. The
    /// message then says only that the value is unchanged. It is not null,
    /// and what it is cannot be told from the message.
    Unchanged,
    /// The value, in its text form.
    Text(String),
    /// The value, in its binary form.
    ///
    /// Not asked for, but allowed by the protocol.
    Binary(Vec<u8>),
}

impl Datum {
    /// The text of a [`Datum::Text`].
    ///
    /// `None` for a null, for an unchanged value and for a binary one.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Datum::Text(text) => Some(text),
            _ => None,
        }
    }
}

/// A row, or the identifying part of one.
///
/// It holds one [`Datum`] for each column of its [`Relation`], in the same
/// order. A key tuple is as long as a whole row: it has a value in each
/// column of the replica identity and null in the others.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tuple(pub Vec<Datum>);

impl Tuple {
    /// The value of the column at position `index`.
    ///
    /// The position is the one [`Relation::column`] gives. `None` if the
    /// tuple is shorter than that.
    pub fn get(&self, index: usize) -> Option<&Datum> {
        self.0.get(index)
    }
}

/// A message from `pgoutput`.
///
/// A row change names its table by the id of a [`Relation`] that the server
/// sent earlier on the same connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// A transaction starts.
    ///
    /// The transaction has already committed when the server sends this.
    /// Everything up to the next [`Commit`](Message::Commit) belongs to it.
    Begin {
        /// Where the transaction's commit is in the log.
        final_lsn: Lsn,
        /// When it committed, in microseconds since 2000-01-01.
        timestamp: i64,
        /// Its id.
        xid: u32,
    },
    /// A transaction ends.
    Commit {
        /// Where the commit is in the log.
        lsn: Lsn,
        /// The position straight after the commit, where the transaction
        /// ends in the log.
        end_lsn: Lsn,
        /// When it committed, in microseconds since 2000-01-01.
        timestamp: i64,
    },
    /// A table is described, before rows of it are.
    Relation(Relation),
    /// A row was inserted.
    Insert {
        /// The table, by the id of its [`Relation`].
        relation: u32,
        /// The row.
        new: Tuple,
    },
    /// A row was updated.
    ///
    /// The server sends `key` or `old` or neither, and never both. Which one
    /// depends on the table's [`ReplicaIdentity`].
    Update {
        /// The table, by the id of its [`Relation`].
        relation: u32,
        /// The identifying part of the old row.
        ///
        /// Sent only when the update changed a column of the replica
        /// identity, and the identity is not the whole row.
        key: Option<Tuple>,
        /// The whole old row.
        ///
        /// Sent only when the table's replica identity is full.
        old: Option<Tuple>,
        /// The row as the update left it.
        ///
        /// A large value that the update did not touch arrives as
        /// [`Datum::Unchanged`].
        new: Tuple,
    },
    /// A row was deleted.
    ///
    /// The message carries exactly one of `key` and `old`.
    Delete {
        /// The table, by the id of its [`Relation`].
        relation: u32,
        /// The identifying part of the row, unless the whole row is given.
        key: Option<Tuple>,
        /// The whole row, if the table's replica identity is full.
        old: Option<Tuple>,
    },
    /// Tables were emptied.
    ///
    /// The message names the tables and not the rows that went.
    Truncate {
        /// The tables, by the ids of their [`Relation`]s.
        relations: Vec<u32>,
    },
    /// Something the controller has no use for: a type, an origin, a
    /// logical message.
    ///
    /// It holds the tag of the message. The rest of the message is not read.
    Other(u8),
}

/// A reader over the bytes of one message, which never reads past the end.
///
/// Integers are big-endian, as the protocol sends them.
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self.at.checked_add(count).ok_or_else(|| short(self.at))?;
        let slice = self.bytes.get(self.at..end).ok_or_else(|| short(self.at))?;
        self.at = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(
            self.take(2)?.try_into().expect("two bytes"),
        ))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("four bytes"),
        ))
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(
            self.take(4)?.try_into().expect("four bytes"),
        ))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(
            self.take(8)?.try_into().expect("eight bytes"),
        ))
    }

    fn lsn(&mut self) -> Result<Lsn> {
        Ok(Lsn(u64::from_be_bytes(
            self.take(8)?.try_into().expect("eight bytes"),
        )))
    }

    /// A string that runs up to the next zero byte, which is how the
    /// protocol writes names.
    fn cstring(&mut self) -> Result<String> {
        let rest = &self.bytes[self.at..];
        let end = rest
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| Error::Decode(format!("unterminated string at byte {}", self.at)))?;
        let text = std::str::from_utf8(&rest[..end])
            .map_err(|_| Error::Decode(format!("invalid UTF-8 at byte {}", self.at)))?
            .to_owned();
        self.at += end + 1;
        Ok(text)
    }

    /// A count of columns, then one value for each: a kind byte, followed
    /// for text and binary values by a length and that many bytes.
    fn tuple(&mut self) -> Result<Tuple> {
        let count = self.i16()?;
        let count = usize::try_from(count)
            .map_err(|_| Error::Decode(format!("{count} columns in a tuple")))?;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let datum = match self.u8()? {
                b'n' => Datum::Null,
                b'u' => Datum::Unchanged,
                b't' => Datum::Text(self.sized_text()?),
                b'b' => Datum::Binary(self.sized()?.to_vec()),
                other => {
                    return Err(Error::Decode(format!(
                        "unknown column kind {:?}",
                        char::from(other)
                    )));
                }
            };
            values.push(datum);
        }
        Ok(Tuple(values))
    }

    /// A four-byte length, and that many bytes.
    fn sized(&mut self) -> Result<&'a [u8]> {
        let length = self.i32()?;
        let length = usize::try_from(length)
            .map_err(|_| Error::Decode(format!("a value of length {length}")))?;
        self.take(length)
    }

    fn sized_text(&mut self) -> Result<String> {
        let at = self.at;
        let bytes = self.sized()?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| Error::Decode(format!("invalid UTF-8 at byte {at}")))
    }

    /// Checks that the message was used up exactly.
    ///
    /// Bytes left over mean that the layout was misread somewhere, so the
    /// whole message is refused.
    fn done(&self) -> Result<()> {
        if self.at == self.bytes.len() {
            Ok(())
        } else {
            Err(Error::Decode(format!(
                "{} bytes left over after the message",
                self.bytes.len() - self.at
            )))
        }
    }
}

/// The error for a message that ends at byte `at`, where more was expected.
fn short(at: usize) -> Error {
    Error::Decode(format!(
        "the message ends at byte {at}, before it is complete"
    ))
}

/// Decodes one message, given without the replication framing around it.
///
/// The first byte says what the message is. A message of no use is returned
/// as [`Message::Other`] as soon as its tag is known, without reading
/// further. Every other message has to be used up exactly.
///
/// # Errors
///
/// [`Error::Decode`] if the message is empty or ends early; if its tag, a
/// replica identity, a part of a row change, or the kind of a column value
/// is not one the decoder knows; if a count or a length is negative; if a
/// name or a text value is not UTF-8; or if bytes are left over. The text
/// says what was wrong, and for most of these at which byte.
///
/// # Examples
///
/// An insert into relation 16385 of a row with two columns, the text `7`
/// and a null:
///
/// ```
/// use morph_postgres_controller::postgres::pgoutput::{Datum, Message, Tuple, decode};
///
/// let mut bytes = vec![b'I'];
/// bytes.extend(16385u32.to_be_bytes()); // the table
/// bytes.push(b'N'); // a new row follows
/// bytes.extend(2i16.to_be_bytes()); // with two columns
/// bytes.push(b't'); // text, with its length
/// bytes.extend(1i32.to_be_bytes());
/// bytes.extend(b"7");
/// bytes.push(b'n'); // null
///
/// let message = decode(&bytes)?;
/// assert_eq!(
///     message,
///     Message::Insert {
///         relation: 16385,
///         new: Tuple(vec![Datum::Text("7".into()), Datum::Null]),
///     }
/// );
/// assert_eq!(message.to_string(), "insert into 16385");
///
/// // A message that stops short is refused.
/// assert!(decode(&bytes[..8]).is_err());
/// # Ok::<(), morph_postgres_controller::Error>(())
/// ```
pub fn decode(bytes: &[u8]) -> Result<Message> {
    let mut cursor = Cursor::new(bytes);
    let tag = cursor.u8()?;
    let message = match tag {
        b'B' => Message::Begin {
            final_lsn: cursor.lsn()?,
            timestamp: cursor.i64()?,
            xid: cursor.u32()?,
        },
        b'C' => {
            // The protocol defines no flags yet.
            let _flags = cursor.u8()?;
            Message::Commit {
                lsn: cursor.lsn()?,
                end_lsn: cursor.lsn()?,
                timestamp: cursor.i64()?,
            }
        }
        b'R' => {
            let id = cursor.u32()?;
            let schema = cursor.cstring()?;
            let name = cursor.cstring()?;
            let replica_identity = match cursor.u8()? {
                b'd' => ReplicaIdentity::Default,
                b'n' => ReplicaIdentity::Nothing,
                b'f' => ReplicaIdentity::Full,
                b'i' => ReplicaIdentity::Index,
                other => {
                    return Err(Error::Decode(format!(
                        "unknown replica identity {:?}",
                        char::from(other)
                    )));
                }
            };
            let count = cursor.i16()?;
            let count = usize::try_from(count)
                .map_err(|_| Error::Decode(format!("{count} columns in a relation")))?;
            let mut columns = Vec::with_capacity(count);
            for _ in 0..count {
                // The lowest bit marks a column of the replica identity. It
                // is the only flag the protocol defines.
                let flags = cursor.u8()?;
                columns.push(Column {
                    key: flags & 1 == 1,
                    name: cursor.cstring()?,
                    type_oid: cursor.u32()?,
                    modifier: cursor.i32()?,
                });
            }
            Message::Relation(Relation {
                id,
                schema,
                name,
                replica_identity,
                columns,
            })
        }
        b'I' => {
            let relation = cursor.u32()?;
            expect(&mut cursor, b'N')?;
            Message::Insert {
                relation,
                new: cursor.tuple()?,
            }
        }
        b'U' => {
            let relation = cursor.u32()?;
            let (mut key, mut old) = (None, None);
            // An old key or an old row may come first. The new row always
            // comes last, and ends the message.
            loop {
                match cursor.u8()? {
                    b'K' => key = Some(cursor.tuple()?),
                    b'O' => old = Some(cursor.tuple()?),
                    b'N' => break,
                    other => {
                        return Err(Error::Decode(format!(
                            "unexpected part {:?} of an update",
                            char::from(other)
                        )));
                    }
                }
            }
            Message::Update {
                relation,
                key,
                old,
                new: cursor.tuple()?,
            }
        }
        b'D' => {
            let relation = cursor.u32()?;
            let (mut key, mut old) = (None, None);
            match cursor.u8()? {
                b'K' => key = Some(cursor.tuple()?),
                b'O' => old = Some(cursor.tuple()?),
                other => {
                    return Err(Error::Decode(format!(
                        "unexpected part {:?} of a delete",
                        char::from(other)
                    )));
                }
            }
            Message::Delete { relation, key, old }
        }
        b'T' => {
            let count = cursor.i32()?;
            // Whether the truncation cascaded or restarted sequences. It
            // makes no difference to which tables were emptied.
            let _options = cursor.u8()?;
            let count = usize::try_from(count)
                .map_err(|_| Error::Decode(format!("{count} relations truncated")))?;
            let mut relations = Vec::with_capacity(count);
            for _ in 0..count {
                relations.push(cursor.u32()?);
            }
            Message::Truncate { relations }
        }
        // Types, origins and logical messages are of no use here, and are
        // not read further.
        b'Y' | b'O' | b'M' => return Ok(Message::Other(tag)),
        other => {
            return Err(Error::Decode(format!(
                "unknown message {:?}",
                char::from(other)
            )));
        }
    };
    cursor.done()?;
    Ok(message)
}

fn expect(cursor: &mut Cursor<'_>, wanted: u8) -> Result<()> {
    let found = cursor.u8()?;
    if found == wanted {
        Ok(())
    } else {
        Err(Error::Decode(format!(
            "expected {:?}, found {:?}",
            char::from(wanted),
            char::from(found)
        )))
    }
}

/// Writes a few words for the log, and never the contents of a row.
///
/// The words say what kind of message it is, and name its transaction,
/// position or table.
impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Message::Begin { xid, .. } => write!(f, "begin {xid}"),
            Message::Commit { end_lsn, .. } => write!(f, "commit at {end_lsn}"),
            Message::Relation(relation) => write!(f, "relation {}", relation.qualified_name()),
            Message::Insert { relation, .. } => write!(f, "insert into {relation}"),
            Message::Update { relation, .. } => write!(f, "update of {relation}"),
            Message::Delete { relation, .. } => write!(f, "delete from {relation}"),
            Message::Truncate { relations } => write!(f, "truncate of {relations:?}"),
            Message::Other(tag) => write!(f, "message {:?}", char::from(*tag)),
        }
    }
}

/// Encoders of the same messages, for tests of whatever decodes them.
///
/// Values are written as text or as null, and a column is given as its
/// name, whether it is part of the key, and the OID of its type.
#[cfg(test)]
pub(crate) mod encode {

    pub(crate) fn begin(final_lsn: u64, timestamp: i64, xid: u32) -> Vec<u8> {
        let mut bytes = vec![b'B'];
        bytes.extend(final_lsn.to_be_bytes());
        bytes.extend(timestamp.to_be_bytes());
        bytes.extend(xid.to_be_bytes());
        bytes
    }

    pub(crate) fn commit(lsn: u64, end_lsn: u64, timestamp: i64) -> Vec<u8> {
        let mut bytes = vec![b'C', 0];
        bytes.extend(lsn.to_be_bytes());
        bytes.extend(end_lsn.to_be_bytes());
        bytes.extend(timestamp.to_be_bytes());
        bytes
    }

    pub(crate) fn relation(
        id: u32,
        schema: &str,
        name: &str,
        identity: u8,
        columns: &[(&str, bool, u32)],
    ) -> Vec<u8> {
        let mut bytes = vec![b'R'];
        bytes.extend(id.to_be_bytes());
        bytes.extend(schema.as_bytes());
        bytes.push(0);
        bytes.extend(name.as_bytes());
        bytes.push(0);
        bytes.push(identity);
        bytes.extend((columns.len() as i16).to_be_bytes());
        for (column, key, oid) in columns {
            bytes.push(u8::from(*key));
            bytes.extend(column.as_bytes());
            bytes.push(0);
            bytes.extend(oid.to_be_bytes());
            bytes.extend((-1i32).to_be_bytes());
        }
        bytes
    }

    pub(crate) fn tuple(values: &[Option<&str>]) -> Vec<u8> {
        let mut bytes = (values.len() as i16).to_be_bytes().to_vec();
        for value in values {
            match value {
                None => bytes.push(b'n'),
                Some(text) => {
                    bytes.push(b't');
                    bytes.extend((text.len() as i32).to_be_bytes());
                    bytes.extend(text.as_bytes());
                }
            }
        }
        bytes
    }

    pub(crate) fn insert(relation: u32, values: &[Option<&str>]) -> Vec<u8> {
        let mut bytes = vec![b'I'];
        bytes.extend(relation.to_be_bytes());
        bytes.push(b'N');
        bytes.extend(tuple(values));
        bytes
    }

    pub(crate) fn update(
        relation: u32,
        key: Option<&[Option<&str>]>,
        values: &[Option<&str>],
    ) -> Vec<u8> {
        let mut bytes = vec![b'U'];
        bytes.extend(relation.to_be_bytes());
        if let Some(key) = key {
            bytes.push(b'K');
            bytes.extend(tuple(key));
        }
        bytes.push(b'N');
        bytes.extend(tuple(values));
        bytes
    }

    pub(crate) fn delete(relation: u32, part: u8, values: &[Option<&str>]) -> Vec<u8> {
        let mut bytes = vec![b'D'];
        bytes.extend(relation.to_be_bytes());
        bytes.push(part);
        bytes.extend(tuple(values));
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::encode;
    use super::*;

    #[test]
    fn a_relation_is_decoded_with_its_columns_and_keys() {
        let bytes = encode::relation(
            16385,
            "public",
            "resources",
            b'd',
            &[
                ("id", true, 23),
                ("resource_type", false, 25),
                ("cpu", false, 23),
            ],
        );
        let Message::Relation(relation) = decode(&bytes).unwrap() else {
            panic!("not a relation");
        };
        assert_eq!(relation.id, 16385);
        assert_eq!(relation.qualified_name(), "public.resources");
        assert_eq!(relation.replica_identity, ReplicaIdentity::Default);
        assert_eq!(relation.columns.len(), 3);
        assert!(relation.columns[0].key);
        assert!(!relation.columns[1].key);
        assert_eq!(relation.columns[1].type_oid, 25);
        assert_eq!(relation.column("cpu"), Some(2));
        assert_eq!(relation.column("nothing"), None);
    }

    #[test]
    fn an_insert_carries_the_new_row() {
        let message = decode(&encode::insert(16385, &[Some("7"), Some("vm"), None])).unwrap();
        assert_eq!(
            message,
            Message::Insert {
                relation: 16385,
                new: Tuple(vec![
                    Datum::Text("7".into()),
                    Datum::Text("vm".into()),
                    Datum::Null
                ]),
            }
        );
    }

    #[test]
    fn an_update_carries_the_new_row_and_the_old_key_if_it_changed() {
        let plain = decode(&encode::update(
            16385,
            None,
            &[Some("7"), Some("vm"), Some("4")],
        ))
        .unwrap();
        assert!(matches!(
            plain,
            Message::Update {
                key: None,
                old: None,
                ..
            }
        ));

        let rekeyed = decode(&encode::update(
            16385,
            Some(&[Some("7"), None, None]),
            &[Some("8"), Some("vm"), Some("4")],
        ))
        .unwrap();
        let Message::Update {
            key: Some(key),
            new,
            ..
        } = rekeyed
        else {
            panic!("no key");
        };
        assert_eq!(key.get(0), Some(&Datum::Text("7".into())));
        assert_eq!(new.get(0), Some(&Datum::Text("8".into())));
    }

    #[test]
    fn a_delete_carries_the_key_or_the_whole_old_row() {
        let keyed = decode(&encode::delete(16385, b'K', &[Some("7"), None, None])).unwrap();
        assert!(matches!(
            keyed,
            Message::Delete {
                key: Some(_),
                old: None,
                ..
            }
        ));
        let full = decode(&encode::delete(
            16385,
            b'O',
            &[Some("7"), Some("vm"), Some("2")],
        ))
        .unwrap();
        assert!(matches!(
            full,
            Message::Delete {
                key: None,
                old: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn transactions_have_a_beginning_and_an_end() {
        assert_eq!(
            decode(&encode::begin(0x16B3748, 700_000_000, 42)).unwrap(),
            Message::Begin {
                final_lsn: Lsn(0x16B3748),
                timestamp: 700_000_000,
                xid: 42
            }
        );
        assert_eq!(
            decode(&encode::commit(0x16B3748, 0x16B3780, 700_000_000)).unwrap(),
            Message::Commit {
                lsn: Lsn(0x16B3748),
                end_lsn: Lsn(0x16B3780),
                timestamp: 700_000_000
            }
        );
    }

    #[test]
    fn unchanged_large_values_and_binary_values_are_kept_apart() {
        let mut bytes = vec![b'I'];
        bytes.extend(1u32.to_be_bytes());
        bytes.push(b'N');
        bytes.extend(2i16.to_be_bytes());
        bytes.push(b'u');
        bytes.push(b'b');
        bytes.extend(2i32.to_be_bytes());
        bytes.extend([0xDE, 0xAD]);
        let Message::Insert { new, .. } = decode(&bytes).unwrap() else {
            panic!();
        };
        assert_eq!(new.0, [Datum::Unchanged, Datum::Binary(vec![0xDE, 0xAD])]);
        assert_eq!(new.0[1].as_text(), None);
    }

    #[test]
    fn what_cannot_be_understood_is_refused_and_says_where() {
        let complaint = |bytes: &[u8]| decode(bytes).unwrap_err().to_string();

        let truncated = &encode::insert(16385, &[Some("7")])[..8];
        assert!(
            complaint(truncated).contains("ends at byte"),
            "{}",
            complaint(truncated)
        );

        let mut trailing = encode::begin(1, 2, 3);
        trailing.push(0);
        assert!(
            complaint(&trailing).contains("left over"),
            "{}",
            complaint(&trailing)
        );

        assert!(complaint(b"?").contains("unknown message"));
        assert!(decode(b"").is_err());

        // An insert of one column whose kind is not one of the four.
        let mut bad_column = vec![b'I'];
        bad_column.extend(1u32.to_be_bytes());
        bad_column.push(b'N');
        bad_column.extend(1i16.to_be_bytes());
        bad_column.push(b'x');
        assert!(
            complaint(&bad_column).contains("column kind"),
            "{}",
            complaint(&bad_column)
        );
    }

    #[test]
    fn every_prefix_of_a_message_is_refused_and_never_panics() {
        let whole: Vec<Vec<u8>> = vec![
            encode::relation(
                16385,
                "public",
                "resources",
                b'f',
                &[("id", true, 23), ("name", false, 25)],
            ),
            encode::insert(16385, &[Some("7"), None]),
            encode::update(16385, Some(&[Some("7"), None]), &[Some("8"), Some("x")]),
            encode::update(16385, None, &[Some("8"), Some("x")]),
            encode::delete(16385, b'K', &[Some("8"), None]),
            encode::delete(16385, b'O', &[Some("8"), Some("x")]),
            encode::begin(1, 2, 3),
            encode::commit(4, 5, 6),
        ];
        for bytes in &whole {
            decode(bytes).unwrap_or_else(|error| panic!("the whole message decodes: {error}"));
            for cut in 1..bytes.len() {
                let error = decode(&bytes[..cut]).unwrap_err().to_string();
                assert!(error.contains("byte"), "prefix of {cut} bytes: {error}");
            }
            let mut longer = bytes.clone();
            longer.push(0);
            assert!(
                decode(&longer)
                    .unwrap_err()
                    .to_string()
                    .contains("left over")
            );
        }
    }

    #[test]
    fn a_relation_with_an_unknown_identity_or_bad_text_is_refused() {
        let mut odd_identity = encode::relation(1, "public", "t", b'z', &[("id", true, 23)]);
        assert!(
            decode(&odd_identity)
                .unwrap_err()
                .to_string()
                .contains("replica identity")
        );
        odd_identity.clear();

        let mut bad_text = encode::relation(1, "public", "t", b'd', &[("id", true, 23)]);
        // The schema name starts right after the type byte and the id: make
        // it invalid UTF-8.
        bad_text[5] = 0xff;
        let error = decode(&bad_text).unwrap_err().to_string();
        assert!(
            error.contains("text") || error.contains("UTF-8") || error.contains("byte"),
            "{error}"
        );
    }

    #[test]
    fn every_replica_identity_is_read_and_every_message_describes_itself() {
        for (byte, identity) in [
            (b'd', ReplicaIdentity::Default),
            (b'n', ReplicaIdentity::Nothing),
            (b'f', ReplicaIdentity::Full),
            (b'i', ReplicaIdentity::Index),
        ] {
            let Message::Relation(relation) =
                decode(&encode::relation(1, "s", "t", byte, &[("id", true, 23)])).unwrap()
            else {
                panic!("not a relation");
            };
            assert_eq!(relation.replica_identity, identity);
        }
        let shown: Vec<String> = [
            decode(&encode::begin(1, 2, 3)).unwrap(),
            decode(&encode::commit(4, 5, 6)).unwrap(),
            decode(&encode::relation(1, "s", "t", b'd', &[("id", true, 23)])).unwrap(),
            decode(&encode::insert(1, &[Some("x")])).unwrap(),
            decode(&encode::update(1, None, &[Some("x")])).unwrap(),
            decode(&encode::delete(1, b'K', &[Some("x")])).unwrap(),
            Message::Truncate {
                relations: vec![1, 2],
            },
            Message::Other(b'Y'),
        ]
        .iter()
        .map(ToString::to_string)
        .collect();
        assert_eq!(
            shown,
            [
                "begin 3",
                "commit at 0/5",
                "relation s.t",
                "insert into 1",
                "update of 1",
                "delete from 1",
                "truncate of [1, 2]",
                "message 'Y'"
            ]
        );
    }

    #[test]
    fn a_truncate_names_its_tables_and_odd_parts_are_refused() {
        let mut truncate = vec![b'T'];
        truncate.extend(2i32.to_be_bytes());
        truncate.push(0);
        truncate.extend(16385u32.to_be_bytes());
        truncate.extend(16386u32.to_be_bytes());
        assert_eq!(
            decode(&truncate).unwrap(),
            Message::Truncate {
                relations: vec![16385, 16386]
            }
        );
        let mut negative = vec![b'T'];
        negative.extend((-1i32).to_be_bytes());
        negative.push(0);
        assert!(
            decode(&negative)
                .unwrap_err()
                .to_string()
                .contains("truncated")
        );

        let mut update = encode::update(1, None, &[Some("x")]);
        update[5] = b'Z';
        let error = decode(&update).unwrap_err().to_string();
        assert!(
            error.contains("unexpected part 'Z' of an update"),
            "{error}"
        );

        let mut delete = encode::delete(1, b'K', &[Some("x")]);
        delete[5] = b'Z';
        let error = decode(&delete).unwrap_err().to_string();
        assert!(error.contains("unexpected part 'Z' of a delete"), "{error}");

        let mut insert = encode::insert(1, &[Some("x")]);
        insert[5] = b'O';
        let error = decode(&insert).unwrap_err().to_string();
        assert!(error.contains("expected 'N', found 'O'"), "{error}");
    }

    #[test]
    fn messages_of_no_use_are_passed_over() {
        assert_eq!(decode(b"Yanything").unwrap(), Message::Other(b'Y'));
        assert_eq!(decode(b"O").unwrap(), Message::Other(b'O'));
    }
}
