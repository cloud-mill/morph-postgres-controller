//! What is known about tables within one replication session, and the row
//! changes that knowledge makes sense of.
//!
//! A decoded change names its table by a number and its values by position.
//! The [`RelationCache`] keeps the descriptions the server has sent on the
//! current connection, and with them turns a change message into a
//! [`RowChange`] whose values can be read by column name. That is what the
//! mapper needs to name a resource, and it is all the values are used for.
//!
//! This step comes after [`super::pgoutput`] has decoded a message and
//! before the core's [`TransactionBuffer`](morph_controller_core::source::TransactionBuffer)
//! gathers what the change touched.

use std::collections::HashMap;
use std::sync::Arc;

use super::pgoutput::{Datum, Message, Relation, Tuple};
use crate::error::{Error, Result};

/// What kind of change happened to a row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeKind {
    /// The row is new.
    Insert,
    /// The row changed.
    Update,
    /// The row is gone.
    Delete,
}

/// A change to one row, with the table it belongs to.
///
/// The values say which row changed, and so which resource to look at. They
/// are never used as the resource's desired state, which is read from the
/// table when the resource is reconciled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowChange {
    /// What happened.
    pub kind: ChangeKind,
    /// The table, as described at the time of the change.
    pub relation: Arc<Relation>,
    /// The row as the change left it, for an insert or an update.
    pub new: Option<Tuple>,
    /// The row as it was, or the identifying part of it.
    ///
    /// For a delete this is the whole row if the table's replica identity is
    /// full, and otherwise the key alone, with every other column null. For
    /// an update it is the whole old row if the identity is full, the old
    /// key if the update changed the key, and otherwise nothing. An insert
    /// has none.
    pub old: Option<Tuple>,
}

impl RowChange {
    /// The value of the column called `name`, as text.
    ///
    /// The new row is looked at first. The old one is looked at if there is
    /// no new row, or if the new row has no text in that column, as with a
    /// null or a large value sent as unchanged. For an update that changed
    /// the key, this gives the new key.
    ///
    /// `None` if the table has no such column, or if neither row has text
    /// there. A column outside the key is null in a key tuple, so it reads
    /// as `None` for a delete from a table whose replica identity is not
    /// full.
    pub fn text(&self, name: &str) -> Option<&str> {
        let index = self.relation.column(name)?;
        [self.new.as_ref(), self.old.as_ref()]
            .into_iter()
            .flatten()
            .find_map(|tuple| tuple.get(index).and_then(Datum::as_text))
    }

    /// The identifying values the change was sent with, as text, by column
    /// name.
    ///
    /// One entry for each column of the replica identity, in column order.
    /// The values come from the new row when the change has one and from
    /// the old row otherwise, without looking from one to the other column
    /// by column as [`text`](Self::text) does.
    pub fn key_values(&self) -> Vec<(&str, Option<&str>)> {
        self.relation
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.key)
            .map(|(index, column)| (column.name.as_str(), self.text_at(index)))
            .collect()
    }

    /// The text at `index` of the new row, or of the old row if the change
    /// has no new one.
    fn text_at(&self, index: usize) -> Option<&str> {
        self.new
            .as_ref()
            .or(self.old.as_ref())
            .and_then(|tuple| tuple.get(index))
            .and_then(Datum::as_text)
    }
}

/// The tables described so far in a replication session.
///
/// `pgoutput` describes a table before the first change to it in a session,
/// and again whenever its definition changes. The description is only valid
/// within the session, so a new connection starts with an empty cache. The
/// cache is keyed by the relation id, which is the server's handle for the
/// table and never a name for anything.
#[derive(Debug, Default)]
pub struct RelationCache {
    relations: HashMap<u32, Arc<Relation>>,
}

impl RelationCache {
    /// An empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Remembers a table's description, in place of any earlier one with
    /// the same id.
    ///
    /// Changes made from earlier messages keep the description they were
    /// made with.
    pub fn insert(&mut self, relation: Relation) {
        self.relations.insert(relation.id, Arc::new(relation));
    }

    /// The description of the table with this relation id.
    ///
    /// # Errors
    ///
    /// [`Error::Decode`] if no description with this id has arrived in the
    /// session. The server describes a table before it sends changes to it,
    /// so this points to a description that was lost or not understood.
    pub fn get(&self, id: u32) -> Result<Arc<Relation>> {
        self.relations.get(&id).cloned().ok_or_else(|| {
            Error::Decode(format!(
                "a change to relation {id} arrived before its description"
            ))
        })
    }

    /// How many tables are described.
    pub fn len(&self) -> usize {
        self.relations.len()
    }

    /// Whether no table is described yet.
    pub fn is_empty(&self) -> bool {
        self.relations.is_empty()
    }

    /// Turns a change message into a change to a row of a known table.
    ///
    /// Messages that are not changes to rows give `None`. The old row of
    /// the change is the whole old row if the message has one, and the old
    /// key otherwise.
    ///
    /// # Errors
    ///
    /// [`Error::Decode`] if the table of an insert, update or delete has not
    /// been described in this session.
    pub fn row_change(&self, message: &Message) -> Result<Option<RowChange>> {
        let change = match message {
            Message::Insert { relation, new } => RowChange {
                kind: ChangeKind::Insert,
                relation: self.get(*relation)?,
                new: Some(new.clone()),
                old: None,
            },
            Message::Update {
                relation,
                key,
                old,
                new,
            } => RowChange {
                kind: ChangeKind::Update,
                relation: self.get(*relation)?,
                new: Some(new.clone()),
                old: old.clone().or_else(|| key.clone()),
            },
            Message::Delete { relation, key, old } => RowChange {
                kind: ChangeKind::Delete,
                relation: self.get(*relation)?,
                new: None,
                old: old.clone().or_else(|| key.clone()),
            },
            _ => return Ok(None),
        };
        Ok(Some(change))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::pgoutput::{Column, ReplicaIdentity};

    fn relation() -> Relation {
        let column = |name: &str, key: bool| Column {
            name: name.to_owned(),
            key,
            type_oid: 25,
            modifier: -1,
        };
        Relation {
            id: 7,
            schema: "public".into(),
            name: "resources".into(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                column("id", true),
                column("resource_type", false),
                column("cpu", false),
            ],
        }
    }

    fn text(values: &[Option<&str>]) -> Tuple {
        Tuple(
            values
                .iter()
                .map(|value| value.map_or(Datum::Null, |text| Datum::Text(text.to_owned())))
                .collect(),
        )
    }

    #[test]
    fn a_change_before_a_description_is_an_error() {
        let cache = RelationCache::new();
        let message = Message::Insert {
            relation: 7,
            new: text(&[Some("1")]),
        };
        assert!(
            cache
                .row_change(&message)
                .unwrap_err()
                .to_string()
                .contains("before its description")
        );
        assert!(cache.is_empty());
    }

    #[test]
    fn values_are_read_from_the_new_row_and_then_the_old() {
        let mut cache = RelationCache::new();
        cache.insert(relation());
        assert_eq!(cache.len(), 1);

        let update = Message::Update {
            relation: 7,
            key: Some(text(&[Some("1"), None, None])),
            old: None,
            new: text(&[Some("2"), Some("vm"), Some("4")]),
        };
        let change = cache.row_change(&update).unwrap().unwrap();
        assert_eq!(change.kind, ChangeKind::Update);
        assert_eq!(change.text("id"), Some("2"), "the new key wins");
        assert_eq!(change.text("cpu"), Some("4"));
        assert_eq!(change.text("nothing"), None);

        let delete = Message::Delete {
            relation: 7,
            key: Some(text(&[Some("2"), None, None])),
            old: None,
        };
        let change = cache.row_change(&delete).unwrap().unwrap();
        assert_eq!(change.kind, ChangeKind::Delete);
        assert_eq!(change.text("id"), Some("2"));
        assert_eq!(change.text("resource_type"), None, "not sent with a key");
        assert_eq!(change.key_values(), [("id", Some("2"))]);

        let begin = Message::Begin {
            final_lsn: Lsn::ZERO,
            timestamp: 0,
            xid: 1,
        };
        assert_eq!(cache.row_change(&begin).unwrap(), None);
    }

    #[test]
    fn messages_that_are_not_row_changes_give_nothing_and_unknown_tables_are_errors() {
        let mut cache = RelationCache::new();
        assert!(cache.get(7).is_err());
        cache.insert(relation());
        cache.insert(relation());
        assert_eq!(cache.len(), 1, "a description replaces the last");
        assert_eq!(cache.get(7).unwrap().name, "resources");
        for message in [
            Message::Commit {
                lsn: Lsn::ZERO,
                end_lsn: Lsn::ZERO,
                timestamp: 0,
            },
            Message::Relation(relation()),
            Message::Truncate { relations: vec![7] },
            Message::Other(b'M'),
        ] {
            assert_eq!(cache.row_change(&message).unwrap(), None);
        }
        let unknown = Message::Delete {
            relation: 8,
            key: None,
            old: Some(text(&[Some("1"), None, None])),
        };
        assert!(matches!(
            cache.row_change(&unknown).unwrap_err(),
            Error::Decode(_)
        ));

        // An insert reads the new row, and a delete with the old row reads
        // every column it carries.
        let insert = Message::Insert {
            relation: 7,
            new: text(&[Some("9"), Some("vm"), None]),
        };
        let change = cache.row_change(&insert).unwrap().unwrap();
        assert_eq!(change.kind, ChangeKind::Insert);
        assert_eq!(change.text("resource_type"), Some("vm"));
        assert_eq!(change.text("cpu"), None, "null is nothing");
        assert_eq!(change.key_values(), [("id", Some("9"))]);
        let delete = Message::Delete {
            relation: 7,
            key: None,
            old: Some(text(&[Some("9"), Some("vm"), Some("2")])),
        };
        let change = cache.row_change(&delete).unwrap().unwrap();
        assert_eq!(change.text("cpu"), Some("2"));
        assert!(format!("{change:?}").contains("Delete"));
    }

    use crate::postgres::lsn::Lsn;
}
