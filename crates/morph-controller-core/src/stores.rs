//! Where Morph keeps its record of each resource.
//!
//! Morph plans against a record of what it has made, and loads and saves
//! that record whole. The controller gives each resource, or each stack of
//! rows, a record of its own, in a store of its own. Reconciling one then
//! never reads, plans for or writes another, and two can be reconciled at
//! once without their saves colliding.
//!
//! [`StateStores`] hands the workers the store for a resource. It also
//! tells the periodic loop which resources have a record, which is how a
//! resource whose row has gone is still looked at. Records in a directory
//! of files or in memory are kept here. Records anywhere else, such as
//! Morph's own table in a database, come through [`RecordStores`], which a
//! front-end that has the connection implements.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use morph::{JsonFileStateStore, MemoryStateStore, ResourceId, StateStore};
use tokio::sync::Mutex;

use crate::config::{MorphConfig, StateConfig};
use crate::error::{Error, Result};

/// Stores for Morph's records, by name, kept somewhere the core does not
/// know about.
///
/// The record of a resource is the state whose name is the resource id,
/// under whatever prefix the implementation chooses.
#[async_trait]
pub trait RecordStores: Send + Sync + fmt::Debug {
    /// The store that holds the record of `resource`, and of nothing else.
    ///
    /// # Errors
    ///
    /// [`Error::Checkpoint`] if the store cannot be connected.
    async fn store_for(&self, resource: &ResourceId) -> Result<Arc<dyn StateStore>>;

    /// Every resource that has a record, in the order of their ids.
    ///
    /// # Errors
    ///
    /// [`Error::Connection`], [`Error::Query`] or [`Error::Checkpoint`] if
    /// the records cannot be listed.
    async fn managed(&self) -> Result<Vec<ResourceId>>;
}

/// The stores that hold Morph's records, one for each resource, by backend.
pub enum StateStores {
    /// In a directory of JSON files.
    ///
    /// The record of a resource is the file `kind/name.json` under the
    /// directory.
    File {
        /// The directory.
        directory: PathBuf,
    },
    /// In memory, and gone when the process ends.
    Memory {
        /// The stores made so far, by resource.
        stores: Mutex<HashMap<ResourceId, Arc<MemoryStateStore>>>,
    },
    /// Wherever a [`RecordStores`] keeps them: Morph's own table in a
    /// database, for the front-end that has the connection.
    Named(Arc<dyn RecordStores>),
}

impl fmt::Debug for StateStores {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StateStores::File { directory } => f
                .debug_struct("File")
                .field("directory", directory)
                .finish(),
            StateStores::Memory { .. } => f.write_str("Memory"),
            StateStores::Named(stores) => f.debug_tuple("Named").field(stores).finish(),
        }
    }
}

impl StateStores {
    /// The `file` or `memory` stores the configuration asks for.
    ///
    /// Nothing is read or written yet.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] if the configuration asks for the `postgres`
    /// backend, which only a front-end with a database can make, through
    /// [`named`](Self::named).
    pub fn new(config: &MorphConfig) -> Result<Self> {
        Ok(match &config.state {
            StateConfig::File { directory } => StateStores::File {
                directory: directory.clone(),
            },
            StateConfig::Memory => StateStores::Memory {
                stores: Mutex::new(HashMap::new()),
            },
            StateConfig::Postgres => {
                return Err(Error::Config(
                    "morph.state asks for records in PostgreSQL, which this controller has no connection for"
                        .into(),
                ));
            }
        })
    }

    /// Stores kept by `stores`, wherever that is.
    pub fn named(stores: Arc<dyn RecordStores>) -> Self {
        StateStores::Named(stores)
    }

    /// The store that holds the record of `resource`, and of nothing else.
    ///
    /// * `file`: a store over the resource's file, made anew at each call.
    ///   Nothing is read or written until Morph uses it. Stores made
    ///   separately for one file do not take turns, and it is the
    ///   scheduler, by never giving a resource two turns at once, that
    ///   keeps two of them from being in use together.
    /// * `memory`: the store made for the resource before, or a new and
    ///   empty one.
    /// * Named: whatever the [`RecordStores`] gives.
    ///
    /// # Errors
    ///
    /// Whatever a [`RecordStores`] fails with. The other backends do not
    /// fail here.
    pub async fn store_for(&self, resource: &ResourceId) -> Result<Arc<dyn StateStore>> {
        Ok(match self {
            StateStores::File { directory } => Arc::new(JsonFileStateStore::new(
                directory
                    .join(resource.kind().as_str())
                    .join(format!("{}.json", resource.name())),
            )),
            StateStores::Memory { stores } => {
                let mut stores = stores.lock().await;
                Arc::clone(
                    stores
                        .entry(resource.clone())
                        .or_insert_with(|| Arc::new(MemoryStateStore::new())),
                ) as Arc<dyn StateStore>
            }
            StateStores::Named(stores) => return stores.store_for(resource).await,
        })
    }

    /// Every resource that has a record, in the order of their ids.
    ///
    /// Having a record is not the same as existing. Nothing here removes a
    /// record when its resource is deleted, so the resource goes on being
    /// listed.
    ///
    /// * `file`: every `kind/name.json` under the directory whose kind and
    ///   name make a resource id. A directory that does not exist means no
    ///   records.
    /// * `memory`: every resource a store was ever asked for.
    /// * Named: whatever the [`RecordStores`] lists.
    ///
    /// # Errors
    ///
    /// * [`Error::Checkpoint`] if the `file` backend cannot read a
    ///   directory that exists.
    /// * Whatever a [`RecordStores`] fails with.
    pub async fn managed(&self) -> Result<Vec<ResourceId>> {
        let mut resources = match self {
            StateStores::File { directory } => {
                let mut found = Vec::new();
                let kinds = match std::fs::read_dir(directory) {
                    Ok(kinds) => kinds,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(found),
                    Err(error) => return Err(Error::Checkpoint(error.to_string())),
                };
                for kind in kinds.flatten().filter(|entry| entry.path().is_dir()) {
                    let kind_name = kind.file_name().to_string_lossy().into_owned();
                    for file in std::fs::read_dir(kind.path())
                        .map_err(|e| Error::Checkpoint(e.to_string()))?
                        .flatten()
                    {
                        let name = file.file_name().to_string_lossy().into_owned();
                        if let Some(name) = name.strip_suffix(".json")
                            && let Ok(resource) = ResourceId::new(kind_name.clone(), name)
                        {
                            found.push(resource);
                        }
                    }
                }
                found
            }
            StateStores::Memory { stores } => stores.lock().await.keys().cloned().collect(),
            StateStores::Named(stores) => return stores.managed().await,
        };
        resources.sort();
        Ok(resources)
    }
}

/// A failure of one of Morph's stores is a failure to keep a record, and is
/// reported as [`Error::Checkpoint`] with the store's own account of it.
impl From<morph::StoreError> for Error {
    fn from(error: morph::StoreError) -> Self {
        Error::Checkpoint(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StateConfig;
    use morph::ObservedState;

    fn config(state: StateConfig) -> MorphConfig {
        MorphConfig {
            state,
            ..MorphConfig::default()
        }
    }

    #[tokio::test]
    async fn the_file_backend_keeps_one_file_per_resource_and_lists_them() {
        let directory = std::env::temp_dir().join(format!("mcc-stores-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        let stores = StateStores::new(&config(StateConfig::File {
            directory: directory.clone(),
        }))
        .unwrap();
        assert!(format!("{stores:?}").contains("File"));
        assert_eq!(
            stores.managed().await.unwrap(),
            Vec::<ResourceId>::new(),
            "no directory yet"
        );

        let web: ResourceId = "vm.web-1".parse().unwrap();
        let store = stores.store_for(&web).await.unwrap();
        store.save(&ObservedState::new()).await.unwrap();
        assert!(directory.join("vm/web-1.json").is_file());

        // Things that are not records are passed over.
        std::fs::write(directory.join("vm/notes.txt"), "x").unwrap();
        std::fs::write(directory.join("vm/bad name.json"), "{}").unwrap();
        std::fs::write(directory.join("stray.json"), "{}").unwrap();
        let listed = stores.managed().await.unwrap();
        assert_eq!(listed, vec![web]);
        let _ = std::fs::remove_dir_all(&directory);
    }

    #[tokio::test]
    async fn the_memory_backend_hands_out_one_store_per_resource_and_forgets_nothing_while_it_runs()
    {
        let stores = StateStores::new(&config(StateConfig::Memory)).unwrap();
        assert!(format!("{stores:?}").contains("Memory"));
        let a: ResourceId = "vm.a".parse().unwrap();
        let b: ResourceId = "vm.b".parse().unwrap();
        let first = stores.store_for(&a).await.unwrap();
        let again = stores.store_for(&a).await.unwrap();
        assert!(Arc::ptr_eq(&first, &again), "the same store each time");
        stores.store_for(&b).await.unwrap();
        assert_eq!(stores.managed().await.unwrap(), vec![a, b]);
    }

    #[derive(Debug)]
    struct Nowhere;

    #[async_trait]
    impl RecordStores for Nowhere {
        async fn store_for(&self, _resource: &ResourceId) -> Result<Arc<dyn StateStore>> {
            Ok(Arc::new(MemoryStateStore::new()))
        }

        async fn managed(&self) -> Result<Vec<ResourceId>> {
            Ok(vec!["vm.named".parse().unwrap()])
        }
    }

    #[tokio::test]
    async fn the_postgres_backend_needs_a_front_end_and_named_stores_are_passed_through() {
        let refused = StateStores::new(&config(StateConfig::Postgres)).unwrap_err();
        assert!(
            matches!(refused, Error::Config(ref m) if m.contains("PostgreSQL")),
            "{refused}"
        );
        let stores = StateStores::named(Arc::new(Nowhere));
        assert!(format!("{stores:?}").contains("Nowhere"));
        let a: ResourceId = "vm.a".parse().unwrap();
        stores
            .store_for(&a)
            .await
            .unwrap()
            .save(&ObservedState::new())
            .await
            .unwrap();
        assert_eq!(
            stores.managed().await.unwrap(),
            vec!["vm.named".parse::<ResourceId>().unwrap()]
        );
    }

    #[tokio::test]
    async fn a_file_where_the_record_directory_should_be_is_a_checkpoint_failure() {
        let file = std::env::temp_dir().join(format!("mcc-not-a-dir-{}", std::process::id()));
        std::fs::write(&file, "not a directory").unwrap();
        let stores = StateStores::new(&config(StateConfig::File {
            directory: file.clone(),
        }))
        .unwrap();
        let error = stores.managed().await.unwrap_err();
        assert!(matches!(error, Error::Checkpoint(_)), "{error}");
        let _ = std::fs::remove_file(&file);
    }
}
