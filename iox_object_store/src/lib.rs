#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

//! Wraps the object_store crate with IOx-specific semantics. The main responsibility of this crate
//! is to be the single source of truth for the paths of files in object storage. There is a
//! specific path type for each IOx-specific reason an object storage file exists. Content of the
//! files is managed outside of this crate.

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use data_types::{error::ErrorLogger, server_id::ServerId};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{
    path::{parsed::DirsAndFileName, Path},
    ObjectStore, ObjectStoreApi, Result,
};
use observability_deps::tracing::warn;
use snafu::{ensure, ResultExt, Snafu};
use std::{str::FromStr, sync::Arc};
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

mod paths;
pub use paths::{
    parquet_file::{ParquetFilePath, ParquetFilePathParseError},
    transaction_file::TransactionFilePath,
};
use paths::{DataPath, RootPath, TombstonePath, TransactionsPath};

/// Detailed metadata about a database.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DatabaseMetadata {
    /// The UUID of the database
    pub uuid: Uuid,
    /// The UTC datetime at which this database was deleted, if applicable
    pub deleted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum IoxObjectStoreError {
    #[snafu(display("{}", source))]
    UnderlyingObjectStoreError { source: object_store::Error },

    #[snafu(display("Cannot create database with UUID `{}`; it already exists", uuid))]
    DatabaseAlreadyExists { uuid: Uuid },

    #[snafu(display(
        "Cannot restore; there is already an active database with UUID `{}`",
        uuid
    ))]
    ActiveDatabaseAlreadyExists { uuid: Uuid },

    #[snafu(display("Could not restore database with UUID `{}`: {}", uuid, source))]
    RestoreFailed {
        uuid: Uuid,
        source: object_store::Error,
    },
}

/// Handles persistence of data for a particular database. Writes within its directory/prefix.
///
/// This wrapper on top of an `ObjectStore` maps IOx specific concepts to ObjectStore locations
#[derive(Debug)]
pub struct IoxObjectStore {
    inner: Arc<ObjectStore>,
    server_id: ServerId,
    uuid: Uuid,
    root_path: RootPath,
    tombstone_path: TombstonePath,
    data_path: DataPath,
    transactions_path: TransactionsPath,
}

impl IoxObjectStore {
    /// List database names in object storage that need to be further checked for generations
    /// and whether they're marked as deleted or not.
    pub async fn list_possible_databases(
        inner: &ObjectStore,
        server_id: ServerId,
    ) -> Result<Vec<Uuid>> {
        let path = paths::all_databases_path(inner, server_id);

        let list_result = inner.list_with_delimiter(&path).await?;

        Ok(list_result
            .common_prefixes
            .into_iter()
            .filter_map(|prefix| {
                let prefix_parsed: DirsAndFileName = prefix.into();
                let last = prefix_parsed
                    .directories
                    .last()
                    .expect("path can't be empty");
                let uuid = Uuid::from_str(&last.encoded().to_string())
                    .log_if_error("invalid database directory")
                    .ok()?;

                Some(uuid)
            })
            .collect())
    }

    /// List databases marked as deleted in in object storage along with their generation IDs and
    /// when they were deleted. Enables a user to choose a generation for a database that they
    /// would want to restore or would want to delete permanently.
    pub async fn list_deleted_databases(
        inner: &ObjectStore,
        server_id: ServerId,
    ) -> Result<Vec<DatabaseMetadata>> {
        Ok(Self::list_detailed_databases(inner, server_id)
            .await?
            .into_iter()
            .filter(|detailed_database| detailed_database.deleted_at.is_some())
            .collect())
    }

    /// List all databases in in object storage along with if/when they were deleted. Useful for
    /// visibility into object storage and finding databases to restore or permanently delete.
    pub async fn list_detailed_databases(
        inner: &ObjectStore,
        server_id: ServerId,
    ) -> Result<Vec<DatabaseMetadata>> {
        let path = paths::all_databases_path(inner, server_id);

        let list_result = inner.list_with_delimiter(&path).await?;

        let mut all_dbs = Vec::new();

        for prefix in list_result.common_prefixes {
            let prefix_parsed: DirsAndFileName = prefix.into();
            let last = prefix_parsed
                .directories
                .last()
                .expect("path can't be empty");

            if let Ok(uuid) = Uuid::from_str(&last.encoded().to_string())
                .log_if_error("invalid database directory")
            {
                let root_path = RootPath::new(inner, server_id, uuid);
                let deleted_at = Self::check_deleted(inner, &root_path).await?;

                all_dbs.push(DatabaseMetadata { uuid, deleted_at });
            }
        }

        Ok(all_dbs)
    }

    // Private function to check if a given database has been deleted or not. If the database is
    // active, this returns `None`. If the database has been deleted, this returns
    // `Some(deleted_at)` where `deleted_at` is the time at which the database was deleted.
    async fn check_deleted(
        inner: &ObjectStore,
        root_path: &RootPath,
    ) -> Result<Option<DateTime<Utc>>> {
        let list_result = inner.list_with_delimiter(&root_path.inner).await?;

        let tombstone_file = root_path.tombstone_path();
        let deleted_at = list_result
            .objects
            .into_iter()
            .find(|object| object.location == tombstone_file.inner)
            .map(|object| object.last_modified);

        Ok(deleted_at)
    }

    /// Create a database-specific wrapper. Takes all the information needed to create a new
    /// root directory of a database. Checks that there isn't already anything under this UUID in
    /// object storage.
    ///
    /// Caller *MUST* ensure there is at most 1 concurrent call of this function with the same
    /// parameters; this function does *NOT* do any locking.
    pub async fn new(
        inner: Arc<ObjectStore>,
        server_id: ServerId,
        uuid: Uuid,
    ) -> Result<Self, IoxObjectStoreError> {
        let root_path = RootPath::new(&inner, server_id, uuid);
        let list_result = inner
            .list_with_delimiter(&root_path.inner)
            .await
            .context(UnderlyingObjectStoreError)?;

        ensure!(
            list_result.objects.is_empty(),
            DatabaseAlreadyExists { uuid }
        );

        Ok(Self::existing(inner, server_id, uuid, root_path))
    }

    /// Look in object storage for an existing, active database with this UUID.
    pub async fn find_existing(
        inner: Arc<ObjectStore>,
        server_id: ServerId,
        uuid: Uuid,
    ) -> Result<Option<Self>, IoxObjectStoreError> {
        let root_path = RootPath::new(&inner, server_id, uuid);
        let list_result = inner
            .list_with_delimiter(&root_path.inner)
            .await
            .context(UnderlyingObjectStoreError)?;

        let rules_file = root_path.rules_path();
        let rules = list_result
            .objects
            .iter()
            .find(|object| object.location == rules_file.inner);

        if rules.is_none() {
            return Ok(None);
        }

        let tombstone_file = root_path.tombstone_path();
        let tombstone = list_result
            .objects
            .iter()
            .find(|object| object.location == tombstone_file.inner);

        if tombstone.is_some() {
            return Ok(None);
        }

        Ok(Some(Self::existing(inner, server_id, uuid, root_path)))
    }

    /// Access the database-specific object storage files for an existing database that has
    /// already been located and verified to be active. Does not check object storage.
    fn existing(
        inner: Arc<ObjectStore>,
        server_id: ServerId,
        uuid: Uuid,
        root_path: RootPath,
    ) -> Self {
        let tombstone_path = root_path.tombstone_path();
        let data_path = root_path.data_path();
        let transactions_path = root_path.transactions_path();

        Self {
            inner,
            server_id,
            uuid,
            root_path,
            tombstone_path,
            data_path,
            transactions_path,
        }
    }

    /// The UUID of the database this object store is for.
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    /// The location in object storage for all files for this database, suitable for logging
    /// or debugging purposes only. Do not parse this, as its format is subject to change!
    pub fn debug_database_path(&self) -> String {
        self.root_path.inner.to_string()
    }

    // Deliberately private; this should not leak outside this crate
    // so assumptions about the object store organization are confined
    // (and can be changed) in this crate
    fn tombstone_path(&self) -> Path {
        self.root_path.tombstone_path().inner
    }

    /// Write the file in the database directory that indicates this database is marked as deleted,
    /// without yet actually deleting this directory or any files it contains in object storage.
    pub async fn write_tombstone(&self) -> Result<()> {
        self.inner.put(&self.tombstone_path(), Bytes::new()).await
    }

    /// Remove the tombstone file to restore a database generation. Will return an error if this
    /// generation is already active or if there is another database generation already active for
    /// this database name. Returns the reactivated IoxObjectStore.
    pub async fn restore_database(
        inner: Arc<ObjectStore>,
        server_id: ServerId,
        uuid: Uuid,
    ) -> Result<Self, IoxObjectStoreError> {
        let root_path = RootPath::new(&inner, server_id, uuid);

        let deleted_at = Self::check_deleted(&inner, &root_path)
            .await
            .context(UnderlyingObjectStoreError)?;

        ensure!(deleted_at.is_some(), ActiveDatabaseAlreadyExists { uuid });

        let tombstone_path = root_path.tombstone_path();

        inner
            .delete(&tombstone_path.inner)
            .await
            .context(RestoreFailed { uuid })?;

        Ok(Self::existing(inner, server_id, uuid, root_path))
    }

    // Catalog transaction file methods ===========================================================

    /// List all the catalog transaction files in object storage for this database.
    pub async fn catalog_transaction_files(
        &self,
    ) -> Result<BoxStream<'static, Result<Vec<TransactionFilePath>>>> {
        Ok(self
            .list(Some(&self.transactions_path.inner))
            .await?
            .map_ok(move |list| {
                list.into_iter()
                    // This `flat_map` ignores any filename in the transactions_path we couldn't
                    // parse as a TransactionFilePath
                    .flat_map(TransactionFilePath::from_absolute)
                    .collect()
            })
            .boxed())
    }

    /// Get the catalog transaction data in this relative path in this database's object store.
    pub async fn get_catalog_transaction_file(
        &self,
        location: &TransactionFilePath,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        let full_path = self.transactions_path.join(location);

        self.inner.get(&full_path).await
    }

    /// Store the data for this parquet file in this database's object store.
    pub async fn put_catalog_transaction_file(
        &self,
        location: &TransactionFilePath,
        bytes: Bytes,
    ) -> Result<()> {
        let full_path = self.transactions_path.join(location);

        self.inner.put(&full_path, bytes).await
    }

    /// Delete all catalog transaction files for this database.
    pub async fn wipe_catalog(&self) -> Result<()> {
        let mut stream = self.catalog_transaction_files().await?;

        while let Some(transaction_file_list) = stream.try_next().await? {
            for transaction_file_path in &transaction_file_list {
                self.delete_catalog_transaction_file(transaction_file_path)
                    .await?;
            }
        }

        Ok(())
    }

    /// Remove the data for this catalog transaction file from this database's object store
    pub async fn delete_catalog_transaction_file(
        &self,
        location: &TransactionFilePath,
    ) -> Result<()> {
        let full_path = self.transactions_path.join(location);

        self.inner.delete(&full_path).await
    }

    // Parquet file methods =======================================================================

    /// List all parquet file paths in object storage for this database.
    pub async fn parquet_files(&self) -> Result<BoxStream<'static, Result<Vec<ParquetFilePath>>>> {
        Ok(self
            .list(Some(&self.data_path.inner))
            .await?
            .map_ok(move |list| {
                list.into_iter()
                    // This `flat_map` ignores any filename in the data_path we couldn't parse as
                    // a ParquetFilePath
                    .flat_map(ParquetFilePath::from_absolute)
                    .collect()
            })
            .boxed())
    }

    /// Get the parquet file data in this relative path in this database's object store.
    pub async fn get_parquet_file(
        &self,
        location: &ParquetFilePath,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        let full_path = self.data_path.join(location);

        self.inner.get(&full_path).await
    }

    /// Store the data for this parquet file in this database's object store.
    pub async fn put_parquet_file(&self, location: &ParquetFilePath, bytes: Bytes) -> Result<()> {
        let full_path = self.data_path.join(location);

        self.inner.put(&full_path, bytes).await
    }

    /// Remove the data for this parquet file from this database's object store
    pub async fn delete_parquet_file(&self, location: &ParquetFilePath) -> Result<()> {
        let full_path = self.data_path.join(location);

        self.inner.delete(&full_path).await
    }

    // Database rule file methods =================================================================

    // Deliberately private; this should not leak outside this crate
    // so assumptions about the object store organization are confined
    // (and can be changed) in this crate
    fn db_rules_path(&self) -> Path {
        self.root_path.rules_path().inner
    }

    /// Get the data for the database rules
    pub async fn get_database_rules_file(&self) -> Result<Bytes> {
        let mut stream = self.inner.get(&self.db_rules_path()).await?;
        let mut bytes = BytesMut::new();

        while let Some(buf) = stream.next().await {
            bytes.extend(buf?);
        }

        Ok(bytes.freeze())
    }

    /// Store the data for the database rules
    pub async fn put_database_rules_file(&self, bytes: Bytes) -> Result<()> {
        self.inner.put(&self.db_rules_path(), bytes).await
    }

    /// Delete the data for the database rules
    pub async fn delete_database_rules_file(&self) -> Result<()> {
        self.inner.delete(&self.db_rules_path()).await
    }

    /// List the relative paths in this database's object store.
    ///
    // Deliberately private; this should not leak outside this crate
    // so assumptions about the object store organization are confined
    // (and can be changed) in this crate
    /// All outside calls should go to one of the more specific listing methods.
    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'static, Result<Vec<Path>>>> {
        let (tx, rx) = channel(4);
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();

        // This is necessary because of the lifetime restrictions on the ObjectStoreApi trait's
        // methods, which might not actually be necessary but fixing it involves changes to the
        // cloud_storage crate that are longer term.
        tokio::spawn(async move {
            match inner.list(prefix.as_ref()).await {
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                }
                Ok(mut stream) => {
                    while let Some(list) = stream.next().await {
                        let _ = tx.send(list).await;
                    }
                }
            }
        });

        Ok(ReceiverStream::new(rx).boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::chunk_metadata::{ChunkAddr, ChunkId};
    use object_store::{parsed_path, path::ObjectStorePath, ObjectStore, ObjectStoreApi};
    use std::num::NonZeroU32;
    use uuid::Uuid;

    /// Creates new test server ID
    fn make_server_id() -> ServerId {
        ServerId::new(NonZeroU32::new(1).unwrap())
    }

    /// Creates a new in-memory object store
    fn make_object_store() -> Arc<ObjectStore> {
        Arc::new(ObjectStore::new_in_memory())
    }

    async fn add_file(object_store: &ObjectStore, location: &Path) {
        let data = Bytes::from("arbitrary data");

        object_store.put(location, data).await.unwrap();
    }

    async fn parquet_files(iox_object_store: &IoxObjectStore) -> Vec<ParquetFilePath> {
        iox_object_store
            .parquet_files()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect()
    }

    async fn add_parquet_file(iox_object_store: &IoxObjectStore, location: &ParquetFilePath) {
        let data = Bytes::from("arbitrary data");

        iox_object_store
            .put_parquet_file(location, data)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn only_lists_relevant_parquet_files() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let server_id_string = server_id.to_string();
        let server_id_str = server_id_string.as_str();
        let uuid = Uuid::new_v4();
        let uuid_string = uuid.to_string();
        let uuid_str = uuid_string.as_str();
        let iox_object_store = IoxObjectStore::new(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap();
        let parquet_uuid = Uuid::new_v4();
        let good_filename = format!("111.{}.parquet", parquet_uuid);
        let good_filename_str = good_filename.as_str();

        // Put a non-database file in
        let path = object_store.path_from_dirs_and_filename(parsed_path!(["foo"]));
        add_file(&object_store, &path).await;

        // Put a file for some other server in
        let path = object_store.path_from_dirs_and_filename(parsed_path!(["12345"]));
        add_file(&object_store, &path).await;

        // Put a file for some other database in
        let other_db_uuid = Uuid::new_v4().to_string();
        let path = object_store
            .path_from_dirs_and_filename(parsed_path!([server_id_str, other_db_uuid.as_str()]));
        add_file(&object_store, &path).await;

        // Put a file in the database dir but not the data dir
        let path = object_store.path_from_dirs_and_filename(parsed_path!(
            [server_id_str, uuid_str],
            good_filename_str
        ));
        add_file(&object_store, &path).await;

        // Put files in the data dir whose names are in the wrong format
        let mut path = object_store.path_from_dirs_and_filename(parsed_path!(
            [server_id_str, uuid_str, "data"],
            "111.parquet"
        ));
        add_file(&object_store, &path).await;
        path.set_file_name(&format!("111.{}.xls", parquet_uuid));
        add_file(&object_store, &path).await;

        // Parquet files should be empty
        let pf = parquet_files(&iox_object_store).await;
        assert!(pf.is_empty(), "{:?}", pf);

        // Add a real parquet file
        let chunk_addr = ChunkAddr {
            db_name: "clouds".into(),
            table_name: "my_table".into(),
            partition_key: "my_partition".into(),
            chunk_id: ChunkId::new_test(13),
        };
        let p1 = ParquetFilePath::new(&chunk_addr);
        add_parquet_file(&iox_object_store, &p1).await;

        // Only the real file should be returned
        let pf = parquet_files(&iox_object_store).await;
        assert_eq!(&pf, &[p1]);
    }

    async fn catalog_transaction_files(
        iox_object_store: &IoxObjectStore,
    ) -> Vec<TransactionFilePath> {
        iox_object_store
            .catalog_transaction_files()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect()
    }

    async fn add_catalog_transaction_file(
        iox_object_store: &IoxObjectStore,
        location: &TransactionFilePath,
    ) {
        let data = Bytes::from("arbitrary data");

        iox_object_store
            .put_catalog_transaction_file(location, data)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn only_lists_relevant_catalog_transaction_files() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let server_id_string = server_id.to_string();
        let server_id_str = server_id_string.as_str();
        let uuid = Uuid::new_v4();
        let uuid_string = uuid.to_string();
        let uuid_str = uuid_string.as_str();
        let iox_object_store = IoxObjectStore::new(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap();
        let txn_uuid = Uuid::new_v4();
        let good_txn_filename = format!("{}.txn", txn_uuid);
        let good_txn_filename_str = good_txn_filename.as_str();

        // Put a non-database file in
        let path = object_store.path_from_dirs_and_filename(parsed_path!(["foo"]));
        add_file(&object_store, &path).await;

        // Put a file for some other server in
        let path = object_store.path_from_dirs_and_filename(parsed_path!(["12345"]));
        add_file(&object_store, &path).await;

        // Put a file for some other database in
        let other_db_uuid = Uuid::new_v4().to_string();
        let path = object_store
            .path_from_dirs_and_filename(parsed_path!([server_id_str, other_db_uuid.as_str()]));
        add_file(&object_store, &path).await;

        // Put a file in the database dir but not the transactions dir
        let path = object_store.path_from_dirs_and_filename(parsed_path!(
            [server_id_str, uuid_str],
            good_txn_filename_str
        ));
        add_file(&object_store, &path).await;

        // Put files in the transactions dir whose names are in the wrong format
        let mut path = object_store
            .path_from_dirs_and_filename(parsed_path!([server_id_str, uuid_str], "111.parquet"));
        add_file(&object_store, &path).await;
        path.set_file_name(&format!("{}.xls", txn_uuid));
        add_file(&object_store, &path).await;

        // Catalog transaction files should be empty
        let ctf = catalog_transaction_files(&iox_object_store).await;
        assert!(ctf.is_empty(), "{:?}", ctf);

        // Add a real transaction file
        let t1 = TransactionFilePath::new_transaction(123, txn_uuid);
        add_catalog_transaction_file(&iox_object_store, &t1).await;
        // Add a real checkpoint file
        let t2 = TransactionFilePath::new_checkpoint(123, txn_uuid);
        add_catalog_transaction_file(&iox_object_store, &t2).await;

        // Only the real files should be returned
        let ctf = catalog_transaction_files(&iox_object_store).await;
        assert_eq!(ctf.len(), 2);
        assert!(ctf.contains(&t1));
        assert!(ctf.contains(&t2));
    }

    fn make_db_rules_path(object_store: &ObjectStore, server_id: ServerId, uuid: Uuid) -> Path {
        let mut p = object_store.new_path();
        p.push_all_dirs(&[server_id.to_string().as_str(), uuid.to_string().as_str()]);
        p.set_file_name("rules.pb");
        p
    }

    #[tokio::test]
    async fn db_rules_should_be_a_file() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let uuid = Uuid::new_v4();
        let rules_path = make_db_rules_path(&object_store, server_id, uuid);
        let iox_object_store = IoxObjectStore::new(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap();

        // PUT
        let original_file_content = Bytes::from("hello world");
        iox_object_store
            .put_database_rules_file(original_file_content.clone())
            .await
            .unwrap();

        let actual_content = object_store
            .get(&rules_path)
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap();

        assert_eq!(original_file_content, actual_content);

        // GET
        let updated_file_content = Bytes::from("goodbye moon");
        let expected_content = updated_file_content.clone();

        object_store
            .put(&rules_path, updated_file_content)
            .await
            .unwrap();

        let actual_content = iox_object_store.get_database_rules_file().await.unwrap();

        assert_eq!(expected_content, actual_content);

        // DELETE
        iox_object_store.delete_database_rules_file().await.unwrap();

        let file_count = object_store
            .list(None)
            .await
            .unwrap()
            .try_fold(0, |a, paths| async move { Ok(a + paths.len()) })
            .await
            .unwrap();

        assert_eq!(file_count, 0);
    }

    #[tokio::test]
    async fn write_tombstone_twice_is_fine() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let uuid = Uuid::new_v4();
        let iox_object_store = IoxObjectStore::new(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap();

        // tombstone file should not exist
        let mut tombstone = object_store.new_path();
        tombstone.push_all_dirs([server_id.to_string().as_str(), uuid.to_string().as_str()]);
        tombstone.set_file_name("DELETED");

        object_store.get(&tombstone).await.err().unwrap();

        iox_object_store.write_tombstone().await.unwrap();

        // tombstone file should exist
        object_store.get(&tombstone).await.unwrap();

        // deleting again should still succeed
        iox_object_store.write_tombstone().await.unwrap();

        // tombstone file should still exist
        object_store.get(&tombstone).await.unwrap();
    }

    #[tokio::test]
    async fn create_new_with_same_uuid_errors() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let uuid = Uuid::new_v4();

        let iox_object_store = IoxObjectStore::new(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap();

        iox_object_store
            .put_database_rules_file(Bytes::new())
            .await
            .unwrap();

        let err = IoxObjectStore::new(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                IoxObjectStoreError::DatabaseAlreadyExists { uuid: err_uuid } if err_uuid == uuid),
            "got: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn create_new_with_any_files_under_uuid_errors() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let uuid = Uuid::new_v4();

        let mut not_rules_path = object_store.new_path();
        not_rules_path.push_all_dirs(&[server_id.to_string().as_str(), uuid.to_string().as_str()]);
        not_rules_path.set_file_name("not_rules.txt");
        object_store
            .put(&not_rules_path, Bytes::new())
            .await
            .unwrap();

        let err = IoxObjectStore::new(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                IoxObjectStoreError::DatabaseAlreadyExists { uuid: err_uuid } if err_uuid == uuid),
            "got: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn delete_then_create_new_with_same_uuid_errors() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let uuid = Uuid::new_v4();

        let iox_object_store = IoxObjectStore::new(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap();

        iox_object_store
            .put_database_rules_file(Bytes::new())
            .await
            .unwrap();
        iox_object_store.write_tombstone().await.unwrap();

        let err = IoxObjectStore::new(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                IoxObjectStoreError::DatabaseAlreadyExists { uuid: err_uuid } if err_uuid == uuid),
            "got: {:?}",
            err
        );
    }

    async fn create_database(
        object_store: Arc<ObjectStore>,
        server_id: ServerId,
        uuid: Uuid,
    ) -> IoxObjectStore {
        let iox_object_store = IoxObjectStore::new(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap();

        iox_object_store
            .put_database_rules_file(Bytes::new())
            .await
            .unwrap();

        iox_object_store
    }

    async fn delete_database(iox_object_store: &IoxObjectStore) {
        iox_object_store.write_tombstone().await.unwrap();
    }

    #[tokio::test]
    async fn list_possible_databases_returns_all_potential_databases() {
        let object_store = make_object_store();
        let server_id = make_server_id();

        // Create a normal database, will be in the list
        let db_normal = Uuid::new_v4();
        create_database(Arc::clone(&object_store), server_id, db_normal).await;

        // Create a database, then delete it - will still be in the list
        let db_deleted = Uuid::new_v4();
        let db_deleted_iox_store =
            create_database(Arc::clone(&object_store), server_id, db_deleted).await;
        delete_database(&db_deleted_iox_store).await;

        // Put a file in a directory that looks like a database directory but has no rules,
        // will still be in the list
        let not_a_db = Uuid::new_v4();
        let mut not_rules_path = object_store.new_path();
        not_rules_path.push_all_dirs(&[
            server_id.to_string().as_str(),
            not_a_db.to_string().as_str(),
        ]);
        not_rules_path.set_file_name("not_rules.txt");
        object_store
            .put(&not_rules_path, Bytes::new())
            .await
            .unwrap();

        // Put a file in a directory that's an invalid UUID - this WON'T be in the list
        let invalid_db = String::from("a");
        let mut invalid_db_rules_path = object_store.new_path();
        invalid_db_rules_path.push_all_dirs(&[
            server_id.to_string().as_str(),
            invalid_db.to_string().as_str(),
        ]);
        invalid_db_rules_path.set_file_name("rules.pb");
        object_store
            .put(&invalid_db_rules_path, Bytes::new())
            .await
            .unwrap();

        let mut possible = IoxObjectStore::list_possible_databases(&object_store, server_id)
            .await
            .unwrap();
        possible.sort();
        let mut expected = vec![db_deleted, db_normal, not_a_db];
        expected.sort();

        assert_eq!(possible, expected);
    }

    #[tokio::test]
    async fn list_deleted_databases_metadata() {
        let object_store = make_object_store();
        let server_id = make_server_id();

        // Create a normal database, will NOT be in the list of deleted databases
        let db_normal = Uuid::new_v4();
        create_database(Arc::clone(&object_store), server_id, db_normal).await;

        // Create a database, then delete it - will be in the list once
        let db_deleted = Uuid::new_v4();
        let db_deleted_iox_store =
            create_database(Arc::clone(&object_store), server_id, db_deleted).await;
        delete_database(&db_deleted_iox_store).await;

        // Put a file in a directory that looks like a database directory but has no rules,
        // won't be in the list because there's no tombstone file
        let not_a_db = Uuid::new_v4();
        let mut not_rules_path = object_store.new_path();
        not_rules_path.push_all_dirs(&[
            server_id.to_string().as_str(),
            not_a_db.to_string().as_str(),
        ]);
        not_rules_path.set_file_name("not_rules.txt");
        object_store
            .put(&not_rules_path, Bytes::new())
            .await
            .unwrap();

        // Put a file in a directory that's an invalid UUID - won't be in the list
        let invalid_db = String::from("a");
        let mut invalid_db_rules_path = object_store.new_path();
        invalid_db_rules_path.push_all_dirs(&[
            server_id.to_string().as_str(),
            invalid_db.to_string().as_str(),
        ]);
        invalid_db_rules_path.set_file_name("rules.pb");
        object_store
            .put(&invalid_db_rules_path, Bytes::new())
            .await
            .unwrap();

        let deleted_dbs = IoxObjectStore::list_deleted_databases(&object_store, server_id)
            .await
            .unwrap();

        dbg!(&deleted_dbs);

        assert_eq!(deleted_dbs.len(), 1);
        assert_eq!(deleted_dbs[0].uuid, db_deleted);
        assert!(deleted_dbs[0].deleted_at.is_some());
    }

    async fn restore_database(
        object_store: Arc<ObjectStore>,
        server_id: ServerId,
        uuid: Uuid,
    ) -> Result<IoxObjectStore, IoxObjectStoreError> {
        IoxObjectStore::restore_database(Arc::clone(&object_store), server_id, uuid).await
    }

    #[tokio::test]
    async fn restore_deleted_database() {
        let object_store = make_object_store();
        let server_id = make_server_id();

        // Create a database
        let db = Uuid::new_v4();
        let db_iox_store = create_database(Arc::clone(&object_store), server_id, db).await;

        // Delete the database
        delete_database(&db_iox_store).await;

        // Restore the database
        restore_database(Arc::clone(&object_store), server_id, db)
            .await
            .unwrap();

        // The database should be in the list of all databases again
        let all_dbs = IoxObjectStore::list_detailed_databases(&object_store, server_id)
            .await
            .unwrap();
        assert_eq!(all_dbs.len(), 1);
    }

    #[tokio::test]
    async fn test_find_existing() {
        let object_store = make_object_store();
        let server_id = make_server_id();

        // Find existing can't find nonexistent database
        let nonexistent = Uuid::new_v4();
        let returned =
            IoxObjectStore::find_existing(Arc::clone(&object_store), server_id, nonexistent)
                .await
                .unwrap();
        assert!(returned.is_none());

        // Create a database
        let db = Uuid::new_v4();
        let db_iox_store = create_database(Arc::clone(&object_store), server_id, db).await;

        // Find existing should return that database
        let returned = IoxObjectStore::find_existing(Arc::clone(&object_store), server_id, db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(returned.uuid(), db);

        // Delete a database
        delete_database(&db_iox_store).await;

        // Find existing can't find deleted database
        let returned = IoxObjectStore::find_existing(Arc::clone(&object_store), server_id, db)
            .await
            .unwrap();
        assert!(returned.is_none());
    }
}
