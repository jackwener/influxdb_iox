use data_types::chunk_metadata::{ChunkAddr, ChunkId};
use object_store::{
    path::{parsed::DirsAndFileName, ObjectStorePath, Path as ObjStoPath},
    Result,
};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::sync::Arc;
use uuid::Uuid;

/// Location of a Parquet file within a database's object store.
/// The exact format is an implementation detail and is subject to change.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ParquetFilePath {
    table_name: Arc<str>,
    partition_key: Arc<str>,
    chunk_id: ChunkId,
}

impl ParquetFilePath {
    /// Create a location for this chunk's parquet file. Calling this twice on the same `ChunkAddr`
    /// will return different `parquet_file::Path`s.
    pub fn new(chunk_addr: &ChunkAddr) -> Self {
        Self {
            table_name: Arc::clone(&chunk_addr.table_name),
            partition_key: Arc::clone(&chunk_addr.partition_key),
            chunk_id: chunk_addr.chunk_id,
        }
    }

    /// Turn this into directories and file names to be added to a root path or to be serialized
    /// in protobuf.
    pub fn relative_dirs_and_file_name(&self) -> DirsAndFileName {
        let mut result = DirsAndFileName::default();
        result.push_all_dirs(&[self.table_name.as_ref(), self.partition_key.as_ref()]);
        result.set_file_name(format!("{}.parquet", self.chunk_id.get()));
        result
    }

    /// Create from serialized protobuf strings.
    pub fn from_relative_dirs_and_file_name(
        dirs_and_file_name: &DirsAndFileName,
    ) -> Result<Self, ParquetFilePathParseError> {
        let mut directories = dirs_and_file_name.directories.iter();
        let table_name = directories
            .next()
            .context(MissingTableName)?
            .to_string()
            .into();
        let partition_key = directories
            .next()
            .context(MissingPartitionKey)?
            .to_string()
            .into();

        ensure!(directories.next().is_none(), UnexpectedDirectory);

        let file_name = dirs_and_file_name
            .file_name
            .as_ref()
            .context(MissingChunkId)?
            .to_string();
        let mut parts = file_name.split('.');
        let chunk_id = parts
            .next()
            .context(MissingChunkId)?
            .parse::<Uuid>()
            .context(InvalidChunkId)?
            .into();
        let ext = parts.next().context(MissingExtension)?;
        ensure!(ext == "parquet", InvalidExtension { ext });
        ensure!(parts.next().is_none(), UnexpectedExtension);

        Ok(Self {
            table_name,
            partition_key,
            chunk_id,
        })
    }

    // Deliberately pub(crate); this transformation should only happen within this crate
    pub(crate) fn from_absolute(
        absolute_path: ObjStoPath,
    ) -> Result<Self, ParquetFilePathParseError> {
        let absolute_path: DirsAndFileName = absolute_path.into();

        let mut absolute_dirs = absolute_path.directories.into_iter().fuse();

        // The number of `next`s here needs to match the total number of directories in
        // iox_object_store data_paths
        absolute_dirs.next(); // server id
        absolute_dirs.next(); // database uuid
        absolute_dirs.next(); // "data"

        let remaining = DirsAndFileName {
            directories: absolute_dirs.collect(),
            file_name: absolute_path.file_name,
        };

        Self::from_relative_dirs_and_file_name(&remaining)
    }
}

impl From<&Self> for ParquetFilePath {
    fn from(borrowed: &Self) -> Self {
        borrowed.clone()
    }
}

#[derive(Snafu, Debug, PartialEq)]
#[allow(missing_docs)]
pub enum ParquetFilePathParseError {
    #[snafu(display("Could not find required table name"))]
    MissingTableName,

    #[snafu(display("Could not find required partition key"))]
    MissingPartitionKey,

    #[snafu(display("Too many directories found"))]
    UnexpectedDirectory,

    #[snafu(display("Could not find required chunk id"))]
    MissingChunkId,

    #[snafu(display("Could not parse chunk id: {}", source))]
    InvalidChunkId { source: uuid::Error },

    #[snafu(display("Could not find required file extension"))]
    MissingExtension,

    #[snafu(display("Extension should have been `parquet`, instead found `{}`", ext))]
    InvalidExtension { ext: String },

    #[snafu(display("Too many extensions found"))]
    UnexpectedExtension,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IoxObjectStore, RootPath};
    use data_types::server_id::ServerId;
    use object_store::{ObjectStore, ObjectStoreApi};
    use std::num::NonZeroU32;

    /// Creates new test server ID
    fn make_server_id() -> ServerId {
        ServerId::new(NonZeroU32::new(1).unwrap())
    }

    /// Creates a new in-memory object store. These tests rely on the `Path`s being of type
    /// `DirsAndFileName` and thus using object_store::path::DELIMITER as the separator
    fn make_object_store() -> Arc<ObjectStore> {
        Arc::new(ObjectStore::new_in_memory())
    }

    #[test]
    fn test_parquet_file_path_deserialization() {
        // Error cases
        use ParquetFilePathParseError::*;

        let mut df = DirsAndFileName::default();
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&df);
        assert!(matches!(result, Err(MissingTableName)), "got {:?}", result);

        df.push_dir("foo");
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&df);
        assert!(
            matches!(result, Err(MissingPartitionKey)),
            "got {:?}",
            result
        );

        df.push_dir("bar");
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&df);
        assert!(
            matches!(result, Err(MissingChunkId { .. })),
            "got {:?}",
            result
        );

        let mut extra = df.clone();
        extra.push_dir("nope");
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&extra);
        assert!(
            matches!(result, Err(UnexpectedDirectory)),
            "got {:?}",
            result
        );

        df.set_file_name("bleh");
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&df);
        assert!(
            matches!(result, Err(InvalidChunkId { .. })),
            "got {:?}",
            result
        );

        df.set_file_name("00000000-0000-0000-0000-00000000000a");
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&df);
        assert!(matches!(result, Err(MissingExtension)), "got {:?}", result);

        df.set_file_name("00000000-0000-0000-0000-00000000000a.exe");
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&df);
        assert!(
            matches!(result, Err(InvalidExtension { .. })),
            "got {:?}",
            result
        );

        df.set_file_name("00000000-0000-0000-0000-00000000000a.parquet.v6");
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&df);
        assert!(
            matches!(result, Err(UnexpectedExtension)),
            "got {:?}",
            result
        );

        // Success case
        df.set_file_name("00000000-0000-0000-0000-00000000000a.parquet");
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&df).unwrap();
        assert_eq!(
            result,
            ParquetFilePath {
                table_name: "foo".into(),
                partition_key: "bar".into(),
                chunk_id: ChunkId::new_test(10),
            }
        );
        let round_trip = result.relative_dirs_and_file_name();
        assert_eq!(round_trip, df);
    }

    #[test]
    fn parquet_file_from_absolute() {
        let object_store = make_object_store();

        // Success case
        let mut path = object_store.new_path();
        path.push_all_dirs(&["server", "uuid", "data", "}*", "aoeu"]);
        path.set_file_name("00000000-0000-0000-0000-00000000000a.parquet");
        let result = ParquetFilePath::from_absolute(path);
        assert_eq!(
            result.unwrap(),
            ParquetFilePath {
                table_name: "}*".into(),
                partition_key: "aoeu".into(),
                chunk_id: ChunkId::new_test(10),
            }
        );

        // Error cases
        use ParquetFilePathParseError::*;

        let mut path = object_store.new_path();
        // incorrect directories are fine, we're assuming that list(data_path) scoped to the
        // right directories so we don't check again on the way out
        path.push_all_dirs(&["server", "uuid", "data", "}*", "aoeu"]);
        // but this file name doesn't contain a chunk id
        path.set_file_name("rules.pb");
        let result = ParquetFilePath::from_absolute(path);
        assert!(
            matches!(result, Err(InvalidChunkId { .. })),
            "got: {:?}",
            result
        );

        let mut path = object_store.new_path();
        path.push_all_dirs(&["server", "uuid", "data", "}*", "aoeu"]);
        // missing file name
        let result = ParquetFilePath::from_absolute(path);
        assert!(matches!(result, Err(MissingChunkId)), "got: {:?}", result);
    }

    #[test]
    fn parquet_file_relative_dirs_and_file_path() {
        let pfp = ParquetFilePath {
            table_name: "}*".into(),
            partition_key: "aoeu".into(),
            chunk_id: ChunkId::new_test(10),
        };
        let dirs_and_file_name = pfp.relative_dirs_and_file_name();
        assert_eq!(
            dirs_and_file_name.to_string(),
            "%7D%2A/aoeu/00000000-0000-0000-0000-00000000000a.parquet".to_string(),
        );
        let round_trip =
            ParquetFilePath::from_relative_dirs_and_file_name(&dirs_and_file_name).unwrap();
        assert_eq!(pfp, round_trip);
    }

    #[test]
    fn data_path_join_with_parquet_file_path() {
        let server_id = make_server_id();
        let db_uuid = Uuid::new_v4();
        let object_store = make_object_store();
        let root_path = RootPath::new(&object_store, server_id, db_uuid);
        let iox_object_store =
            IoxObjectStore::existing(Arc::clone(&object_store), server_id, db_uuid, root_path);

        let pfp = ParquetFilePath {
            table_name: "}*".into(),
            partition_key: "aoeu".into(),
            chunk_id: ChunkId::new_test(10),
        };

        let path = iox_object_store.data_path.join(&pfp);

        let mut expected_path = object_store.new_path();
        expected_path.push_all_dirs(&[
            &server_id.to_string(),
            &db_uuid.to_string(),
            "data",
            "}*",
            "aoeu",
        ]);
        expected_path.set_file_name("00000000-0000-0000-0000-00000000000a.parquet");

        assert_eq!(path, expected_path);
    }
}
