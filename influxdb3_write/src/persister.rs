//! This is the implementation of the `Persister` used to write data from the buffer to object
//! storage.

use crate::PersistedSnapshotVersion;
use crate::paths::ParquetFilePath;
use crate::paths::SnapshotInfoFilePath;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::execution::memory_pool::UnboundedMemoryPool;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures_util::pin_mut;
use futures_util::stream::TryStreamExt;
use futures_util::stream::{FuturesOrdered, StreamExt};
use influxdb3_cache::parquet_cache::ParquetFileDataToCache;
use iox_time::TimeProvider;
use object_store::ObjectStore;
use object_store::path::Path as ObjPath;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::format::FileMetaData;
use std::io::Write;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum PersisterError {
    #[error("datafusion error: {0}")]
    DataFusion(#[from] DataFusionError),

    #[error("serde_json error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("object_store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("parquet error: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),

    #[error("tried to serialize a parquet file with no rows")]
    NoRows,

    #[error("parse int error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("unexpected persister error: {0:?}")]
    Unexpected(#[from] anyhow::Error),
}

impl From<PersisterError> for DataFusionError {
    fn from(error: PersisterError) -> Self {
        match error {
            PersisterError::DataFusion(e) => e,
            PersisterError::ObjectStore(e) => DataFusionError::ObjectStore(e),
            PersisterError::ParquetError(e) => DataFusionError::ParquetError(e),
            _ => DataFusionError::External(Box::new(error)),
        }
    }
}

pub type Result<T, E = PersisterError> = std::result::Result<T, E>;

pub const DEFAULT_OBJECT_STORE_URL: &str = "iox://influxdb3/";

/// The persister is the primary interface with object storage where InfluxDB stores all Parquet
/// data, catalog information, as well as WAL and snapshot data.
#[derive(Debug)]
pub struct Persister {
    /// This is used by the query engine to know where to read parquet files from. This assumes
    /// that there is a `ParquetStorage` with an id of `influxdb3` and that this url has been
    /// registered with the query execution context.
    object_store_url: ObjectStoreUrl,
    /// The interface to the object store being used
    object_store: Arc<dyn ObjectStore>,
    /// Prefix used for all paths in the object store for this persister
    node_identifier_prefix: String,
    /// time provider
    time_provider: Arc<dyn TimeProvider>,
    pub(crate) mem_pool: Arc<dyn MemoryPool>,
}

impl Persister {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        node_identifier_prefix: impl Into<String>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            object_store_url: ObjectStoreUrl::parse(DEFAULT_OBJECT_STORE_URL).unwrap(),
            object_store,
            node_identifier_prefix: node_identifier_prefix.into(),
            time_provider,
            mem_pool: Arc::new(UnboundedMemoryPool::default()),
        }
    }

    /// Get the Object Store URL
    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    async fn serialize_to_parquet(
        &self,
        batches: SendableRecordBatchStream,
    ) -> Result<ParquetBytes> {
        serialize_to_parquet(Arc::clone(&self.mem_pool), batches).await
    }

    /// Get the host identifier prefix
    pub fn node_identifier_prefix(&self) -> &str {
        &self.node_identifier_prefix
    }

    /// Loads the most recently persisted N snapshot parquet file lists from object storage.
    ///
    /// This is intended to be used on server start.
    pub async fn load_snapshots(
        &self,
        mut most_recent_n: usize,
    ) -> Result<Vec<PersistedSnapshotVersion>> {
        let mut futures = FuturesOrdered::new();
        let mut offset: Option<ObjPath> = None;

        while most_recent_n > 0 {
            let count = if most_recent_n > 1000 {
                most_recent_n -= 1000;
                1000
            } else {
                let count = most_recent_n;
                most_recent_n = 0;
                count
            };

            let mut snapshot_list = if let Some(offset) = offset {
                self.object_store.list_with_offset(
                    Some(&SnapshotInfoFilePath::dir(&self.node_identifier_prefix)),
                    &offset,
                )
            } else {
                self.object_store.list(Some(&SnapshotInfoFilePath::dir(
                    &self.node_identifier_prefix,
                )))
            };

            // Why not collect into a Result<Vec<ObjectMeta>, object_store::Error>>
            // like we could with Iterators? Well because it's a stream it ends up
            // using different traits and can't really do that. So we need to loop
            // through to return any errors that might have occurred, then do an
            // unstable sort (which is faster and we know won't have any
            // duplicates) since these can arrive out of order, and then issue gets
            // on the n most recent snapshots that we want and is returned in order
            // of the moste recent to least.
            let mut list = Vec::new();
            while let Some(item) = snapshot_list.next().await {
                list.push(item?);
            }

            list.sort_unstable_by(|a, b| a.location.cmp(&b.location));

            let len = list.len();
            let end = if len <= count { len } else { count };

            async fn get_snapshot(
                location: ObjPath,
                object_store: Arc<dyn ObjectStore>,
            ) -> Result<PersistedSnapshotVersion> {
                let bytes = object_store.get(&location).await?.bytes().await?;
                serde_json::from_slice(&bytes).map_err(Into::into)
            }

            for item in &list[0..end] {
                futures.push_back(get_snapshot(
                    item.location.clone(),
                    Arc::clone(&self.object_store),
                ));
            }

            if end == 0 {
                break;
            }

            // Get the last path in the array to use as an offset. This assumes
            // we sorted the list as we can't guarantee otherwise the order of
            // the list call to the object store.
            offset = Some(list[end - 1].location.clone());
        }

        let mut results = Vec::new();
        while let Some(result) = futures.next().await {
            results.push(result?);
        }
        Ok(results)
    }

    /// Loads a Parquet file from ObjectStore
    #[cfg(test)]
    pub async fn load_parquet_file(&self, path: ParquetFilePath) -> Result<Bytes> {
        Ok(self.object_store.get(&path).await?.bytes().await?)
    }

    /// Persists the snapshot file
    pub async fn persist_snapshot(
        &self,
        persisted_snapshot: &PersistedSnapshotVersion,
    ) -> Result<()> {
        let snapshot_file_path = SnapshotInfoFilePath::new(
            self.node_identifier_prefix.as_str(),
            match persisted_snapshot {
                PersistedSnapshotVersion::V1(ps) => ps.snapshot_sequence_number,
            },
        );
        let json = serde_json::to_vec_pretty(persisted_snapshot)?;
        self.object_store
            .put(snapshot_file_path.as_ref(), json.into())
            .await?;
        Ok(())
    }

    /// Writes a [`SendableRecordBatchStream`] to the Parquet format and persists it to Object Store
    /// at the given path. Returns the number of bytes written and the file metadata.
    pub async fn persist_parquet_file(
        &self,
        path: ParquetFilePath,
        record_batch: SendableRecordBatchStream,
    ) -> Result<(u64, FileMetaData, ParquetFileDataToCache)> {
        // so we have serialized parquet file bytes
        let parquet = self.serialize_to_parquet(record_batch).await?;
        let bytes_written = parquet.bytes.len() as u64;
        let put_result = self
            .object_store
            // this bytes.clone() is cheap - uses underlying Bytes::clone
            .put(path.as_ref(), parquet.bytes.clone().into())
            .await?;

        let to_cache = ParquetFileDataToCache::new(
            path.as_ref(),
            self.time_provider.now().date_time(),
            parquet.bytes,
            put_result,
        );

        Ok((bytes_written, parquet.meta_data, to_cache))
    }

    /// Returns the configured `ObjectStore` that data is loaded from and persisted to.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.object_store)
    }
}

pub async fn serialize_to_parquet(
    mem_pool: Arc<dyn MemoryPool>,
    batches: SendableRecordBatchStream,
) -> Result<ParquetBytes> {
    // The ArrowWriter::write() call will return an error if any subsequent
    // batch does not match this schema, enforcing schema uniformity.
    let schema = batches.schema();

    let stream = batches;
    let mut bytes = Vec::new();
    pin_mut!(stream);

    // Construct the arrow serializer with the metadata as part of the parquet
    // file properties.
    let mut writer = TrackedMemoryArrowWriter::try_new(&mut bytes, Arc::clone(&schema), mem_pool)?;

    while let Some(batch) = stream.try_next().await? {
        writer.write(batch)?;
    }

    let writer_meta = writer.close()?;
    if writer_meta.num_rows == 0 {
        return Err(PersisterError::NoRows);
    }

    Ok(ParquetBytes {
        meta_data: writer_meta,
        bytes: Bytes::from(bytes),
    })
}

#[derive(Debug)]
pub struct ParquetBytes {
    pub bytes: Bytes,
    pub meta_data: FileMetaData,
}

/// Wraps an [`ArrowWriter`] to track its buffered memory in a
/// DataFusion [`MemoryPool`]
#[derive(Debug)]
pub struct TrackedMemoryArrowWriter<W: Write + Send> {
    /// The inner ArrowWriter
    inner: ArrowWriter<W>,
    /// DataFusion memory reservation with
    reservation: MemoryReservation,
}

/// The number of rows to write in each row group of the parquet file
pub const ROW_GROUP_WRITE_SIZE: usize = 1_000_000; // Increased from 100,000 for better compaction

impl<W: Write + Send> TrackedMemoryArrowWriter<W> {
    /// create a new `TrackedMemoryArrowWriter<`
    pub fn try_new(sink: W, schema: SchemaRef, mem_pool: Arc<dyn MemoryPool>) -> Result<Self> {
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_max_row_group_size(ROW_GROUP_WRITE_SIZE)
            .build();
        let inner = ArrowWriter::try_new(sink, schema, Some(props))?;
        let consumer = MemoryConsumer::new("InfluxDB3 ParquetWriter (TrackedMemoryArrowWriter)");
        let reservation = consumer.register(&mem_pool);

        Ok(Self { inner, reservation })
    }

    /// Push a `RecordBatch` into the underlying writer, updating the
    /// tracked allocation
    pub fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // writer encodes the batch into its internal buffers
        self.inner.write(&batch)?;

        // In progress memory, in bytes
        let in_progress_size = self.inner.in_progress_size();

        // update the allocation with the pool.
        self.reservation.try_resize(in_progress_size)?;

        Ok(())
    }

    /// closes the writer, flushing any remaining data and returning
    /// the written [`FileMetaData`]
    ///
    /// [`FileMetaData`]: parquet::format::FileMetaData
    pub fn close(self) -> Result<parquet::format::FileMetaData> {
        // reservation is returned on drop
        Ok(self.inner.close()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DatabaseTables, ParquetFile, ParquetFileId, PersistedSnapshot, PersistedSnapshotVersion,
    };
    use influxdb3_catalog::catalog::CatalogSequenceNumber;
    use influxdb3_id::{DbId, SerdeVecMap, TableId};
    use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;
    use pretty_assertions::assert_eq;
    use {
        arrow::array::Int32Array, arrow::datatypes::DataType, arrow::datatypes::Field,
        arrow::datatypes::Schema, chrono::Utc,
        datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder,
        object_store::local::LocalFileSystem,
    };

    #[tokio::test]
    async fn persist_snapshot_info_file() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider);
        let info_file = PersistedSnapshotVersion::V1(PersistedSnapshot {
            node_id: "test_host".to_string(),
            next_file_id: ParquetFileId::from(0),
            snapshot_sequence_number: SnapshotSequenceNumber::new(0),
            wal_file_sequence_number: WalFileSequenceNumber::new(0),
            catalog_sequence_number: CatalogSequenceNumber::new(0),
            databases: SerdeVecMap::new(),
            removed_files: SerdeVecMap::new(),
            min_time: 0,
            max_time: 1,
            row_count: 0,
            parquet_size_bytes: 0,
        });

        persister.persist_snapshot(&info_file).await.unwrap();
    }

    #[tokio::test]
    async fn persist_and_load_snapshot_info_files() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider);
        let info_file = PersistedSnapshotVersion::V1(PersistedSnapshot {
            node_id: "test_host".to_string(),
            next_file_id: ParquetFileId::from(0),
            snapshot_sequence_number: SnapshotSequenceNumber::new(0),
            wal_file_sequence_number: WalFileSequenceNumber::new(0),
            catalog_sequence_number: CatalogSequenceNumber::default(),
            databases: SerdeVecMap::new(),
            removed_files: SerdeVecMap::new(),
            max_time: 1,
            min_time: 0,
            row_count: 0,
            parquet_size_bytes: 0,
        });
        let info_file_2 = PersistedSnapshotVersion::V1(PersistedSnapshot {
            node_id: "test_host".to_string(),
            next_file_id: ParquetFileId::from(1),
            snapshot_sequence_number: SnapshotSequenceNumber::new(1),
            wal_file_sequence_number: WalFileSequenceNumber::new(1),
            catalog_sequence_number: CatalogSequenceNumber::default(),
            databases: SerdeVecMap::new(),
            removed_files: SerdeVecMap::new(),
            min_time: 0,
            max_time: 1,
            row_count: 0,
            parquet_size_bytes: 0,
        });
        let info_file_3 = PersistedSnapshotVersion::V1(PersistedSnapshot {
            node_id: "test_host".to_string(),
            next_file_id: ParquetFileId::from(2),
            snapshot_sequence_number: SnapshotSequenceNumber::new(2),
            wal_file_sequence_number: WalFileSequenceNumber::new(2),
            catalog_sequence_number: CatalogSequenceNumber::default(),
            databases: SerdeVecMap::new(),
            removed_files: SerdeVecMap::new(),
            min_time: 0,
            max_time: 1,
            row_count: 0,
            parquet_size_bytes: 0,
        });

        persister.persist_snapshot(&info_file).await.unwrap();
        persister.persist_snapshot(&info_file_2).await.unwrap();
        persister.persist_snapshot(&info_file_3).await.unwrap();

        let snapshots = persister.load_snapshots(2).await.unwrap();
        assert_eq!(snapshots.len(), 2);
        // The most recent files are first
        assert_eq!(snapshots[0].v1_ref().next_file_id.as_u64(), 2);
        assert_eq!(snapshots[0].v1_ref().wal_file_sequence_number.as_u64(), 2);
        assert_eq!(snapshots[0].v1_ref().snapshot_sequence_number.as_u64(), 2);
        assert_eq!(snapshots[1].v1_ref().next_file_id.as_u64(), 1);
        assert_eq!(snapshots[1].v1_ref().wal_file_sequence_number.as_u64(), 1);
        assert_eq!(snapshots[1].v1_ref().snapshot_sequence_number.as_u64(), 1);
    }

    #[tokio::test]
    async fn persist_and_load_snapshot_info_files_with_fewer_than_requested() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider);
        let info_file = PersistedSnapshotVersion::V1(PersistedSnapshot {
            node_id: "test_host".to_string(),
            next_file_id: ParquetFileId::from(0),
            snapshot_sequence_number: SnapshotSequenceNumber::new(0),
            wal_file_sequence_number: WalFileSequenceNumber::new(0),
            catalog_sequence_number: CatalogSequenceNumber::default(),
            databases: SerdeVecMap::new(),
            removed_files: SerdeVecMap::new(),
            min_time: 0,
            max_time: 1,
            row_count: 0,
            parquet_size_bytes: 0,
        });
        persister.persist_snapshot(&info_file).await.unwrap();
        let snapshots = persister.load_snapshots(2).await.unwrap();
        // We asked for the most recent 2 but there should only be 1
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].v1_ref().wal_file_sequence_number.as_u64(), 0);
    }

    #[tokio::test]
    /// This test makes sure that the logic for offset lists works
    async fn persist_and_load_over_1000_snapshot_info_files() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider);
        for id in 0..1001 {
            let info_file = PersistedSnapshotVersion::V1(PersistedSnapshot {
                node_id: "test_host".to_string(),
                next_file_id: ParquetFileId::from(id),
                snapshot_sequence_number: SnapshotSequenceNumber::new(id),
                wal_file_sequence_number: WalFileSequenceNumber::new(id),
                catalog_sequence_number: CatalogSequenceNumber::new(id),
                databases: SerdeVecMap::new(),
                removed_files: SerdeVecMap::new(),
                min_time: 0,
                max_time: 1,
                row_count: 0,
                parquet_size_bytes: 0,
            });
            persister.persist_snapshot(&info_file).await.unwrap();
        }
        let snapshots = persister.load_snapshots(1500).await.unwrap();
        // We asked for the most recent 1500 so there should be 1001 of them
        assert_eq!(snapshots.len(), 1001);
        assert_eq!(snapshots[0].v1_ref().next_file_id.as_u64(), 1000);
        assert_eq!(
            snapshots[0].v1_ref().wal_file_sequence_number.as_u64(),
            1000
        );
        assert_eq!(
            snapshots[0].v1_ref().snapshot_sequence_number.as_u64(),
            1000
        );
        assert_eq!(snapshots[0].v1_ref().catalog_sequence_number.get(), 1000);
    }

    #[tokio::test]
    // This test makes sure that the proper next_file_id is used if a parquet file
    // is added
    async fn persist_add_parquet_file_and_load_snapshot() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider);
        let mut info_file = PersistedSnapshot::new(
            "test_host".to_string(),
            SnapshotSequenceNumber::new(0),
            WalFileSequenceNumber::new(0),
            CatalogSequenceNumber::new(0),
        );

        for _ in 0..=9875 {
            let _id = ParquetFileId::new();
        }

        info_file.add_parquet_file(
            DbId::from(0),
            TableId::from(0),
            crate::ParquetFile {
                // Use a number that will be bigger than what's created in the
                // PersistedSnapshot automatically
                id: ParquetFileId::new(),
                path: "test".into(),
                size_bytes: 5,
                row_count: 5,
                chunk_time: 5,
                min_time: 0,
                max_time: 1,
            },
        );
        persister
            .persist_snapshot(&PersistedSnapshotVersion::V1(info_file))
            .await
            .unwrap();
        let snapshots = persister.load_snapshots(10).await.unwrap();
        assert_eq!(snapshots.len(), 1);
        // Should be the next available id after the largest number
        assert_eq!(snapshots[0].v1_ref().next_file_id.as_u64(), 9877);
        assert_eq!(snapshots[0].v1_ref().wal_file_sequence_number.as_u64(), 0);
        assert_eq!(snapshots[0].v1_ref().snapshot_sequence_number.as_u64(), 0);
        assert_eq!(snapshots[0].v1_ref().catalog_sequence_number.get(), 0);
    }

    #[tokio::test]
    async fn load_snapshot_works_with_no_exising_snapshots() {
        let store = InMemory::new();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let persister = Persister::new(Arc::new(store), "test_host", time_provider);

        let snapshots = persister.load_snapshots(100).await.unwrap();
        assert!(snapshots.is_empty());
    }

    #[test]
    fn persisted_snapshot_structure() {
        let databases = [
            (
                DbId::new(0),
                DatabaseTables {
                    tables: [
                        (
                            TableId::new(0),
                            vec![
                                ParquetFile::create_for_test("1.parquet"),
                                ParquetFile::create_for_test("2.parquet"),
                            ],
                        ),
                        (
                            TableId::new(1),
                            vec![
                                ParquetFile::create_for_test("3.parquet"),
                                ParquetFile::create_for_test("4.parquet"),
                            ],
                        ),
                    ]
                    .into(),
                },
            ),
            (
                DbId::new(1),
                DatabaseTables {
                    tables: [
                        (
                            TableId::new(0),
                            vec![
                                ParquetFile::create_for_test("5.parquet"),
                                ParquetFile::create_for_test("6.parquet"),
                            ],
                        ),
                        (
                            TableId::new(1),
                            vec![
                                ParquetFile::create_for_test("7.parquet"),
                                ParquetFile::create_for_test("8.parquet"),
                            ],
                        ),
                    ]
                    .into(),
                },
            ),
        ]
        .into();
        let snapshot = PersistedSnapshotVersion::V1(PersistedSnapshot {
            node_id: "host".to_string(),
            next_file_id: ParquetFileId::new(),
            snapshot_sequence_number: SnapshotSequenceNumber::new(0),
            wal_file_sequence_number: WalFileSequenceNumber::new(0),
            catalog_sequence_number: CatalogSequenceNumber::new(0),
            parquet_size_bytes: 1_024,
            row_count: 1,
            min_time: 0,
            max_time: 1,
            removed_files: SerdeVecMap::new(),
            databases,
        });
        insta::assert_json_snapshot!(snapshot);
    }

    #[tokio::test]
    async fn get_parquet_bytes() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider);

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let stream_builder = RecordBatchReceiverStreamBuilder::new(Arc::clone(&schema), 5);

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let batch1 = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(id_array)]).unwrap();

        let id_array = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let batch2 = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(id_array)]).unwrap();

        stream_builder.tx().send(Ok(batch1)).await.unwrap();
        stream_builder.tx().send(Ok(batch2)).await.unwrap();

        let parquet = persister
            .serialize_to_parquet(stream_builder.build())
            .await
            .unwrap();

        // Assert we've written all the expected rows
        assert_eq!(parquet.meta_data.num_rows, 10);
    }

    #[tokio::test]
    async fn persist_and_load_parquet_bytes() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider);

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let stream_builder = RecordBatchReceiverStreamBuilder::new(Arc::clone(&schema), 5);

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let batch1 = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(id_array)]).unwrap();

        let id_array = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let batch2 = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(id_array)]).unwrap();

        stream_builder.tx().send(Ok(batch1)).await.unwrap();
        stream_builder.tx().send(Ok(batch2)).await.unwrap();

        let path = ParquetFilePath::new(
            "test_host",
            "db_one",
            0,
            "table_one",
            0,
            Utc::now().timestamp_nanos_opt().unwrap(),
            WalFileSequenceNumber::new(1),
        );
        let (bytes_written, meta, _) = persister
            .persist_parquet_file(path.clone(), stream_builder.build())
            .await
            .unwrap();

        // Assert we've written all the expected rows
        assert_eq!(meta.num_rows, 10);

        let bytes = persister.load_parquet_file(path).await.unwrap();

        // Assert that we have a file of bytes > 0
        assert!(!bytes.is_empty());
        assert_eq!(bytes.len() as u64, bytes_written);
    }
}
