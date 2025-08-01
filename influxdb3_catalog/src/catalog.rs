//! Implementation of the Catalog that sits entirely in memory.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use bimap::BiHashMap;
use influxdb3_authz::Actions;
use influxdb3_authz::Permission;
use influxdb3_authz::ResourceIdentifier;
use influxdb3_authz::ResourceType;
use influxdb3_authz::TokenInfo;
use influxdb3_authz::TokenProvider;
use influxdb3_id::{
    CatalogId, ColumnId, DbId, DistinctCacheId, LastCacheId, NodeId, SerdeVecMap, TableId, TokenId,
    TriggerId,
};
use influxdb3_process::ProcessUuidGetter;
use influxdb3_shutdown::ShutdownToken;
use influxdb3_telemetry::ProcessingEngineMetrics;
use iox_time::{Time, TimeProvider};
use metric::Registry;
use metrics::CatalogMetrics;
use object_store::ObjectStore;
use observability_deps::tracing::{debug, error, info, trace, warn};
use parking_lot::RwLock;
use rand::RngCore;
use rand::rngs::OsRng;
use schema::{Schema, SchemaBuilder};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use sha2::Sha512;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::hash::Hash;
use std::iter;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

mod metrics;
mod update;
use schema::sort::SortKey;
pub use schema::{InfluxColumnType, InfluxFieldType};
pub use update::HardDeletionTime;
pub use update::{CatalogUpdate, CreateDatabaseOptions, DatabaseCatalogTransaction, Prompt};

use crate::channel::{CatalogSubscriptions, CatalogUpdateReceiver};
use crate::log::GenerationBatch;
use crate::log::GenerationOp;
use crate::log::{
    ClearRetentionPeriodLog, CreateAdminTokenDetails, CreateScopedTokenDetails, CreateDatabaseLog, DatabaseBatch,
    DatabaseCatalogOp, NodeBatch, NodeCatalogOp, NodeMode, RegenerateAdminTokenDetails,
    RegisterNodeLog, SetRetentionPeriodLog, StopNodeLog, TokenBatch, TokenCatalogOp,
    TriggerSpecificationDefinition,
};
use crate::object_store::ObjectStoreCatalog;
use crate::resource::CatalogResource;
use crate::snapshot::CatalogSnapshot;
use crate::snapshot::versions::Snapshot;
use crate::{
    CatalogError, Result,
    log::{
        AddFieldsLog, CatalogBatch, CreateTableLog, DeleteDistinctCacheLog, DeleteLastCacheLog,
        DeleteTriggerLog, DistinctCacheDefinition, FieldDefinition, LastCacheDefinition,
        OrderedCatalogBatch, SoftDeleteDatabaseLog, SoftDeleteTableLog, TriggerDefinition,
        TriggerIdentifier,
        versions::v3::{DeleteBatch, DeleteOp},
    },
};

const SOFT_DELETION_TIME_FORMAT: &str = "%Y%m%dT%H%M%S";

pub const INTERNAL_DB_NAME: &str = "_internal";

pub const TIME_COLUMN_NAME: &str = "time";

const DEFAULT_OPERATOR_TOKEN_NAME: &str = "_admin";

/// Limit for the number of tag columns on a table (removed crippled limit)
pub(crate) const NUM_TAG_COLUMNS_LIMIT: usize = usize::MAX;

/// Represents the deletion status of a database or table in the catalog
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DeletionStatus {
    /// The resource has been soft deleted but not yet hard deleted
    Soft,
    /// The resource has been hard deleted with the duration since deletion
    Hard(Duration),
    /// The resource was not found in the catalog
    NotFound,
}

/// The sequence number of a batch of WAL operations.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct CatalogSequenceNumber(u64);

impl CatalogSequenceNumber {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn get(&self) -> u64 {
        self.0
    }
}

impl From<u64> for CatalogSequenceNumber {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

static CATALOG_WRITE_PERMIT: Mutex<CatalogSequenceNumber> =
    Mutex::const_new(CatalogSequenceNumber::new(0));

/// Convenience type alias for the write permit on the catalog
///
/// This is a mutex that, when a lock is acquired, holds the next catalog sequence number at the
/// time that the permit was acquired.
pub type CatalogWritePermit = MutexGuard<'static, CatalogSequenceNumber>;

pub struct Catalog {
    // The Catalog stores a reference to the metric registry so that other components in the
    // system that are initialized from/with the catalog can easily access it as needed
    metric_registry: Arc<Registry>,
    state: parking_lot::Mutex<CatalogState>,
    subscriptions: Arc<tokio::sync::RwLock<CatalogSubscriptions>>,
    time_provider: Arc<dyn TimeProvider>,
    /// Connection to the object store for managing persistence and updates to the catalog
    store: ObjectStoreCatalog,
    metrics: Arc<CatalogMetrics>,
    /// In-memory representation of the catalog
    pub(crate) inner: RwLock<InnerCatalog>,
    limits: CatalogLimits,
    args: CatalogArgs,
}

/// Custom implementation of `Debug` for the `Catalog` type to avoid serializing the object store
impl std::fmt::Debug for Catalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalog")
            .field("inner", &self.inner)
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
enum CatalogState {
    Active,
    Shutdown,
}

impl CatalogState {
    fn is_shutdown(&self) -> bool {
        matches!(self, Self::Shutdown)
    }
}

const CATALOG_CHECKPOINT_INTERVAL: u64 = 100;

#[derive(Clone, Copy, Debug)]
pub struct CatalogArgs {
    pub default_hard_delete_duration: Duration,
}

impl CatalogArgs {
    pub fn new(default_hard_delete_duration: Duration) -> Self {
        Self {
            default_hard_delete_duration,
        }
    }
}

impl Default for CatalogArgs {
    fn default() -> Self {
        Self {
            default_hard_delete_duration: Catalog::DEFAULT_HARD_DELETE_DURATION,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CatalogLimits {
    num_dbs: usize,
    num_tables: usize,
    num_columns_per_table: usize,
}

impl Default for CatalogLimits {
    fn default() -> Self {
        Self {
            num_dbs: Catalog::NUM_DBS_LIMIT,
            num_tables: Catalog::NUM_TABLES_LIMIT,
            num_columns_per_table: Catalog::NUM_COLUMNS_PER_TABLE_LIMIT,
        }
    }
}

impl Catalog {
    /// Limit for the number of Databases that InfluxDB 3 Core can have
    pub const NUM_DBS_LIMIT: usize = usize::MAX;
    /// Limit for the number of columns per table that InfluxDB 3 Core can have
    pub const NUM_COLUMNS_PER_TABLE_LIMIT: usize = usize::MAX;
    /// Limit for the number of tables across all DBs that InfluxDB 3 Core can have
    pub const NUM_TABLES_LIMIT: usize = usize::MAX;
    /// Default duration for hard deletion of soft-deleted databases and tables
    pub const DEFAULT_HARD_DELETE_DURATION: Duration = Duration::from_secs(10 * 365 * 24 * 60 * 60); // 10 years, effectively infinite

    pub async fn new(
        node_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
    ) -> Result<Self> {
        Self::new_with_args(
            node_id,
            store,
            time_provider,
            metric_registry,
            CatalogArgs::default(),
        )
        .await
    }

    pub async fn new_with_args(
        node_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        args: CatalogArgs,
    ) -> Result<Self> {
        let node_id = node_id.into();
        let store =
            ObjectStoreCatalog::new(Arc::clone(&node_id), CATALOG_CHECKPOINT_INTERVAL, store);
        let subscriptions = Default::default();
        let metrics = Arc::new(CatalogMetrics::new(&metric_registry));
        let catalog = store
            .load_or_create_catalog()
            .await
            .map(RwLock::new)
            .map(|inner| Self {
                metric_registry,
                state: parking_lot::Mutex::new(CatalogState::Active),
                subscriptions,
                time_provider,
                store,
                metrics,
                inner,
                limits: Default::default(),
                args,
            })?;

        create_internal_db(&catalog).await;
        catalog.metrics.operation_observer(
            catalog
                .subscribe_to_updates("catalog_operation_metrics")
                .await,
        );
        Ok(catalog)
    }

    pub async fn new_with_shutdown(
        node_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        shutdown_token: ShutdownToken,
        process_uuid_getter: Arc<dyn ProcessUuidGetter>,
    ) -> Result<Arc<Self>> {
        let node_id = node_id.into();
        let catalog =
            Arc::new(Self::new(Arc::clone(&node_id), store, time_provider, metric_registry).await?);
        let catalog_cloned = Arc::clone(&catalog);
        tokio::spawn(async move {
            shutdown_token.wait_for_shutdown().await;
            info!(
                node_id = node_id.as_ref(),
                "updating node state to stopped in catalog"
            );
            if let Err(error) = catalog_cloned
                .update_node_state_stopped(node_id.as_ref(), process_uuid_getter)
                .await
            {
                error!(
                    ?error,
                    node_id = node_id.as_ref(),
                    "encountered error while updating node to stopped state in catalog"
                );
            }
        });
        Ok(catalog)
    }

    pub fn metric_registry(&self) -> Arc<Registry> {
        Arc::clone(&self.metric_registry)
    }

    pub fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }

    pub fn set_state_shutdown(&self) {
        *self.state.lock() = CatalogState::Shutdown;
    }

    fn num_dbs_limit(&self) -> usize {
        self.limits.num_dbs
    }

    fn num_tables_limit(&self) -> usize {
        self.limits.num_tables
    }

    fn num_columns_per_table_limit(&self) -> usize {
        self.limits.num_columns_per_table
    }

    fn default_hard_delete_duration(&self) -> Duration {
        self.args.default_hard_delete_duration
    }

    pub fn object_store_prefix(&self) -> Arc<str> {
        Arc::clone(&self.store.prefix)
    }

    pub fn catalog_uuid(&self) -> Uuid {
        self.inner.read().catalog_uuid
    }

    pub async fn subscribe_to_updates(&self, name: &'static str) -> CatalogUpdateReceiver {
        self.subscriptions.write().await.subscribe(name)
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.store.object_store()
    }

    pub fn snapshot(&self) -> CatalogSnapshot {
        self.inner.read().snapshot()
    }

    pub fn update_from_snapshot(&self, snapshot: CatalogSnapshot) {
        let mut inner = self.inner.write();
        *inner = InnerCatalog::from_snapshot(snapshot);
    }

    /// Acquire a permit to write the provided `CatalogBatch` to object store
    ///
    /// This issues a `Prompt` to signal retry or success. The provided `sequence` is checked
    /// against the current catalog's sequence. If it is behind, due to some other concurrent
    /// update to the catalog, a retry is issued, so that the caller can re-compose the catalog
    /// batch using the latest state of the catalog and try again.
    pub async fn get_permit_and_verify_catalog_batch(
        &self,
        catalog_batch: CatalogBatch,
        sequence: CatalogSequenceNumber,
    ) -> Prompt<(OrderedCatalogBatch, CatalogWritePermit)> {
        // Get the write permit, and update its contents with the next catalog sequence number. If
        // the `catalog_batch` provided results in an update, i.e., changes the catalog, then this
        // will be the sequence number that the catalog is updated to.
        let mut permit = CATALOG_WRITE_PERMIT.lock().await;
        if sequence != self.sequence_number() {
            self.metrics.catalog_operation_retries.inc(1);
            return Prompt::Retry(());
        }
        *permit = self.sequence_number().next();
        trace!(
            next_sequence = permit.get(),
            "got permit to write to catalog"
        );
        Prompt::Success((OrderedCatalogBatch::new(catalog_batch, *permit), permit))
    }

    /// Apply an `OrderedCatalogBatch` to this catalog
    ///
    /// # Implementation note
    ///
    /// This accepts a `_permit`, which is not used, and is just a way to ensure that the caller
    /// has a handle on the write permit at the time of invocation.
    pub(crate) fn apply_ordered_catalog_batch(
        &self,
        batch: &OrderedCatalogBatch,
        _permit: &CatalogWritePermit,
    ) -> CatalogBatch {
        let batch_sequence = batch.sequence_number().get();
        let current_sequence = self.sequence_number().get();
        assert_eq!(
            batch_sequence,
            current_sequence + 1,
            "catalog batch received out of order"
        );
        let catalog_batch = self
            .inner
            .write()
            .apply_catalog_batch(batch.batch(), batch.sequence_number())
            .expect("ordered catalog batch should succeed when applied")
            .expect("ordered catalog batch should contain changes");
        catalog_batch.into_batch()
    }

    pub fn node(&self, node_id: &str) -> Option<Arc<NodeDefinition>> {
        self.inner.read().nodes.get_by_name(node_id)
    }

    pub fn next_db_id(&self) -> DbId {
        self.inner.read().databases.next_id()
    }

    pub(crate) fn db_or_create(
        &self,
        db_name: &str,
        retention_period: Option<Duration>,
        now_time_ns: i64,
    ) -> Result<(Arc<DatabaseSchema>, Option<CatalogBatch>)> {
        match self.db_schema(db_name) {
            Some(db) => Ok((db, None)),
            None => {
                let mut inner = self.inner.write();

                if inner.database_count() >= self.num_dbs_limit() {
                    return Err(CatalogError::TooManyDbs(self.num_dbs_limit()));
                }

                info!(database_name = db_name, "creating new database");
                let db_id = inner.databases.get_and_increment_next_id();
                let db_name = db_name.into();
                let db = Arc::new(DatabaseSchema::new(db_id, Arc::clone(&db_name)));
                let batch = CatalogBatch::database(
                    now_time_ns,
                    db.id,
                    db.name(),
                    vec![DatabaseCatalogOp::CreateDatabase(CreateDatabaseLog {
                        database_id: db.id,
                        database_name: Arc::clone(&db.name),
                        retention_period,
                    })],
                );
                Ok((db, Some(batch)))
            }
        }
    }

    pub fn db_name_to_id(&self, db_name: &str) -> Option<DbId> {
        self.inner.read().databases.name_to_id(db_name)
    }

    pub fn db_id_to_name(&self, db_id: &DbId) -> Option<Arc<str>> {
        self.inner.read().databases.id_to_name(db_id)
    }

    pub fn db_schema(&self, db_name: &str) -> Option<Arc<DatabaseSchema>> {
        self.inner.read().databases.get_by_name(db_name)
    }

    pub fn db_schema_by_id(&self, db_id: &DbId) -> Option<Arc<DatabaseSchema>> {
        self.inner.read().databases.get_by_id(db_id)
    }

    /// List names of databases that have not been deleted
    pub fn db_names(&self) -> Vec<String> {
        self.inner
            .read()
            .databases
            .resource_iter()
            .filter(|db| !db.deleted)
            .map(|db| db.name.to_string())
            .collect()
    }

    pub fn list_db_schema(&self) -> Vec<Arc<DatabaseSchema>> {
        self.inner
            .read()
            .databases
            .resource_iter()
            .cloned()
            .collect()
    }

    /// Returns the deletion status of a database by its ID.
    ///
    /// If the database exists as is not marked for deletion, `None` is returned.
    pub fn database_deletion_status(&self, db_id: DbId) -> Option<DeletionStatus> {
        let inner = self.inner.read();

        database_or_deletion_status(inner.databases.get_by_id(&db_id), &self.time_provider).err()
    }

    /// Returns the deletion status of a table by its ID within a specific database.
    ///
    /// If the table exists and is not marked for deletion, `None` is returned.
    pub fn table_deletion_status(&self, db_id: DbId, table_id: TableId) -> Option<DeletionStatus> {
        let inner = self.inner.read();

        match database_or_deletion_status(inner.databases.get_by_id(&db_id), &self.time_provider) {
            Ok(db_schema) => table_deletion_status(&db_schema, table_id, &self.time_provider),
            Err(status) => Some(status),
        }
    }

    pub fn sequence_number(&self) -> CatalogSequenceNumber {
        self.inner.read().sequence
    }

    pub fn clone_inner(&self) -> InnerCatalog {
        self.inner.read().clone()
    }

    pub fn catalog_id(&self) -> Arc<str> {
        Arc::clone(&self.inner.read().catalog_id)
    }

    pub fn db_exists(&self, db_id: DbId) -> bool {
        self.inner.read().db_exists(db_id)
    }

    /// Get active triggers by database and trigger name
    // NOTE: this could be id-based in future
    pub fn active_triggers(&self) -> Vec<(Arc<str>, Arc<str>)> {
        let inner = self.inner.read();
        inner
            .databases
            .resource_iter()
            .flat_map(|db| {
                db.processing_engine_triggers
                    .resource_iter()
                    .filter_map(move |trigger| {
                        if trigger.disabled {
                            None
                        } else {
                            Some((Arc::clone(&db.name), Arc::clone(&trigger.trigger_name)))
                        }
                    })
            })
            .collect()
    }

    pub fn get_tokens(&self) -> Vec<Arc<TokenInfo>> {
        self.inner
            .read()
            .tokens
            .repo()
            .iter()
            .map(|(_, token_info)| Arc::clone(token_info))
            .collect()
    }

    pub async fn create_admin_token(&self, regenerate: bool) -> Result<(Arc<TokenInfo>, String)> {
        // if regen, if token is present already create a new token and hash and update the
        // existing token otherwise we should insert to catalog (essentially an upsert)
        let (token, hash) = create_token_and_hash();
        self.catalog_update_with_retry(|| {
            if regenerate {
                let default_admin_token = self
                    .inner
                    .read()
                    .tokens
                    .repo()
                    .get_by_name(DEFAULT_OPERATOR_TOKEN_NAME);

                if default_admin_token.is_none() {
                    return Err(CatalogError::MissingAdminTokenToUpdate);
                }

                // now just update the hash and updated at
                Ok(CatalogBatch::Token(TokenBatch {
                    time_ns: self.time_provider.now().timestamp_nanos(),
                    ops: vec![TokenCatalogOp::RegenerateAdminToken(
                        RegenerateAdminTokenDetails {
                            token_id: default_admin_token.unwrap().as_ref().id,
                            hash: hash.clone(),
                            updated_at: self.time_provider.now().timestamp_millis(),
                        },
                    )],
                }))
            } else {
                // validate name
                if self
                    .inner
                    .read()
                    .tokens
                    .repo()
                    .contains_name(DEFAULT_OPERATOR_TOKEN_NAME)
                {
                    return Err(CatalogError::TokenNameAlreadyExists(
                        DEFAULT_OPERATOR_TOKEN_NAME.to_owned(),
                    ));
                }

                let (token_id, created_at, expiry) = {
                    let mut inner = self.inner.write();
                    let token_id = inner.tokens.get_and_increment_next_id();
                    let created_at = self.time_provider.now();
                    let expiry = None;
                    (token_id, created_at.timestamp_millis(), expiry)
                };

                Ok(CatalogBatch::Token(TokenBatch {
                    time_ns: created_at,
                    ops: vec![TokenCatalogOp::CreateAdminToken(CreateAdminTokenDetails {
                        token_id,
                        name: Arc::from(DEFAULT_OPERATOR_TOKEN_NAME),
                        hash: hash.clone(),
                        created_at,
                        updated_at: None,
                        expiry,
                    })],
                }))
            }
        })
        .await?;

        let token_info = {
            self.inner
                .read()
                .tokens
                .repo()
                .get_by_name(DEFAULT_OPERATOR_TOKEN_NAME)
                .expect("token info must be present after token creation by name")
        };

        // we need to pass these details back, especially this token as this is what user should
        // send in subsequent requests
        Ok((token_info, token))
    }

    pub async fn create_named_admin_token_with_permission(
        &self,
        token_name: String,
        expiry_secs: Option<u64>,
    ) -> Result<(Arc<TokenInfo>, String)> {
        let (token, hash) = create_token_and_hash();
        self.catalog_update_with_retry(|| {
            if self.inner.read().tokens.repo().contains_name(&token_name) {
                return Err(CatalogError::TokenNameAlreadyExists(token_name.clone()));
            }

            let (token_id, created_at, expiry) = {
                let mut inner = self.inner.write();
                let token_id = inner.tokens.get_and_increment_next_id();
                let created_at = self.time_provider.now();
                let expiry = expiry_secs.map(|secs| {
                    created_at
                        .checked_add(Duration::from_secs(secs))
                        .expect("duration not to overflow")
                        .timestamp_millis()
                });
                (token_id, created_at.timestamp_millis(), expiry)
            };

            Ok(CatalogBatch::Token(TokenBatch {
                time_ns: created_at,
                ops: vec![TokenCatalogOp::CreateAdminToken(CreateAdminTokenDetails {
                    token_id,
                    name: Arc::from(token_name.as_str()),
                    hash: hash.clone(),
                    created_at,
                    updated_at: None,
                    expiry,
                })],
            }))
        })
        .await?;

        let token_info = {
            self.inner
                .read()
                .tokens
                .repo()
                .get_by_name(&token_name)
                .expect("token info must be present after token creation by name")
        };

        // we need to pass these details back, especially this token as this is what user should
        // send in subsequent requests
        Ok((token_info, token))
    }

    pub async fn create_scoped_token(
        &self,
        token_name: String,
        permissions: Vec<influxdb3_authz::Permission>,
        expiry_secs: Option<u64>,
    ) -> Result<(Arc<TokenInfo>, String)> {
        let (token, hash) = create_token_and_hash();
        self.catalog_update_with_retry(|| {
            if self.inner.read().tokens.repo().contains_name(&token_name) {
                return Err(CatalogError::TokenNameAlreadyExists(token_name.clone()));
            }

            let (token_id, created_at, expiry) = {
                let mut inner = self.inner.write();
                let token_id = inner.tokens.get_and_increment_next_id();
                let created_at = self.time_provider.now();
                let expiry = expiry_secs.map(|secs| {
                    created_at
                        .checked_add(Duration::from_secs(secs))
                        .expect("duration not to overflow")
                        .timestamp_millis()
                });
                (token_id, created_at.timestamp_millis(), expiry)
            };

            Ok(CatalogBatch::Token(TokenBatch {
                time_ns: created_at,
                ops: vec![TokenCatalogOp::CreateScopedToken(CreateScopedTokenDetails {
                    token_id,
                    name: Arc::from(token_name.as_str()),
                    hash: hash.clone(),
                    created_at,
                    updated_at: None,
                    expiry,
                    permissions: permissions.clone(),
                })],
            }))
        })
        .await?;

        let token_info = {
            self.inner
                .read()
                .tokens
                .repo()
                .get_by_name(&token_name)
                .expect("token info must be present after token creation by name")
        };

        // we need to pass these details back, especially this token as this is what user should
        // send in subsequent requests
        Ok((token_info, token))
    }

    // Return a map of all retention periods indexed by their combined database & table IDs.
    pub fn get_retention_period_cutoff_map(&self) -> BTreeMap<(DbId, TableId), i64> {
        self.list_db_schema()
            .into_iter()
            .flat_map(|db_schema| {
                db_schema
                    .tables()
                    .filter_map(|table_def| {
                        let db_id = db_schema.id();
                        let table_id = table_def.id();
                        db_schema
                            .get_retention_period_cutoff_ts_nanos(self.time_provider())
                            .map(|cutoff| ((db_id, table_id), cutoff))
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub fn get_generation_duration(&self, level: u8) -> Option<Duration> {
        self.inner
            .read()
            .generation_config
            .duration_for_level(level)
    }

    pub fn list_generation_durations(&self) -> Vec<(u8, Duration)> {
        self.inner
            .read()
            .generation_config
            .generation_durations
            .iter()
            .map(|(level, duration)| (*level, *duration))
            .collect()
    }
}

async fn create_internal_db(catalog: &Catalog) {
    let result = catalog.create_database(INTERNAL_DB_NAME).await;
    // what is the best outcome if "_internal" cannot be created?
    match result {
        Ok(_) => info!("created internal database"),
        Err(err) => {
            match err {
                CatalogError::AlreadyExists => {
                    // this is probably ok
                    debug!("not creating internal db as it exists already");
                }
                _ => {
                    // all other errors are unexpected state
                    error!(?err, "unexpected error when creating internal db");
                    panic!("cannot create internal db");
                }
            }
        }
    };
}

impl Catalog {
    /// Create new `Catalog` that uses an in-memory object store.
    ///
    /// # Note
    ///
    /// This is intended as a convenience constructor for testing
    pub async fn new_in_memory(catalog_id: impl Into<Arc<str>>) -> Result<Self> {
        use iox_time::MockProvider;
        use object_store::memory::InMemory;

        let store = Arc::new(InMemory::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let metric_registry = Default::default();
        Self::new(catalog_id.into(), store, time_provider, metric_registry).await
    }

    /// Create new `Catalog` that uses an in-memory object store with additional configuration
    /// arguments.
    ///
    /// # Note
    ///
    /// This is intended as a convenience constructor for testing
    pub async fn new_in_memory_with_args(
        catalog_id: impl Into<Arc<str>>,
        time_provider: Arc<dyn TimeProvider>,
        args: CatalogArgs,
    ) -> Result<Self> {
        use object_store::memory::InMemory;

        let store = Arc::new(InMemory::new());
        let metric_registry = Default::default();
        Self::new_with_args(
            catalog_id.into(),
            store,
            time_provider,
            metric_registry,
            args,
        )
        .await
    }

    /// Create a new `Catalog` with the specified checkpoint interval
    ///
    /// # Note
    ///
    /// This is intended for testing purposes.
    pub async fn new_with_checkpoint_interval(
        catalog_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        checkpoint_interval: u64,
    ) -> Result<Self> {
        let store = ObjectStoreCatalog::new(catalog_id, checkpoint_interval, store);
        let inner = store.load_or_create_catalog().await?;
        let subscriptions = Default::default();

        let catalog = Self {
            state: parking_lot::Mutex::new(CatalogState::Active),
            subscriptions,
            time_provider,
            store,
            metrics: Arc::new(CatalogMetrics::new(&metric_registry)),
            metric_registry,
            inner: RwLock::new(inner),
            limits: Default::default(),
            args: Default::default(),
        };

        create_internal_db(&catalog).await;
        Ok(catalog)
    }
}

impl TokenProvider for Catalog {
    fn get_token(&self, token_hash: Vec<u8>) -> Option<Arc<TokenInfo>> {
        self.inner.read().tokens.hash_to_info(token_hash)
    }
}

impl ProcessingEngineMetrics for Catalog {
    fn num_triggers(&self) -> (u64, u64, u64, u64) {
        self.inner.read().num_triggers()
    }
}

fn database_or_deletion_status(
    db_schema: Option<Arc<DatabaseSchema>>,
    time_provider: &Arc<dyn TimeProvider>,
) -> Result<Arc<DatabaseSchema>, DeletionStatus> {
    match db_schema {
        Some(db_schema) if db_schema.deleted => Err(db_schema
            .hard_delete_time
            .and_then(|time| {
                time_provider
                    .now()
                    .checked_duration_since(time)
                    .map(DeletionStatus::Hard)
            })
            .unwrap_or(DeletionStatus::Soft)),
        Some(db_schema) => Ok(db_schema),
        None => Err(DeletionStatus::NotFound),
    }
}

fn table_deletion_status(
    db_schema: &DatabaseSchema,
    table_id: TableId,
    time_provider: &dyn TimeProvider,
) -> Option<DeletionStatus> {
    match db_schema.tables.get_by_id(&table_id) {
        Some(table_def) if table_def.deleted => Some(
            table_def
                .hard_delete_time
                .and_then(|time| {
                    time_provider
                        .now()
                        .checked_duration_since(time)
                        .map(DeletionStatus::Hard)
                })
                .unwrap_or(DeletionStatus::Soft),
        ),
        Some(_) => None,
        None => Some(DeletionStatus::NotFound),
    }
}

/// General purpose type for storing a collection of things in the catalog
///
/// Each item in the repository has a unique identifier and name. The repository tracks the next
/// identifier that will be used for a new resource added to the repository, with the assumption
/// that identifiers are monotonically increasing unsigned integers.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Repository<I: CatalogId, R: CatalogResource> {
    /// Store for items in the repository
    pub(crate) repo: SerdeVecMap<I, Arc<R>>,
    /// Bi-directional map of identifiers to names in the repository
    pub(crate) id_name_map: BiHashMap<I, Arc<str>>,
    /// The next identifier that will be used when a new resource is added to the repository
    pub(crate) next_id: I,
}

impl<I: CatalogId, R: CatalogResource> Repository<I, R> {
    pub fn new() -> Self {
        Self {
            repo: SerdeVecMap::new(),
            id_name_map: BiHashMap::new(),
            next_id: I::default(),
        }
    }

    pub(crate) fn get_and_increment_next_id(&mut self) -> I {
        let next_id = self.next_id;
        self.next_id = self.next_id.next();
        next_id
    }

    pub(crate) fn next_id(&self) -> I {
        self.next_id
    }

    pub(crate) fn set_next_id(&mut self, id: I) {
        self.next_id = id;
    }

    pub fn name_to_id(&self, name: &str) -> Option<I> {
        self.id_name_map.get_by_right(name).copied()
    }

    pub fn id_to_name(&self, id: &I) -> Option<Arc<str>> {
        self.id_name_map.get_by_left(id).cloned()
    }

    pub fn get_by_name(&self, name: &str) -> Option<Arc<R>> {
        self.id_name_map
            .get_by_right(name)
            .and_then(|id| self.repo.get(id))
            .cloned()
    }

    pub fn get_by_id(&self, id: &I) -> Option<Arc<R>> {
        self.repo.get(id).cloned()
    }

    pub fn contains_id(&self, id: &I) -> bool {
        self.repo.contains_key(id)
    }

    pub fn contains_name(&self, name: &str) -> bool {
        self.id_name_map.contains_right(name)
    }

    pub fn len(&self) -> usize {
        self.repo.len()
    }

    pub fn is_empty(&self) -> bool {
        self.repo.is_empty()
    }

    /// Check if a resource exists in the repository by `id`
    ///
    /// # Panics
    ///
    /// This panics if the `id` is in the id-to-name map, but not in the actual repository map, as
    /// that would be a bad state for the repository to be in.
    fn id_exists(&self, id: &I) -> bool {
        let id_in_map = self.id_name_map.contains_left(id);
        let id_in_repo = self.repo.contains_key(id);
        assert_eq!(
            id_in_map, id_in_repo,
            "id map and repository are in an inconsistent state, \
            in map: {id_in_map}, in repo: {id_in_repo}"
        );
        id_in_repo
    }

    /// Check if a resource exists in the repository by `id` and `name`
    ///
    /// # Panics
    ///
    /// This panics if the `id` is in the id-to-name map, but not in the actual repository map, as
    /// that would be a bad state for the repository to be in.
    fn id_and_name_exists(&self, id: &I, name: &str) -> bool {
        let name_in_map = self.id_name_map.contains_right(name);
        self.id_exists(id) && name_in_map
    }

    /// Insert a new resource to the repository
    pub(crate) fn insert(&mut self, id: I, resource: impl Into<Arc<R>>) -> Result<()> {
        let resource = resource.into();
        if self.id_and_name_exists(&id, resource.name().as_ref()) {
            return Err(CatalogError::AlreadyExists);
        }
        self.id_name_map.insert(id, resource.name());
        self.repo.insert(id, resource);
        self.next_id = match self.next_id.cmp(&id) {
            Ordering::Less | Ordering::Equal => id.next(),
            Ordering::Greater => self.next_id,
        };
        Ok(())
    }

    /// Update an existing resource in the repository
    pub(crate) fn update(&mut self, id: I, resource: impl Into<Arc<R>>) -> Result<()> {
        let resource = resource.into();
        if !self.id_exists(&id) {
            return Err(CatalogError::NotFound);
        }
        self.id_name_map.insert(id, resource.name());
        self.repo.insert(id, resource);
        Ok(())
    }

    pub(crate) fn remove(&mut self, id: &I) {
        self.id_name_map.remove_by_left(id);
        self.repo.shift_remove(id);
    }

    pub fn iter(&self) -> impl Iterator<Item = (&I, &Arc<R>)> {
        self.repo.iter()
    }

    pub fn id_iter(&self) -> impl Iterator<Item = &I> {
        self.repo.keys()
    }

    pub fn resource_iter(&self) -> impl Iterator<Item = &Arc<R>> {
        self.repo.values()
    }
}

impl<I: CatalogId, R: CatalogResource> Default for Repository<I, R> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RetentionPeriod {
    Indefinite,
    Duration(Duration),
}

#[derive(Debug, Clone)]
pub struct InnerCatalog {
    /// A unique monotonically increasing sequence to differentiate the catalog state as it changes
    /// over time.
    pub(crate) sequence: CatalogSequenceNumber,
    /// The `catalog_id` is the user-provided value used to prefix catalog paths on the object store
    pub(crate) catalog_id: Arc<str>,
    /// The `catalog_uuid` is a unique identifier to distinguish catalog instantiations
    pub(crate) catalog_uuid: Uuid,
    /// Global generation settings to configure the layout of persisted parquet files
    pub(crate) generation_config: GenerationConfig,
    /// Collection of nodes in the catalog
    pub(crate) nodes: Repository<NodeId, NodeDefinition>,
    /// Collection of databases in the catalog
    pub(crate) databases: Repository<DbId, DatabaseSchema>,
    /// Collection of tokens in the catalog
    pub(crate) tokens: TokenRepository,
}

impl InnerCatalog {
    pub(crate) fn new(catalog_id: Arc<str>, catalog_uuid: Uuid) -> Self {
        Self {
            sequence: CatalogSequenceNumber::new(0),
            catalog_id,
            catalog_uuid,
            nodes: Repository::default(),
            databases: Repository::default(),
            tokens: TokenRepository::default(),
            // TODO(tjh): using default here will result in an empty config; some type state could
            // help us prevent starting a catalog that avoids this case, but we also need to keep
            // backward compatibility so, just defaulting this for now...
            generation_config: Default::default(),
        }
    }

    pub fn sequence_number(&self) -> CatalogSequenceNumber {
        self.sequence
    }

    pub fn database_count(&self) -> usize {
        self.databases
            .iter()
            // count if not db deleted _and_ not internal
            .filter(|db| !db.1.deleted && db.1.name().as_ref() != INTERNAL_DB_NAME)
            .count()
    }

    pub fn table_count(&self) -> usize {
        self.databases
            .resource_iter()
            .map(|db| db.table_count())
            .sum()
    }

    /// Verifies _and_ applies the `CatalogBatch` to the catalog.
    pub(crate) fn apply_catalog_batch(
        &mut self,
        catalog_batch: &CatalogBatch,
        sequence: CatalogSequenceNumber,
    ) -> Result<Option<OrderedCatalogBatch>> {
        debug!(
            n_ops = catalog_batch.n_ops(),
            current_sequence = self.sequence_number().get(),
            applied_sequence = sequence.get(),
            "apply catalog batch"
        );
        let updated = match catalog_batch {
            CatalogBatch::Node(root_batch) => self.apply_node_batch(root_batch)?,
            CatalogBatch::Database(database_batch) => self.apply_database_batch(database_batch)?,
            CatalogBatch::Token(token_batch) => self.apply_token_batch(token_batch)?,
            CatalogBatch::Delete(delete_batch) => self.apply_delete_batch(delete_batch)?,
            CatalogBatch::Generation(generation_batch) => {
                self.apply_generation_batch(generation_batch)?
            }
        };

        Ok(updated.then(|| {
            self.sequence = sequence;
            OrderedCatalogBatch::new(catalog_batch.clone(), sequence)
        }))
    }

    fn apply_node_batch(&mut self, node_batch: &NodeBatch) -> Result<bool> {
        let mut updated = false;
        for op in &node_batch.ops {
            updated |= match op {
                NodeCatalogOp::RegisterNode(RegisterNodeLog {
                    node_id,
                    instance_id,
                    registered_time_ns,
                    core_count,
                    mode,
                    ..
                }) => {
                    if let Some(mut node) = self.nodes.get_by_name(node_id) {
                        if &node.instance_id != instance_id {
                            return Err(CatalogError::InvalidNodeRegistration);
                        }
                        let n = Arc::make_mut(&mut node);
                        n.mode = mode.clone();
                        n.core_count = *core_count;
                        n.state = NodeState::Running {
                            registered_time_ns: *registered_time_ns,
                        };
                        self.nodes
                            .update(node_batch.node_catalog_id, node)
                            .expect("existing node should update");
                    } else {
                        let new_node = Arc::new(NodeDefinition {
                            node_id: Arc::clone(node_id),
                            node_catalog_id: node_batch.node_catalog_id,
                            instance_id: Arc::clone(instance_id),
                            mode: mode.clone(),
                            core_count: *core_count,
                            state: NodeState::Running {
                                registered_time_ns: *registered_time_ns,
                            },
                        });
                        self.nodes
                            .insert(node_batch.node_catalog_id, new_node)
                            .expect("there should not already be a node");
                    }
                    true
                }
                NodeCatalogOp::StopNode(StopNodeLog {
                    stopped_time_ns, ..
                }) => {
                    let mut new_node = self
                        .nodes
                        .get_by_id(&node_batch.node_catalog_id)
                        .expect("node should exist");
                    let n = Arc::make_mut(&mut new_node);
                    n.state = NodeState::Stopped {
                        stopped_time_ns: *stopped_time_ns,
                    };
                    self.nodes
                        .update(node_batch.node_catalog_id, new_node)
                        .expect("there should be a node to update");
                    true
                }
            };
        }
        Ok(updated)
    }

    fn apply_token_batch(&mut self, token_batch: &TokenBatch) -> Result<bool> {
        let mut is_updated = false;
        for op in &token_batch.ops {
            is_updated |= match op {
                TokenCatalogOp::CreateAdminToken(create_admin_token_details) => {
                    let mut token_info = TokenInfo::new(
                        create_admin_token_details.token_id,
                        Arc::clone(&create_admin_token_details.name),
                        create_admin_token_details.hash.clone(),
                        create_admin_token_details.created_at,
                        create_admin_token_details.expiry,
                    );

                    token_info.set_permissions(vec![Permission {
                        resource_type: ResourceType::Wildcard,
                        resource_identifier: ResourceIdentifier::Wildcard,
                        actions: Actions::Wildcard,
                    }]);
                    // add the admin token itself
                    self.tokens
                        .add_token(create_admin_token_details.token_id, token_info)?;
                    true
                }
                TokenCatalogOp::CreateScopedToken(create_scoped_token_details) => {
                    let mut token_info = TokenInfo::new(
                        create_scoped_token_details.token_id,
                        Arc::clone(&create_scoped_token_details.name),
                        create_scoped_token_details.hash.clone(),
                        create_scoped_token_details.created_at,
                        create_scoped_token_details.expiry,
                    );

                    token_info.set_permissions(create_scoped_token_details.permissions.clone());
                    // add the scoped token
                    self.tokens
                        .add_token(create_scoped_token_details.token_id, token_info)?;
                    true
                }
                TokenCatalogOp::RegenerateAdminToken(regenerate_admin_token_details) => {
                    self.tokens.update_admin_token_hash(
                        regenerate_admin_token_details.token_id,
                        regenerate_admin_token_details.hash.clone(),
                        regenerate_admin_token_details.updated_at,
                    )?;
                    true
                }
                TokenCatalogOp::DeleteToken(delete_token_details) => {
                    self.tokens
                        .delete_token(delete_token_details.token_name.to_owned())?;
                    true
                }
            };
        }

        Ok(is_updated)
    }

    fn apply_database_batch(&mut self, database_batch: &DatabaseBatch) -> Result<bool> {
        if let Some(db) = self.databases.get_by_id(&database_batch.database_id) {
            let Some(new_db) = DatabaseSchema::new_if_updated_from_batch(&db, database_batch)?
            else {
                return Ok(false);
            };
            self.databases
                .update(db.id, new_db)
                .expect("existing database should be updated");
        } else {
            let new_db = DatabaseSchema::new_from_batch(database_batch)?;
            self.databases
                .insert(new_db.id, new_db)
                .expect("new database should be inserted");
        };
        Ok(true)
    }

    fn apply_delete_batch(&mut self, delete_batch: &DeleteBatch) -> Result<bool> {
        let mut updated = false;
        for op in &delete_batch.ops {
            match op {
                DeleteOp::DeleteDatabase(db_id) => {
                    // Remove the database from schema
                    if self.databases.get_by_id(db_id).is_some() {
                        self.databases.remove(db_id);
                        updated = true;
                    }
                }
                DeleteOp::DeleteTable(db_id, table_id) => {
                    // Remove the table from the database schema
                    if let Some(mut db_schema) = self.databases.get_by_id(db_id) {
                        if db_schema.tables.get_by_id(table_id).is_some() {
                            Arc::make_mut(&mut db_schema).tables.remove(table_id);
                            self.databases.update(*db_id, db_schema)?;
                            updated = true;
                        }
                    }
                }
            }
        }
        Ok(updated)
    }

    fn apply_generation_batch(&mut self, generation_batch: &GenerationBatch) -> Result<bool> {
        let mut updated = false;
        for op in &generation_batch.ops {
            match op {
                GenerationOp::SetGenerationDuration(log) => {
                    updated |= self
                        .generation_config
                        .set_duration(log.level, log.duration)?;
                }
            }
        }
        Ok(updated)
    }

    pub fn db_exists(&self, db_id: DbId) -> bool {
        self.databases.get_by_id(&db_id).is_some()
    }

    pub fn num_triggers(&self) -> (u64, u64, u64, u64) {
        self.databases
            .iter()
            .map(|(_, db)| db.trigger_count_by_type())
            .fold(
                (0, 0, 0, 0),
                |(
                    mut overall_wal_count,
                    mut overall_all_wal_count,
                    mut overall_schedule_count,
                    mut overall_request_count,
                ),
                 (wal_count, all_wal_count, schedule_count, request_count)| {
                    overall_wal_count += wal_count;
                    overall_all_wal_count += all_wal_count;
                    overall_schedule_count += schedule_count;
                    overall_request_count += request_count;
                    (
                        overall_wal_count,
                        overall_all_wal_count,
                        overall_schedule_count,
                        overall_request_count,
                    )
                },
            )
    }
}

#[derive(Debug, Clone, Default)]
pub struct GenerationConfig {
    /// Map of generation levels to their duration
    pub(crate) generation_durations: BTreeMap<u8, Duration>,
}

impl GenerationConfig {
    fn set_duration(&mut self, level: impl Into<u8>, duration: Duration) -> Result<bool> {
        let level = level.into();
        match self.generation_durations.entry(level) {
            Entry::Occupied(occupied_entry) => {
                let existing = *occupied_entry.get();
                if existing != duration {
                    Err(CatalogError::CannotChangeGenerationDuration {
                        level,
                        existing: existing.into(),
                        attempted: duration.into(),
                    })
                } else {
                    Ok(false)
                }
            }
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(duration);
                Ok(true)
            }
        }
    }

    fn duration_for_level(&self, level: u8) -> Option<Duration> {
        self.generation_durations.get(&level).copied()
    }
}

/// The definition of a node in the catalog
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct NodeDefinition {
    /// User-provided, unique name for the node
    ///
    /// # Note
    ///
    /// The naming may be a bit confusing for this. This may be more aptly named `node_name`;
    /// however, it is `node_id`, because this corresponds to the user-provided `--node-id` that is
    /// used to identify the node on server start. The unique and automatically generated catalog
    /// identifier for the node is stored in `node_catalog_id`.
    pub(crate) node_id: Arc<str>,
    /// Unique identifier for the node in the catalog
    pub(crate) node_catalog_id: NodeId,
    /// A UUID generated when the node is first registered into the catalog
    pub(crate) instance_id: Arc<str>,
    /// The mode the node is operating in
    pub(crate) mode: Vec<NodeMode>,
    /// The number of cores this node is using
    pub(crate) core_count: u64,
    /// The state of the node
    pub(crate) state: NodeState,
}

impl NodeDefinition {
    pub fn instance_id(&self) -> Arc<str> {
        Arc::clone(&self.instance_id)
    }

    pub fn node_id(&self) -> Arc<str> {
        Arc::clone(&self.node_id)
    }

    pub fn node_catalog_id(&self) -> NodeId {
        self.node_catalog_id
    }

    pub fn modes(&self) -> &Vec<NodeMode> {
        &self.mode
    }

    pub fn is_running(&self) -> bool {
        match self.state {
            NodeState::Running { .. } => true,
            NodeState::Stopped { .. } => false,
        }
    }

    pub fn core_count(&self) -> u64 {
        self.core_count
    }

    pub fn state(&self) -> NodeState {
        self.state
    }
}

/// The state of a node in an InfluxDB 3 cluster
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum NodeState {
    /// A node is set to `Running` when first started and registered into the catalog
    Running { registered_time_ns: i64 },
    /// A node is set to `Stopped` during graceful shutdown
    Stopped { stopped_time_ns: i64 },
}

/// Definition of a database in the catalog
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DatabaseSchema {
    /// Unique identifier for the database
    pub id: DbId,
    /// Unique user-provided name for the database
    pub name: Arc<str>,
    /// Tables contained in the database
    pub tables: Repository<TableId, TableDefinition>,
    /// Retention period for the database
    pub retention_period: RetentionPeriod,
    /// Processing engine triggers configured on the database
    pub processing_engine_triggers: Repository<TriggerId, TriggerDefinition>,
    /// Whether this database has been flagged as deleted
    pub deleted: bool,
    /// The time when the database is scheduled to be hard deleted.
    pub hard_delete_time: Option<Time>,
}

impl DatabaseSchema {
    pub fn new(id: DbId, name: Arc<str>) -> Self {
        Self {
            id,
            name,
            tables: Repository::new(),
            retention_period: RetentionPeriod::Indefinite,
            processing_engine_triggers: Repository::new(),
            deleted: false,
            hard_delete_time: None,
        }
    }

    pub fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }

    pub fn table_count(&self) -> usize {
        self.tables.iter().filter(|table| !table.1.deleted).count()
    }

    /// Validates the updates in the `CatalogBatch` are compatible with this schema. If
    /// everything is compatible and there are no updates to the existing schema, None will be
    /// returned, otherwise a new `DatabaseSchema` will be returned with the updates applied.
    pub fn new_if_updated_from_batch(
        db_schema: &DatabaseSchema,
        database_batch: &DatabaseBatch,
    ) -> Result<Option<Self>> {
        trace!(
            name = ?db_schema.name,
            deleted = ?db_schema.deleted,
            full_batch = ?database_batch,
            "updating / adding to catalog"
        );

        let mut schema = Cow::Borrowed(db_schema);

        for catalog_op in &database_batch.ops {
            schema = catalog_op.update_schema(schema)?;
        }
        // If there were updates then it will have become owned, so we should return the new schema.
        if let Cow::Owned(schema) = schema {
            Ok(Some(schema))
        } else {
            Ok(None)
        }
    }

    pub fn new_from_batch(database_batch: &DatabaseBatch) -> Result<Self> {
        let db_schema = Self::new(
            database_batch.database_id,
            Arc::clone(&database_batch.database_name),
        );
        let new_db = DatabaseSchema::new_if_updated_from_batch(&db_schema, database_batch)?
            .expect("database must be new");
        Ok(new_db)
    }

    /// Create a new empty table definition with the given `table_name` in the database.
    pub(crate) fn create_new_empty_table(
        &mut self,
        table_name: impl Into<Arc<str>>,
    ) -> Result<Arc<TableDefinition>> {
        let table_id = self.tables.get_and_increment_next_id();
        let table_def = Arc::new(TableDefinition::new_empty(table_id, table_name.into()));
        self.tables.insert(table_id, Arc::clone(&table_def))?;
        Ok(table_def)
    }

    /// Update a table in the database. This fails if the table doesn't exist.
    pub(crate) fn update_table(
        &mut self,
        table_id: TableId,
        table_def: Arc<TableDefinition>,
    ) -> Result<()> {
        self.tables.update(table_id, table_def)
    }

    /// Insert a [`TableDefinition`] to the `tables` map and also update the `table_map` and
    /// increment the database next id.
    ///
    /// # Implementation Note
    ///
    /// This method is intended for table definitions being inserted from a log, where the `TableId`
    /// is known, but the table does not yet exist in this instance of the `DatabaseSchema`, i.e.,
    /// on catalog initialization/replay.
    pub fn insert_table_from_log(&mut self, table_id: TableId, table_def: Arc<TableDefinition>) {
        self.tables
            .insert(table_id, table_def)
            .expect("table inserted from the log should not already exist");
    }

    pub fn table_schema_by_id(&self, table_id: &TableId) -> Option<Schema> {
        self.tables
            .get_by_id(table_id)
            .map(|table| table.influx_schema().clone())
    }

    pub fn table_definition(&self, table_name: impl AsRef<str>) -> Option<Arc<TableDefinition>> {
        self.tables.get_by_name(table_name.as_ref())
    }

    pub fn table_definition_by_id(&self, table_id: &TableId) -> Option<Arc<TableDefinition>> {
        self.tables.get_by_id(table_id)
    }

    pub fn table_ids(&self) -> Vec<TableId> {
        self.tables.id_iter().copied().collect()
    }

    pub fn table_names(&self) -> Vec<Arc<str>> {
        self.tables
            .resource_iter()
            .map(|td| Arc::clone(&td.table_name))
            .collect()
    }

    pub fn table_exists(&self, table_id: &TableId) -> bool {
        self.tables.get_by_id(table_id).is_some()
    }

    pub fn tables(&self) -> impl Iterator<Item = Arc<TableDefinition>> + use<'_> {
        self.tables.resource_iter().map(Arc::clone)
    }

    pub fn table_name_to_id(&self, table_name: impl AsRef<str>) -> Option<TableId> {
        self.tables.name_to_id(table_name.as_ref())
    }

    pub fn table_id_to_name(&self, table_id: &TableId) -> Option<Arc<str>> {
        self.tables.id_to_name(table_id)
    }

    pub fn list_distinct_caches(&self) -> Vec<Arc<DistinctCacheDefinition>> {
        self.tables
            .resource_iter()
            .filter(|t| !t.deleted)
            .flat_map(|t| t.distinct_caches.resource_iter())
            .cloned()
            .collect()
    }

    pub fn list_last_caches(&self) -> Vec<Arc<LastCacheDefinition>> {
        self.tables
            .resource_iter()
            .filter(|t| !t.deleted)
            .flat_map(|t| t.last_caches.resource_iter())
            .cloned()
            .collect()
    }

    pub fn trigger_count_by_type(&self) -> (u64, u64, u64, u64) {
        self.processing_engine_triggers.iter().fold(
            (0, 0, 0, 0),
            |(mut wal_count, mut all_wal_count, mut schedule_count, mut request_count),
             (_, trigger)| {
                match trigger.trigger {
                    // wal
                    TriggerSpecificationDefinition::SingleTableWalWrite { .. } => wal_count += 1,
                    TriggerSpecificationDefinition::AllTablesWalWrite => all_wal_count += 1,
                    // schedule
                    TriggerSpecificationDefinition::Schedule { .. }
                    | TriggerSpecificationDefinition::Every { .. } => schedule_count += 1,
                    // request
                    TriggerSpecificationDefinition::RequestPath { .. } => request_count += 1,
                };
                (wal_count, all_wal_count, schedule_count, request_count)
            },
        )
    }

    // Return the oldest allowable timestamp for the given table according to the
    // currently-available set of retention policies. This is returned as a number of nanoseconds
    // since the Unix Epoch.
    pub fn get_retention_period_cutoff_ts_nanos(
        &self,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Option<i64> {
        let retention_period = match self.retention_period {
            RetentionPeriod::Duration(d) => Some(d.as_nanos() as u64),
            RetentionPeriod::Indefinite => None,
        }?;

        let now = time_provider.now().timestamp_nanos();
        Some(now - retention_period as i64)
    }

    /// Returns the deletion status of a table by its table ID
    ///
    /// If the table exists and is not deleted, returns `None`.
    pub fn table_deletion_status(
        &self,
        table_id: TableId,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Option<DeletionStatus> {
        table_deletion_status(self, table_id, &time_provider)
    }
}

/// Trait for schema objects that can be marked as deleted.
pub trait DeletedSchema: Sized {
    /// Check if the schema is marked as deleted.
    fn is_deleted(&self) -> bool;
}

/// A trait for types that can filter themselves based on deletion status.
///
/// This trait provides a convenient way to filter out deleted items by converting
/// them to `None` if they are marked as deleted. It is typically implemented on
/// types that also implement [`DeletedSchema`].
///
/// # Examples
///
/// ```ignore
/// // Get a database schema and filter out if deleted
/// let Some(db) = catalog.db_schema("my_db").not_deleted() else { continue };
/// ```
pub trait IfNotDeleted {
    /// The type that is returned when the item is not deleted.
    type T;

    /// Returns `Some(self)` if the item is not deleted, otherwise returns `None`.
    ///
    /// This method provides a convenient way to filter out deleted items
    /// from the catalog without explicit conditional checks.
    fn if_not_deleted(self) -> Option<Self::T>;
}

impl DeletedSchema for DatabaseSchema {
    fn is_deleted(&self) -> bool {
        self.deleted
    }
}

impl IfNotDeleted for DatabaseSchema {
    type T = Self;

    fn if_not_deleted(self) -> Option<Self::T> {
        (!self.deleted).then_some(self)
    }
}

impl DeletedSchema for TableDefinition {
    fn is_deleted(&self) -> bool {
        self.deleted
    }
}

impl IfNotDeleted for TableDefinition {
    type T = Self;

    fn if_not_deleted(self) -> Option<Self::T> {
        (!self.deleted).then_some(self)
    }
}

impl<T: DeletedSchema> DeletedSchema for Option<T> {
    fn is_deleted(&self) -> bool {
        self.as_ref().is_some_and(DeletedSchema::is_deleted)
    }
}

impl<T: DeletedSchema> IfNotDeleted for Option<T> {
    type T = T;

    fn if_not_deleted(self) -> Option<Self::T> {
        self.and_then(|d| (!d.is_deleted()).then_some(d))
    }
}

impl<T: DeletedSchema> DeletedSchema for Arc<T> {
    fn is_deleted(&self) -> bool {
        self.as_ref().is_deleted()
    }
}

impl<T: DeletedSchema> IfNotDeleted for Arc<T> {
    type T = Self;

    fn if_not_deleted(self) -> Option<Self::T> {
        (!self.is_deleted()).then_some(self)
    }
}

trait UpdateDatabaseSchema {
    fn update_schema<'a>(&self, schema: Cow<'a, DatabaseSchema>)
    -> Result<Cow<'a, DatabaseSchema>>;
}

impl UpdateDatabaseSchema for DatabaseCatalogOp {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        match &self {
            DatabaseCatalogOp::CreateDatabase(create_database) => {
                if create_database.database_id != schema.id
                    || create_database.database_name != schema.name
                {
                    panic!(
                        "Create database call received by a mismatched DatabaseSchema. This should not be possible."
                    )
                }
                schema.to_mut().retention_period = match create_database.retention_period {
                    Some(duration) => RetentionPeriod::Duration(duration),
                    None => RetentionPeriod::Indefinite,
                };

                Ok(schema)
            }
            DatabaseCatalogOp::CreateTable(create_table) => create_table.update_schema(schema),
            DatabaseCatalogOp::AddFields(field_additions) => field_additions.update_schema(schema),
            DatabaseCatalogOp::CreateDistinctCache(distinct_cache_definition) => {
                distinct_cache_definition.update_schema(schema)
            }
            DatabaseCatalogOp::DeleteDistinctCache(delete_distinct_cache) => {
                delete_distinct_cache.update_schema(schema)
            }
            DatabaseCatalogOp::CreateLastCache(create_last_cache) => {
                create_last_cache.update_schema(schema)
            }
            DatabaseCatalogOp::DeleteLastCache(delete_last_cache) => {
                delete_last_cache.update_schema(schema)
            }
            DatabaseCatalogOp::SoftDeleteDatabase(delete_database) => {
                delete_database.update_schema(schema)
            }
            DatabaseCatalogOp::SoftDeleteTable(delete_table) => delete_table.update_schema(schema),
            DatabaseCatalogOp::CreateTrigger(create_trigger) => {
                create_trigger.update_schema(schema)
            }
            DatabaseCatalogOp::DeleteTrigger(delete_trigger) => {
                delete_trigger.update_schema(schema)
            }
            DatabaseCatalogOp::EnableTrigger(trigger_identifier) => {
                EnableTrigger(trigger_identifier.clone()).update_schema(schema)
            }
            DatabaseCatalogOp::DisableTrigger(trigger_identifier) => {
                DisableTrigger(trigger_identifier.clone()).update_schema(schema)
            }
            DatabaseCatalogOp::SetRetentionPeriod(update) => update.update_schema(schema),
            DatabaseCatalogOp::ClearRetentionPeriod(update) => update.update_schema(schema),
        }
    }
}

impl UpdateDatabaseSchema for CreateTableLog {
    fn update_schema<'a>(
        &self,
        mut database_schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        match database_schema.tables.get_by_id(&self.table_id) {
            Some(existing_table) => {
                debug!("creating existing table");
                if let Cow::Owned(updated_table) = existing_table.check_and_add_new_fields(self)? {
                    database_schema
                        .to_mut()
                        .update_table(self.table_id, Arc::new(updated_table))?;
                }
            }
            None => {
                debug!(log = ?self, "creating new table from log");
                let new_table = TableDefinition::new_from_op(self);
                database_schema
                    .to_mut()
                    .insert_table_from_log(new_table.table_id, Arc::new(new_table));
            }
        }

        debug!("updated schema for create table");
        Ok(database_schema)
    }
}

impl UpdateDatabaseSchema for SoftDeleteDatabaseLog {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let owned = schema.to_mut();
        // If it isn't already deleted, then we must generate a "deleted" name for the schema,
        // based on the deletion_time
        if !owned.deleted {
            let deletion_time = Time::from_timestamp_nanos(self.deletion_time);
            owned.name = make_new_name_using_deleted_time(&self.database_name, deletion_time);
            owned.deleted = true;
        }
        owned.hard_delete_time = self.hard_deletion_time.map(Time::from_timestamp_nanos);
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for SoftDeleteTableLog {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        // unlike other table ops, this is not an error.
        if !schema.tables.contains_id(&self.table_id) {
            return Ok(schema);
        }
        let mut_schema = schema.to_mut();
        if let Some(mut deleted_table) = mut_schema.tables.get_by_id(&self.table_id) {
            let new_table_def = Arc::make_mut(&mut deleted_table);
            // If it isn't already deleted, then we must generate a "deleted" name for the schema,
            // based on the deletion_time
            if !new_table_def.deleted {
                let deletion_time = Time::from_timestamp_nanos(self.deletion_time);
                let table_name = make_new_name_using_deleted_time(&self.table_name, deletion_time);
                new_table_def.deleted = true;
                new_table_def.table_name = table_name;
            }
            new_table_def.hard_delete_time =
                self.hard_deletion_time.map(Time::from_timestamp_nanos);
            mut_schema
                .tables
                .update(new_table_def.table_id, deleted_table)
                .expect("the table should exist");
        }
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for SetRetentionPeriodLog {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let mut_schema = schema.to_mut();
        mut_schema.retention_period = self.retention_period;
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for ClearRetentionPeriodLog {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let mut_schema = schema.to_mut();
        mut_schema.retention_period = RetentionPeriod::Indefinite;
        Ok(schema)
    }
}

struct EnableTrigger(TriggerIdentifier);
struct DisableTrigger(TriggerIdentifier);

impl UpdateDatabaseSchema for EnableTrigger {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let Some(trigger) = schema
            .processing_engine_triggers
            .get_by_name(&self.0.trigger_name)
        else {
            return Err(CatalogError::ProcessingEngineTriggerNotFound {
                database_name: self.0.db_name.to_string(),
                trigger_name: self.0.trigger_name.to_string(),
            });
        };
        if !trigger.disabled {
            return Ok(schema);
        }
        let mut mut_trigger = schema
            .processing_engine_triggers
            .get_by_id(&trigger.trigger_id)
            .expect("already checked containment");
        Arc::make_mut(&mut mut_trigger).disabled = false;
        schema
            .to_mut()
            .processing_engine_triggers
            .update(trigger.trigger_id, mut_trigger)
            .expect("existing trigger should update");
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for DisableTrigger {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let Some(trigger) = schema
            .processing_engine_triggers
            .get_by_name(&self.0.trigger_name)
        else {
            return Err(CatalogError::ProcessingEngineTriggerNotFound {
                database_name: self.0.db_name.to_string(),
                trigger_name: self.0.trigger_name.to_string(),
            });
        };
        if trigger.disabled {
            return Ok(schema);
        }
        let mut mut_trigger = schema
            .processing_engine_triggers
            .get_by_id(&trigger.trigger_id)
            .expect("already checked containment");
        Arc::make_mut(&mut mut_trigger).disabled = true;
        schema
            .to_mut()
            .processing_engine_triggers
            .update(trigger.trigger_id, mut_trigger)
            .expect("existing trigger should update");
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for TriggerDefinition {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        if let Some(current) = schema
            .processing_engine_triggers
            .get_by_name(&self.trigger_name)
        {
            if current.as_ref() == self {
                return Ok(schema);
            }
            return Err(CatalogError::ProcessingEngineTriggerExists {
                database_name: schema.name.to_string(),
                trigger_name: self.trigger_name.to_string(),
            });
        }
        schema
            .to_mut()
            .processing_engine_triggers
            .insert(self.trigger_id, Arc::new(self.clone()))
            .expect("new trigger should insert");
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for DeleteTriggerLog {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let Some(trigger) = schema
            .processing_engine_triggers
            .get_by_name(&self.trigger_name)
        else {
            // deleting a non-existent trigger is a no-op to make it idempotent.
            return Ok(schema);
        };
        if !trigger.disabled && !self.force {
            if self.force {
                warn!("deleting running trigger {}", self.trigger_name);
            } else {
                return Err(CatalogError::ProcessingEngineTriggerRunning {
                    trigger_name: self.trigger_name.to_string(),
                });
            }
        }
        schema
            .to_mut()
            .processing_engine_triggers
            .remove(&trigger.trigger_id);

        Ok(schema)
    }
}

fn make_new_name_using_deleted_time(name: &str, deletion_time: Time) -> Arc<str> {
    Arc::from(format!(
        "{}-{}",
        name,
        deletion_time.date_time().format(SOFT_DELETION_TIME_FORMAT)
    ))
}

/// Definition of a table in the catalog
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TableDefinition {
    /// Unique identifier of the table in the catalog
    pub table_id: TableId,
    /// User-provided unique name for the table
    pub table_name: Arc<str>,
    /// The IOx/Arrow schema for the table
    pub schema: Schema,
    /// Column definitions for the table
    pub columns: Repository<ColumnId, ColumnDefinition>,
    /// List of column identifiers that form the series key for the table
    ///
    /// The series key along with the `time` column form the primary key for the table. The series
    /// key is determined as the order of tags provided when the table is first created, either by
    /// a write of line protocol, or by an explicit table creation.
    ///
    /// The series key is used as the sort order, i.e., sort key, for the table during persistence.
    pub series_key: Vec<ColumnId>,
    /// The names of the columns in the table's series key
    pub series_key_names: Vec<Arc<str>>,
    /// The sort key for the table when persisted to storage.
    pub sort_key: SortKey,
    /// Last cache definitions for the table
    pub last_caches: Repository<LastCacheId, LastCacheDefinition>,
    /// Distinct cache definitions for the table
    pub distinct_caches: Repository<DistinctCacheId, DistinctCacheDefinition>,
    /// Whether this table has been set as deleted
    pub deleted: bool,
    /// The time when the table is scheduled to be hard deleted.
    pub hard_delete_time: Option<Time>,
}

impl TableDefinition {
    /// Create new empty `TableDefinition`
    pub fn new_empty(table_id: TableId, table_name: Arc<str>) -> Self {
        Self::new(table_id, table_name, vec![], vec![])
            .expect("empty table should create without error")
    }

    /// Create a new [`TableDefinition`]
    ///
    /// Ensures the provided columns will be ordered before constructing the schema.
    pub fn new(
        table_id: TableId,
        table_name: Arc<str>,
        columns: Vec<(ColumnId, Arc<str>, InfluxColumnType)>,
        series_key: Vec<ColumnId>,
    ) -> Result<Self> {
        // Use a BTree to ensure that the columns are ordered:
        let mut ordered_columns = BTreeMap::new();
        for (col_id, name, column_type) in &columns {
            ordered_columns.insert(name.as_ref(), (col_id, column_type));
        }
        let mut schema_builder = SchemaBuilder::with_capacity(columns.len());
        schema_builder.measurement(table_name.as_ref());
        let mut columns = Repository::new();
        for (name, (col_id, column_type)) in ordered_columns {
            schema_builder.influx_column(name, *column_type);
            let not_nullable = matches!(column_type, InfluxColumnType::Timestamp);
            assert!(
                columns
                    .insert(
                        *col_id,
                        Arc::new(ColumnDefinition::new(
                            *col_id,
                            name,
                            *column_type,
                            !not_nullable
                        )),
                    )
                    .is_ok(),
                "table definition initialized with duplicate column ids"
            );
        }
        let series_key_names = series_key
            .clone()
            .into_iter()
            .map(|id| {
                columns
                    .id_to_name(&id)
                    // NOTE: should this be an error instead of panic?
                    .expect("invalid column id in series key definition")
            })
            .collect::<Vec<Arc<str>>>();
        schema_builder.with_series_key(&series_key_names);
        let schema = schema_builder.build().expect("schema should be valid");

        let sort_key =
            Self::make_sort_key(&series_key_names, columns.contains_name(TIME_COLUMN_NAME));

        Ok(Self {
            table_id,
            table_name,
            schema,
            columns,
            series_key,
            series_key_names,
            sort_key,
            last_caches: Repository::new(),
            distinct_caches: Repository::new(),
            deleted: false,
            hard_delete_time: None,
        })
    }

    fn make_sort_key(series_key_names: &[Arc<str>], add_time: bool) -> SortKey {
        let iter = series_key_names.iter().cloned();
        if add_time {
            SortKey::from_columns(iter.chain(iter::once(TIME_COLUMN_NAME.into())))
        } else {
            SortKey::from_columns(iter)
        }
    }

    /// Create a new table definition from a catalog op
    pub fn new_from_op(table_definition: &CreateTableLog) -> Self {
        let mut columns = Vec::with_capacity(table_definition.field_definitions.len());
        for field_def in &table_definition.field_definitions {
            columns.push((
                field_def.id,
                Arc::clone(&field_def.name),
                field_def.data_type.into(),
            ));
        }
        Self::new(
            table_definition.table_id,
            Arc::clone(&table_definition.table_name),
            columns,
            table_definition.key.clone(),
        )
        .expect("tables defined from ops should not exceed column limits")
    }

    pub(crate) fn check_and_add_new_fields(
        &self,
        table_definition: &CreateTableLog,
    ) -> Result<Cow<'_, Self>> {
        Self::add_fields(Cow::Borrowed(self), &table_definition.field_definitions)
    }

    pub(crate) fn add_fields<'a>(
        mut table: Cow<'a, Self>,
        fields: &Vec<FieldDefinition>,
    ) -> Result<Cow<'a, Self>> {
        let mut new_fields: Vec<(ColumnId, Arc<str>, InfluxColumnType)> =
            Vec::with_capacity(fields.len());
        for field_def in fields {
            if let Some(existing_type) = table
                .columns
                .get_by_id(&field_def.id)
                .map(|def| def.data_type)
            {
                if existing_type != field_def.data_type.into() {
                    return Err(CatalogError::FieldTypeMismatch {
                        table_name: table.table_name.to_string(),
                        column_name: field_def.name.to_string(),
                        existing: existing_type,
                        attempted: field_def.data_type.into(),
                    });
                }
            } else {
                new_fields.push((
                    field_def.id,
                    Arc::clone(&field_def.name),
                    field_def.data_type.into(),
                ));
            }
        }

        if !new_fields.is_empty() {
            let table = table.to_mut();
            table.add_columns(new_fields)?;
        }
        Ok(table)
    }

    /// Check if the column exists in the [`TableDefinition`]
    pub fn column_exists(&self, column: impl AsRef<str>) -> bool {
        self.columns.contains_name(column.as_ref())
    }

    pub(crate) fn add_column(
        &mut self,
        column_name: Arc<str>,
        column_type: InfluxColumnType,
    ) -> Result<ColumnId> {
        let col_id = self.columns.get_and_increment_next_id();
        self.add_columns(vec![(col_id, column_name, column_type)])?;
        Ok(col_id)
    }

    /// Add the columns to this [`TableDefinition`]
    ///
    /// This ensures that the resulting schema has its columns ordered
    pub fn add_columns(
        &mut self,
        columns: Vec<(ColumnId, Arc<str>, InfluxColumnType)>,
    ) -> Result<()> {
        // Use BTree to insert existing and new columns, and use that to generate the
        // resulting schema, to ensure column order is consistent:
        let mut cols = BTreeMap::new();
        for col_def in self.columns.resource_iter().cloned() {
            cols.insert(Arc::clone(&col_def.name), col_def);
        }

        let mut sort_key_changed = false;

        for (id, name, column_type) in columns {
            let nullable = name.as_ref() != TIME_COLUMN_NAME;
            assert!(
                cols.insert(
                    Arc::clone(&name),
                    Arc::new(ColumnDefinition::new(
                        id,
                        Arc::clone(&name),
                        column_type,
                        nullable
                    ))
                )
                .is_none(),
                "attempted to add existing column"
            );
            // add new tags to the series key in the order provided
            if matches!(column_type, InfluxColumnType::Tag) && !self.series_key.contains(&id) {
                self.series_key.push(id);
                self.series_key_names.push(name);
                sort_key_changed = true;
            } else if matches!(column_type, InfluxColumnType::Timestamp)
                && !self.series_key.contains(&id)
            {
                sort_key_changed = true;
            }
        }

        let mut schema_builder = SchemaBuilder::with_capacity(cols.len());
        schema_builder.measurement(self.table_name.as_ref());
        for (name, col_def) in &cols {
            schema_builder.influx_column(name.as_ref(), col_def.data_type);
        }
        schema_builder.with_series_key(&self.series_key_names);
        let schema = schema_builder.build().expect("schema should be valid");
        self.schema = schema;

        let mut new_columns = Repository::new();
        for col in cols.values().cloned() {
            new_columns
                .insert(col.id, col)
                .expect("should be a new column");
        }
        self.columns = new_columns;

        if sort_key_changed {
            self.sort_key = Self::make_sort_key(
                &self.series_key_names,
                self.columns.contains_name(TIME_COLUMN_NAME),
            );
        }

        Ok(())
    }

    pub fn index_column_ids(&self) -> Vec<ColumnId> {
        self.columns
            .iter()
            .filter_map(|(id, def)| match def.data_type {
                InfluxColumnType::Tag => Some(*id),
                InfluxColumnType::Field(_) | InfluxColumnType::Timestamp => None,
            })
            .collect()
    }

    pub fn influx_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn num_columns(&self) -> usize {
        self.influx_schema().len()
    }

    pub fn num_tag_columns(&self) -> usize {
        self.columns
            .resource_iter()
            .filter(|c| matches!(c.data_type, InfluxColumnType::Tag))
            .count()
    }

    pub fn field_type_by_name(&self, name: impl AsRef<str>) -> Option<InfluxColumnType> {
        self.columns
            .get_by_name(name.as_ref())
            .map(|def| def.data_type)
    }

    // TODO(trevor): remove thid API in favour of the Repository APIs
    pub fn column_name_to_id(&self, name: impl AsRef<str>) -> Option<ColumnId> {
        self.columns.name_to_id(name.as_ref())
    }

    // TODO(trevor): remove thid API in favour of the Repository APIs
    pub fn column_id_to_name(&self, id: &ColumnId) -> Option<Arc<str>> {
        self.columns.id_to_name(id)
    }

    // TODO(trevor): remove thid API in favour of the Repository APIs
    pub fn column_name_to_id_unchecked(&self, name: impl AsRef<str>) -> ColumnId {
        self.columns
            .name_to_id(name.as_ref())
            .expect("Column exists in mapping")
    }

    // TODO(trevor): remove thid API in favour of the Repository APIs
    pub fn column_id_to_name_unchecked(&self, id: &ColumnId) -> Arc<str> {
        self.columns
            .id_to_name(id)
            .expect("Column exists in mapping")
    }

    // TODO(trevor): remove thid API in favour of the Repository APIs
    pub fn column_definition(&self, name: impl AsRef<str>) -> Option<Arc<ColumnDefinition>> {
        self.columns.get_by_name(name.as_ref())
    }

    // TODO(trevor): remove thid API in favour of the Repository APIs
    pub fn column_definition_by_id(&self, id: &ColumnId) -> Option<Arc<ColumnDefinition>> {
        self.columns.get_by_id(id)
    }

    pub fn series_key_ids(&self) -> &[ColumnId] {
        &self.series_key
    }

    pub fn series_key_names(&self) -> &[Arc<str>] {
        &self.series_key_names
    }
}

trait TableUpdate {
    fn table_id(&self) -> TableId;
    fn table_name(&self) -> Arc<str>;
    fn update_table<'a>(&self, table: Cow<'a, TableDefinition>)
    -> Result<Cow<'a, TableDefinition>>;
}

impl<T: TableUpdate> UpdateDatabaseSchema for T {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let Some(table) = schema.tables.get_by_id(&self.table_id()) else {
            return Err(CatalogError::TableNotFound {
                db_name: Arc::clone(&schema.name),
                table_name: Arc::clone(&self.table_name()),
            });
        };
        if let Cow::Owned(new_table) = self.update_table(Cow::Borrowed(table.as_ref()))? {
            schema
                .to_mut()
                .update_table(new_table.table_id, Arc::new(new_table))?;
        }
        Ok(schema)
    }
}

impl TableUpdate for AddFieldsLog {
    fn table_id(&self) -> TableId {
        self.table_id
    }
    fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.table_name)
    }
    fn update_table<'a>(
        &self,
        table: Cow<'a, TableDefinition>,
    ) -> Result<Cow<'a, TableDefinition>> {
        TableDefinition::add_fields(table, &self.field_definitions)
    }
}

impl TableUpdate for DistinctCacheDefinition {
    fn table_id(&self) -> TableId {
        self.table_id
    }
    fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.table_name)
    }
    fn update_table<'a>(
        &self,
        mut table: Cow<'a, TableDefinition>,
    ) -> Result<Cow<'a, TableDefinition>> {
        table
            .to_mut()
            .distinct_caches
            .insert(self.cache_id, self.clone())?;
        Ok(table)
    }
}

impl TableUpdate for DeleteDistinctCacheLog {
    fn table_id(&self) -> TableId {
        self.table_id
    }
    fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.table_name)
    }
    fn update_table<'a>(
        &self,
        mut table: Cow<'a, TableDefinition>,
    ) -> Result<Cow<'a, TableDefinition>> {
        table.to_mut().distinct_caches.remove(&self.cache_id);
        Ok(table)
    }
}

impl TableUpdate for LastCacheDefinition {
    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.table)
    }

    fn update_table<'a>(
        &self,
        mut table: Cow<'a, TableDefinition>,
    ) -> Result<Cow<'a, TableDefinition>> {
        table.to_mut().last_caches.insert(self.id, self.clone())?;
        Ok(table)
    }
}

impl TableUpdate for DeleteLastCacheLog {
    fn table_id(&self) -> TableId {
        self.table_id
    }
    fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.table_name)
    }

    fn update_table<'a>(
        &self,
        mut table: Cow<'a, TableDefinition>,
    ) -> Result<Cow<'a, TableDefinition>> {
        table.to_mut().last_caches.remove(&self.id);
        Ok(table)
    }
}

/// Definition of a column in the catalog
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ColumnDefinition {
    /// Unique identifier of the column in the catalog
    pub id: ColumnId,
    /// User-provided unique name for the column
    pub name: Arc<str>,
    /// Influx type of the column
    pub data_type: InfluxColumnType,
    /// Whether this column can hold `NULL` values
    pub nullable: bool,
}

impl ColumnDefinition {
    pub fn new(
        id: ColumnId,
        name: impl Into<Arc<str>>,
        data_type: InfluxColumnType,
        nullable: bool,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

/// Stores tokens in the catalog. Wraps a [`Repository`] while providing additional functionality
/// needed for looking up tokens at runtime.
#[derive(Debug, Clone, Default)]
pub(crate) struct TokenRepository {
    /// The collection of tokens
    repo: Repository<TokenId, TokenInfo>,
    /// Bi-directional map for quick lookup of tokens by their hash
    hash_lookup_map: BiHashMap<TokenId, Vec<u8>>,
}

impl TokenRepository {
    pub(crate) fn new(
        repo: Repository<TokenId, TokenInfo>,
        hash_lookup_map: BiHashMap<TokenId, Vec<u8>>,
    ) -> Self {
        Self {
            repo,
            hash_lookup_map,
        }
    }

    pub(crate) fn repo(&self) -> &Repository<TokenId, TokenInfo> {
        &self.repo
    }

    pub(crate) fn get_and_increment_next_id(&mut self) -> TokenId {
        self.repo.get_and_increment_next_id()
    }

    pub(crate) fn hash_to_info(&self, hash: Vec<u8>) -> Option<Arc<TokenInfo>> {
        let id = self
            .hash_lookup_map
            .get_by_right(&hash)
            .map(|id| id.to_owned())?;
        self.repo.get_by_id(&id)
    }

    pub(crate) fn add_token(&mut self, token_id: TokenId, token_info: TokenInfo) -> Result<()> {
        self.hash_lookup_map
            .insert(token_id, token_info.hash.clone());
        self.repo.insert(token_id, token_info)?;
        Ok(())
    }

    pub(crate) fn update_admin_token_hash(
        &mut self,
        token_id: TokenId,
        hash: Vec<u8>,
        updated_at: i64,
    ) -> Result<()> {
        let mut token_info = self
            .repo
            .get_by_id(&token_id)
            .ok_or_else(|| CatalogError::MissingAdminTokenToUpdate)?;
        let updatable = Arc::make_mut(&mut token_info);

        updatable.hash = hash.clone();
        updatable.updated_at = Some(updated_at);
        updatable.updated_by = Some(token_id);
        self.repo.update(token_id, token_info)?;
        self.hash_lookup_map.insert(token_id, hash);
        Ok(())
    }

    pub(crate) fn delete_token(&mut self, token_name: String) -> Result<()> {
        let token_id = self
            .repo
            .name_to_id(&token_name)
            .ok_or_else(|| CatalogError::NotFound)?;
        self.repo.remove(&token_id);
        self.hash_lookup_map.remove_by_left(&token_id);
        Ok(())
    }
}

impl CatalogResource for TokenInfo {
    type Identifier = TokenId;

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }
}

fn create_token_and_hash() -> (String, Vec<u8>) {
    let token = {
        let mut token = String::from("apiv3_");
        let mut key = [0u8; 64];
        OsRng.fill_bytes(&mut key);
        token.push_str(&B64.encode(key));
        token
    };
    (token.clone(), Sha512::digest(&token).to_vec())
}

#[cfg(test)]
mod tests {

    use crate::{
        log::{
            FieldDataType, LastCacheSize, LastCacheTtl, MaxAge, MaxCardinality, create,
            versions::v3::{DeleteBatch, DeleteOp},
        },
        object_store::CatalogFilePath,
        serialize::{serialize_catalog_file, verify_and_deserialize_catalog_checkpoint_file},
    };

    use super::*;
    use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
    use iox_time::MockProvider;
    use object_store::{local::LocalFileSystem, memory::InMemory};
    use pretty_assertions::assert_eq;
    use test_helpers::assert_contains;

    #[test_log::test(tokio::test)]
    async fn catalog_serialization() {
        let catalog = Catalog::new_in_memory("sample-host-id").await.unwrap();
        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test_table_1",
                &["tag_1", "tag_2", "tag_3"],
                &[
                    ("string_field", FieldDataType::String),
                    ("bool_field", FieldDataType::Boolean),
                    ("i64_field", FieldDataType::Integer),
                    ("u64_field", FieldDataType::UInteger),
                    ("float_field", FieldDataType::Float),
                ],
            )
            .await
            .unwrap();
        catalog
            .create_table(
                "test_db",
                "test_table_2",
                &["tag_1", "tag_2", "tag_3"],
                &[
                    ("string_field", FieldDataType::String),
                    ("bool_field", FieldDataType::Boolean),
                    ("i64_field", FieldDataType::Integer),
                    ("u64_field", FieldDataType::UInteger),
                    ("float_field", FieldDataType::Float),
                ],
            )
            .await
            .unwrap();

        insta::allow_duplicates! {
            insta::with_settings!({
                sort_maps => true,
                description => "catalog serialization to help catch breaking changes"
            }, {
                let snapshot = catalog.snapshot();
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                // Serialize/deserialize to ensure roundtrip
                let serialized = serialize_catalog_file(&snapshot).unwrap();
                let snapshot = verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap() ;
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                catalog.update_from_snapshot(snapshot);
                assert_eq!(catalog.db_name_to_id("test_db"), Some(DbId::from(1)));
            });
        }
    }

    #[test]
    fn add_columns_updates_schema_and_column_map() {
        let mut database = DatabaseSchema {
            id: DbId::from(0),
            name: "test".into(),
            tables: Repository::new(),
            retention_period: RetentionPeriod::Indefinite,
            processing_engine_triggers: Default::default(),
            deleted: false,
            hard_delete_time: None,
        };
        database
            .tables
            .insert(
                TableId::from(0),
                Arc::new(
                    TableDefinition::new(
                        TableId::from(0),
                        "test".into(),
                        vec![
                            (
                                ColumnId::from(0),
                                "test".into(),
                                InfluxColumnType::Field(InfluxFieldType::String),
                            ),
                            (ColumnId::from(1), "test999".into(), InfluxColumnType::Tag),
                        ],
                        vec![ColumnId::from(1)],
                    )
                    .unwrap(),
                ),
            )
            .unwrap();

        let mut table = database.tables.get_by_id(&TableId::from(0)).unwrap();
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.column_id_to_name_unchecked(&0.into()), "test".into());
        assert_eq!(
            table.column_id_to_name_unchecked(&1.into()),
            "test999".into()
        );
        assert_eq!(table.series_key.len(), 1);
        assert_eq!(table.series_key_names.len(), 1);
        assert_eq!(table.sort_key, SortKey::from_columns(vec!["test999"]));
        assert_eq!(table.schema.primary_key(), &["test999"]);

        // add time and verify key is updated
        Arc::make_mut(&mut table)
            .add_columns(vec![(
                ColumnId::from(2),
                TIME_COLUMN_NAME.into(),
                InfluxColumnType::Timestamp,
            )])
            .unwrap();
        assert_eq!(table.series_key.len(), 1);
        assert_eq!(table.series_key_names.len(), 1);
        assert_eq!(
            table.sort_key,
            SortKey::from_columns(vec!["test999", TIME_COLUMN_NAME])
        );
        assert_eq!(table.schema.primary_key(), &["test999", TIME_COLUMN_NAME]);

        Arc::make_mut(&mut table)
            .add_columns(vec![(
                ColumnId::from(3),
                "test2".into(),
                InfluxColumnType::Tag,
            )])
            .unwrap();

        // Verify the series key, series key names and sort key are updated when a tag column is added,
        // and that the "time" column is still at the end.
        assert_eq!(table.series_key.len(), 2);
        assert_eq!(table.series_key_names, &["test999".into(), "test2".into()]);
        assert_eq!(
            table.sort_key,
            SortKey::from_columns(vec!["test999", "test2", TIME_COLUMN_NAME])
        );

        let schema = table.influx_schema();
        assert_eq!(
            schema.field(0).0,
            InfluxColumnType::Field(InfluxFieldType::String)
        );
        assert_eq!(schema.field(1).0, InfluxColumnType::Tag);
        assert_eq!(schema.field(2).0, InfluxColumnType::Tag);

        assert_eq!(table.columns.len(), 4);
        assert_eq!(table.column_name_to_id_unchecked("test2"), 3.into());

        // Verify the schema is updated.
        assert_eq!(table.schema.len(), 4);
        assert_eq!(table.schema.measurement(), Some(&"test".to_owned()));
        let pk = table.schema.primary_key();
        assert_eq!(pk, &["test999", "test2", TIME_COLUMN_NAME]);
    }

    #[tokio::test]
    async fn serialize_series_keys() {
        let catalog = Catalog::new_in_memory("sample-host-id").await.unwrap();
        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test_table_1",
                &["tag_1", "tag_2", "tag_3"],
                &[("field", FieldDataType::String)],
            )
            .await
            .unwrap();

        insta::allow_duplicates! {
            insta::with_settings!({
                sort_maps => true,
                description => "catalog serialization to help catch breaking changes"
            }, {
                let snapshot = catalog.snapshot();
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                // Serialize/deserialize to ensure roundtrip
                let serialized = serialize_catalog_file(&snapshot).unwrap();
                let snapshot = verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap() ;
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                catalog.update_from_snapshot(snapshot);
                assert_eq!(catalog.db_name_to_id("test_db"), Some(DbId::from(1)));
            });
        }
    }

    #[tokio::test]
    async fn serialize_last_cache() {
        let catalog = Catalog::new_in_memory("sample-host-id").await.unwrap();
        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test",
                &["tag_1", "tag_2", "tag_3"],
                &[("field", FieldDataType::String)],
            )
            .await
            .unwrap();
        catalog
            .create_last_cache(
                "test_db",
                "test",
                Some("test_table_last_cache"),
                Some(&["tag_1", "tag_3"]),
                Some(&["field"]),
                LastCacheSize::new(1).unwrap(),
                LastCacheTtl::from_secs(600),
            )
            .await
            .unwrap();

        insta::allow_duplicates! {
            insta::with_settings!({
                sort_maps => true,
                description => "catalog serialization to help catch breaking changes"
            }, {
                let snapshot = catalog.snapshot();
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                // Serialize/deserialize to ensure roundtrip
                let serialized = serialize_catalog_file(&snapshot).unwrap();
                let snapshot = verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap() ;
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                catalog.update_from_snapshot(snapshot);
                assert_eq!(catalog.db_name_to_id("test_db"), Some(DbId::from(1)));
            });
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_serialize_distinct_cache() {
        let catalog = Catalog::new_in_memory("sample-host-id").await.unwrap();
        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test_table",
                &["tag_1", "tag_2", "tag_3"],
                &[("field", FieldDataType::String)],
            )
            .await
            .unwrap();
        catalog
            .create_distinct_cache(
                "test_db",
                "test_table",
                Some("test_cache"),
                &["tag_1", "tag_2"],
                MaxCardinality::from_usize_unchecked(100),
                MaxAge::from_secs(10),
            )
            .await
            .unwrap();

        insta::allow_duplicates! {
            insta::with_settings!({
                sort_maps => true,
                description => "catalog serialization to help catch breaking changes"
            }, {
                let snapshot = catalog.snapshot();
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                // Serialize/deserialize to ensure roundtrip
                let serialized = serialize_catalog_file(&snapshot).unwrap();
                let snapshot = verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap() ;
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                catalog.update_from_snapshot(snapshot);
                assert_eq!(catalog.db_name_to_id("test_db"), Some(DbId::from(1)));
            });
        }
    }

    #[tokio::test]
    async fn test_catalog_id() {
        let catalog = Catalog::new_in_memory("sample-host-id").await.unwrap();
        assert_eq!("sample-host-id", catalog.catalog_id().as_ref());
    }

    /// See: https://github.com/influxdata/influxdb/issues/25524
    #[test_log::test(tokio::test)]
    async fn apply_catalog_batch_fails_for_add_fields_on_nonexist_table() {
        let catalog = Catalog::new_in_memory("host").await.unwrap();
        catalog.create_database("foo").await.unwrap();
        let db_id = catalog.db_name_to_id("foo").unwrap();
        let catalog_batch = create::catalog_batch(
            db_id,
            "foo",
            0,
            [create::add_fields_op(
                db_id,
                "foo",
                TableId::new(0),
                "banana",
                [create::field_def(
                    ColumnId::new(0),
                    "papaya",
                    FieldDataType::String,
                )],
            )],
        );
        debug!("getting write lock");
        let mut inner = catalog.inner.write();
        let sequence = inner.sequence_number();
        let err = inner
            .apply_catalog_batch(&catalog_batch, sequence.next())
            .expect_err("should fail to apply AddFields operation for non-existent table");
        assert_contains!(err.to_string(), "Table banana not in DB schema for foo");
    }

    #[tokio::test]
    async fn test_check_and_mark_table_as_deleted() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
        catalog.create_database("test").await.unwrap();
        catalog
            .create_table(
                "test",
                "boo",
                &["tag_1", "tag_2", "tag_3"],
                &[("field", FieldDataType::String)],
            )
            .await
            .unwrap();

        assert!(
            !catalog
                .db_schema("test")
                .unwrap()
                .table_definition("boo")
                .unwrap()
                .deleted
        );

        catalog
            .soft_delete_table("test", "boo", HardDeletionTime::Never)
            .await
            .unwrap();

        assert!(
            catalog
                .db_schema("test")
                .unwrap()
                .table_definition("boo-19700101T000000")
                .unwrap()
                .deleted
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_hard_delete_table() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

        // Create database and table
        catalog.create_database("test").await.unwrap();
        catalog
            .create_table(
                "test",
                "boo",
                &["tag_1", "tag_2"],
                &[("field", FieldDataType::String)],
            )
            .await
            .unwrap();

        // Get database and table IDs
        let db_id = catalog.db_name_to_id("test").unwrap();
        let table_id = catalog
            .db_schema("test")
            .unwrap()
            .table_definition("boo")
            .unwrap()
            .table_id;

        // Verify table exists
        assert!(
            catalog
                .db_schema("test")
                .unwrap()
                .table_definition("boo")
                .is_some()
        );

        // Hard delete the table
        catalog.hard_delete_table(&db_id, &table_id).await.unwrap();

        // Verify table is removed from the database schema
        assert!(
            catalog
                .db_schema("test")
                .unwrap()
                .table_definition("boo")
                .is_none(),
            "Table should be removed after hard deletion"
        );

        // Verify database still exists
        assert!(
            catalog.db_schema("test").is_some(),
            "Database should still exist after table hard deletion"
        );

        assert!(
            catalog
                .db_schema("test")
                .unwrap()
                .table_definition("boo")
                .is_none(),
            "Table boo should be hard deleted"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_hard_delete_multiple_tables() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

        // Create database and multiple tables
        catalog.create_database("test").await.unwrap();
        catalog
            .create_table(
                "test",
                "table1",
                &["tag"],
                &[("field", FieldDataType::Float)],
            )
            .await
            .unwrap();
        catalog
            .create_table(
                "test",
                "table2",
                &["tag"],
                &[("field", FieldDataType::Integer)],
            )
            .await
            .unwrap();
        catalog
            .create_table(
                "test",
                "table3",
                &["tag"],
                &[("field", FieldDataType::String)],
            )
            .await
            .unwrap();

        // Get database and table IDs
        let db_id = catalog.db_name_to_id("test").unwrap();
        let db_schema = catalog.db_schema("test").unwrap();
        let table_id_1 = db_schema.table_definition("table1").unwrap().table_id;
        let table_id_2 = db_schema.table_definition("table2").unwrap().table_id;
        let table_id_3 = db_schema.table_definition("table3").unwrap().table_id;

        // Hard delete all tables
        catalog
            .hard_delete_table(&db_id, &table_id_1)
            .await
            .unwrap();
        catalog
            .hard_delete_table(&db_id, &table_id_2)
            .await
            .unwrap();
        catalog
            .hard_delete_table(&db_id, &table_id_3)
            .await
            .unwrap();

        // Verify all tables have been hard deleted
        let db_schema_after = catalog.db_schema("test").unwrap();
        assert!(
            db_schema_after.table_definition("table1").is_none(),
            "Table table1 should be hard deleted"
        );
        assert!(
            db_schema_after.table_definition("table2").is_none(),
            "Table table2 should be hard deleted"
        );
        assert!(
            db_schema_after.table_definition("table3").is_none(),
            "Table table3 should be hard deleted"
        );

        // Verify database still exists
        assert!(
            catalog.db_schema("test").is_some(),
            "Database should still exist after all tables are hard deleted"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_hard_delete_nonexistent_table() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

        // Try to delete table from non-existent database
        let fake_db_id = DbId::from(999);
        let fake_table_id = TableId::from(123);
        let result = catalog.hard_delete_table(&fake_db_id, &fake_table_id).await;
        assert!(matches!(result, Err(CatalogError::NotFound)));

        // Create database but try to delete non-existent table
        catalog.create_database("test").await.unwrap();
        let db_id = catalog.db_name_to_id("test").unwrap();
        let result = catalog.hard_delete_table(&db_id, &fake_table_id).await;
        assert!(matches!(result, Err(CatalogError::NotFound)));
    }

    #[test_log::test(tokio::test)]
    async fn test_hard_delete_table_after_soft_delete() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

        // Create database and table
        catalog.create_database("test").await.unwrap();
        catalog
            .create_table("test", "boo", &["tag"], &[("field", FieldDataType::String)])
            .await
            .unwrap();

        // Get database and table IDs
        let db_id = catalog.db_name_to_id("test").unwrap();
        let table_id = catalog
            .db_schema("test")
            .unwrap()
            .table_definition("boo")
            .unwrap()
            .table_id;

        // First soft delete the table
        catalog
            .soft_delete_table("test", "boo", HardDeletionTime::Never)
            .await
            .unwrap();

        // Verify table is soft deleted
        assert!(
            catalog
                .db_schema("test")
                .unwrap()
                .table_definition("boo-19700101T000000")
                .unwrap()
                .deleted
        );

        // Now hard delete the table
        catalog.hard_delete_table(&db_id, &table_id).await.unwrap();

        // Verify the soft-deleted table is now completely removed
        assert!(
            catalog
                .db_schema("test")
                .unwrap()
                .table_definition("boo-19700101T000000")
                .is_none(),
            "Soft-deleted table should be removed after hard deletion"
        );

        // Verify database still exists
        assert!(
            catalog.db_schema("test").is_some(),
            "Database should still exist after table hard deletion"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_hard_delete_table_with_snapshot() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

        // Create database and tables
        catalog.create_database("test").await.unwrap();
        catalog
            .create_table(
                "test",
                "table1",
                &["tag"],
                &[("field", FieldDataType::Float)],
            )
            .await
            .unwrap();
        catalog
            .create_table(
                "test",
                "table2",
                &["tag"],
                &[("field", FieldDataType::Integer)],
            )
            .await
            .unwrap();

        // Get database and table IDs
        let db_id = catalog.db_name_to_id("test").unwrap();
        let db_schema = catalog.db_schema("test").unwrap();
        let table_id_1 = db_schema.table_definition("table1").unwrap().table_id;

        // Hard delete one table
        catalog
            .hard_delete_table(&db_id, &table_id_1)
            .await
            .unwrap();

        // Take a snapshot
        let snapshot = catalog.snapshot();

        // Serialize and deserialize the snapshot
        let serialized = serialize_catalog_file(&snapshot).unwrap();
        let deserialized = verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap();

        // Create a new catalog from the snapshot
        let new_catalog = Catalog::new_in_memory("test-host-2").await.unwrap();
        new_catalog.update_from_snapshot(deserialized);

        // Verify the new catalog has the same state as the original after hard deletion
        let new_db_schema = new_catalog.db_schema("test").unwrap();
        assert!(
            new_db_schema.table_definition("table1").is_none(),
            "Table1 should remain deleted in the new catalog"
        );
        assert!(
            new_db_schema.table_definition("table2").is_some(),
            "Table2 should still exist in the new catalog"
        );

        // Verify the database still exists
        assert!(
            new_catalog.db_schema("test").is_some(),
            "Database should exist in the new catalog"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_hard_delete_database() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

        // Create databases
        catalog.create_database("test").await.unwrap();
        catalog.create_database("test2").await.unwrap();

        // Create tables in test database
        catalog
            .create_table(
                "test",
                "table1",
                &["tag"],
                &[("field", FieldDataType::String)],
            )
            .await
            .unwrap();
        catalog
            .create_table(
                "test",
                "table2",
                &["tag"],
                &[("field", FieldDataType::Float)],
            )
            .await
            .unwrap();

        // Get database ID
        let db_id = catalog.db_name_to_id("test").unwrap();

        // Hard delete the database
        catalog.hard_delete_database(&db_id).await.unwrap();

        // Verify database is completely removed
        assert!(
            catalog.db_schema("test").is_none(),
            "Database 'test' should be removed after hard deletion"
        );

        // Verify test2 database still exists
        assert!(
            catalog.db_schema("test2").is_some(),
            "Database 'test2' should still exist"
        );

        // Verify we can't look up the deleted database by name
        assert!(
            catalog.db_name_to_id("test").is_none(),
            "Should not be able to look up deleted database by name"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_hard_delete_nonexistent_database() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

        // Try to delete non-existent database
        let fake_db_id = DbId::from(999);
        let result = catalog.hard_delete_database(&fake_db_id).await;
        assert!(matches!(result, Err(CatalogError::NotFound)));
    }

    #[test_log::test(tokio::test)]
    async fn test_hard_delete_internal_database() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

        // Get internal database ID
        let internal_db_id = catalog.db_name_to_id("_internal").unwrap();

        // Try to hard delete internal database
        let result = catalog.hard_delete_database(&internal_db_id).await;
        assert!(matches!(
            result,
            Err(CatalogError::CannotDeleteInternalDatabase)
        ));

        // Verify internal database still exists
        assert!(
            catalog.db_schema("_internal").is_some(),
            "Internal database should still exist after failed deletion attempt"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_hard_delete_database_overrides_table_deletions() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

        // Create database and tables
        catalog.create_database("test").await.unwrap();
        catalog
            .create_table(
                "test",
                "table1",
                &["tag"],
                &[("field", FieldDataType::String)],
            )
            .await
            .unwrap();
        catalog
            .create_table(
                "test",
                "table2",
                &["tag"],
                &[("field", FieldDataType::Float)],
            )
            .await
            .unwrap();

        // Get IDs
        let db_id = catalog.db_name_to_id("test").unwrap();
        let db_schema = catalog.db_schema("test").unwrap();
        let table1_id = db_schema.table_definition("table1").unwrap().table_id;
        let _table2_id = db_schema.table_definition("table2").unwrap().table_id;

        // First hard delete a table
        catalog.hard_delete_table(&db_id, &table1_id).await.unwrap();

        // Verify table1 is deleted but database still exists
        assert!(
            catalog
                .db_schema("test")
                .unwrap()
                .table_definition("table1")
                .is_none(),
            "Table1 should be deleted"
        );
        assert!(
            catalog.db_schema("test").is_some(),
            "Database should still exist after table deletion"
        );

        // Now hard delete the database
        catalog.hard_delete_database(&db_id).await.unwrap();

        // Verify the entire database is now gone
        assert!(
            catalog.db_schema("test").is_none(),
            "Database should be removed after hard deletion"
        );
        assert!(
            catalog.db_name_to_id("test").is_none(),
            "Database should not be found by name after hard deletion"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_hard_delete_database_with_snapshot() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

        // Create databases
        catalog.create_database("test1").await.unwrap();
        catalog.create_database("test2").await.unwrap();

        // Create tables
        catalog
            .create_table(
                "test1",
                "table1",
                &["tag"],
                &[("field", FieldDataType::Float)],
            )
            .await
            .unwrap();
        catalog
            .create_table(
                "test2",
                "table1",
                &["tag"],
                &[("field", FieldDataType::Integer)],
            )
            .await
            .unwrap();

        // Get database IDs
        let db1_id = catalog.db_name_to_id("test1").unwrap();
        let _db2_id = catalog.db_name_to_id("test2").unwrap();

        // Hard delete one database
        catalog.hard_delete_database(&db1_id).await.unwrap();

        // Verify test1 is deleted but test2 still exists before snapshot
        assert!(
            catalog.db_schema("test1").is_none(),
            "test1 database should be deleted"
        );
        assert!(
            catalog.db_schema("test2").is_some(),
            "test2 database should still exist"
        );

        // Take a snapshot
        let snapshot = catalog.snapshot();

        // Serialize and deserialize the snapshot
        let serialized = serialize_catalog_file(&snapshot).unwrap();
        let deserialized = verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap();

        // Create a new catalog from the snapshot
        let new_catalog = Catalog::new_in_memory("test-host-2").await.unwrap();
        new_catalog.update_from_snapshot(deserialized);

        // Verify the new catalog has the same state
        assert!(
            new_catalog.db_schema("test1").is_none(),
            "test1 database should remain deleted in new catalog"
        );
        assert!(
            new_catalog.db_schema("test2").is_some(),
            "test2 database should still exist in new catalog"
        );

        // Verify test2's table still exists
        assert!(
            new_catalog
                .db_schema("test2")
                .unwrap()
                .table_definition("table1")
                .is_some(),
            "test2's table should still exist in new catalog"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_hard_delete_database_after_soft_delete() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

        // Create database
        catalog.create_database("test").await.unwrap();
        catalog
            .create_table(
                "test",
                "table1",
                &["tag"],
                &[("field", FieldDataType::String)],
            )
            .await
            .unwrap();

        // Get database ID before soft delete
        let db_id = catalog.db_name_to_id("test").unwrap();

        // Soft delete the database
        catalog
            .soft_delete_database("test", HardDeletionTime::Never)
            .await
            .unwrap();

        // Find the soft-deleted database by iterating databases
        let db_schema = catalog
            .inner
            .read()
            .databases
            .get_by_id(&db_id)
            .expect("database should exist");
        assert!(db_schema.deleted);

        // Hard delete the database
        catalog.hard_delete_database(&db_id).await.unwrap();

        // Verify the database is completely removed
        assert!(
            catalog.inner.read().databases.get_by_id(&db_id).is_none(),
            "Database should be completely removed after hard deletion"
        );

        // Verify we can't find it by name either
        assert!(
            catalog.db_name_to_id("test").is_none(),
            "Should not be able to find hard deleted database by name"
        );
        assert!(
            catalog.db_name_to_id("test-19700101T000000").is_none(),
            "Should not be able to find soft-deleted name after hard deletion"
        );
    }

    // NOTE(trevor/catalog-refactor): this test predates the object-store based catalog, where
    // ordering is still enforced, but it is different. This test mainly verifies that when
    // `OrderedCatalogBatch`s are sorted, they are sorted into the correct order of application.
    // This may still be relevant, but for now am ignoring this test I'm not sure where exactly this
    // is needed; this test is the only piece of code currently that relies on the `PartialOrd`/`Ord`
    // implementations on the `OrderedCatalogBatch` type.
    //
    // Original comment: tests that sorting catalog ops by the sequence number returned from
    // apply_catalog_batch fixes potential ordering issues.
    #[test_log::test(tokio::test)]
    #[ignore]
    async fn test_out_of_order_ops() {
        let catalog = Catalog::new_in_memory("host").await.unwrap();
        let db_id = DbId::new(0);
        let db_name = Arc::from("foo");
        let table_id = TableId::new(0);
        let table_name = Arc::from("bar");
        let table_definition = CreateTableLog {
            database_id: db_id,
            database_name: Arc::clone(&db_name),
            table_name: Arc::clone(&table_name),
            table_id,
            field_definitions: vec![
                FieldDefinition::new(ColumnId::from(0), "tag_1", FieldDataType::Tag),
                FieldDefinition::new(ColumnId::from(1), "time", FieldDataType::Timestamp),
                FieldDefinition::new(ColumnId::from(2), "field", FieldDataType::String),
            ],
            key: vec![ColumnId::from(0)],
        };
        let create_op = CatalogBatch::database(
            0,
            db_id,
            Arc::clone(&db_name),
            vec![DatabaseCatalogOp::CreateTable(table_definition.clone())],
        );
        let add_column_op = CatalogBatch::database(
            0,
            db_id,
            Arc::clone(&db_name),
            vec![DatabaseCatalogOp::AddFields(AddFieldsLog {
                database_name: Arc::clone(&db_name),
                database_id: db_id,
                table_name,
                table_id,
                field_definitions: vec![FieldDefinition::new(
                    ColumnId::from(3),
                    "tag_2",
                    FieldDataType::Tag,
                )],
            })],
        );
        debug!("apply create op");
        let create_ordered_op = catalog
            .inner
            .write()
            .apply_catalog_batch(&create_op, catalog.sequence_number().next())
            .expect("apply create op")
            .expect("should be able to create");
        debug!("apply add column op");
        let add_column_op = catalog
            .inner
            .write()
            .apply_catalog_batch(&add_column_op, catalog.sequence_number().next())
            .expect("apply add column op")
            .expect("should produce operation");
        let mut ordered_batches = vec![add_column_op, create_ordered_op];
        ordered_batches.sort();

        let replayed_catalog = Catalog::new_in_memory("host").await.unwrap();
        debug!(?ordered_batches, "apply sorted ops");
        let permit = CATALOG_WRITE_PERMIT.lock().await;
        for ordered_batch in ordered_batches {
            replayed_catalog.apply_ordered_catalog_batch(&ordered_batch, &permit);
        }
        let original_table = catalog
            .db_schema_by_id(&db_id)
            .unwrap()
            .table_definition_by_id(&table_id)
            .unwrap();
        let replayed_table = catalog
            .db_schema_by_id(&db_id)
            .unwrap()
            .table_definition_by_id(&table_id)
            .unwrap();

        assert_eq!(original_table, replayed_table);
    }

    #[test_log::test(tokio::test)]
    async fn deleted_dbs_dont_count() {
        let catalog = Catalog::new_in_memory("test").await.unwrap();

        for i in 0..Catalog::NUM_DBS_LIMIT {
            let db_name = format!("test-db-{i}");
            catalog.create_database(&db_name).await.unwrap();
        }

        // check the count of databases:
        assert_eq!(
            Catalog::NUM_DBS_LIMIT,
            catalog.inner.read().database_count()
        );

        // now create another database, this should NOT be allowed:
        let db_name = "a-database-too-far";
        catalog
            .create_database(db_name)
            .await
            .expect_err("should not be able to create more than permitted number of databases");

        // now delete a database:
        let db_name = format!("test-db-{}", Catalog::NUM_DBS_LIMIT - 1);
        catalog
            .soft_delete_database(&db_name, HardDeletionTime::Never)
            .await
            .unwrap();

        // check again, count should have gone down:
        assert_eq!(
            Catalog::NUM_DBS_LIMIT - 1,
            catalog.inner.read().database_count()
        );

        // now create another database (using same name as the deleted one), this should be allowed:
        catalog
            .create_database(&db_name)
            .await
            .expect("can create a database again");

        // check new count:
        assert_eq!(
            Catalog::NUM_DBS_LIMIT,
            catalog.inner.read().database_count()
        );
    }

    #[test_log::test(tokio::test)]
    async fn deleted_tables_dont_count() {
        let catalog = Catalog::new_in_memory("test").await.unwrap();

        let mut txn = catalog.begin("test-db").unwrap();

        // create as many tables as are allowed:
        for i in 0..Catalog::NUM_TABLES_LIMIT {
            let table_name = format!("test-table-{i}");
            txn.table_or_create(&table_name).unwrap();
            txn.column_or_create(&table_name, "field", FieldDataType::String)
                .unwrap();
            txn.column_or_create(&table_name, "time", FieldDataType::Timestamp)
                .unwrap();
        }
        catalog.commit(txn).await.unwrap();

        assert_eq!(
            Catalog::NUM_TABLES_LIMIT,
            catalog.inner.read().table_count()
        );

        // should not be able to create another table:
        let table_name = "a-table-too-far";
        catalog
            .create_table(
                "test-db",
                table_name,
                &["tag"],
                &[("field", FieldDataType::String)],
            )
            .await
            .expect_err("should not be able to exceed table limit");

        catalog
            .soft_delete_table(
                "test-db",
                format!("test-table-{}", Catalog::NUM_TABLES_LIMIT - 1).as_str(),
                HardDeletionTime::Never,
            )
            .await
            .unwrap();

        assert_eq!(
            Catalog::NUM_TABLES_LIMIT - 1,
            catalog.inner.read().table_count()
        );

        // now create it again, this should be allowed:
        catalog
            .create_table(
                "test-db",
                table_name,
                &["tag"],
                &[("field", FieldDataType::String)],
            )
            .await
            .expect("should be created");

        assert_eq!(
            Catalog::NUM_TABLES_LIMIT,
            catalog.inner.read().table_count()
        );
    }

    #[test_log::test(tokio::test)]
    async fn retention_period_cutoff_map() {
        use iox_time::MockProvider;
        let now = Time::from_timestamp(60 * 60 * 24, 0).unwrap();
        let time_provider = Arc::new(MockProvider::new(now));
        let catalog =
            Catalog::new_in_memory_with_args("test", time_provider as _, CatalogArgs::default())
                .await
                .unwrap();

        let testdb1 = "test-db";
        let mut txn = catalog.begin(testdb1).unwrap();

        for i in 0..4 {
            let table_name = format!("test-table-{i}");
            txn.table_or_create(&table_name).unwrap();
            txn.column_or_create(&table_name, "field", FieldDataType::String)
                .unwrap();
            txn.column_or_create(&table_name, "time", FieldDataType::Timestamp)
                .unwrap();
        }
        catalog.commit(txn).await.unwrap();

        let testdb2 = "test-db-2";
        let mut txn = catalog.begin(testdb2).unwrap();

        for i in 0..4 {
            let table_name = format!("test-table-{i}");
            txn.table_or_create(&table_name).unwrap();
            txn.column_or_create(&table_name, "field", FieldDataType::String)
                .unwrap();
            txn.column_or_create(&table_name, "time", FieldDataType::Timestamp)
                .unwrap();
        }
        catalog.commit(txn).await.unwrap();

        let database_retention = Duration::from_secs(15);
        let database_cutoff = now - database_retention;

        // set per-table and database-level retention periods on table 2
        catalog
            .set_retention_period_for_database(testdb2, database_retention)
            .await
            .expect("must be able to set retention for database");

        let map = catalog.get_retention_period_cutoff_map();
        assert_eq!(map.len(), 4, "expect 4 entries in resulting map");

        // validate tables where there is either a table or a database retention set
        for (db_name, table_name, expected_cutoff) in [
            (testdb2, "test-table-0", database_cutoff.timestamp_nanos()),
            (testdb2, "test-table-1", database_cutoff.timestamp_nanos()),
            (testdb2, "test-table-2", database_cutoff.timestamp_nanos()),
            (testdb2, "test-table-3", database_cutoff.timestamp_nanos()),
        ] {
            let db_schema = catalog
                .db_schema(db_name)
                .expect("must be able to get expected database schema");
            let table_def = db_schema
                .table_definition(table_name)
                .expect("must be able to get expected table definition");
            let cutoff = map
                .get(&(db_schema.id(), table_def.id()))
                .expect("expected retention period must exist");
            assert_eq!(
                *cutoff, expected_cutoff,
                "expected cutoff must match actual"
            );
        }

        // validate tables with no retention set
        for (db_name, table_name) in [
            (testdb1, "test-table-0"),
            (testdb1, "test-table-1"),
            (testdb1, "test-table-2"),
            (testdb1, "test-table-3"),
        ] {
            let db_schema = catalog
                .db_schema(db_name)
                .expect("must be able to get expected database schema");
            let table_def = db_schema
                .table_definition(table_name)
                .expect("must be able to get expected table definition");
            let v = map.get(&(db_schema.id(), table_def.id()));
            assert!(
                v.is_none(),
                "no retention period cutoff expected for {db_name}/{table_name}"
            );
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_catalog_file_ordering() {
        let local_disk =
            Arc::new(LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

        let init = async || {
            Catalog::new(
                "test",
                Arc::clone(&local_disk) as _,
                Arc::clone(&time_provider) as _,
                Default::default(),
            )
            .await
            .unwrap()
        };

        let catalog = init().await;

        // create a database, then a table, then add fields to that table
        // on reload, the add fields would fail if it was applied before the creation of the
        // table...
        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test_tbl",
                &["t1"],
                &[("f1", FieldDataType::String)],
            )
            .await
            .unwrap();
        let mut txn = catalog.begin("test_db").unwrap();
        txn.column_or_create("test_tbl", "f2", FieldDataType::Integer)
            .unwrap();
        catalog.commit(txn).await.unwrap();

        drop(catalog);

        let catalog = init().await;

        insta::assert_json_snapshot!(catalog.snapshot(), {
            ".catalog_uuid" => "[uuid]"
        });
    }

    #[test_log::test(tokio::test)]
    async fn test_load_from_catalog_checkpoint() {
        let obj_store =
            Arc::new(LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

        let init = async || {
            // create a catalog that checkpoints every 10 sequences
            Catalog::new_with_checkpoint_interval(
                "test",
                Arc::clone(&obj_store) as _,
                Arc::clone(&time_provider) as _,
                Default::default(),
                10,
            )
            .await
            .unwrap()
        };

        let catalog = init().await;

        // make changes to create catalog operations that get persisted to the log:
        catalog.create_database("test_db").await.unwrap();
        for i in 0..10 {
            catalog
                .create_table(
                    "test_db",
                    format!("table_{i}").as_str(),
                    &["t1"],
                    &[("f1", FieldDataType::String)],
                )
                .await
                .unwrap();
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        let prefix = catalog.object_store_prefix();
        drop(catalog);

        // delete up to the 10th catalog file so that when we re-init, we know it is loading
        // from the checkpoint:
        for i in 1..=10 {
            obj_store
                .delete(
                    CatalogFilePath::log(prefix.as_ref(), CatalogSequenceNumber::new(i)).as_ref(),
                )
                .await
                .unwrap();
        }

        // catalog should load successfully:
        let catalog = init().await;

        // we created 10 tables so the catalog should have 10:
        assert_eq!(10, catalog.db_schema("test_db").unwrap().tables.len());
    }

    #[test_log::test(tokio::test)]
    async fn test_load_many_files_with_default_checkpoint_interval() {
        let obj_store =
            Arc::new(LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap());
        let obj_store = Arc::new(RequestCountedObjectStore::new(obj_store as _));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

        let init = async || {
            // create a catalog that checkpoints every 10 sequences
            Catalog::new(
                "test",
                Arc::clone(&obj_store) as _,
                Arc::clone(&time_provider) as _,
                Default::default(),
            )
            .await
            .unwrap()
        };

        let catalog = init().await;
        catalog.create_database("foo").await.unwrap();
        for i in 0..100 {
            let table_name = format!("table_{i}");
            catalog
                .create_table(
                    "foo",
                    &table_name,
                    &["t1"],
                    &[("f1", FieldDataType::String)],
                )
                .await
                .unwrap();
            let mut txn = catalog.begin("foo").unwrap();
            txn.column_or_create(&table_name, "f2", FieldDataType::String)
                .unwrap();
            catalog.commit(txn).await.unwrap();
        }

        let checkpoint_read_count = obj_store.total_read_request_count(
            CatalogFilePath::checkpoint(catalog.object_store_prefix().as_ref()).as_ref(),
        );
        // checkpoint would have been attempted to be read on initialization, hence it is 1:
        assert_eq!(1, checkpoint_read_count);

        let first_log_read_count = obj_store.total_read_request_count(
            CatalogFilePath::log(
                catalog.object_store_prefix().as_ref(),
                CatalogSequenceNumber::new(1),
            )
            .as_ref(),
        );
        // this file should never have been read:
        assert_eq!(0, first_log_read_count);

        let last_log_read_count = obj_store.total_read_request_count(
            CatalogFilePath::log(
                catalog.object_store_prefix().as_ref(),
                catalog.sequence_number(),
            )
            .as_ref(),
        );
        // this file should never have been read:
        assert_eq!(0, last_log_read_count);

        // drop the catalog and re-initialize:
        drop(catalog);
        let catalog = init().await;

        let checkpoint_read_count = obj_store.total_read_request_count(
            CatalogFilePath::checkpoint(catalog.object_store_prefix().as_ref()).as_ref(),
        );
        // re-init will read the checkpoint again:
        assert_eq!(2, checkpoint_read_count);

        let first_log_read_count = obj_store.total_read_request_count(
            CatalogFilePath::log(
                catalog.object_store_prefix().as_ref(),
                CatalogSequenceNumber::new(1),
            )
            .as_ref(),
        );
        // this file should still not have been read, since it would have been covered by a
        // recent checkpoint:
        assert_eq!(0, first_log_read_count);

        let last_log_read_count = obj_store.total_read_request_count(
            CatalogFilePath::log(
                catalog.object_store_prefix().as_ref(),
                catalog.sequence_number(),
            )
            .as_ref(),
        );
        // this file should have been read on re-init, as it would not be covered by a
        // checkpoint:
        assert_eq!(1, last_log_read_count);
    }

    #[test_log::test(tokio::test)]
    async fn apply_catalog_batch_fails_for_add_fields_past_tag_limit() {
        let catalog = Catalog::new_in_memory("host").await.unwrap();
        catalog.create_database("foo").await.unwrap();
        let tags = (0..NUM_TAG_COLUMNS_LIMIT)
            .map(|i| format!("tag_{i}"))
            .collect::<Vec<_>>();
        catalog
            .create_table("foo", "bar", &tags, &[("f1", FieldDataType::String)])
            .await
            .unwrap();

        let mut txn = catalog.begin("foo").unwrap();
        let err = txn
            .column_or_create("bar", "tag_too_much", FieldDataType::Tag)
            .unwrap_err();
        assert_contains!(
            err.to_string(),
            "Update to schema would exceed number of tag columns per table limit of 250 columns"
        );
    }

    #[test_log::test(tokio::test)]
    async fn apply_catalog_batch_fails_to_create_table_with_too_many_tags() {
        let catalog = Catalog::new_in_memory("host").await.unwrap();
        catalog.create_database("foo").await.unwrap();
        let tags = (0..NUM_TAG_COLUMNS_LIMIT + 1)
            .map(|i| format!("tag_{i}"))
            .collect::<Vec<_>>();
        let err = catalog
            .create_table("foo", "bar", &tags, &[("f1", FieldDataType::String)])
            .await;
        assert_contains!(
            err.unwrap_err().to_string(),
            "Update to schema would exceed number of tag columns per table limit of 250 columns"
        );
    }

    #[tokio::test]
    async fn test_catalog_gen1_duration_can_only_set_once() {
        // setup:
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let time: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let create_catalog = async || {
            Catalog::new(
                "test-node",
                Arc::clone(&store),
                Arc::clone(&time),
                Default::default(),
            )
            .await
            .unwrap()
        };
        let catalog = create_catalog().await;
        let duration = Duration::from_secs(10);
        // setting the first time succeeds:
        catalog.set_gen1_duration(duration).await.unwrap();
        assert_eq!(catalog.get_generation_duration(1), Some(duration));
        // setting again with the same duration is an AlreadyExists error:
        let err = catalog.set_gen1_duration(duration).await.unwrap_err();
        assert!(matches!(err, CatalogError::AlreadyExists));
        // setting again with a different duraiton is a different error case:
        let other_duration = Duration::from_secs(20);
        let err = catalog.set_gen1_duration(other_duration).await.unwrap_err();
        assert!(matches!(
            err,
            CatalogError::CannotChangeGenerationDuration {
                level: 1,
                existing,
                ..
            } if existing == duration.into()
        ));
        // drop and recreate the catalog:
        drop(catalog);
        let catalog = create_catalog().await;
        // the gen1 duration should still be set:
        assert_eq!(catalog.get_generation_duration(1), Some(duration));
    }

    #[tokio::test]
    async fn test_catalog_with_empty_gen_durations_can_be_set() {
        // setup:
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let time: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let create_catalog = async || {
            Catalog::new(
                "test-node",
                Arc::clone(&store),
                Arc::clone(&time),
                Default::default(),
            )
            .await
            .unwrap()
        };
        // only initialize the catalog so it is persisted to object store with an empty generation
        // configuration
        let catalog = create_catalog().await;
        let expected_catalog_uuid = catalog.catalog_uuid();

        // drop the catalog and re-initialize from object store:
        drop(catalog);
        let catalog = create_catalog().await;
        let actual_catalog_uuid = catalog.catalog_uuid();
        assert_eq!(expected_catalog_uuid, actual_catalog_uuid);
        assert!(catalog.get_generation_duration(1).is_none());

        // set the gen1 duration, which should work:
        let duration = Duration::from_secs(10);
        catalog.set_gen1_duration(duration).await.unwrap();
        assert_eq!(catalog.get_generation_duration(1), Some(duration));
    }

    #[test]
    fn test_deleted_objects_initialization() {
        let catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());
        // Test that catalog initializes successfully
        assert_eq!(catalog.catalog_id.as_ref(), "test-catalog");
    }

    #[test]
    fn test_apply_delete_batch_delete_database() {
        let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());
        let db_id = DbId::from(1);

        // Create a database first
        let db_schema = DatabaseSchema::new(db_id, "test_db".into());
        catalog.databases.insert(db_id, db_schema).unwrap();

        let delete_batch = DeleteBatch {
            time_ns: 1000,
            ops: vec![DeleteOp::DeleteDatabase(db_id)],
        };

        let result = catalog.apply_delete_batch(&delete_batch).unwrap();
        assert!(result);
    }

    #[test]
    fn test_apply_delete_batch_delete_table() {
        let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());
        let db_id = DbId::from(1);
        let table_id = TableId::from(1);

        // Create a database and table first
        let mut db_schema = DatabaseSchema::new(db_id, "test_db".into());
        let table_def = TableDefinition::new_empty(table_id, "test_table".into());
        db_schema.tables.insert(table_id, table_def).unwrap();
        catalog.databases.insert(db_id, db_schema).unwrap();

        let delete_batch = DeleteBatch {
            time_ns: 1000,
            ops: vec![DeleteOp::DeleteTable(db_id, table_id)],
        };

        let result = catalog.apply_delete_batch(&delete_batch).unwrap();
        assert!(result);
    }

    #[test]
    fn test_apply_delete_batch_multiple_tables() {
        let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());
        let db_id = DbId::from(1);
        let table_id_1 = TableId::from(1);
        let table_id_2 = TableId::from(2);
        let table_id_3 = TableId::from(3);

        // Create a database and tables first
        let mut db_schema = DatabaseSchema::new(db_id, "test_db".into());
        db_schema
            .tables
            .insert(
                table_id_1,
                TableDefinition::new_empty(table_id_1, "table1".into()),
            )
            .unwrap();
        db_schema
            .tables
            .insert(
                table_id_2,
                TableDefinition::new_empty(table_id_2, "table2".into()),
            )
            .unwrap();
        db_schema
            .tables
            .insert(
                table_id_3,
                TableDefinition::new_empty(table_id_3, "table3".into()),
            )
            .unwrap();
        catalog.databases.insert(db_id, db_schema).unwrap();

        let delete_batch = DeleteBatch {
            time_ns: 1000,
            ops: vec![
                DeleteOp::DeleteTable(db_id, table_id_1),
                DeleteOp::DeleteTable(db_id, table_id_2),
                DeleteOp::DeleteTable(db_id, table_id_3),
            ],
        };

        let result = catalog.apply_delete_batch(&delete_batch).unwrap();
        assert!(result);
    }

    #[test]
    fn test_apply_delete_batch_mixed_operations() {
        let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());
        let db_id_1 = DbId::from(1);
        let db_id_2 = DbId::from(2);
        let table_id_1 = TableId::from(1);
        let table_id_2 = TableId::from(2);

        // Create databases and tables first
        let mut db_schema_1 = DatabaseSchema::new(db_id_1, "test_db1".into());
        db_schema_1
            .tables
            .insert(
                table_id_1,
                TableDefinition::new_empty(table_id_1, "table1".into()),
            )
            .unwrap();
        db_schema_1
            .tables
            .insert(
                table_id_2,
                TableDefinition::new_empty(table_id_2, "table2".into()),
            )
            .unwrap();
        catalog.databases.insert(db_id_1, db_schema_1).unwrap();

        let db_schema_2 = DatabaseSchema::new(db_id_2, "test_db2".into());
        catalog.databases.insert(db_id_2, db_schema_2).unwrap();

        let delete_batch = DeleteBatch {
            time_ns: 1000,
            ops: vec![
                DeleteOp::DeleteTable(db_id_1, table_id_1),
                DeleteOp::DeleteTable(db_id_1, table_id_2),
                DeleteOp::DeleteDatabase(db_id_2),
            ],
        };

        let result = catalog.apply_delete_batch(&delete_batch).unwrap();
        assert!(result);
    }

    #[test]
    fn test_apply_delete_batch_database_overrides_tables() {
        let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());
        let db_id = DbId::from(1);
        let table_id = TableId::from(1);

        // Create a database and table first
        let mut db_schema = DatabaseSchema::new(db_id, "test_db".into());
        db_schema
            .tables
            .insert(
                table_id,
                TableDefinition::new_empty(table_id, "test_table".into()),
            )
            .unwrap();
        catalog.databases.insert(db_id, db_schema).unwrap();

        // First delete a table
        let delete_batch_1 = DeleteBatch {
            time_ns: 1000,
            ops: vec![DeleteOp::DeleteTable(db_id, table_id)],
        };
        catalog.apply_delete_batch(&delete_batch_1).unwrap();

        // Then delete the database
        let delete_batch_2 = DeleteBatch {
            time_ns: 2000,
            ops: vec![DeleteOp::DeleteDatabase(db_id)],
        };
        catalog.apply_delete_batch(&delete_batch_2).unwrap();
    }

    #[test_log::test(tokio::test)]
    async fn test_catalog_batch_delete_serialization() {
        let db_id = DbId::from(1);
        let table_id = TableId::from(1);

        let delete_batch = CatalogBatch::delete(
            1000,
            vec![
                DeleteOp::DeleteDatabase(db_id),
                DeleteOp::DeleteTable(DbId::from(2), table_id),
            ],
        );

        // Test basic properties
        assert_eq!(delete_batch.n_ops(), 2);
        assert!(delete_batch.as_delete().is_some());

        // Test serialization roundtrip
        let serialized = serde_json::to_string(&delete_batch).unwrap();
        let deserialized: CatalogBatch = serde_json::from_str(&serialized).unwrap();

        if let CatalogBatch::Delete(batch) = deserialized {
            assert_eq!(batch.time_ns, 1000);
            assert_eq!(batch.ops.len(), 2);
            match &batch.ops[0] {
                DeleteOp::DeleteDatabase(id) => assert_eq!(*id, db_id),
                _ => panic!("Expected DeleteDatabase operation"),
            }
            match &batch.ops[1] {
                DeleteOp::DeleteTable(db, tbl) => {
                    assert_eq!(*db, DbId::from(2));
                    assert_eq!(*tbl, table_id);
                }
                _ => panic!("Expected DeleteTable operation"),
            }
        } else {
            panic!("Expected Delete variant");
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_catalog_with_deleted_objects_snapshot() {
        let catalog = Catalog::new_in_memory("test-host").await.unwrap();

        // Create a database and table
        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test_table",
                &["tag1"],
                &[("field1", FieldDataType::Float)],
            )
            .await
            .unwrap();

        // Get the IDs
        let db_id = catalog.db_name_to_id("test_db").unwrap();
        let db_schema = catalog.db_schema("test_db").unwrap();
        let table_def = db_schema.table_definition("test_table").unwrap();
        let table_id = table_def.table_id;

        // Apply delete operations directly to inner catalog for testing
        let delete_batch = DeleteBatch {
            time_ns: 1000,
            ops: vec![DeleteOp::DeleteTable(db_id, table_id)],
        };

        catalog
            .inner
            .write()
            .apply_delete_batch(&delete_batch)
            .unwrap();

        // Verify table is deleted from database schema
        let db_schema_after = catalog.db_schema("test_db").unwrap();
        assert!(
            db_schema_after.table_definition("test_table").is_none(),
            "Table should be deleted from schema"
        );

        // Create a snapshot
        let snapshot = catalog.snapshot();

        // Test serialization/deserialization roundtrip
        let serialized = serialize_catalog_file(&snapshot).unwrap();
        let deserialized = verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap();

        // Create a new catalog from the snapshot
        let new_catalog = Catalog::new_in_memory("test-host-2").await.unwrap();
        new_catalog.update_from_snapshot(deserialized);

        // Verify the new catalog has the same state - table is deleted
        let new_db_schema = new_catalog.db_schema("test_db").unwrap();
        assert!(
            new_db_schema.table_definition("test_table").is_none(),
            "Table should remain deleted in new catalog"
        );
        assert!(
            new_catalog.db_schema("test_db").is_some(),
            "Database should still exist in new catalog"
        );
    }

    #[test]
    fn test_database_deletion_removes_from_deleted_tables() {
        let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());
        let db_id_1 = DbId::from(1);
        let db_id_2 = DbId::from(2);
        let table_id_1 = TableId::from(1);
        let table_id_2 = TableId::from(2);
        let table_id_3 = TableId::from(3);

        // First, delete some tables from both databases
        let delete_batch_1 = DeleteBatch {
            time_ns: 1000,
            ops: vec![
                DeleteOp::DeleteTable(db_id_1, table_id_1),
                DeleteOp::DeleteTable(db_id_1, table_id_2),
                DeleteOp::DeleteTable(db_id_2, table_id_3),
            ],
        };
        catalog.apply_delete_batch(&delete_batch_1).unwrap();

        // Now delete database 1
        let delete_batch_2 = DeleteBatch {
            time_ns: 2000,
            ops: vec![DeleteOp::DeleteDatabase(db_id_1)],
        };
        catalog.apply_delete_batch(&delete_batch_2).unwrap();
    }

    #[test]
    fn test_apply_delete_batch_removes_database_from_schema() {
        let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());

        // Create a database
        let db_id = DbId::from(1);
        let db_name = Arc::from("test_db");
        let db_schema = DatabaseSchema::new(db_id, Arc::clone(&db_name));
        catalog
            .databases
            .insert(db_id, Arc::new(db_schema))
            .unwrap();

        // Verify database exists
        assert!(catalog.databases.get_by_id(&db_id).is_some());

        // Delete the database
        let delete_batch = DeleteBatch {
            time_ns: 1000,
            ops: vec![DeleteOp::DeleteDatabase(db_id)],
        };

        let result = catalog.apply_delete_batch(&delete_batch).unwrap();
        assert!(result);

        // Verify database is removed from schema
        assert!(catalog.databases.get_by_id(&db_id).is_none());
    }

    #[test]
    fn test_apply_delete_batch_removes_table_from_schema() {
        let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());

        // Create a database with a table
        let db_id = DbId::from(1);
        let db_name = Arc::from("test_db");
        let mut db_schema = DatabaseSchema::new(db_id, Arc::clone(&db_name));

        let table_id = TableId::from(1);
        let table_name = Arc::from("test_table");
        let table_def = TableDefinition::new_empty(table_id, Arc::clone(&table_name));
        db_schema
            .tables
            .insert(table_id, Arc::new(table_def))
            .unwrap();

        catalog
            .databases
            .insert(db_id, Arc::new(db_schema))
            .unwrap();

        // Verify table exists
        let db = catalog.databases.get_by_id(&db_id).unwrap();
        assert!(db.tables.get_by_id(&table_id).is_some());

        // Delete the table
        let delete_batch = DeleteBatch {
            time_ns: 1000,
            ops: vec![DeleteOp::DeleteTable(db_id, table_id)],
        };

        let result = catalog.apply_delete_batch(&delete_batch).unwrap();
        assert!(result);

        // Verify table is removed from schema
        let db = catalog.databases.get_by_id(&db_id).unwrap();
        assert!(db.tables.get_by_id(&table_id).is_none());
    }

    /// Tests that deleting a table from a database schema that has multiple Arc references
    /// is correctly handled.
    #[test]
    fn test_apply_delete_batch_database_delete_table_correctness_with_multiple_schema_references() {
        let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());

        let db_id = DbId::from(1);

        // Database 1 with 2 tables
        let mut db_schema_1 = DatabaseSchema::new(db_id, Arc::from("db1"));
        let table_id_1 = TableId::from(1);
        let table_id_2 = TableId::from(2);
        db_schema_1
            .tables
            .insert(
                table_id_1,
                Arc::new(TableDefinition::new_empty(table_id_1, Arc::from("table1"))),
            )
            .unwrap();
        db_schema_1
            .tables
            .insert(
                table_id_2,
                Arc::new(TableDefinition::new_empty(table_id_2, Arc::from("table2"))),
            )
            .unwrap();
        catalog
            .databases
            .insert(db_id, Arc::new(db_schema_1))
            .unwrap();

        // Create an additional reference to the database schema
        let _db_schema = catalog.databases.get_by_id(&db_id).unwrap();

        // Delete table from db1 and entire db2
        let delete_batch = DeleteBatch {
            time_ns: 1000,
            ops: vec![DeleteOp::DeleteTable(db_id, table_id_1)],
        };

        let result = catalog.apply_delete_batch(&delete_batch).unwrap();
        assert!(result);

        let new_db_schema = catalog.databases.get_by_id(&db_id).unwrap();
        assert!(new_db_schema.tables.get_by_id(&table_id_1).is_none());
        assert!(new_db_schema.tables.get_by_id(&table_id_2).is_some());
    }

    #[test]
    fn test_apply_delete_batch_database_deletion_removes_all_tables() {
        let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());

        // Create a database with multiple tables
        let db_id = DbId::from(1);
        let db_name = Arc::from("test_db");
        let mut db_schema = DatabaseSchema::new(db_id, Arc::clone(&db_name));

        let table_ids = vec![TableId::from(1), TableId::from(2), TableId::from(3)];
        for (i, table_id) in table_ids.iter().enumerate() {
            let table_name = Arc::from(format!("table_{i}"));
            let table_def = TableDefinition::new_empty(*table_id, table_name);
            db_schema
                .tables
                .insert(*table_id, Arc::new(table_def))
                .unwrap();
        }

        catalog
            .databases
            .insert(db_id, Arc::new(db_schema))
            .unwrap();

        // Verify all tables exist
        let db = catalog.databases.get_by_id(&db_id).unwrap();
        for table_id in &table_ids {
            assert!(db.tables.get_by_id(table_id).is_some());
        }

        // Delete the database
        let delete_batch = DeleteBatch {
            time_ns: 1000,
            ops: vec![DeleteOp::DeleteDatabase(db_id)],
        };

        let result = catalog.apply_delete_batch(&delete_batch).unwrap();
        assert!(result);

        // Verify database and all its tables are removed from schema
        assert!(catalog.databases.get_by_id(&db_id).is_none());
    }

    #[test]
    fn test_apply_delete_batch_mixed_operations_with_schema_removal() {
        let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());

        // Create two databases with tables
        let db_id_1 = DbId::from(1);
        let db_id_2 = DbId::from(2);

        // Database 1 with 2 tables
        let mut db_schema_1 = DatabaseSchema::new(db_id_1, Arc::from("db1"));
        let table_id_1 = TableId::from(1);
        let table_id_2 = TableId::from(2);
        db_schema_1
            .tables
            .insert(
                table_id_1,
                Arc::new(TableDefinition::new_empty(table_id_1, Arc::from("table1"))),
            )
            .unwrap();
        db_schema_1
            .tables
            .insert(
                table_id_2,
                Arc::new(TableDefinition::new_empty(table_id_2, Arc::from("table2"))),
            )
            .unwrap();

        // Database 2 with 1 table
        let mut db_schema_2 = DatabaseSchema::new(db_id_2, Arc::from("db2"));
        let table_id_3 = TableId::from(3);
        db_schema_2
            .tables
            .insert(
                table_id_3,
                Arc::new(TableDefinition::new_empty(table_id_3, Arc::from("table3"))),
            )
            .unwrap();

        catalog
            .databases
            .insert(db_id_1, Arc::new(db_schema_1))
            .unwrap();
        catalog
            .databases
            .insert(db_id_2, Arc::new(db_schema_2))
            .unwrap();

        // Delete table from db1 and entire db2
        let delete_batch = DeleteBatch {
            time_ns: 1000,
            ops: vec![
                DeleteOp::DeleteTable(db_id_1, table_id_1),
                DeleteOp::DeleteDatabase(db_id_2),
            ],
        };

        let result = catalog.apply_delete_batch(&delete_batch).unwrap();
        assert!(result);

        // Verify db1 still exists with only table2
        let db1 = catalog.databases.get_by_id(&db_id_1).unwrap();
        assert!(db1.tables.get_by_id(&table_id_1).is_none());
        assert!(db1.tables.get_by_id(&table_id_2).is_some());

        // Verify db2 is completely removed
        assert!(catalog.databases.get_by_id(&db_id_2).is_none());
    }

    #[test]
    fn test_apply_delete_batch_table_deletion_after_database_deletion() {
        let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());

        let db_id = DbId::from(1);
        let table_id = TableId::from(1);

        // First delete the database
        let delete_batch_1 = DeleteBatch {
            time_ns: 1000,
            ops: vec![DeleteOp::DeleteDatabase(db_id)],
        };
        catalog.apply_delete_batch(&delete_batch_1).unwrap();

        // Then try to delete a table from the deleted database
        let delete_batch_2 = DeleteBatch {
            time_ns: 2000,
            ops: vec![DeleteOp::DeleteTable(db_id, table_id)],
        };
        let result = catalog.apply_delete_batch(&delete_batch_2).unwrap();

        // Should return false since no changes were made (database already deleted)
        assert!(!result);
    }

    #[test]
    fn test_serialization_format() {
        let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());
        let db_id_1 = DbId::from(2);
        let db_id_2 = DbId::from(5);
        let db_id_3 = DbId::from(3);
        let db_id_4 = DbId::from(9);
        let table_id_1 = TableId::from(1);
        let table_id_2 = TableId::from(6);
        let table_id_3 = TableId::from(7);
        let table_id_4 = TableId::from(2);

        // Create the exact scenario from the user's example
        let delete_batch = DeleteBatch {
            time_ns: 1000,
            ops: vec![
                DeleteOp::DeleteDatabase(db_id_1),          // 2
                DeleteOp::DeleteDatabase(db_id_2),          // 5
                DeleteOp::DeleteTable(db_id_3, table_id_1), // 3: [1, 6, 7]
                DeleteOp::DeleteTable(db_id_3, table_id_2),
                DeleteOp::DeleteTable(db_id_3, table_id_3),
                DeleteOp::DeleteTable(db_id_4, table_id_4), // 9: [2]
            ],
        };
        catalog.apply_delete_batch(&delete_batch).unwrap();

        insta::allow_duplicates! {
            insta::with_settings!({
                sort_maps => true,
                description => "Catalog snapshot with deleted objects"
            }, {
                let snapshot = catalog.snapshot();

                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
            })
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_database_hard_delete_time_never() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
        catalog.create_database("test_db").await.unwrap();

        // Get database ID before soft delete
        let db_id = catalog.db_name_to_id("test_db").unwrap();

        // Soft delete with Never
        catalog
            .soft_delete_database("test_db", HardDeletionTime::Never)
            .await
            .unwrap();

        // Verify hard_delete_time is None
        let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
        assert!(db_schema.deleted);
        assert!(db_schema.hard_delete_time.is_none());
    }

    #[test_log::test(tokio::test)]
    async fn test_database_hard_delete_time_default() {
        use iox_time::MockProvider;
        let now = Time::from_timestamp_nanos(1000000000);
        let time_provider = Arc::new(MockProvider::new(now));
        let catalog = Catalog::new_in_memory_with_args(
            "test-catalog",
            Arc::clone(&time_provider) as _,
            CatalogArgs::default(),
        )
        .await
        .unwrap();

        catalog.create_database("test_db").await.unwrap();

        // Get database ID before soft delete
        let db_id = catalog.db_name_to_id("test_db").unwrap();

        // Soft delete with Default
        catalog
            .soft_delete_database("test_db", HardDeletionTime::Default)
            .await
            .unwrap();

        // Verify hard_delete_time is set to now + default duration
        let expected_time = now + Catalog::DEFAULT_HARD_DELETE_DURATION;
        let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
        assert!(db_schema.deleted);
        assert_eq!(db_schema.hard_delete_time, Some(expected_time));
    }

    #[test_log::test(tokio::test)]
    async fn test_database_hard_delete_time_specific_timestamp() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
        catalog.create_database("test_db").await.unwrap();

        // Get database ID before soft delete
        let db_id = catalog.db_name_to_id("test_db").unwrap();

        let specific_time = Time::from_timestamp_nanos(5000000000);

        // Soft delete with specific timestamp
        catalog
            .soft_delete_database("test_db", HardDeletionTime::Timestamp(specific_time))
            .await
            .unwrap();

        // Verify hard_delete_time is set to the specific time
        let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
        assert!(db_schema.deleted);
        assert_eq!(db_schema.hard_delete_time, Some(specific_time));
    }

    #[test_log::test(tokio::test)]
    async fn test_database_hard_delete_time_now() {
        use iox_time::MockProvider;
        let now = Time::from_timestamp_nanos(2000000000);
        let time_provider = Arc::new(MockProvider::new(now));
        let catalog = Catalog::new_in_memory_with_args(
            "test-catalog",
            Arc::clone(&time_provider) as _,
            CatalogArgs::default(),
        )
        .await
        .unwrap();

        catalog.create_database("test_db").await.unwrap();

        // Get database ID before soft delete
        let db_id = catalog.db_name_to_id("test_db").unwrap();

        // Soft delete with Now
        catalog
            .soft_delete_database("test_db", HardDeletionTime::Now)
            .await
            .unwrap();

        // Verify hard_delete_time is set to current time
        let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
        assert!(db_schema.deleted);
        assert_eq!(db_schema.hard_delete_time, Some(now));
    }

    #[test_log::test(tokio::test)]
    async fn test_database_hard_delete_time_serialization() {
        use iox_time::MockProvider;
        let now = Time::from_timestamp_nanos(3000000000);
        let time_provider = Arc::new(MockProvider::new(now));
        let catalog = Catalog::new_in_memory_with_args(
            "test-catalog",
            Arc::clone(&time_provider) as _,
            CatalogArgs::default(),
        )
        .await
        .unwrap();

        catalog.create_database("test_db").await.unwrap();

        // Get database ID before soft delete
        let db_id = catalog.db_name_to_id("test_db").unwrap();

        // Soft delete with Default hard delete time
        catalog
            .soft_delete_database("test_db", HardDeletionTime::Default)
            .await
            .unwrap();

        // Take a snapshot
        let snapshot = catalog.snapshot();

        // Verify hard_delete_time is in the snapshot
        let expected_time = now + Catalog::DEFAULT_HARD_DELETE_DURATION;
        let db_snapshot = snapshot.databases.repo.get(&db_id).unwrap();
        assert_eq!(
            db_snapshot.hard_delete_time,
            Some(expected_time.timestamp_nanos())
        );

        // Test deserialization
        let new_catalog = Catalog::new_in_memory("test-catalog-2").await.unwrap();
        new_catalog.update_from_snapshot(snapshot);

        let restored_db_schema = new_catalog.db_schema_by_id(&db_id).unwrap();
        assert!(restored_db_schema.deleted);
        assert_eq!(restored_db_schema.hard_delete_time, Some(expected_time));
    }

    #[test_log::test(tokio::test)]
    async fn test_database_deletion_status_existing_not_deleted() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
        catalog.create_database("test_db").await.unwrap();

        let db_id = catalog.db_name_to_id("test_db").unwrap();

        // Database exists and is not deleted - should return None
        assert_eq!(catalog.database_deletion_status(db_id), None);
    }

    #[test_log::test(tokio::test)]
    async fn test_database_deletion_status_soft_deleted() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
        catalog.create_database("test_db").await.unwrap();

        let db_id = catalog.db_name_to_id("test_db").unwrap();

        // Soft delete the database
        catalog
            .soft_delete_database("test_db", HardDeletionTime::Never)
            .await
            .unwrap();

        // Should return Soft status
        assert_eq!(
            catalog.database_deletion_status(db_id),
            Some(DeletionStatus::Soft)
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_database_deletion_status_hard_deleted() {
        use iox_time::MockProvider;
        use std::time::Duration;

        let now = Time::from_timestamp_nanos(1_000_000_000);
        let time_provider = Arc::new(MockProvider::new(now));
        let catalog = Catalog::new_in_memory_with_args(
            "test-catalog",
            Arc::clone(&time_provider) as _,
            CatalogArgs::default(),
        )
        .await
        .unwrap();

        catalog.create_database("test_db").await.unwrap();
        let db_id = catalog.db_name_to_id("test_db").unwrap();

        // Soft delete the database with immediate hard deletion
        catalog
            .soft_delete_database("test_db", HardDeletionTime::Now)
            .await
            .unwrap();

        // Advance time to simulate hard deletion has occurred
        let future_time = now + Duration::from_secs(3600); // 1 hour later
        time_provider.set(future_time);

        // Should return Hard status with duration
        match catalog.database_deletion_status(db_id) {
            Some(DeletionStatus::Hard(duration)) => {
                // Duration should be approximately 1 hour
                assert!(duration >= Duration::from_secs(3599));
                assert!(duration <= Duration::from_secs(3601));
            }
            other => panic!("Expected Hard deletion status, got {other:?}"),
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_database_deletion_status_scheduled_for_hard_deletion() {
        use iox_time::MockProvider;
        use std::time::Duration;

        let now = Time::from_timestamp_nanos(1_000_000_000);
        let time_provider = Arc::new(MockProvider::new(now));
        let catalog = Catalog::new_in_memory_with_args(
            "test-catalog",
            Arc::clone(&time_provider) as _,
            CatalogArgs::default(),
        )
        .await
        .unwrap();

        catalog.create_database("test_db").await.unwrap();
        let db_id = catalog.db_name_to_id("test_db").unwrap();

        // Soft delete with future hard deletion time
        let future_deletion_time = now + Duration::from_secs(7200); // 2 hours from now
        catalog
            .soft_delete_database("test_db", HardDeletionTime::Timestamp(future_deletion_time))
            .await
            .unwrap();

        // Should still return Soft status since hard deletion time hasn't arrived
        assert_eq!(
            catalog.database_deletion_status(db_id),
            Some(DeletionStatus::Soft)
        );

        // Advance time past the hard deletion time
        let past_deletion_time = future_deletion_time + Duration::from_secs(600); // 10 minutes after
        time_provider.set(past_deletion_time);

        // Now should return Hard status
        match catalog.database_deletion_status(db_id) {
            Some(DeletionStatus::Hard(duration)) => {
                // Duration should be approximately 10 minutes
                assert!(duration >= Duration::from_secs(599));
                assert!(duration <= Duration::from_secs(601));
            }
            other => panic!("Expected Hard deletion status, got {other:?}"),
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_database_deletion_status_not_found() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

        // Non-existent database ID
        let non_existent_id = DbId::from(999);

        // Should return NotFound status
        assert_eq!(
            catalog.database_deletion_status(non_existent_id),
            Some(DeletionStatus::NotFound)
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_database_deletion_status_in_deleted_set() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
        catalog.create_database("test_db").await.unwrap();

        let db_id = catalog.db_name_to_id("test_db").unwrap();

        // Manually remove database to simulate hard deletion cleanup
        {
            let mut inner = catalog.inner.write();
            inner.databases.remove(&db_id);
        }

        // Should return NotFound status
        assert_eq!(
            catalog.database_deletion_status(db_id),
            Some(DeletionStatus::NotFound)
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_table_deletion_status_existing_not_deleted() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test_table",
                &["tag1"],
                &[("field1", FieldDataType::String)],
            )
            .await
            .unwrap();

        let db_schema = catalog.db_schema("test_db").unwrap();
        let table_id = db_schema.table_name_to_id("test_table").unwrap();

        // Table exists and is not deleted - should return None
        assert_eq!(
            db_schema.table_deletion_status(table_id, catalog.time_provider()),
            None
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_table_deletion_status_soft_deleted() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test_table",
                &["tag1"],
                &[("field1", FieldDataType::String)],
            )
            .await
            .unwrap();

        let db_schema = catalog.db_schema("test_db").unwrap();
        let table_id = db_schema.table_name_to_id("test_table").unwrap();

        // Soft delete the table
        catalog
            .soft_delete_table("test_db", "test_table", HardDeletionTime::Never)
            .await
            .unwrap();

        // Get updated schema after deletion
        let updated_db_schema = catalog.db_schema("test_db").unwrap();

        // Should return Soft status
        assert_eq!(
            updated_db_schema.table_deletion_status(table_id, catalog.time_provider()),
            Some(DeletionStatus::Soft)
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_table_deletion_status_hard_deleted() {
        use iox_time::MockProvider;
        use std::time::Duration;

        let now = Time::from_timestamp_nanos(1_000_000_000);
        let time_provider = Arc::new(MockProvider::new(now));
        let catalog = Catalog::new_in_memory_with_args(
            "test-catalog",
            Arc::clone(&time_provider) as _,
            CatalogArgs::default(),
        )
        .await
        .unwrap();

        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test_table",
                &["tag1"],
                &[("field1", FieldDataType::String)],
            )
            .await
            .unwrap();

        let db_schema = catalog.db_schema("test_db").unwrap();
        let table_id = db_schema.table_name_to_id("test_table").unwrap();

        // Soft delete the table with immediate hard deletion
        catalog
            .soft_delete_table("test_db", "test_table", HardDeletionTime::Now)
            .await
            .unwrap();

        // Advance time to simulate hard deletion has occurred
        let future_time = now + Duration::from_secs(3600); // 1 hour later
        time_provider.set(future_time);

        // Get updated schema after deletion
        let updated_db_schema = catalog.db_schema("test_db").unwrap();

        // Should return Hard status with duration
        match updated_db_schema.table_deletion_status(table_id, Arc::clone(&time_provider) as _) {
            Some(DeletionStatus::Hard(duration)) => {
                // Duration should be approximately 1 hour
                assert!(duration >= Duration::from_secs(3599));
                assert!(duration <= Duration::from_secs(3601));
            }
            other => panic!("Expected Hard deletion status, got {other:?}"),
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_table_deletion_status_scheduled_for_hard_deletion() {
        use iox_time::MockProvider;
        use std::time::Duration;

        let now = Time::from_timestamp_nanos(1_000_000_000);
        let time_provider = Arc::new(MockProvider::new(now));
        let catalog = Catalog::new_in_memory_with_args(
            "test-catalog",
            Arc::clone(&time_provider) as _,
            CatalogArgs::default(),
        )
        .await
        .unwrap();

        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test_table",
                &["tag1"],
                &[("field1", FieldDataType::String)],
            )
            .await
            .unwrap();

        let db_schema = catalog.db_schema("test_db").unwrap();
        let table_id = db_schema.table_name_to_id("test_table").unwrap();

        // Soft delete with future hard deletion time
        let future_deletion_time = now + Duration::from_secs(7200); // 2 hours from now
        catalog
            .soft_delete_table(
                "test_db",
                "test_table",
                HardDeletionTime::Timestamp(future_deletion_time),
            )
            .await
            .unwrap();

        // Get updated schema after deletion
        let updated_db_schema = catalog.db_schema("test_db").unwrap();

        // Should still return Soft status since hard deletion time hasn't arrived
        assert_eq!(
            updated_db_schema.table_deletion_status(table_id, Arc::clone(&time_provider) as _),
            Some(DeletionStatus::Soft)
        );

        // Advance time past the hard deletion time
        let past_deletion_time = future_deletion_time + Duration::from_secs(600); // 10 minutes after
        time_provider.set(past_deletion_time);

        // Get updated schema (should be the same instance since it's time-based)
        let final_db_schema = catalog.db_schema("test_db").unwrap();

        // Now should return Hard status
        match final_db_schema.table_deletion_status(table_id, Arc::clone(&time_provider) as _) {
            Some(DeletionStatus::Hard(duration)) => {
                // Duration should be approximately 10 minutes
                assert!(duration >= Duration::from_secs(599));
                assert!(duration <= Duration::from_secs(601));
            }
            other => panic!("Expected Hard deletion status, got {other:?}"),
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_table_deletion_status_not_found() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
        catalog.create_database("test_db").await.unwrap();

        let db_schema = catalog.db_schema("test_db").unwrap();
        // Non-existent table ID
        let non_existent_id = TableId::from(999);

        // Should return NotFound status
        assert_eq!(
            db_schema.table_deletion_status(non_existent_id, catalog.time_provider()),
            Some(DeletionStatus::NotFound)
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_table_deletion_status_multiple_tables() {
        use iox_time::MockProvider;
        use std::time::Duration;

        let now = Time::from_timestamp_nanos(1_000_000_000);
        let time_provider = Arc::new(MockProvider::new(now));
        let catalog = Catalog::new_in_memory_with_args(
            "test-catalog",
            Arc::clone(&time_provider) as _,
            CatalogArgs::default(),
        )
        .await
        .unwrap();

        catalog.create_database("test_db").await.unwrap();

        // Create multiple tables
        catalog
            .create_table(
                "test_db",
                "table1",
                &["tag1"],
                &[("field1", FieldDataType::String)],
            )
            .await
            .unwrap();
        catalog
            .create_table(
                "test_db",
                "table2",
                &["tag1"],
                &[("field1", FieldDataType::String)],
            )
            .await
            .unwrap();
        catalog
            .create_table(
                "test_db",
                "table3",
                &["tag1"],
                &[("field1", FieldDataType::String)],
            )
            .await
            .unwrap();

        let db_schema = catalog.db_schema("test_db").unwrap();
        let table1_id = db_schema.table_name_to_id("table1").unwrap();
        let table2_id = db_schema.table_name_to_id("table2").unwrap();
        let table3_id = db_schema.table_name_to_id("table3").unwrap();

        // Leave table1 as is (not deleted)
        // Soft delete table2
        catalog
            .soft_delete_table("test_db", "table2", HardDeletionTime::Never)
            .await
            .unwrap();
        // Hard delete table3
        catalog
            .soft_delete_table("test_db", "table3", HardDeletionTime::Now)
            .await
            .unwrap();

        // Advance time for table3 hard deletion
        time_provider.set(now + Duration::from_secs(1800)); // 30 minutes later

        // Get updated schema
        let updated_db_schema = catalog.db_schema("test_db").unwrap();

        // Test all three tables
        assert_eq!(
            updated_db_schema.table_deletion_status(table1_id, Arc::clone(&time_provider) as _),
            None
        ); // Not deleted
        assert_eq!(
            updated_db_schema.table_deletion_status(table2_id, Arc::clone(&time_provider) as _),
            Some(DeletionStatus::Soft)
        ); // Soft deleted
        match updated_db_schema.table_deletion_status(table3_id, Arc::clone(&time_provider) as _) {
            Some(DeletionStatus::Hard(duration)) => {
                // Should be around 30 minutes
                assert!(duration >= Duration::from_secs(1799));
                assert!(duration <= Duration::from_secs(1801));
            }
            other => panic!("Expected Hard deletion status for table3, got {other:?}"),
        }
    }

    // Tests for idempotent default hard deletion behavior

    #[test_log::test(tokio::test)]
    async fn test_database_soft_delete_default_preserves_existing_hard_delete_time() {
        // Test that soft deleting a database with Default preserves existing hard_delete_time
        use iox_time::MockProvider;
        let now = Time::from_timestamp_nanos(1000000000);
        let time_provider = Arc::new(MockProvider::new(now));
        let catalog = Catalog::new_in_memory_with_args(
            "test-catalog",
            Arc::clone(&time_provider) as _,
            CatalogArgs::default(),
        )
        .await
        .unwrap();

        catalog.create_database("test_db").await.unwrap();

        // Get database ID before soft delete
        let db_id = catalog.db_name_to_id("test_db").unwrap();

        // First soft delete with a specific timestamp
        let specific_time = Time::from_timestamp_nanos(5000000000);
        catalog
            .soft_delete_database("test_db", HardDeletionTime::Timestamp(specific_time))
            .await
            .unwrap();

        // Verify the database is soft deleted with the specific hard_delete_time
        let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
        assert!(db_schema.deleted);
        assert_eq!(db_schema.hard_delete_time, Some(specific_time));

        // Get the renamed database name using the ID
        let renamed_db_name = catalog
            .db_schema_by_id(&db_id)
            .expect("soft-deleted database should exist")
            .name();

        // Now soft delete again with Default using the renamed name
        // This should return AlreadyDeleted since nothing changes
        let result = catalog
            .soft_delete_database(&renamed_db_name, HardDeletionTime::Default)
            .await;

        // Should get AlreadyDeleted error since hard_delete_time doesn't change
        assert!(
            matches!(result, Err(CatalogError::AlreadyDeleted)),
            "Expected AlreadyDeleted error, got {result:?}"
        );

        // Verify hard_delete_time is unchanged
        let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
        assert!(db_schema.deleted);
        assert_eq!(db_schema.hard_delete_time, Some(specific_time));
    }

    #[test_log::test(tokio::test)]
    async fn test_database_soft_delete_default_sets_new_when_none_exists() {
        // Test that soft deleting a database with Default sets new hard_delete_time when none exists
        use iox_time::MockProvider;
        let now = Time::from_timestamp_nanos(1000000000);
        let time_provider = Arc::new(MockProvider::new(now));
        let catalog = Catalog::new_in_memory_with_args(
            "test-catalog",
            Arc::clone(&time_provider) as _,
            CatalogArgs::default(),
        )
        .await
        .unwrap();

        catalog.create_database("test_db").await.unwrap();

        // Get database ID before soft delete
        let db_id = catalog.db_name_to_id("test_db").unwrap();

        // Soft delete with Default - should set new hard_delete_time
        catalog
            .soft_delete_database("test_db", HardDeletionTime::Default)
            .await
            .unwrap();

        // Verify hard_delete_time is set to now + default duration
        let expected_time = now + Catalog::DEFAULT_HARD_DELETE_DURATION;
        let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
        assert!(db_schema.deleted);
        assert_eq!(db_schema.hard_delete_time, Some(expected_time));
    }

    #[test_log::test(tokio::test)]
    async fn test_database_soft_delete_default_multiple_calls_idempotent() {
        // Test that multiple soft delete calls with Default are idempotent
        use iox_time::MockProvider;
        let now = Time::from_timestamp_nanos(1000000000);
        let time_provider = Arc::new(MockProvider::new(now));
        let catalog = Catalog::new_in_memory_with_args(
            "test-catalog",
            Arc::clone(&time_provider) as _,
            CatalogArgs::default(),
        )
        .await
        .unwrap();

        catalog.create_database("test_db").await.unwrap();

        // Get database ID before soft delete
        let db_id = catalog.db_name_to_id("test_db").unwrap();

        // First soft delete with a specific timestamp
        let specific_time = Time::from_timestamp_nanos(5000000000);
        catalog
            .soft_delete_database("test_db", HardDeletionTime::Timestamp(specific_time))
            .await
            .unwrap();

        // Verify initial state
        let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
        assert!(db_schema.deleted);
        assert_eq!(db_schema.hard_delete_time, Some(specific_time));

        // Get the renamed database name using the ID
        let renamed_db_name = catalog
            .db_schema_by_id(&db_id)
            .expect("soft-deleted database should exist")
            .name();

        // Call soft delete with Default multiple times - all should be idempotent
        for i in 1..=3 {
            let result = catalog
                .soft_delete_database(&renamed_db_name, HardDeletionTime::Default)
                .await;

            // Should always get AlreadyDeleted since nothing changes
            assert!(
                matches!(result, Err(CatalogError::AlreadyDeleted)),
                "Call {i} expected AlreadyDeleted error, got {result:?}"
            );

            // Verify hard_delete_time remains unchanged
            let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
            assert!(db_schema.deleted);
            assert_eq!(
                db_schema.hard_delete_time,
                Some(specific_time),
                "hard_delete_time should remain unchanged after call {}",
                i
            );
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_database_soft_delete_override_existing_with_specific_time() {
        // Test that soft deleting with specific time overrides existing hard_delete_time
        use iox_time::MockProvider;
        let now = Time::from_timestamp_nanos(1000000000);
        let time_provider = Arc::new(MockProvider::new(now));
        let catalog = Catalog::new_in_memory_with_args(
            "test-catalog",
            Arc::clone(&time_provider) as _,
            CatalogArgs::default(),
        )
        .await
        .unwrap();

        catalog.create_database("test_db").await.unwrap();

        // Get database ID before soft delete
        let db_id = catalog.db_name_to_id("test_db").unwrap();

        // First soft delete with Default
        catalog
            .soft_delete_database("test_db", HardDeletionTime::Default)
            .await
            .unwrap();

        // Verify initial state with default hard_delete_time
        let expected_default_time = now + Catalog::DEFAULT_HARD_DELETE_DURATION;
        let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
        assert!(db_schema.deleted);
        assert_eq!(db_schema.hard_delete_time, Some(expected_default_time));

        // Get the renamed database name using the ID
        let renamed_db_name = catalog
            .db_schema_by_id(&db_id)
            .expect("soft-deleted database should exist")
            .name();

        // Now soft delete again with a specific timestamp - should update the hard_delete_time
        let new_specific_time = Time::from_timestamp_nanos(7000000000);
        catalog
            .soft_delete_database(
                &renamed_db_name,
                HardDeletionTime::Timestamp(new_specific_time),
            )
            .await
            .unwrap();

        // Verify hard_delete_time was updated to the new specific time
        let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
        assert!(db_schema.deleted);
        assert_eq!(db_schema.hard_delete_time, Some(new_specific_time));
    }

    #[test_log::test(tokio::test)]
    async fn test_table_soft_delete_default_preserves_existing_hard_delete_time() {
        // Test that soft deleting a table with Default preserves existing hard_delete_time
        use iox_time::MockProvider;
        let now = Time::from_timestamp_nanos(1000000000);
        let time_provider = Arc::new(MockProvider::new(now));
        let catalog = Catalog::new_in_memory_with_args(
            "test-catalog",
            Arc::clone(&time_provider) as _,
            CatalogArgs::default(),
        )
        .await
        .unwrap();

        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test_table",
                &["tag1", "tag2"],
                &[("field1", FieldDataType::String)],
            )
            .await
            .unwrap();

        // Get the table ID before soft delete
        let db_schema = catalog.db_schema("test_db").unwrap();
        let table_id = db_schema.table_name_to_id("test_table").unwrap();

        // First soft delete with a specific timestamp
        let specific_time = Time::from_timestamp_nanos(5000000000);
        catalog
            .soft_delete_table(
                "test_db",
                "test_table",
                HardDeletionTime::Timestamp(specific_time),
            )
            .await
            .unwrap();

        // Get the renamed table using the table ID
        let db_schema = catalog.db_schema("test_db").unwrap();
        let table_def = db_schema
            .table_definition_by_id(&table_id)
            .expect("soft-deleted table should exist");
        let renamed_table_name = Arc::<str>::clone(&table_def.table_name);

        // Verify the table is soft deleted with the specific hard_delete_time
        assert!(table_def.deleted);
        assert_eq!(table_def.hard_delete_time, Some(specific_time));

        // Now soft delete again with Default using the renamed name
        // This should return AlreadyDeleted since nothing changes
        let result = catalog
            .soft_delete_table("test_db", &renamed_table_name, HardDeletionTime::Default)
            .await;

        // Should get AlreadyDeleted error since hard_delete_time doesn't change
        assert!(
            matches!(result, Err(CatalogError::AlreadyDeleted)),
            "Expected AlreadyDeleted error, got {result:?}"
        );

        // Verify hard_delete_time is unchanged
        let db_schema = catalog.db_schema("test_db").unwrap();
        let table_def = db_schema.table_definition(&renamed_table_name).unwrap();
        assert!(table_def.deleted);
        assert_eq!(table_def.hard_delete_time, Some(specific_time));
    }

    #[test_log::test(tokio::test)]
    async fn test_table_soft_delete_default_sets_new_when_none_exists() {
        // Test that soft deleting a table with Default sets new hard_delete_time when none exists
        use iox_time::MockProvider;
        let now = Time::from_timestamp_nanos(1000000000);
        let time_provider = Arc::new(MockProvider::new(now));
        let catalog = Catalog::new_in_memory_with_args(
            "test-catalog",
            Arc::clone(&time_provider) as _,
            CatalogArgs::default(),
        )
        .await
        .unwrap();

        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test_table",
                &["tag1"],
                &[("field1", FieldDataType::Float)],
            )
            .await
            .unwrap();

        // Get the table ID before soft delete
        let db_schema = catalog.db_schema("test_db").unwrap();
        let table_id = db_schema.table_name_to_id("test_table").unwrap();

        // Soft delete with Default - should set new hard_delete_time
        catalog
            .soft_delete_table("test_db", "test_table", HardDeletionTime::Default)
            .await
            .unwrap();

        // Get the table using the table ID
        let db_schema = catalog.db_schema("test_db").unwrap();
        let table_def = db_schema
            .table_definition_by_id(&table_id)
            .expect("soft-deleted table should exist");

        // Verify hard_delete_time is set to now + default duration
        let expected_time = now + Catalog::DEFAULT_HARD_DELETE_DURATION;
        assert!(table_def.deleted);
        assert_eq!(table_def.hard_delete_time, Some(expected_time));
    }

    #[test_log::test(tokio::test)]
    async fn test_table_soft_delete_default_multiple_calls_idempotent() {
        // Test that multiple soft delete calls with Default are idempotent
        use iox_time::MockProvider;
        let now = Time::from_timestamp_nanos(1000000000);
        let time_provider = Arc::new(MockProvider::new(now));
        let catalog = Catalog::new_in_memory_with_args(
            "test-catalog",
            Arc::clone(&time_provider) as _,
            CatalogArgs::default(),
        )
        .await
        .unwrap();

        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test_table",
                &["tag1", "tag2"],
                &[("field1", FieldDataType::Integer)],
            )
            .await
            .unwrap();

        // Get the table ID before soft delete
        let db_schema = catalog.db_schema("test_db").unwrap();
        let table_id = db_schema.table_name_to_id("test_table").unwrap();

        // First soft delete with a specific timestamp
        let specific_time = Time::from_timestamp_nanos(5000000000);
        catalog
            .soft_delete_table(
                "test_db",
                "test_table",
                HardDeletionTime::Timestamp(specific_time),
            )
            .await
            .unwrap();

        // Get the renamed table name using the table ID
        let db_schema = catalog.db_schema("test_db").unwrap();
        let table_def = db_schema
            .table_definition_by_id(&table_id)
            .expect("soft-deleted table should exist");
        let renamed_table_name = Arc::<str>::clone(&table_def.table_name);

        // Call soft delete with Default multiple times - all should be idempotent
        for i in 1..=3 {
            let result = catalog
                .soft_delete_table("test_db", &renamed_table_name, HardDeletionTime::Default)
                .await;

            // Should always get AlreadyDeleted since nothing changes
            assert!(
                matches!(result, Err(CatalogError::AlreadyDeleted)),
                "Call {i} expected AlreadyDeleted error, got {result:?}"
            );

            // Verify hard_delete_time remains unchanged
            let db_schema = catalog.db_schema("test_db").unwrap();
            let table_def = db_schema.table_definition_by_id(&table_id).unwrap();
            assert!(table_def.deleted);
            assert_eq!(
                table_def.hard_delete_time,
                Some(specific_time),
                "hard_delete_time should remain unchanged after call {}",
                i
            );
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_table_soft_delete_override_existing_with_specific_time() {
        // Test that soft deleting with specific time overrides existing hard_delete_time
        use iox_time::MockProvider;
        let now = Time::from_timestamp_nanos(1000000000);
        let time_provider = Arc::new(MockProvider::new(now));
        let catalog = Catalog::new_in_memory_with_args(
            "test-catalog",
            Arc::clone(&time_provider) as _,
            CatalogArgs::default(),
        )
        .await
        .unwrap();

        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test_table",
                &["tag1"],
                &[("field1", FieldDataType::UInteger)],
            )
            .await
            .unwrap();

        // Get the table ID before soft delete
        let db_schema = catalog.db_schema("test_db").unwrap();
        let table_id = db_schema.table_name_to_id("test_table").unwrap();

        // First soft delete with Default
        catalog
            .soft_delete_table("test_db", "test_table", HardDeletionTime::Default)
            .await
            .unwrap();

        // Get the renamed table and verify initial state
        let db_schema = catalog.db_schema("test_db").unwrap();
        let table_def = db_schema
            .table_definition_by_id(&table_id)
            .expect("soft-deleted table should exist");
        let renamed_table_name = Arc::<str>::clone(&table_def.table_name);

        // Verify initial state with default hard_delete_time
        let expected_default_time = now + Catalog::DEFAULT_HARD_DELETE_DURATION;
        assert!(table_def.deleted);
        assert_eq!(table_def.hard_delete_time, Some(expected_default_time));

        // Now soft delete again with a specific timestamp - should update the hard_delete_time
        let new_specific_time = Time::from_timestamp_nanos(7000000000);
        catalog
            .soft_delete_table(
                "test_db",
                &renamed_table_name,
                HardDeletionTime::Timestamp(new_specific_time),
            )
            .await
            .unwrap();

        // Verify hard_delete_time was updated to the new specific time
        let db_schema = catalog.db_schema("test_db").unwrap();
        let table_def = db_schema.table_definition_by_id(&table_id).unwrap();
        assert!(table_def.deleted);
        assert_eq!(table_def.hard_delete_time, Some(new_specific_time));
    }
}
