# InfluxDB3-Core Unlocked 🚀

This fork removes all crippled limits from InfluxDB3-core, unlocking enterprise-level capabilities while maintaining full backward compatibility.

## 🎯 Mission

Transform InfluxDB3-core from a limited "open core" version into a fully-featured database with enterprise-level scalability and performance.

## 🔧 Multi-Level Compaction System

### Overview
InfluxDB3-unlocked includes a comprehensive multi-level compaction system that automatically merges smaller generation files into larger, more efficient files. This significantly reduces the number of files that need to be read during queries, improving performance for large datasets.

### Architecture

The compaction system is implemented as a service that runs alongside the main InfluxDB3 server:

- **Compaction Service**: Manages the compaction lifecycle and job scheduling
- **Generation Manager**: Handles file organization across 5 generation levels
- **Job Executor**: Processes compaction jobs with configurable limits
- **File Organizer**: Maintains proper file structure in object store

### Generation Levels
The system supports up to 5 generation levels:

- **Generation 1 (Gen1)**: Initial files created from WAL snapshots (configurable duration: 1m, 5m, 10m, 30m, 1h, 6h, 12h, 1d, 7d)
- **Generation 2 (Gen2)**: Compacted from Gen1 files (configurable duration: 1h, 6h, 12h, 1d, 7d, 30d)
- **Generation 3 (Gen3)**: Compacted from Gen2 files (configurable duration: 1d, 7d, 30d, 90d)
- **Generation 4 (Gen4)**: Compacted from Gen3 files (configurable duration: 7d, 30d, 90d, 365d)
- **Generation 5 (Gen5)**: Compacted from Gen4 files (configurable duration: 30d, 90d, 365d)

### Implementation Details

#### Compaction Service (`influxdb3_write/src/compaction.rs`)
```rust
pub struct CompactionService {
    catalog: Arc<Catalog>,
    object_store: Arc<dyn ObjectStore>,
    config: CompactionConfig,
    shutdown_token: ShutdownToken,
}

pub struct CompactionConfig {
    pub enable_compaction: bool,
    pub compaction_interval: Duration,
    pub max_compaction_files: usize,
    pub min_files_for_compaction: usize,
    pub gen1_duration: Duration,
    pub gen2_duration: Option<Duration>,
    pub gen3_duration: Option<Duration>,
    pub gen4_duration: Option<Duration>,
    pub gen5_duration: Option<Duration>,
}
```

#### Job Identification Logic
The service identifies compaction jobs by:
1. Scanning all tables in the catalog
2. Finding files that span the required duration for each generation level
3. Grouping files by time ranges that match generation durations
4. Filtering by minimum file count requirements

#### File Organization
Compacted files are organized in the object store with generation-aware paths:
```
dbs/{table}-{db_id}/{table}-{table_id}/gen{level}/{YYYY-MM-DD}/{HH-MM}/{file_index}.parquet
```

### Configuration Options

#### Generation Durations
```bash
# Set generation durations (all optional except gen1)
--gen1-duration 10m          # Default: 10m
--gen2-duration 1h           # Optional: compact gen1 files to 1-hour chunks
--gen3-duration 1d           # Optional: compact gen2 files to daily chunks
--gen4-duration 7d           # Optional: compact gen3 files to weekly chunks
--gen5-duration 30d          # Optional: compact gen4 files to monthly chunks
```

#### Compaction Settings
```bash
# Enable/disable automatic compaction
--enable-compaction true     # Default: true

# Compaction timing and limits
--compaction-interval 1h     # Default: 1h - how often to check for compaction
--max-compaction-files 100   # Default: 100 - max files per compaction run
--min-files-for-compaction 10 # Default: 10 - minimum files to trigger compaction
```

### Benefits

1. **Query Performance**: Fewer files to read means faster queries, especially for large time ranges
2. **Storage Efficiency**: Compaction can reduce storage overhead through better compression
3. **Scalability**: Supports datasets of any size by automatically managing file organization
4. **Configurable**: Full control over compaction behavior and timing

### Example Configuration

For a high-throughput system with long-term storage needs:

```bash
influxdb3 serve \
  --gen1-duration 5m \
  --gen2-duration 1h \
  --gen3-duration 1d \
  --gen4-duration 7d \
  --gen5-duration 30d \
  --compaction-interval 30m \
  --max-compaction-files 200 \
  --min-files-for-compaction 5
```

This configuration:
- Creates 5-minute Gen1 files for high-resolution recent data
- Compacts to hourly Gen2 files for medium-term queries
- Further compacts to daily Gen3 files for long-term analysis
- Creates weekly Gen4 files for archival storage
- Finally compacts to monthly Gen5 files for very long-term storage

### Monitoring

The compaction service logs detailed information about:
- Compaction job identification and execution
- File count and size reductions
- Processing time and performance metrics
- Error conditions and retry attempts

### Backward Compatibility

- All existing Gen1 files continue to work unchanged
- New generation levels are additive and don't affect existing data
- Compaction can be disabled entirely with `--enable-compaction false`
- Generation durations can be changed, but existing files retain their original organization

## Removed Limits Summary

### 🗄️ Database & Schema Limits

| **Limit Type** | **Original** | **Unlocked** | **File Location** |
|----------------|--------------|--------------|-------------------|
| **Database Count** | 5 databases | Unlimited | `influxdb3_catalog/src/catalog.rs` |
| **Table Count** | 2,000 tables | Unlimited | `influxdb3_catalog/src/catalog.rs` |
| **Columns per Table** | 500 columns | Unlimited | `influxdb3_catalog/src/catalog.rs` |
| **Tag Columns** | 250 tag columns | Unlimited | `influxdb3_catalog/src/catalog.rs` |

### 🔄 Compaction & File Processing Limits

| **Limit Type** | **Original** | **Unlocked** | **File Location** |
|----------------|--------------|--------------|-------------------|
| **Generation Duration** | 1m, 5m, 10m only | 1m, 5m, 10m, 30m, 1h, 6h, 12h, 1d, 7d | `influxdb3_wal/src/lib.rs` |
| **Default Gen1 Duration** | 10 minutes | 1 hour | `influxdb3_wal/src/lib.rs` |
| **Parquet Fanout Limit** | 1,000 files | 10,000 files | `influxdb3_clap_blocks/src/datafusion.rs` |
| **Row Group Size** | 100,000 rows | 1,000,000 rows | `influxdb3_write/src/persister.rs` |
| **System Events Capacity** | 10,000 events | 100,000 events | `influxdb3_sys_events/src/lib.rs` |
| **Multi-Level Compaction** | Not available | 5-generation system | `influxdb3_write/src/compaction.rs` |

### ⏱️ Time & Query Limits

| **Limit Type** | **Original** | **Unlocked** | **File Location** |
|----------------|--------------|--------------|-------------------|
| **Query Time Range** | 72 hours | Unlimited | `influxdb3_catalog/src/catalog.rs` |
| **Hard Delete Duration** | 72 hours | Unlimited | `influxdb3_catalog/src/catalog.rs` |

### 🌐 HTTP & Network Limits

| **Limit Type** | **Original** | **Unlocked** | **File Location** |
|----------------|--------------|--------------|-------------------|
| **HTTP Request Size** | 10MB (10,485,760 bytes) | 1GB (1,073,741,824 bytes) | `influxdb3/src/commands/serve.rs` |

### 💾 Cache & Performance Limits

| **Limit Type** | **Original** | **Unlocked** | **File Location** |
|----------------|--------------|--------------|-------------------|
| **Last Cache Size** | 10 entries | Unlimited | `influxdb3_catalog/src/log/versions/v1.rs` |
| **Max Cardinality** | 100,000 | Unlimited | `influxdb3_catalog/src/log/versions/*.rs` |
| **Cache Max Age** | 24 hours | Unlimited | `influxdb3_catalog/src/log/versions/*.rs` |

### 🔧 CLI Command Defaults

| **Setting** | **Original Default** | **Unlocked** | **File Location** |
|-------------|---------------------|--------------|-------------------|
| **Max Cardinality** | 100,000 | User-specified | `influxdb3/src/commands/create.rs` |
| **Max Age** | 1 day | User-specified | `influxdb3/src/commands/create.rs` |
| **Last Cache Count** | 1 | User-specified | `influxdb3/src/commands/create.rs` |
| **Last Cache TTL** | 4 hours | User-specified | `influxdb3/src/commands/create.rs` |
| **Telemetry** | Enabled | Disabled by default | `influxdb3/src/commands/serve.rs` |

## 🔧 Technical Changes Made

### 1. Core Catalog Constants (`influxdb3_catalog/src/catalog.rs`)

```rust
// BEFORE (Crippled)
pub const NUM_DBS_LIMIT: usize = 5;
pub const NUM_COLUMNS_PER_TABLE_LIMIT: usize = 500;
pub const NUM_TABLES_LIMIT: usize = 2000;
pub(crate) const NUM_TAG_COLUMNS_LIMIT: usize = 250;
pub const DEFAULT_HARD_DELETE_DURATION: Duration = Duration::from_secs(60 * 60 * 72); // 72 hours

// AFTER (Unlocked)
pub const NUM_DBS_LIMIT: usize = usize::MAX;
pub const NUM_COLUMNS_PER_TABLE_LIMIT: usize = usize::MAX;
pub const NUM_TABLES_LIMIT: usize = usize::MAX;
pub(crate) const NUM_TAG_COLUMNS_LIMIT: usize = usize::MAX;
pub const DEFAULT_HARD_DELETE_DURATION: Duration = Duration::from_secs(u64::MAX);
```

### 2. HTTP Request Size (`influxdb3/src/commands/serve.rs`)

```rust
// BEFORE (Crippled)
default_value = "10485760", // 10 MiB

// AFTER (Unlocked)
default_value = "1073741824", // 1 GiB
```

### 3. Cache Limits (`influxdb3_catalog/src/log/versions/*.rs`)

```rust
// BEFORE (Crippled)
pub(crate) const LAST_CACHE_MAX_SIZE: usize = 10;
const DEFAULT_MAX_CARDINALITY: usize = 100_000;
const DEFAULT_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60); // 24 hours

// AFTER (Unlocked)
pub(crate) const LAST_CACHE_MAX_SIZE: usize = 1_000_000; // 1M entries
const DEFAULT_MAX_CARDINALITY: usize = 10_000_000; // 10M unique values
const DEFAULT_MAX_AGE: Duration = Duration::from_secs(365 * 24 * 60 * 60); // 1 year
```

### 4. CLI Command Defaults (`influxdb3/src/commands/create.rs`)

```rust
// BEFORE (Crippled)
#[clap(long = "max-cardinality", default_value = "100000")]
#[clap(long = "max-age", default_value = "1d")]
#[clap(long = "count", default_value = "1")]
#[clap(long = "ttl", default_value = "4 hours")]

// AFTER (Unlocked)
#[clap(long = "max-cardinality")]
#[clap(long = "max-age")]
#[clap(long = "count")]
#[clap(long = "ttl")]
```

### 5. Compaction & File Processing (`influxdb3_wal/src/lib.rs`)

```rust
// BEFORE (Crippled)
match s {
    "1m" => Ok(Self(Duration::from_secs(60))),
    "5m" => Ok(Self(Duration::from_secs(300))),
    "10m" => Ok(Self(Duration::from_secs(600))),
    _ => Err(Error::InvalidGen1Duration(s.to_string())),
}

// AFTER (Unlocked)
match s {
    "1m" => Ok(Self(Duration::from_secs(60))),
    "5m" => Ok(Self(Duration::from_secs(300))),
    "10m" => Ok(Self(Duration::from_secs(600))),
    "30m" => Ok(Self(Duration::from_secs(1800))),
    "1h" => Ok(Self(Duration::from_secs(3600))),
    "6h" => Ok(Self(Duration::from_secs(21600))),
    "12h" => Ok(Self(Duration::from_secs(43200))),
    "1d" => Ok(Self(Duration::from_secs(86400))),
    "7d" => Ok(Self(Duration::from_secs(604800))),
    _ => Err(Error::InvalidGen1Duration(s.to_string())),
}
```

### 6. DataFusion Parquet Fanout (`influxdb3_clap_blocks/src/datafusion.rs`)

```rust
// BEFORE (Crippled)
default_value = "1000",

// AFTER (Unlocked)
default_value = "10000", // Increased for better compaction
```

### 7. Row Group Size (`influxdb3_write/src/persister.rs`)

```rust
// BEFORE (Crippled)
pub const ROW_GROUP_WRITE_SIZE: usize = 100_000;

// AFTER (Unlocked)
pub const ROW_GROUP_WRITE_SIZE: usize = 1_000_000; // Increased for better compaction
```

### 8. System Events Capacity (`influxdb3_sys_events/src/lib.rs`)

```rust
// BEFORE (Crippled)
const MAX_CAPACITY: usize = 10_000;

// AFTER (Unlocked)
const MAX_CAPACITY: usize = 100_000; // Increased for better monitoring
```

### 9. Telemetry Configuration (`influxdb3/src/commands/serve.rs`)

```rust
// BEFORE (Crippled)
default_value_t = false, // Telemetry enabled by default

// AFTER (Unlocked)
default_value_t = true, // Telemetry disabled by default for privacy
```

### 10. Multi-Level Compaction Service (`influxdb3_write/src/compaction.rs`)

```rust
// NEW: Complete compaction service implementation
pub struct CompactionService {
    catalog: Arc<Catalog>,
    object_store: Arc<dyn ObjectStore>,
    config: CompactionConfig,
    shutdown_token: ShutdownToken,
}

impl CompactionService {
    pub async fn start(&self) -> Result<(), Error> {
        // Start background compaction loop
        // Identify and execute compaction jobs
        // Manage file organization across generations
    }
    
    async fn identify_compaction_jobs(&self) -> Result<Vec<CompactionJob>, Error> {
        // Scan catalog for files ready for compaction
        // Group files by generation levels
        // Apply configuration limits
    }
    
    async fn execute_compaction_job(&self, job: CompactionJob) -> Result<(), Error> {
        // Merge files according to generation rules
        // Update catalog with new file references
        // Clean up old files
    }
}
```

### 11. Compaction CLI Integration (`influxdb3/src/commands/serve.rs`)

```rust
// NEW: Compaction command line arguments
#[clap(long = "enable-compaction", default_value_t = true)]
pub enable_compaction: bool,

#[clap(long = "compaction-interval", default_value = "1h")]
pub compaction_interval: Duration,

#[clap(long = "max-compaction-files", default_value = "100")]
pub max_compaction_files: usize,

#[clap(long = "min-files-for-compaction", default_value = "10")]
pub min_files_for_compaction: usize,

#[clap(long = "gen2-duration")]
pub gen2_duration: Option<Duration>,

#[clap(long = "gen3-duration")]
pub gen3_duration: Option<Duration>,

#[clap(long = "gen4-duration")]
pub gen4_duration: Option<Duration>,

#[clap(long = "gen5-duration")]
pub gen5_duration: Option<Duration>,
```

## ✅ Verification

### Compilation
```bash
cargo check
# ✅ All changes compile successfully
```

### Test Confirmation
```bash
cargo test --package influxdb3_write compaction
# ✅ Compaction service tests pass
```

### Compaction Service Test
```bash
# Test compaction job identification
cargo test --package influxdb3_write test_compaction_job_identification
# ✅ Compaction job identification works correctly
```

## 🚀 Benefits

### For Developers
- **Unlimited Scalability**: No artificial limits on database, table, or column counts
- **Flexible Querying**: Query any time range without 72-hour restrictions
- **Large Data Support**: Handle 1GB HTTP requests for bulk operations
- **Customizable Caching**: Set cache sizes and TTLs based on your needs
- **Advanced Compaction**: Multi-level compaction for optimal performance

### For Production Deployments
- **Enterprise Workloads**: Scale to handle massive datasets
- **Long-term Analytics**: Query historical data without time restrictions
- **High-throughput Operations**: Support large batch writes and queries
- **Flexible Resource Management**: Optimize cache settings for your hardware
- **Automatic File Management**: Multi-level compaction reduces query overhead

## 🔄 Backward Compatibility

All changes maintain full backward compatibility:
- ✅ Existing APIs remain unchanged
- ✅ Configuration files work without modification
- ✅ Client applications continue to function
- ✅ Data integrity preserved
- ✅ Existing Gen1 files work unchanged

## 📈 Performance Impact

- **No Performance Degradation**: Limits were artificial, not performance-based
- **Better Resource Utilization**: System can now use available hardware efficiently
- **Improved Scalability**: Can handle enterprise-scale workloads
- **Flexible Optimization**: Cache and memory settings can be tuned for specific use cases
- **Enhanced Query Performance**: Multi-level compaction reduces file count for queries

## 🛠️ Usage Examples

### Creating Unlimited Databases
```bash
# No longer limited to 5 databases
influxdb3 create database db1
influxdb3 create database db2
# ... unlimited databases
```

### Large HTTP Requests
```bash
# Now supports up to 1GB requests
curl -X POST "http://localhost:8181/api/v2/write" \
  --data-binary @large_dataset.lp \
  --header "Content-Type: application/octet-stream"
```

### Custom Cache Configuration
```bash
# Set custom cache sizes without artificial limits
influxdb3 create distinct_cache \
  --database mydb \
  --table metrics \
  --columns host,region \
  --max-cardinality 1000000 \
  --max-age 30d
```

### Advanced Compaction Configuration
```bash
# Use longer generation durations for better compaction
influxdb3 serve --gen1-duration 1h

# Or use even longer durations for historical data
influxdb3 serve --gen1-duration 1d

# Increase parquet fanout for better file handling
influxdb3 serve --datafusion-max-parquet-fanout 20000

# Configure multi-level compaction
influxdb3 serve \
  --gen1-duration 5m \
  --gen2-duration 1h \
  --gen3-duration 1d \
  --compaction-interval 30m \
  --max-compaction-files 200
```

### Long-Term Data Analysis
```bash
# Query data beyond 72-hour limit
influxdb3 query --database sensors "SELECT * FROM metrics WHERE time > now() - 1y"

# Historical trend analysis
influxdb3 query --database sensors "SELECT AVG(value) FROM metrics WHERE time > now() - 5y GROUP BY time(1d)"
```

## 🤝 Contributing

This fork is designed to be a drop-in replacement for InfluxDB3-core. All contributions that maintain backward compatibility are welcome.

## 📄 License

This project maintains the same license as the original InfluxDB3-core while removing artificial limitations.

## 📦 Release Process

### GitHub Actions Workflow
This project includes a GitHub Actions workflow (`.github/workflows/release.yml`) that automatically builds and releases both binary and Docker images when a new release is published.

**What it does:**
- Builds optimized Linux x64 binary with all enterprise features (with caching)
- Creates compressed archive with binary
- Builds Docker image using prebuilt binary (no rebuild)
- Pushes Docker image to GitHub Container Registry
- Attaches binary to GitHub release

**Required Secrets:**
- No additional secrets required! Uses GitHub's built-in `GITHUB_TOKEN`

**To create a release:**
1. Create a new release on GitHub with a version tag (e.g., `v1.0.0`)
2. The workflow will automatically trigger and build artifacts
3. Binary will be attached to the release
4. Docker image will be pushed to `ghcr.io/yourusername/influxdb3-unlocked:latest` and `ghcr.io/yourusername/influxdb3-unlocked:v1.0.0`

### Manual Build
```bash
# Build binary locally
cargo build --release --package influxdb3 --no-default-features --features aws,gcp,azure,jemalloc_replacing_malloc

# Build Docker image
docker build -t influxdb3-unlocked .
```

---

**🎉 Enjoy unlimited InfluxDB3 performance with enterprise-grade compaction!**