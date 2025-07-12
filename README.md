# InfluxDB3 Unlocked 🚀

> **InfluxDB3 Core without artificial limitations**

This fork removes the crippled limits from InfluxDB3-core (72h query restrictions, low retention limits, database/table caps), unlocking enterprise-grade capabilities while maintaining full backward compatibility.

## ✨ What's Unlocked

### 🗄️ **Database & Storage**
- **Unlimited databases** - Scale to any number of databases
- **Unlimited tables** - No table count restrictions
- **Unlimited columns** - No per-table column limits
- **Unlimited tag columns** - Full tag flexibility

### ⏱️ **Time & Query Performance**
- **Unlimited query time ranges** - Query any historical data
- **Unlimited retention periods** - Keep data as long as needed
- **1GB HTTP request size** - Handle large batch operations

### 🔧 **Advanced Compaction**
- **Extended generation durations**: 1m, 5m, 10m, 30m, 1h, 6h, 12h, 1d, 7d
- **Multi-level compaction** - Optimize for different time scales
- **Enhanced parquet fanout**: 10,000 files
- **Larger row groups**: 1M rows for better compression
- **Increased system capacity**: 100K events

### 💾 **Flexible Caching**
- **Large last cache**: 1M entries
- **High cardinality support**: 10M unique values
- **Configurable TTL** - Set cache retention as needed

### 🔒 **Privacy & Control**
- **Telemetry disabled by default** - No data collection unless explicitly enabled
- **User-controlled settings** - All limits configurable via CLI or environment

## 🚀 Quick Start

### Docker (Recommended)

```bash
# Pull the image
docker pull ghcr.io/metrico/influxdb3-unlocked:latest

# Run with default settings (all limits removed)
docker run -p 8181:8181 ghcr.io/metrico/influxdb3-unlocked:latest serve

# Run with custom data directory
docker run -p 8181:8181 \
  -v /data:/var/lib/influxdb3 \
  ghcr.io/metrico/influxdb3-unlocked:latest serve

# Run with custom configuration
docker run -p 8181:8181 \
  -e INFLUXDB3_DATA_DIR=/var/lib/influxdb3 \
  -e INFLUXDB3_BIND_ADDR=0.0.0.0:8181 \
  -e INFLUXDB3_LOG_FILTER=info \
  ghcr.io/metrico/influxdb3-unlocked:latest serve
```

### Binary Download

Download the latest release from [GitHub Releases](https://github.com/metrico/influxdb3-unlocked/releases) or build from source:

```bash
# Extract and run
tar -xzf influxdb3-unlocked-*.tar.gz
./influxdb3 serve
```

## ⚙️ Configuration

### Environment Variables

```bash
# Core settings
INFLUXDB3_DATA_DIR=/var/lib/influxdb3
INFLUXDB3_BIND_ADDR=0.0.0.0:8181
INFLUXDB3_LOG_FILTER=info

# Advanced compaction (unlocked durations)
INFLUXDB3_GENERATION_DURATION=1h
INFLUXDB3_COMPACTION_LEVELS=1h,6h,1d,7d

# Performance tuning
INFLUXDB3_DATAFUSION_MAX_PARQUET_FANOUT=10000
INFLUXDB3_ROW_GROUP_WRITE_SIZE=1000000
```

### Command Line Examples

```bash
# Start with custom generation duration
influxdb3 serve --generation-duration 1h

# Start with multiple compaction levels
influxdb3 serve \
  --generation-duration 1h \
  --compaction-levels 1h,6h,1d,7d

# Create unlimited databases
influxdb3 create database db1
influxdb3 create database db2
# ... unlimited databases

# Create tables with unlimited columns
influxdb3 create table db1.metrics \
  --columns timestamp,value,host,region,service,version,environment
# ... unlimited columns
```

## 🔧 Key Settings

### **Generation Duration** (WAL Flush Frequency)
Controls how often data is flushed from memory to disk as Parquet files.

```bash
# Default: 10 minutes (frequent flushing)
influxdb3 serve --generation-duration 10m

# For better performance: 1 hour
influxdb3 serve --generation-duration 1h

# For historical data: 1 day
influxdb3 serve --generation-duration 1d

# Available options: 1m, 5m, 10m, 30m, 1h, 6h, 12h, 1d, 7d
```

### **Compaction Levels** (Multi-level Compaction)
Defines how data is compacted across multiple time periods for better query performance.

```bash
# Default: Single level (10m)
influxdb3 serve --compaction-levels 10m

# Recommended: Multi-level compaction
influxdb3 serve --compaction-levels 1h,6h,1d,7d

# Aggressive: Longer periods for historical data
influxdb3 serve --compaction-levels 6h,1d,7d,30d
```

### **Cache Configuration**
Configure distinct caches for improved query performance.

```bash
# Create cache with custom settings
influxdb3 create distinct_cache \
  --database mydb \
  --table metrics \
  --columns host,region \
  --max-cardinality 1000000 \
  --max-age 24h

# Available max-age formats: 1h, 24h, 7d, 30d, etc.
```

### **Performance Tuning**

```bash
# Increase parquet fanout for better file handling
influxdb3 serve --datafusion-max-parquet-fanout 20000

# Increase row group size for better compression
influxdb3 serve --row-group-write-size 2000000

# Set custom HTTP request size limit (default: 1GB)
influxdb3 serve --max-http-request-size 2gb
```

### **Telemetry Configuration**

```bash
# Telemetry is disabled by default for privacy
# To enable telemetry (sends data to InfluxData):
influxdb3 serve --disable-telemetry-upload=false

# To use a custom telemetry endpoint:
influxdb3 serve --telemetry-endpoint https://your-telemetry-server.com

# Environment variable equivalent:
export INFLUXDB3_TELEMETRY_DISABLE_UPLOAD=false
```

### **Docker Environment Variables**

```bash
# Run with custom settings
docker run -p 8181:8181 \
  -e INFLUXDB3_GENERATION_DURATION=1h \
  -e INFLUXDB3_COMPACTION_LEVELS=1h,6h,1d,7d \
  -e INFLUXDB3_DATAFUSION_MAX_PARQUET_FANOUT=15000 \
  -e INFLUXDB3_LOG_FILTER=debug \
  ghcr.io/metrico/influxdb3-unlocked:latest serve
```

## 🔄 Backward Compatibility

✅ **100% Compatible** with existing InfluxDB3 applications:
- All APIs remain unchanged
- Configuration files work without modification
- Client applications continue to function
- Data integrity preserved

## 📊 Performance Impact

- **No performance degradation** - limits were artificial, not performance-based
- **Better resource utilization** - system can use available hardware efficiently
- **Improved scalability** - handles enterprise-scale workloads
- **Flexible optimization** - cache and memory settings can be tuned

## 🏗️ Building from Source

```bash
# Clone the repository
git clone https://github.com/metrico/influxdb3-unlocked.git
cd influxdb3-unlocked

# Build with all enterprise features
cargo build --release --package influxdb3 \
  --no-default-features \
  --features aws,gcp,azure,jemalloc_replacing_malloc

# Run the built binary
./target/release/influxdb3 serve
```

## 🔧 Development

### Prerequisites
- Rust 1.88+
- Python 3.x development headers
- Build tools (gcc, make, etc.)

### Testing
```bash
# Run all tests
cargo test

# Run specific test suite
cargo test --package influxdb3
```

## 📦 Releases

This project includes automated GitHub Actions that:
- Build optimized Linux x64 binaries
- Create Docker images
- Push to GitHub Container Registry
- Create GitHub releases with binaries

### Docker Images
- `ghcr.io/metrico/influxdb3-unlocked:latest`
- `ghcr.io/metrico/influxdb3-unlocked:v3.3.0-nightly` (versioned)

## 🤝 Contributing

This fork is designed to be a drop-in replacement for InfluxDB3-core. All contributions that maintain backward compatibility are welcome.

## 📚 Documentation

For detailed usage, configuration, and API documentation, refer to the [official InfluxDB3 documentation](https://docs.influxdata.com/influxdb/v3/).

## 📄 License

This project maintains full compliance with the original InfluxDB3-core licenses:

- **Apache License 2.0** - See [LICENSE-APACHE](LICENSE-APACHE)
- **MIT License** - See [LICENSE-MIT](LICENSE-MIT)

This fork is fully compliant with all included licenses. 

## 🔗 Links

- **Original Project**: [InfluxDB3-core](https://github.com/influxdata/influxdb3)
- **Documentation**: [UNLOCK.md](UNLOCK.md) - Detailed technical changes
- **Issues**: [GitHub Issues](https://github.com/metrico/influxdb3-unlocked/issues)
- **Releases**: [GitHub Releases](https://github.com/metrico/influxdb3-unlocked/releases)

---

**🎉 Enjoy unlocked InfluxDB3 Core performance!**
