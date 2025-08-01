<img width="300" alt="influx-unlocked" src="https://github.com/user-attachments/assets/cabf6239-2d95-4531-8bf0-054b6a97d5fe" />

# InfluxDB3 Unlocked 🚀

> **InfluxDB3 Core without artificial limitations**

This fork removes the crippled limits from InfluxDB3-core (72h query restrictions, low retention limits, database/table caps), unlocking enterprise-grade capabilities while maintaining full backward compatibility.

## ✨ What's Unlocked

### 🗄️ **Database & Storage**
- **Unlimited databases** - Scale to any number of databases
- **Unlimited tables** - No table count restrictions
- **Unlimited tags/columns** - No per-table column limits

### ⏱️ **Time & Query Performance**
- **Unlimited query time ranges** - Query any historical data
- **Unlimited retention periods** - Keep data as long as needed
- **1GB HTTP request size** - Handle large batch operations

### 🔧 **Advanced Multi-Level Compaction**
- **Extended generation durations**: 1m, 5m, 10m, 30m, 1h, 6h, 12h, 1d, 7d
- **Multi-level compaction system**: Automatic merging of files across 5 generation levels
- **Configurable compaction**: Control timing, file limits, and generation durations
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

### Required Parameters

InfluxDB3 requires two essential parameters to start:

- **`--object-store`**: Storage backend (use `file` for local storage)
- **`--node-id`**: Unique identifier for this instance (use `local1` for single instances)

### Docker (Recommended)

```bash
# Pull the image
docker pull ghcr.io/metrico/influxdb3-unlocked:latest

# Basic run with local storage
docker run -p 8181:8181 \
  -v /data:/var/lib/influxdb3 \
  ghcr.io/metrico/influxdb3-unlocked:latest serve \
  --object-store file \
  --node-id local1

# Run with advanced compaction
docker run -p 8181:8181 \
  -v /data:/var/lib/influxdb3 \
  ghcr.io/metrico/influxdb3-unlocked:latest serve \
  --object-store file \
  --node-id local1 \
  --gen1-duration 5m \
  --gen2-duration 1h \
  --gen3-duration 1d \
  --compaction-interval 30m

# Run with S3/MinIO storage
docker run -p 8181:8181 \
  -e INFLUXDB3_OBJECT_STORE=s3 \
  -e INFLUXDB3_NODE_IDENTIFIER_PREFIX=host01 \
  -e INFLUXDB3_BUCKET=my-influxdb-bucket \
  -e INFLUXDB3_AWS_ACCESS_KEY_ID=your-access-key \
  -e INFLUXDB3_AWS_SECRET_ACCESS_KEY=your-secret-key \
  -e INFLUXDB3_AWS_ENDPOINT=http://minio:9000 \
  -e INFLUXDB3_AWS_ALLOW_HTTP=true \
  ghcr.io/metrico/influxdb3-unlocked:latest serve
```

### Binary Download

```bash
# Download and run
curl -fsSL https://github.com/metrico/influxdb3-unlocked/releases/latest/download/influxdb3
./influxdb3 serve --object-store file --node-id local1

# Run with advanced compaction
./influxdb3 serve \
  --object-store file \
  --node-id local1 \
  --gen1-duration 5m \
  --gen2-duration 1h \
  --gen3-duration 1d \
  --compaction-interval 30m
```

### Health Check

```bash
curl http://127.0.0.1:8181/health
# Expected response: OK
```

## 🔐 Authentication & Token Management

InfluxDB3-unlocked includes a robust authentication system with token-based access control. By default, authentication is **enabled** for all endpoints.

### 🚀 Quick Authentication Setup

#### **1. Start Server with Authentication (Default)**
```bash
# Start with authentication enabled (default behavior)
./influxdb3 serve \
  --object-store file \
  --data-dir /tmp/influxdb3-data \
  --node-id local1
```

#### **2. Create Your First Admin Token**
```bash
# Create the initial admin token
./influxdb3 create token --admin

# Output:
# Created admin token: apiv3_sc5R-bIEVeHjI9x4sOcbFJu9G1Ohjx0skXpEoc_kvMjRF7A06t24KiI3VgHkenn38XivvCy0vIztd0R7nb9GYQ
# 
# Use this token in HTTP requests:
# Authorization: Bearer apiv3_sc5R-bIEVeHjI9x4sOcbFJu9G1Ohjx0skXpEoc_kvMjRF7A06t24KiI3VgHkenn38XivvCy0vIztd0R7nb9GYQ
```

#### **3. Use Token for API Calls**
```bash
# Set token as environment variable
export INFLUXDB3_AUTH_TOKEN="apiv3_sc5R-bIEVeHjI9x4sOcbFJu9G1Ohjx0skXpEoc_kvMjRF7A06t24KiI3VgHkenn38XivvCy0vIztd0R7nb9GYQ"

# Test authentication with a query
./influxdb3 query "SHOW DATABASES"
```

### 🔑 Token Management Commands

#### **Create Admin Tokens**
```bash
# Create default admin token
./influxdb3 create token --admin

# Create named admin token with expiry
./influxdb3 create token --admin --name "production_token" --expiry 30d

# Create short-lived token for testing
./influxdb3 create token --admin --name "test_token" --expiry 1h

# Regenerate admin token (requires confirmation)
./influxdb3 create token --admin --regenerate

# Create scoped tokens with specific permissions
./influxdb3 create token --name "read_only_token" --permissions "db:sensors:read"
./influxdb3 create token --name "write_token" --permissions "db:sensors:write"
./influxdb3 create token --name "full_access_token" --permissions "db:sensors:read" --permissions "db:sensors:write"

# Create tokens with wildcard permissions
./influxdb3 create token --name "all_db_read" --permissions "db:*:read"
./influxdb3 create token --name "token_manager" --permissions "token:*:read" --permissions "token:*:write"

# Create scoped tokens with expiry
./influxdb3 create token --name "temp_token" --permissions "db:temp_db:read" --expiry 24h
```

#### **Token Expiry Options**
```bash
# Available expiry formats
--expiry 30d      # 30 days
--expiry 1w       # 1 week  
--expiry 24h      # 24 hours
--expiry 1h       # 1 hour
--expiry 30m      # 30 minutes
```

### 🔒 Authentication Modes

#### **Full Authentication (Default)**
```bash
# All endpoints require authentication
./influxdb3 serve \
  --object-store file \
  --data-dir /tmp/influxdb3-data \
  --node-id local1

# Use tokens for all API calls
curl -H "Authorization: Bearer YOUR_TOKEN" http://localhost:8181/api/v3/query_sql
```

#### **Selective Authentication**
```bash
# Disable auth for specific endpoints only
./influxdb3 serve \
  --object-store file \
  --data-dir /tmp/influxdb3-data \
  --node-id local1 \
  --disable-authz health,ping,metrics

# Available options: health, ping, metrics
# Other endpoints still require authentication
```

#### **No Authentication (Development)**
```bash
# Disable authentication completely
./influxdb3 serve \
  --object-store file \
  --data-dir /tmp/influxdb3-data \
  --node-id local1 \
  --without-auth

# All API endpoints accessible without tokens
# Note: Token creation endpoints are disabled in this mode
```

### 🌐 HTTP API Authentication

#### **Using Tokens in HTTP Requests**
```bash
# Query with Bearer token
curl -G "http://localhost:8181/api/v3/query_sql" \
  --header 'Authorization: Bearer apiv3_sc5R-bIEVeHjI9x4sOcbFJu9G1Ohjx0skXpEoc_kvMjRF7A06t24KiI3VgHkenn38XivvCy0vIztd0R7nb9GYQ' \
  --data-urlencode "db=sensors" \
  --data-urlencode "q=SELECT * FROM home LIMIT 5"

# Write data with token
curl -X POST "http://localhost:8181/api/v2/write?org=company&bucket=sensors" \
  --header 'Authorization: Bearer apiv3_sc5R-bIEVeHjI9x4sOcbFJu9G1Ohjx0skXpEoc_kvMjRF7A06t24KiI3VgHkenn38XivvCy0vIztd0R7nb9GYQ' \
  --data-binary 'home,room=kitchen temp=72.1 1640995200000000000'
```

#### **Environment Variable Authentication**
```bash
# Set token for all CLI commands
export INFLUXDB3_AUTH_TOKEN="apiv3_sc5R-bIEVeHjI9x4sOcbFJu9G1Ohjx0skXpEoc_kvMjRF7A06t24KiI3VgHkenn38XivvCy0vIztd0R7nb9GYQ"

# Commands will automatically use the token
./influxdb3 query "SHOW DATABASES"
./influxdb3 write --database sensors "home,room=kitchen temp=72.1"
```

### 🔧 Advanced Token Features

#### **Token Security**
- **SHA-512 Hashing**: All tokens are securely hashed
- **Automatic Expiry**: Tokens can be set with expiration times
- **Regeneration**: Admin tokens can be regenerated for security
- **Named Tokens**: Create multiple tokens with descriptive names
- **Scoped Permissions**: Create tokens with specific database and action permissions

#### **Token Storage**
- **Persistent**: Tokens are stored in the catalog and persisted to object store
- **In-Memory**: Fast token validation with in-memory caching
- **Automatic Cleanup**: Expired tokens are automatically removed

#### **Scoped Token Permissions**

Scoped tokens allow you to create tokens with specific permissions instead of full admin access. This follows the principle of least privilege for better security.

##### **Permission Format**
```bash
resource_type:resource_name:action
```

##### **Supported Resource Types**
- **`db`**: Database permissions
- **`token`**: Token management permissions

##### **Supported Actions**
- **Database Actions**: `read`, `write`, `create`
- **Token Actions**: `read`, `write`, `create`, `delete`

##### **Examples**
```bash
# Database-specific permissions
db:sensors:read          # Read-only access to 'sensors' database
db:sensors:write         # Write-only access to 'sensors' database
db:sensors:read,write    # Read and write access to 'sensors' database

# Wildcard permissions
db:*:read                # Read access to all databases
token:*:read             # Read access to all tokens

# Multiple permissions
--permissions "db:sensors:read" --permissions "db:logs:write"
```

##### **Use Cases**
- **Application Tokens**: Create tokens with minimal required permissions
- **Monitoring Tokens**: Read-only access for monitoring tools
- **Backup Tokens**: Read access for backup processes
- **Development Tokens**: Limited access for development environments

#### **Best Practices**
```bash
# 1. Use named tokens for different environments
./influxdb3 create token --admin --name "production" --expiry 90d
./influxdb3 create token --admin --name "staging" --expiry 30d
./influxdb3 create token --admin --name "development" --expiry 7d

# 2. Set appropriate expiry times
# Production: 90 days
# Staging: 30 days  
# Development: 7 days
# Testing: 1 hour

# 3. Regenerate tokens regularly
./influxdb3 create token --admin --regenerate

# 4. Use environment variables in production
export INFLUXDB3_AUTH_TOKEN="your_production_token"
```

## 🔧 Advanced Compaction Configuration

### Multi-Level Compaction System

InfluxDB3-unlocked includes a comprehensive multi-level compaction system that automatically merges smaller generation files into larger, more efficient files. This significantly reduces the number of files that need to be read during queries, improving performance for large datasets.

### Generation Levels

The system supports up to 5 generation levels:

- **Generation 1 (Gen1)**: Initial files created from WAL snapshots
- **Generation 2 (Gen2)**: Compacted from Gen1 files
- **Generation 3 (Gen3)**: Compacted from Gen2 files
- **Generation 4 (Gen4)**: Compacted from Gen3 files
- **Generation 5 (Gen5)**: Compacted from Gen4 files

### Configuration Options

```bash
# Generation durations (all optional except gen1)
--gen1-duration 10m          # Default: 10m
--gen2-duration 1h           # Optional: compact gen1 files to 1-hour chunks
--gen3-duration 1d           # Optional: compact gen2 files to daily chunks
--gen4-duration 7d           # Optional: compact gen3 files to weekly chunks
--gen5-duration 30d          # Optional: compact gen4 files to monthly chunks

# Compaction settings
--enable-compaction true     # Default: true
--compaction-interval 1h     # Default: 1h - how often to check for compaction
--max-compaction-files 100   # Default: 100 - max files per compaction run
--min-files-for-compaction 10 # Default: 10 - minimum files to trigger compaction
```

### Example Configurations

#### High-Throughput System
```bash
influxdb3 serve \
  --object-store file \
  --node-id local1 \
  --gen1-duration 5m \
  --gen2-duration 1h \
  --gen3-duration 1d \
  --compaction-interval 30m \
  --max-compaction-files 200 \
  --min-files-for-compaction 5
```

#### Long-Term Storage System
```bash
influxdb3 serve \
  --object-store file \
  --node-id local1 \
  --gen1-duration 1h \
  --gen2-duration 6h \
  --gen3-duration 1d \
  --gen4-duration 7d \
  --gen5-duration 30d \
  --compaction-interval 2h
```

## 📊 Usage Examples

### Insert Data

#### Metrics (Line Protocol)
```bash
# Insert sensor data
echo 'home,room=kitchen temp=72.1,humidity=45.2 1640995200000000000' | \
curl -v "http://127.0.0.1:8181/api/v2/write?org=company&bucket=sensors" --data-binary @-

# Insert multiple metrics
cat << EOF | curl -v "http://127.0.0.1:8181/api/v2/write?org=company&bucket=sensors" --data-binary @-
home,room=kitchen temp=72.1,humidity=45.2 1640995200000000000
home,room=living temp=70.5,humidity=42.1 1640995200000000000
home,room=bedroom temp=68.9,humidity=48.7 1640995200000000000
EOF

# Large batch insert (up to 1GB)
curl -X POST "http://127.0.0.1:8181/api/v2/write?org=company&bucket=sensors" \
  --data-binary @large_dataset.lp
```

#### Logs (Syslog Format)
```bash
# Insert log entry
echo 'syslog,appname=myapp,facility=console,host=myhost,severity=warning facility_code=14i,message="warning message here",severity_code=4i,procid="12345",timestamp=1640995200000000000,version=1' | \
curl -v "http://127.0.0.1:8181/api/v2/write?org=company&bucket=logs" --data-binary @-
```

#### Traces (OpenTelemetry)
```bash
# Insert trace span
echo 'spans end_time_unix_nano="2025-01-26 20:50:25.6893952 +0000 UTC",instrumentation_library_name="tracegen",kind="SPAN_KIND_INTERNAL",name="okey-dokey",net.peer.ip="1.2.3.4",parent_span_id="d5270e78d85f570f",peer.service="tracegen-client",service.name="tracegen",span.kind="server",span_id="4c28227be6a010e1",status_code="STATUS_CODE_OK",trace_id="7d4854815225332c9834e6dbf85b9380"' | \
curl -v "http://127.0.0.1:8181/api/v2/write?org=company&bucket=traces" --data-binary @-
```

### Query Data

#### List Databases
```bash
influxdb3 query "SHOW DATABASES"
```

#### Query Metrics
```bash
# Query temperature data
influxdb3 query --database sensors "SELECT * FROM home WHERE temp > 70 LIMIT 5"

# Time range query (no 72-hour limit!)
influxdb3 query --database sensors "SELECT room, temp FROM home WHERE time > now() - 30d"

# Long-term historical analysis
influxdb3 query --database sensors "SELECT room, AVG(temp) as avg_temp FROM home WHERE time > now() - 1y GROUP BY room"

# Aggregation
influxdb3 query --database sensors "SELECT room, AVG(temp) as avg_temp FROM home GROUP BY room"
```

#### Query Logs
```bash
# Search by message content
influxdb3 query --database logs "SELECT * FROM syslog WHERE message LIKE '%warning%'"

# Regex search
influxdb3 query --database logs "SELECT * FROM syslog WHERE message ~ '.+warning'"

# Filter by severity
influxdb3 query --database logs "SELECT * FROM syslog WHERE severity = 'error'"

# Long-term log analysis
influxdb3 query --database logs "SELECT COUNT(*) FROM syslog WHERE time > now() - 6m GROUP BY severity"
```

#### Query Traces
```bash
# Query all spans
influxdb3 query --database traces "SELECT * FROM spans"

# Filter by service
influxdb3 query --database traces "SELECT * FROM spans WHERE service.name = 'tracegen'"

# Find slow operations
influxdb3 query --database traces "SELECT name, end_time_unix_nano - time as duration FROM spans ORDER BY duration DESC LIMIT 10"

# Long-term trace analysis
influxdb3 query --database traces "SELECT service.name, COUNT(*) as span_count FROM spans WHERE time > now() - 30d GROUP BY service.name"
```

## 🔧 Configuration

### Environment Variables

```bash
# Required settings
INFLUXDB3_OBJECT_STORE=file                    # Storage backend (file, memory, s3, etc.)
INFLUXDB3_NODE_IDENTIFIER_PREFIX=local1        # Unique node identifier

# Core settings
INFLUXDB3_HTTP_BIND_ADDR=0.0.0.0:8181
INFLUXDB3_LOG_FILTER=info

# Performance tuning
INFLUXDB3_GEN1_DURATION=1h                     # WAL flush frequency
INFLUXDB3_GEN1_LOOKBACK_DURATION=1month        # Startup data loading
INFLUXDB3_DATAFUSION_MAX_PARQUET_FANOUT=10000  # Parquet file handling
INFLUXDB3_MAX_HTTP_REQUEST_SIZE=1073741824     # 1GB request limit

# Compaction settings
INFLUXDB3_ENABLE_COMPACTION=true               # Enable multi-level compaction
INFLUXDB3_COMPACTION_INTERVAL=1h               # Compaction check interval
INFLUXDB3_MAX_COMPACTION_FILES=100             # Max files per compaction run
INFLUXDB3_MIN_FILES_FOR_COMPACTION=10          # Min files to trigger compaction

# S3/MinIO settings (when using object-store=s3)
INFLUXDB3_BUCKET=my-influxdb-bucket
INFLUXDB3_AWS_ACCESS_KEY_ID=your-access-key
INFLUXDB3_AWS_SECRET_ACCESS_KEY=your-secret-key
INFLUXDB3_AWS_ENDPOINT=http://minio:9000
INFLUXDB3_AWS_ALLOW_HTTP=true

# Authentication settings
INFLUXDB3_START_WITHOUT_AUTH=false              # Disable authentication completely
INFLUXDB3_DISABLE_AUTHZ=health,ping,metrics     # Disable auth for specific endpoints
```

### Authentication Configuration

> **📖 For detailed authentication setup and token management, see the [Authentication & Token Management](#-authentication--token-management) section above.**

InfluxDB3 supports different authentication modes for the HTTP API:

#### **No Authentication** (Development/Testing)
```bash
# Disable authentication completely
influxdb3 serve \
  --object-store file \
  --node-id local1 \
  --without-auth

# All API endpoints will be accessible without tokens
# Note: Token creation endpoints are disabled in this mode
```

#### **Selective Authentication** (Production)
```bash
# Disable auth for specific endpoints only
influxdb3 serve \
  --object-store file \
  --node-id local1 \
  --disable-authz health,ping,metrics

# Available options: health, ping, metrics
# Other endpoints still require authentication
```

#### **Full Authentication** (Default)
```bash
# All endpoints require authentication
influxdb3 serve \
  --object-store file \
  --node-id local1

# Use tokens for all API calls
```

### Object Store Options

#### Local File Storage
```bash
influxdb3 serve --object-store file --node-id local1
```

#### S3-Compatible Storage (MinIO, AWS S3)
```bash
# MinIO
influxdb3 serve \
  --object-store s3 \
  --node-id host01 \
  --bucket my-influxdb-bucket \
  --aws-access-key-id your-access-key \
  --aws-secret-access-key your-secret-key \
  --aws-endpoint http://minio:9000 \
  --aws-allow-http

# AWS S3
influxdb3 serve \
  --object-store s3 \
  --node-id host01 \
  --bucket my-influxdb-bucket \
  --aws-access-key-id your-access-key \
  --aws-secret-access-key your-secret-key \
  --aws-region us-east-1
```

#### In-Memory Storage (Testing)
```bash
influxdb3 serve --object-store memory --node-id test1
```

## 🔌 Integrations

### HTTP API

InfluxDB3 supports both Flight (gRPC) APIs and HTTP APIs. For HTTP queries, use the `/api/v3/query_sql` or `/api/v3/query_influxql` endpoints.

#### Query with URL Parameters
```bash
# GET request with URL-encoded parameters (with authentication)
curl -G "http://localhost:8181/api/v3/query_sql" \
  --header 'Authorization: Bearer YOUR_TOKEN' \
  --data-urlencode "db=sensors" \
  --data-urlencode "q=SELECT * FROM home WHERE temp > 70 LIMIT 5"

# GET request without authentication (when using --without-auth)
curl -G "http://localhost:8181/api/v3/query_sql" \
  --data-urlencode "db=sensors" \
  --data-urlencode "q=SELECT * FROM home WHERE temp > 70 LIMIT 5"

# Response formats: pretty, jsonl, parquet, csv, json (default)
curl -G "http://localhost:8181/api/v3/query_sql" \
  --header 'Authorization: Bearer YOUR_TOKEN' \
  --data-urlencode "db=sensors" \
  --data-urlencode "q=SELECT room, AVG(temp) FROM home GROUP BY room" \
  --data-urlencode "format=csv"
```

#### Query with JSON Payload
```bash
# POST request with JSON parameters (with authentication)
curl http://localhost:8181/api/v3/query_sql \
  --header 'Authorization: Bearer YOUR_TOKEN' \
  --header 'Content-Type: application/json' \
  --data '{
    "db": "sensors",
    "q": "SELECT * FROM home WHERE time > now() - 1h",
    "format": "json"
  }'

# POST request without authentication (when using --without-auth)
curl http://localhost:8181/api/v3/query_sql \
  --header 'Content-Type: application/json' \
  --data '{
    "db": "sensors",
    "q": "SELECT * FROM home WHERE time > now() - 1h",
    "format": "json"
  }'
```

#### Write Data via HTTP API
```bash
# Write metrics using HTTP API (with authentication)
curl -X POST "http://localhost:8181/api/v2/write?org=company&bucket=sensors" \
  --header 'Authorization: Bearer YOUR_TOKEN' \
  --data-binary 'home,room=kitchen temp=72.1,humidity=45.2 1640995200000000000'

# Write metrics without authentication (when using --without-auth)
curl -X POST "http://localhost:8181/api/v2/write?org=company&bucket=sensors" \
  --data-binary 'home,room=kitchen temp=72.1,humidity=45.2 1640995200000000000'

# Write multiple data points
curl -X POST "http://localhost:8181/api/v2/write?org=company&bucket=sensors" \
  --header 'Authorization: Bearer YOUR_TOKEN' \
  --data-binary 'home,room=kitchen temp=72.1,humidity=45.2 1640995200000000000
home,room=living temp=70.5,humidity=42.1 1640995200000000000'
```

### Python Client

Install the official InfluxDB3 Python client:

```bash
pip install influxdb3-python
```

#### Basic Usage
```python
from influxdb_client_3 import InfluxDBClient3

# Connect to your database
client = InfluxDBClient3(
    token='YOUR_TOKEN',
    host='http://localhost:8181',
    database='sensors'
)

# Write data
client.write('home,room=kitchen temp=72.1,humidity=45.2 1640995200000000000')

# Query data
result = client.query('SELECT * FROM home WHERE temp > 70 LIMIT 5')
for record in result:
    print(f"Room: {record['room']}, Temp: {record['temp']}")

# Close connection
client.close()
```

#### Advanced Usage
```python
from influxdb_client_3 import InfluxDBClient3
import pandas as pd

client = InfluxDBClient3(
    token='YOUR_TOKEN',
    host='http://localhost:8181',
    database='sensors'
)

# Query with time range (no 72-hour limit!)
query = """
SELECT room, temp, humidity 
FROM home 
WHERE time > now() - 30d 
ORDER BY time DESC
"""

# Get results as pandas DataFrame
df = client.query_dataframe(query)
print(df.head())

# Write data from pandas DataFrame
data = pd.DataFrame({
    'room': ['kitchen', 'living', 'bedroom'],
    'temp': [72.1, 70.5, 68.9],
    'humidity': [45.2, 42.1, 48.7]
})

client.write_dataframe(data, data_frame_measurement_name='home')

client.close()
```


### InfluxDB3 Explorer
Use the [InfluxDB3 Explorer](https://docs.influxdata.com/influxdb3/explorer/) with your unlocked InfluxDB3 server

<img width="1317" height="952" alt="InfluxDB-3-Explorer-08-01-2025_09_33_PM" src="https://github.com/user-attachments/assets/c015cedf-677e-4493-9721-2e99eeb2d359" />

### Grafana Integration

Use the [FlightSQL datasource](https://github.com/influxdata/grafana-flightsql-datasource) in Grafana:

```bash
# Grafana configuration
# Host: http://localhost:8181
# Database: your_database_name
# Authentication: None (or configure as needed)
```

### Generic FlightSQL Drivers

- [flightsql-dbapi-python](https://github.com/influxdata/flightsql-dbapi)
- [influxdb_iox_client-rust](https://crates.io/crates/influxdb_iox_client)
- [influxdb-iox-client-go](https://github.com/influxdata/influxdb-iox-client-go)

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
- **Enhanced compaction** - multi-level compaction improves query performance

## 🏗️ Building from Source

```bash
# Clone and build
git clone https://github.com/metrico/influxdb3-unlocked.git
cd influxdb3-unlocked
cargo build --release --package influxdb3

# Run the built binary
./target/release/influxdb3 serve --object-store file --node-id local1
```

## 📦 Releases

Automated GitHub Actions provide:
- Optimized Linux x64 binaries
- Docker images pushed to GitHub Container Registry
- GitHub releases with binaries

### Docker Images
- `ghcr.io/metrico/influxdb3-unlocked:latest`
- `ghcr.io/metrico/influxdb3-unlocked:v3.3.0-nightly` (versioned)

## 📚 Documentation

For detailed technical changes and implementation details, see [UNLOCK.md](UNLOCK.md).

## 📄 License

This project maintains full compliance with the original InfluxDB3-core licenses:

- **Apache License 2.0** - See [LICENSE-APACHE](LICENSE-APACHE)
- **MIT License** - See [LICENSE-MIT](LICENSE-MIT)

## 🔗 Links

- **Original Project**: [InfluxDB3-core](https://github.com/influxdata/influxdb3)
- **Documentation**: [UNLOCK.md](UNLOCK.md) - Detailed technical changes
- **Issues**: [GitHub Issues](https://github.com/metrico/influxdb3-unlocked/issues)
- **Releases**: [GitHub Releases](https://github.com/metrico/influxdb3-unlocked/releases)

---

**🎉 Enjoy unlocked InfluxDB3 Core performance!**
