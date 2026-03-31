# Configuration Guide

Complete configuration reference for Mammoth Engine.

## Table of Contents

- [Configuration File](#configuration-file)
- [Command Line Options](#command-line-options)
- [Environment Variables](#environment-variables)
- [Engine Settings](#engine-settings)
- [Network Settings](#network-settings)
- [Storage Settings](#storage-settings)
- [Replication Settings](#replication-settings)
- [Sharding Settings](#sharding-settings)
- [Security Settings](#security-settings)
- [Performance Tuning](#performance-tuning)
- [Logging Configuration](#logging-configuration)
- [Example Configurations](#example-configurations)

## Configuration File

Mammoth Engine uses YAML configuration files. Default location: `/etc/mammoth/mammoth.conf`

```yaml
# mammoth.conf

# Server identity
server:
  id: "node1"
  data_dir: "/var/lib/mammoth"
  log_dir: "/var/log/mammoth"

# Network configuration
network:
  bind_address: "0.0.0.0"
  port: 27017
  max_connections: 1000
  connection_timeout: "30s"
  read_timeout: "30s"
  write_timeout: "30s"

# Storage engine configuration
storage:
  memtable_size: 67108864        # 64 MB
  memtable_count: 4
  wal_size: 1073741824           # 1 GB
  wal_sync_interval: "10s"
  sstable_size: 134217728        # 128 MB
  block_size: 4096
  compression: true
  compression_type: "zstd"
  bloom_filter_enabled: true
  bloom_filter_bits_per_key: 10
  cache_size: 268435456          # 256 MB

# Compaction settings
compaction:
  level0_threshold: 4
  level_ratio: 10
  background_threads: 2
  max_concurrent: 2

# TLS/SSL configuration
tls:
  enabled: false
  cert_file: "/etc/mammoth/server.crt"
  key_file: "/etc/mammoth/server.key"
  ca_file: "/etc/mammoth/ca.crt"
  require_client_cert: false

# Authentication
auth:
  enabled: true
  mechanism: "SCRAM-SHA-256"
  require_auth: true
  default_role: "readWrite"

# Authorization
authorization:
  enabled: true
  rbac_enabled: true
  audit_enabled: true
  audit_log: "/var/log/mammoth/audit.log"

# Replication configuration
replication:
  enabled: false
  replica_set_name: "rs0"
  repl_set_members:
    - "node1:27017"
    - "node2:27017"
    - "node3:27017"
  election_timeout: "2s"
  heartbeat_interval: "500ms"
  replication_lag_threshold: "10s"

# Sharding configuration
sharding:
  enabled: false
  config_servers:
    - "config1:27019"
    - "config2:27019"
    - "config3:27019"
  balancer_enabled: true
  balancer_window_start: "02:00"
  balancer_window_end: "06:00"
  chunk_size: 64
  max_chunks_per_shard: 1000

# Performance tuning
performance:
  query_cache_size: 1000
  query_cache_ttl: "10m"
  max_query_time: "60s"
  sort_buffer_size: 67108864     # 64 MB
  aggregation_memory_limit: 1073741824  # 1 GB
  index_build_memory_limit: 536870912   # 512 MB

# Logging configuration
logging:
  level: "info"
  format: "json"
  output: "file"
  file: "/var/log/mammoth/mammoth.log"
  max_size: 100                  # MB
  max_backups: 5
  max_age: 30                    # days
  compress: true

# Monitoring and metrics
monitoring:
  enabled: true
  prometheus_enabled: true
  prometheus_port: 9090
  metrics_interval: "10s"
  health_check_interval: "5s"

# Backup configuration
backup:
  enabled: true
  schedule: "0 2 * * *"         # Daily at 2 AM
  retention: 7                  # Keep 7 days
  destination: "/backup/mammoth"
  compression: true
  encryption:
    enabled: false
    key_file: "/etc/mammoth/backup.key"
```

## Command Line Options

```bash
mammoth [options]

Options:
  --config, -c <file>          Configuration file path
  --data-dir, -d <path>        Data directory
  --log-dir <path>             Log directory
  --port, -p <port>            Listen port (default: 27017)
  --bind <address>             Bind address (default: 0.0.0.0)
  --replica-set <name>         Replica set name
  --shardsvr                   Run as shard server
  --configsvr                  Run as config server
  --auth                       Enable authentication
  --tls                        Enable TLS
  --cert <file>                TLS certificate file
  --key <file>                 TLS key file
  --verbose, -v                Verbose logging
  --version                    Show version
  --help                       Show help
```

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `MAMMOTH_CONFIG` | Configuration file path | `/etc/mammoth/mammoth.conf` |
| `MAMMOTH_DATA_DIR` | Data directory | `/var/lib/mammoth` |
| `MAMMOTH_LOG_DIR` | Log directory | `/var/log/mammoth` |
| `MAMMOTH_PORT` | Listen port | `27017` |
| `MAMMOTH_BIND` | Bind address | `0.0.0.0` |
| `MAMMOTH_REPLICA_SET` | Replica set name | `rs0` |
| `MAMMOTH_AUTH_ENABLED` | Enable authentication | `true` |
| `MAMMOTH_TLS_ENABLED` | Enable TLS | `false` |
| `MAMMOTH_TLS_CERT` | TLS certificate path | `/etc/mammoth/server.crt` |
| `MAMMOTH_TLS_KEY` | TLS key path | `/etc/mammoth/server.key` |
| `MAMMOTH_LOG_LEVEL` | Log level | `info` |
| `MAMMOTH_METRICS_ENABLED` | Enable metrics | `true` |

## Engine Settings

### Memtable Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `memtable_size` | 64 MB | Maximum memtable size before flush |
| `memtable_count` | 4 | Number of active memtables |
| `wal_size` | 1 GB | Write-ahead log size limit |
| `wal_sync_interval` | 10s | WAL sync interval |

### SSTable Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `sstable_size` | 128 MB | Target SSTable size |
| `block_size` | 4 KB | Block size for SSTables |
| `compression` | true | Enable compression |
| `compression_type` | zstd | Compression algorithm (zstd, snappy, lz4) |
| `bloom_filter_enabled` | true | Enable bloom filters |
| `bloom_filter_bits_per_key` | 10 | Bloom filter bits per key |

### Cache Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `cache_size` | 256 MB | Block cache size |
| `query_cache_size` | 1000 | Query plan cache entries |
| `query_cache_ttl` | 10m | Query cache TTL |

## Network Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `bind_address` | 0.0.0.0 | Network interface to bind |
| `port` | 27017 | TCP port |
| `max_connections` | 1000 | Maximum concurrent connections |
| `connection_timeout` | 30s | Connection timeout |
| `read_timeout` | 30s | Read timeout |
| `write_timeout` | 30s | Write timeout |

## Storage Settings

### Path Configuration

```yaml
storage:
  data_dir: "/var/lib/mammoth"

  # Subdirectories (auto-created)
  # /var/lib/mammoth/
  #   ├── sstables/       # SSTable files
  #   ├── wal/            # Write-ahead logs
  #   ├── manifest/       # Manifest files
  #   ├── snapshots/      # Database snapshots
  #   └── tmp/            # Temporary files
```

### File Descriptors

Mammoth requires sufficient file descriptors. Recommended minimum:

```bash
# /etc/security/limits.conf
mammoth soft nofile 64000
mammoth hard nofile 64000
```

## Replication Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `enabled` | false | Enable replication |
| `replica_set_name` | rs0 | Replica set name |
| `election_timeout` | 2s | Leader election timeout |
| `heartbeat_interval` | 500ms | Heartbeat interval |
| `replication_lag_threshold` | 10s | Max acceptable lag |

### Replica Set Configuration

```yaml
replication:
  enabled: true
  replica_set_name: "rs0"
  repl_set_members:
    - "192.168.1.10:27017"
    - "192.168.1.11:27017"
    - "192.168.1.12:27017"

  # Optional: Priority configuration
  member_priority:
    "192.168.1.10:27017": 2    # Preferred primary
    "192.168.1.11:27017": 1
    "192.168.1.12:27017": 1
```

## Sharding Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `enabled` | false | Enable sharding |
| `chunk_size` | 64 | Chunk size in MB |
| `max_chunks_per_shard` | 1000 | Max chunks before warning |
| `balancer_enabled` | true | Enable balancer |
| `balancer_window_start` | 02:00 | Balancer window start |
| `balancer_window_end` | 06:00 | Balancer window end |

### Config Server Configuration

```yaml
sharding:
  enabled: true
  config_servers:
    - "config1.example.com:27019"
    - "config2.example.com:27019"
    - "config3.example.com:27019"

  # Shard servers
  shards:
    - "shard1/rs1.example.com:27018"
    - "shard2/rs2.example.com:27018"
```

## Security Settings

### Authentication

```yaml
auth:
  enabled: true
  mechanism: "SCRAM-SHA-256"
  require_auth: true

  # Password policies
  password_policy:
    min_length: 8
    require_uppercase: true
    require_lowercase: true
    require_numbers: true
    require_special: true
    expiration_days: 90
```

### TLS Configuration

```yaml
tls:
  enabled: true
  cert_file: "/etc/mammoth/server.crt"
  key_file: "/etc/mammoth/server.key"
  ca_file: "/etc/mammoth/ca.crt"
  require_client_cert: false

  # TLS settings
  min_version: "1.2"
  cipher_suites:
    - "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
    - "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
```

### Audit Logging

```yaml
authorization:
  audit_enabled: true
  audit_log: "/var/log/mammoth/audit.log"
  audit_filter:
    - "authenticate"
    - "dropDatabase"
    - "dropCollection"
    - "createUser"
    - "dropUser"
```

## Performance Tuning

### Query Optimization

```yaml
performance:
  # Query execution
  max_query_time: "60s"
  max_sort_memory: "64MB"

  # Aggregation
  aggregation_memory_limit: "1GB"
  allow_disk_use: true

  # Index builds
  index_build_memory_limit: "512MB"
  index_build_background: true

  # Connection pooling
  max_connections: 1000
  connection_pool_size: 100

  # Background tasks
  compaction_threads: 2
  flush_threads: 2
```

### Memory Tuning Guidelines

| Memory | Recommended Settings |
|--------|---------------------|
| 4 GB | `cache_size: 1GB`, `memtable_size: 32MB` |
| 8 GB | `cache_size: 2GB`, `memtable_size: 64MB` |
| 16 GB | `cache_size: 4GB`, `memtable_size: 128MB` |
| 32 GB | `cache_size: 8GB`, `memtable_size: 256MB` |
| 64 GB+ | `cache_size: 16GB+`, `memtable_size: 512MB` |

## Logging Configuration

### Log Levels

- `debug` - Detailed debug information
- `info` - General information
- `warn` - Warning messages
- `error` - Error messages
- `fatal` - Fatal errors only

### Log Formats

```yaml
# JSON format (default for production)
logging:
  format: "json"
  output: "file"
  file: "/var/log/mammoth/mammoth.log"

# Text format (for development)
logging:
  format: "text"
  output: "stdout"

# Multiple outputs
logging:
  outputs:
    - type: "file"
      file: "/var/log/mammoth/mammoth.log"
    - type: "syslog"
      facility: "local0"
```

### Log Rotation

```yaml
logging:
  max_size: 100          # MB
  max_backups: 5         # Number of backups
  max_age: 30            # Days
  compress: true         # Compress rotated logs
```

## Example Configurations

### Development Environment

```yaml
server:
  id: "dev"
  data_dir: "./data"

network:
  bind_address: "127.0.0.1"
  port: 27017

storage:
  memtable_size: 16777216    # 16 MB
  cache_size: 67108864       # 64 MB

auth:
  enabled: false

logging:
  level: "debug"
  format: "text"
  output: "stdout"
```

### Production Single Node

```yaml
server:
  id: "prod-01"
  data_dir: "/var/lib/mammoth"

network:
  bind_address: "0.0.0.0"
  port: 27017
  max_connections: 2000

storage:
  memtable_size: 134217728   # 128 MB
  cache_size: 4294967296     # 4 GB
  compression: true

auth:
  enabled: true
  require_auth: true

tls:
  enabled: true
  cert_file: "/etc/mammoth/server.crt"
  key_file: "/etc/mammoth/server.key"

logging:
  level: "info"
  file: "/var/log/mammoth/mammoth.log"
  max_size: 500
```

### Production Replica Set

```yaml
server:
  id: "rs0-node1"
  data_dir: "/var/lib/mammoth"

network:
  bind_address: "0.0.0.0"
  port: 27017

replication:
  enabled: true
  replica_set_name: "rs0"
  repl_set_members:
    - "192.168.1.10:27017"
    - "192.168.1.11:27017"
    - "192.168.1.12:27017"

storage:
  memtable_size: 134217728
  cache_size: 8589934592     # 8 GB

auth:
  enabled: true
  require_auth: true

tls:
  enabled: true
  cert_file: "/etc/mammoth/server.crt"
  key_file: "/etc/mammoth/server.key"

backup:
  enabled: true
  schedule: "0 2 * * *"
  destination: "/backup/mammoth"
```

### Sharded Cluster

```yaml
# Config Server
server:
  id: "config1"
  data_dir: "/var/lib/mammoth"

network:
  port: 27019

sharding:
  enabled: true
  configsvr: true

---
# Shard Server
server:
  id: "shard1-node1"
  data_dir: "/var/lib/mammoth"

network:
  port: 27018

replication:
  enabled: true
  replica_set_name: "shard1"

sharding:
  enabled: true
  shardsvr: true

# Config servers
config_servers:
  - "config1:27019"
  - "config2:27019"
  - "config3:27019"
```

## Configuration Validation

Test configuration before starting:

```bash
mammoth --config /etc/mammoth/mammoth.conf --validate
```

Reload configuration without restart (where supported):

```bash
# Send SIGHUP to reload logging config
kill -HUP $(cat /var/run/mammoth.pid)
```

## Troubleshooting Configuration

### Common Issues

**Port already in use:**
```
Error: bind: address already in use
```
Solution: Check `lsof -i :27017` or change port.

**Permission denied:**
```
Error: open /var/lib/mammoth: permission denied
```
Solution: `chown -R mammoth:mammoth /var/lib/mammoth`

**Too many open files:**
```
Error: too many open files
```
Solution: Increase `ulimit -n` or check storage.memtable_count.

**Invalid YAML:**
```
Error: yaml: line 10: could not find expected ':'
```
Solution: Validate YAML syntax with `yamllint`.
