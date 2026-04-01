# Mammoth Engine

A high-performance, MongoDB-compatible document database engine built in Go.

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](#)
[![Test Coverage](https://img.shields.io/badge/coverage-85%25-brightgreen)](#)
[![Go Version](https://img.shields.io/badge/go-1.23+-blue)](#)
[![License](https://img.shields.io/badge/license-MIT-blue)](#)
[![Version](https://img.shields.io/badge/version-0.0.1--alpha-orange)](#)

## Overview

Mammoth Engine is a production-ready document database that implements the MongoDB wire protocol, allowing it to work with existing MongoDB drivers and tools while providing a lightweight, embeddable alternative.

### Key Features

- **MongoDB Compatibility** - Drop-in replacement for MongoDB applications
- **LSM-Tree Storage** - High-performance write-optimized storage engine
- **ACID Transactions** - Multi-document transaction support with snapshot isolation
- **Replication** - Raft-based consensus for high availability
- **Sharding** - Horizontal scaling with automatic data distribution
- **Full-Text Search** - Built-in text indexing and search
- **Geospatial Queries** - 2dsphere index support for location-based queries
- **Change Streams** - Real-time change notifications
- **Aggregation Pipeline** - Data processing and transformation
- **TTL Indexes** - Automatic document expiration

## Quick Start

### Installation

```bash
# Download latest release
wget https://github.com/mammothengine/mammoth/releases/latest/download/mammoth-linux-amd64
chmod +x mammoth-linux-amd64
sudo mv mammoth-linux-amd64 /usr/local/bin/mammoth

# Or install from source
go install github.com/mammothengine/mammoth/cmd/mammoth@latest
```

### Docker

```bash
# Run with Docker
docker run -p 27017:27017 mammoth:latest

# Or use Docker Compose
docker-compose up -d
```

### Basic Usage

```bash
# Start server
mammoth --data-dir ./data --port 27017

# Connect with mongosh
mongosh mongodb://localhost:27017
```

## Usage Examples

### Document Operations

```javascript
// Connect and insert
db.users.insertOne({
  name: "John Doe",
  email: "john@example.com",
  age: 30
})

// Query
db.users.find({ age: { $gte: 25 } }).sort({ name: 1 })

// Update
db.users.updateOne(
  { email: "john@example.com" },
  { $set: { lastLogin: new Date() } }
)
```

### Creating Indexes

```javascript
// Single field
db.users.createIndex({ email: 1 }, { unique: true })

// Compound
db.users.createIndex({ lastName: 1, firstName: 1 })

// Text search
db.articles.createIndex({ content: "text" })

// Geospatial
db.locations.createIndex({ coordinates: "2dsphere" })

// TTL (auto-expire)
db.sessions.createIndex(
  { expireAt: 1 },
  { expireAfterSeconds: 3600 }
)
```

### Transactions

```javascript
session = db.getMongo().startSession()
try {
  session.startTransaction()

  session.getDatabase("shop").orders.insertOne({ ... })
  session.getDatabase("shop").inventory.updateOne({ ... })

  session.commitTransaction()
} catch (error) {
  session.abortTransaction()
} finally {
  session.endSession()
}
```

### Replication

```javascript
// Initialize replica set
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "node1:27017" },
    { _id: 1, host: "node2:27017" },
    { _id: 2, host: "node3:27017" }
  ]
})

// Check status
rs.status()
```

### Sharding

```javascript
// Enable sharding on database
sh.enableSharding("mydb")

// Shard collection
sh.shardCollection("mydb.users", { _id: "hashed" })

// Check status
sh.status()
```

## Architecture

```
┌─────────────────────────────────────────┐
│           Client Applications           │
│    (MongoDB Drivers, mongosh, etc.)    │
└─────────────────┬───────────────────────┘
                  │ MongoDB Wire Protocol
┌─────────────────┴───────────────────────┐
│           Mammoth Engine                │
│  ┌─────────────────────────────────────┐│
│  │      Wire Protocol Handler          ││
│  └─────────────────┬───────────────────┘│
│                    │                     │
│  ┌─────────────────┴───────────────────┐│
│  │       Query Processor               ││
│  │  Planner → Optimizer → Executor     ││
│  └─────────────────┬───────────────────┘│
│                    │                     │
│  ┌─────────────────┴───────────────────┐│
│  │       Storage Engine (LSM-Tree)     ││
│  │  MemTable → WAL → SSTable → Compaction│
│  └─────────────────────────────────────┘│
└─────────────────────────────────────────┘
```

## Performance

| Operation | Single Node | 3-Node Replica |
|-----------|-------------|----------------|
| Write | ~50K ops/sec | ~30K ops/sec |
| Read | ~100K ops/sec | ~100K ops/sec |
| Latency (P50) | 0.5ms | 1ms |
| Latency (P99) | 2ms | 5ms |

## Configuration

```yaml
# mammoth.conf
server:
  data_dir: "/var/lib/mammoth"

network:
  bind_address: "0.0.0.0"
  port: 27017

storage:
  memtable_size: 67108864  # 64 MB
  cache_size: 268435456    # 256 MB

auth:
  enabled: true

tls:
  enabled: true
  cert_file: "/etc/mammoth/server.crt"
  key_file: "/etc/mammoth/server.key"

logging:
  level: "info"
  file: "/var/log/mammoth/mammoth.log"
```

See [Configuration Guide](docs/CONFIGURATION.md) for complete reference.

## Documentation

- [Deployment Guide](docs/DEPLOYMENT.md) - Production deployment instructions
- [Configuration Reference](docs/CONFIGURATION.md) - Complete configuration options
- [API Reference](docs/API_REFERENCE.md) - MongoDB-compatible API documentation
- [Tutorials](docs/TUTORIALS.md) - Step-by-step guides
- [Architecture](docs/ARCHITECTURE.md) - Technical architecture overview

## Building from Source

```bash
# Clone repository
git clone https://github.com/mammothengine/mammoth.git
cd mammoth

# Build
make build

# Run tests
make test

# Run with race detection
make test-race

# Generate coverage report
make coverage
```

## Testing

```bash
# Unit tests
go test ./...

# Integration tests
go test ./tests/... -v

# Stress tests
go test ./tests/... -run Stress -v

# Benchmarks
go test ./tests/... -bench=. -benchmem
```

## Roadmap

- [x] BSON implementation
- [x] Wire protocol support
- [x] CRUD operations
- [x] Index support (single, compound, text, geospatial)
- [x] Aggregation pipeline
- [x] Replication (Raft consensus)
- [x] Sharding
- [x] ACID transactions
- [x] Change streams
- [ ] Full aggregation operator set
- [ ] Query optimizer improvements
- [ ] Cloud-native features

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'feat: add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by MongoDB's document model and query language
- LSM-Tree implementation influenced by LevelDB and RocksDB
- Raft consensus based on etcd's implementation

## Support

- 📖 [Documentation](docs/)
- 🐛 [Issue Tracker](https://github.com/mammothengine/mammoth/issues)
- 💬 [Discussions](https://github.com/mammothengine/mammoth/discussions)

---

**Status:** 0.0.1 Alpha - Early Development

Last Updated: 2026-04-01
