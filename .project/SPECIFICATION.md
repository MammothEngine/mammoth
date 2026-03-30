# Mammoth Engine — SPECIFICATION

> **The Untamed Document Engine**
> A modern, MongoDB-compatible document database engine written in pure Go.
> Single binary. Zero dependencies. Built from scratch.

**Version:** 0.1.0-draft
**Author:** Ersin / ECOSTACK TECHNOLOGY OÜ
**License:** Apache 2.0
**Repository:** github.com/mammothengine/mammoth
**Website:** mammothengine.com

---

## Table of Contents

1. [Vision & Philosophy](#1-vision--philosophy)
2. [Project Goals & Non-Goals](#2-project-goals--non-goals)
3. [Architecture Overview](#3-architecture-overview)
4. [Storage Engine](#4-storage-engine)
5. [Data Model & Document Format](#5-data-model--document-format)
6. [Wire Protocol & MongoDB Compatibility](#6-wire-protocol--mongodb-compatibility)
7. [Query Engine](#7-query-engine)
8. [Indexing](#8-indexing)
9. [Transaction Support (ACID)](#9-transaction-support-acid)
10. [Replication (Raft)](#10-replication-raft)
11. [Sharding & Horizontal Scaling](#11-sharding--horizontal-scaling)
12. [Security](#12-security)
13. [Observability](#13-observability)
14. [Embedded Mode](#14-embedded-mode)
15. [CLI & Administration](#15-cli--administration)
16. [Client SDKs](#16-client-sdks)
17. [Performance Targets](#17-performance-targets)
18. [Deployment & Distribution](#18-deployment--distribution)
19. [Roadmap & Milestones](#19-roadmap--milestones)
20. [References & Inspirations](#20-references--inspirations)

---

## 1. Vision & Philosophy

### 1.1 What is Mammoth Engine?

Mammoth Engine is a **MongoDB-compatible document database engine** written in **pure Go** with **zero external dependencies**. It delivers as a **single static binary** that replaces the entire MongoDB ecosystem (mongod, mongos, config servers) with one unified process.

### 1.2 Why Mammoth Engine Exists

MongoDB is the de facto standard for document databases, but it carries significant operational burden:

- **Complex deployment**: Separate mongod, mongos, and config server processes
- **Heavy resource usage**: ~1GB+ RAM baseline, JVM-like memory behavior
- **Licensing concerns**: SSPL license creates legal ambiguity for cloud providers and SaaS companies
- **Legacy baggage**: BSON spec includes deprecated types, MMAPv1 ghosts, inconsistent APIs
- **No embedded mode**: Cannot use MongoDB as a library (unlike SQLite for relational data)
- **Operational complexity**: Replica set initialization, shard key selection, balancer tuning

Mammoth Engine addresses every one of these pain points while maintaining **full wire protocol compatibility** with MongoDB, enabling zero-effort migration.

### 1.3 Core Philosophy

| Principle | Description |
|-----------|-------------|
| **#NOFORKANYMORE** | Built entirely from scratch. No forked code, no vendored engines, no CGo. Every line is original. |
| **Zero Dependencies** | Pure Go standard library only. No RocksDB, no LevelDB, no BoltDB. The storage engine is handcrafted. |
| **Single Binary** | One static binary contains everything: storage engine, query engine, replication, sharding, HTTP API, admin UI. |
| **MongoDB Compatible** | Wire protocol (OP_MSG) compatibility means existing MongoDB drivers and tools work out of the box. |
| **Embedded First** | Can be imported as a Go library and used without a server process, like SQLite. |
| **Secure by Default** | TLS, auth, encryption at rest — all enabled by default, not afterthoughts. |
| **Observable by Default** | Prometheus metrics, structured logging, and health endpoints built in from day one. |

---

## 2. Project Goals & Non-Goals

### 2.1 Goals

- [ ] Full MongoDB wire protocol compatibility (OP_MSG)
- [ ] BSON input/output on wire, optimized internal binary format for storage
- [ ] LSM-Tree based storage engine with WAL, written in pure Go
- [ ] ACID transactions with MVCC (multi-document, multi-collection)
- [ ] Secondary indexes: single field, compound, multikey, text, geospatial, TTL, partial, wildcard
- [ ] Aggregation pipeline with all major stages ($match, $group, $sort, $project, $unwind, $lookup, $facet, $bucket, $merge, $out)
- [ ] Raft-based replication for high availability
- [ ] Automatic sharding with hash-based and range-based strategies
- [ ] Embedded mode (use as Go library, no server process)
- [ ] Single binary deployment, cross-compiled for linux/darwin/windows on amd64/arm64
- [ ] Built-in admin web UI (SPA, no external dependencies)
- [ ] Encryption at rest (AES-256-GCM)
- [ ] TLS for all network communication
- [ ] Authentication (SCRAM-SHA-256, x509)
- [ ] Role-based access control (RBAC)
- [ ] Change streams (real-time notifications)
- [ ] Prometheus metrics endpoint
- [ ] Structured JSON logging
- [ ] Schema validation (JSON Schema)
- [ ] Capped collections
- [ ] GridFS-compatible large file storage

### 2.2 Non-Goals

- **Not a multi-model database**: No graph queries, no column-family, no time-series specific optimizations (v1)
- **Not a distributed SQL database**: Document model only, no SQL interface
- **Not a cloud service**: Self-hosted only. No managed cloud offering in scope.
- **Not a MongoDB proxy**: Unlike FerretDB, Mammoth has its own native storage engine. No Postgres/SQLite backend.
- **Not a MongoDB fork**: Zero MongoDB source code. Wire protocol compatibility does not mean code reuse.

---

## 3. Architecture Overview

### 3.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Mammoth Engine Binary                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐  ┌───────────┐ │
│  │  MongoDB     │  │  HTTP/REST   │  │  gRPC      │  │  Admin    │ │
│  │  Wire Proto  │  │  API         │  │  API       │  │  Web UI   │ │
│  │  (OP_MSG)    │  │              │  │  (internal)│  │  (SPA)    │ │
│  └──────┬───────┘  └──────┬───────┘  └─────┬──────┘  └─────┬─────┘ │
│         │                 │                │              │       │
│  ┌──────▼─────────────────▼────────────────▼──────────────▼─────┐ │
│  │                    Connection Manager                         │ │
│  │           (goroutine-per-connection, connection pool)          │ │
│  └──────────────────────────┬────────────────────────────────────┘ │
│                             │                                     │
│  ┌──────────────────────────▼────────────────────────────────────┐ │
│  │                      Query Engine                             │ │
│  │  ┌──────────┐  ┌──────────────┐  ┌────────────────────────┐  │ │
│  │  │  BSON     │  │  Query       │  │  Aggregation Pipeline  │  │ │
│  │  │  Parser   │  │  Planner &   │  │  Executor              │  │ │
│  │  │          │  │  Optimizer   │  │                        │  │ │
│  │  └──────────┘  └──────────────┘  └────────────────────────┘  │ │
│  └──────────────────────────┬────────────────────────────────────┘ │
│                             │                                     │
│  ┌──────────────────────────▼────────────────────────────────────┐ │
│  │                   Transaction Manager                         │ │
│  │              (MVCC, Snapshot Isolation, 2PC)                   │ │
│  └──────────────────────────┬────────────────────────────────────┘ │
│                             │                                     │
│  ┌──────────────────────────▼────────────────────────────────────┐ │
│  │                    Catalog & Metadata                         │ │
│  │     (databases, collections, indexes, users, config)          │ │
│  └──────────────────────────┬────────────────────────────────────┘ │
│                             │                                     │
│  ┌──────────────────────────▼────────────────────────────────────┐ │
│  │                    Storage Engine (LSM-Tree)                   │ │
│  │  ┌───────────┐  ┌───────────┐  ┌─────────┐  ┌─────────────┐ │ │
│  │  │  Active    │  │ Immutable │  │  SSTable │  │  Bloom      │ │ │
│  │  │ Memtable  │  │ Memtables │  │  Levels  │  │  Filters    │ │ │
│  │  │ (SkipList)│  │           │  │  0..N    │  │             │ │ │
│  │  └───────────┘  └───────────┘  └─────────┘  └─────────────┘ │ │
│  │  ┌───────────┐  ┌───────────┐  ┌─────────┐  ┌─────────────┐ │ │
│  │  │  WAL      │  │ Compaction│  │  Block   │  │  Compression│ │ │
│  │  │  (Write-  │  │  Manager  │  │  Cache   │  │  (Snappy/   │ │ │
│  │  │  Ahead)   │  │           │  │  (LRU)   │  │   LZ4/Zstd) │ │ │
│  │  └───────────┘  └───────────┘  └─────────┘  └─────────────┘ │ │
│  └──────────────────────────┬────────────────────────────────────┘ │
│                             │                                     │
│  ┌──────────────────────────▼────────────────────────────────────┐ │
│  │                    Replication (Raft)                          │ │
│  │     (Leader Election, Log Replication, Snapshot Transfer)      │ │
│  └──────────────────────────┬────────────────────────────────────┘ │
│                             │                                     │
│  ┌──────────────────────────▼────────────────────────────────────┐ │
│  │                    Shard Manager                               │ │
│  │     (Router, Chunk Metadata, Auto-Balance, Migration)          │ │
│  └───────────────────────────────────────────────────────────────┘ │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.2 Go Package Structure

```
mammoth/
├── cmd/
│   └── mammoth/              # Main binary entrypoint
│       └── main.go
├── pkg/
│   ├── bson/                 # BSON encoder/decoder (pure Go)
│   ├── wire/                 # MongoDB wire protocol (OP_MSG)
│   ├── engine/               # Storage engine (LSM-Tree)
│   │   ├── wal/              # Write-ahead log
│   │   ├── memtable/         # Concurrent skip list memtable
│   │   ├── sstable/          # SSTable reader/writer
│   │   ├── compaction/       # Compaction strategies
│   │   ├── bloom/            # Bloom filter implementation
│   │   ├── cache/            # Block cache (LRU)
│   │   └── compression/      # Snappy, LZ4, Zstd (pure Go)
│   ├── document/             # Document model & internal format
│   ├── index/                # Index implementations
│   │   ├── btree/            # B+Tree for secondary indexes
│   │   ├── text/             # Inverted index for text search
│   │   ├── geo/              # Geospatial (S2 geometry)
│   │   └── manager.go        # Index lifecycle management
│   ├── query/                # Query engine
│   │   ├── parser/           # Query DSL parser
│   │   ├── planner/          # Query plan generation
│   │   ├── optimizer/        # Plan optimization
│   │   ├── executor/         # Plan execution (iterator model)
│   │   └── aggregation/      # Aggregation pipeline stages
│   ├── txn/                  # Transaction manager (MVCC)
│   ├── catalog/              # Database/collection metadata
│   ├── auth/                 # Authentication & authorization
│   │   ├── scram/            # SCRAM-SHA-256
│   │   └── rbac/             # Role-based access control
│   ├── replication/          # Raft consensus
│   │   ├── raft/             # Raft protocol implementation
│   │   ├── oplog/            # Operation log
│   │   └── snapshot/         # Snapshot transfer
│   ├── sharding/             # Horizontal scaling
│   │   ├── router/           # Query routing
│   │   ├── chunk/            # Chunk management
│   │   └── balancer/         # Auto-balance
│   ├── server/               # Network server
│   │   ├── mongo/            # MongoDB protocol server
│   │   ├── http/             # REST API server
│   │   └── grpc/             # Internal gRPC (replication, sharding)
│   ├── admin/                # Admin web UI (embedded SPA)
│   ├── config/               # Configuration management
│   ├── crypto/               # Encryption at rest, TLS helpers
│   ├── metrics/              # Prometheus metrics
│   └── logger/               # Structured JSON logging
├── embed/                    # Embedded mode public API
│   └── mammoth.go            # import "mammothengine/embed"
├── internal/                 # Internal utilities
│   ├── mmap/                 # Memory-mapped file I/O
│   ├── syncutil/             # Sync primitives, lock-free structures
│   └── testutil/             # Test helpers
├── docs/                     # Documentation
├── scripts/                  # Build, test, benchmark scripts
├── Makefile
├── go.mod
├── SPECIFICATION.md
├── IMPLEMENTATION.md
├── TASKS.md
├── BRANDING.md
├── LICENSE
└── README.md
```

### 3.3 Deployment Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Standalone** | Single mammoth process, single node | Development, small apps, testing |
| **Replica Set** | 3+ nodes with Raft consensus | Production HA, read scaling |
| **Sharded Cluster** | Multiple replica sets + router | Horizontal scaling, large datasets |
| **Embedded** | Go library, no network server | Edge/IoT, desktop apps, testing |

---

## 4. Storage Engine

### 4.1 Overview

The storage engine is an **LSM-Tree (Log-Structured Merge-Tree)** implementation written entirely in Go. No CGo, no embedded C libraries.

### 4.2 Write Path

```
Client Write
    │
    ▼
┌──────────────────┐
│   WAL (fsync)    │  ← Durability guarantee. Sequential append.
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Active Memtable │  ← Concurrent skip list. Lock-free reads,
│   (Skip List)    │    minimal locking on writes.
└────────┬─────────┘
         │ (size >= memtable_size_threshold, default 64MB)
         ▼
┌──────────────────┐
│Immutable Memtable│  ← Frozen, scheduled for flush.
│   (read-only)    │    New active memtable created.
└────────┬─────────┘
         │ (background flush goroutine)
         ▼
┌──────────────────┐
│  Level 0 SSTable │  ← Flushed directly. May overlap with other L0 files.
└────────┬─────────┘
         │ (compaction trigger: L0 file count >= 4)
         ▼
┌──────────────────┐
│  Level 1..N      │  ← Sorted, non-overlapping key ranges per level.
│  SSTables        │    Size ratio: 10x per level (configurable).
└──────────────────┘
```

### 4.3 Read Path

```
Client Read (key lookup)
    │
    ▼
Active Memtable ──found──▶ Return
    │ not found
    ▼
Immutable Memtables (newest first) ──found──▶ Return
    │ not found
    ▼
Level 0 SSTables (all files, newest first)
    │ For each file:
    │   1. Check Bloom filter → skip if negative
    │   2. Binary search index block → find data block
    │   3. Search data block → return if found
    │ not found
    ▼
Level 1..N SSTables
    │ For each level:
    │   1. Binary search on file key ranges → find candidate file
    │   2. Check Bloom filter → skip if negative
    │   3. Binary search index block → find data block
    │   4. Search data block → return if found
    │ not found
    ▼
Return: key not found
```

### 4.4 SSTable Format

```
┌─────────────────────────────────────────┐
│          Data Blocks                     │
│  ┌─────────────────────────────────────┐ │
│  │ Block 0: [key1:val1, key2:val2, ...] │ │  ← Sorted key-value pairs
│  │ Block 1: [key5:val5, key6:val6, ...] │ │     Prefix compression on keys
│  │ ...                                  │ │     Configurable block size (4KB default)
│  └─────────────────────────────────────┘ │
├─────────────────────────────────────────┤
│          Meta Blocks                     │
│  ┌─────────────────────────────────────┐ │
│  │ Bloom Filter (per SSTable)          │ │  ← 10 bits per key, ~1% FPR
│  │ Stats (key count, size, range)      │ │
│  │ Compression dictionary (optional)   │ │
│  └─────────────────────────────────────┘ │
├─────────────────────────────────────────┤
│          Index Block                     │
│  ┌─────────────────────────────────────┐ │
│  │ [last_key_block_0 → offset_0]      │ │  ← Binary searchable
│  │ [last_key_block_1 → offset_1]      │ │     Points to data blocks
│  │ ...                                  │ │
│  └─────────────────────────────────────┘ │
├─────────────────────────────────────────┤
│          Footer (48 bytes)               │
│  ┌─────────────────────────────────────┐ │
│  │ meta_index_offset (8B)              │ │
│  │ meta_index_size (8B)                │ │
│  │ index_offset (8B)                   │ │
│  │ index_size (8B)                     │ │
│  │ version (4B)                        │ │
│  │ checksum (8B)                       │ │
│  │ magic_number (4B): 0x4D414D4D      │ │  ← "MAMM"
│  └─────────────────────────────────────┘ │
└─────────────────────────────────────────┘
```

### 4.5 WAL Format

```
┌───────────────────────────────────────┐
│  WAL Segment File (max 64MB)          │
│  ┌─────────────────────────────────┐  │
│  │ Record Header (13 bytes)        │  │
│  │   checksum    : uint32 (CRC-32) │  │
│  │   length      : uint32          │  │
│  │   type        : uint8           │  │  ← Full / First / Middle / Last
│  │   sequence_no : uint32          │  │
│  ├─────────────────────────────────┤  │
│  │ Record Payload (variable)       │  │
│  │   operation   : uint8           │  │  ← Put / Delete / BatchStart / BatchEnd
│  │   key_length  : uint32          │  │
│  │   key_data    : []byte          │  │
│  │   val_length  : uint32          │  │
│  │   val_data    : []byte          │  │
│  └─────────────────────────────────┘  │
│  ... more records ...                 │
└───────────────────────────────────────┘
```

### 4.6 Compaction

**Strategy: Leveled Compaction (default)**

- Level 0: Flushed memtables, overlapping allowed. Max 4 files.
- Level 1+: Non-overlapping key ranges. Each level is 10x larger than previous.
- Compaction picks overlapping files from L(n) and L(n+1), merges them, writes new L(n+1) files.
- Tombstone (delete markers) garbage collected when reaching bottom level.

**Configurable alternatives:**
- **Size-Tiered**: For write-heavy workloads (higher space amplification, lower write amplification)
- **FIFO**: For TTL-based data (event logs, time-series cache)

### 4.7 Compression

All implemented in pure Go (no CGo):

| Algorithm | Use Case | Ratio | Speed |
|-----------|----------|-------|-------|
| **Snappy** (default) | General purpose | ~2x | Fastest |
| **LZ4** | Balanced | ~2.5x | Fast |
| **Zstd** | Maximum compression | ~3.5x | Medium |
| **None** | Low-latency reads | 1x | N/A |

Configurable per-level: e.g., Snappy for L0-L2, Zstd for L3+.

### 4.8 Block Cache

- **LRU cache** for frequently accessed SSTable data blocks
- Default size: 25% of available RAM (configurable)
- Sharded by block offset to reduce lock contention (16 shards default)
- Cache key: `{sstable_id}:{block_offset}`
- Supports direct I/O bypass for large sequential scans

### 4.9 Memory-Mapped I/O

- Optional mmap for SSTable reads (configurable, default off)
- When enabled: OS page cache manages SSTable access
- When disabled: Direct I/O + block cache (more predictable memory usage)
- Mmap useful for read-heavy workloads where data fits in RAM

---

## 5. Data Model & Document Format

### 5.1 External Format: BSON

Wire protocol uses standard BSON for MongoDB compatibility. All MongoDB drivers serialize/deserialize BSON natively.

**Supported BSON types:**

| Type | ID | Description |
|------|----|-------------|
| Double | 0x01 | 64-bit IEEE 754 float |
| String | 0x02 | UTF-8 string |
| Document | 0x03 | Embedded document |
| Array | 0x04 | Array (document with "0", "1", ... keys) |
| Binary | 0x05 | Binary data with subtype |
| ObjectId | 0x07 | 12-byte unique identifier |
| Boolean | 0x08 | true / false |
| DateTime | 0x09 | UTC milliseconds since epoch |
| Null | 0x0A | Null value |
| Regex | 0x0B | Regular expression |
| Int32 | 0x10 | 32-bit signed integer |
| Timestamp | 0x11 | Internal MongoDB timestamp |
| Int64 | 0x12 | 64-bit signed integer |
| Decimal128 | 0x13 | 128-bit decimal floating point |
| MinKey | 0xFF | Minimum key (internal) |
| MaxKey | 0x7F | Maximum key (internal) |

**Deliberately NOT supported (MongoDB legacy):**
- `Undefined` (0x06) — deprecated
- `DBPointer` (0x0C) — deprecated
- `Symbol` (0x0E) — deprecated
- `Code` (0x0D, 0x0F) — security risk, rarely used

### 5.2 Internal Format: Mammoth Binary Format (MBF)

Documents are converted from BSON to MBF for storage. MBF is optimized for:

- **Faster field access**: Field offsets stored in header, O(1) field lookup
- **Smaller size**: Varint encoding for lengths, no redundant type markers
- **Version tracking**: Built-in document version for MVCC

```
MBF Document Layout:
┌─────────────────────────────────────────┐
│  Header (variable)                       │
│    magic       : uint16 (0x4D42 = "MB") │
│    version     : uint8                   │
│    flags       : uint8                   │
│    field_count : varint                  │
│    total_size  : varint                  │
│    field_index : [{name_hash, offset}]   │  ← Sorted by hash for binary search
├─────────────────────────────────────────┤
│  Field Data (variable)                   │
│    For each field:                       │
│      type     : uint8                    │
│      name_len : varint                   │
│      name     : []byte (UTF-8)           │
│      val_len  : varint                   │
│      value    : []byte (type-specific)   │
├─────────────────────────────────────────┤
│  Footer                                  │
│    checksum   : uint32 (CRC-32C)        │
└─────────────────────────────────────────┘
```

### 5.3 ObjectID Generation

Default: MongoDB-compatible 12-byte ObjectID:

```
┌────────────┬──────────────┬──────────────┐
│ Timestamp  │    Random    │   Counter    │
│  4 bytes   │   5 bytes    │   3 bytes    │
│ (seconds)  │  (per-proc)  │ (increment)  │
└────────────┴──────────────┴──────────────┘
```

Optional alternatives (configurable per collection):
- **UUIDv7**: 128-bit, timestamp-sortable, RFC 9562 compliant
- **ULID**: 128-bit, Crockford Base32, timestamp-sortable
- **Custom**: User-provided `_id` of any BSON type

### 5.4 Storage Key Encoding

Internal key format for LSM-Tree:

```
┌──────────────┬─────────────────┬───────────────┬───────────────┐
│ namespace_id │   document_id   │   version     │   type        │
│  (varint)    │  (12+ bytes)    │  (uint64 BE)  │  (uint8)      │
│              │                 │  (inverted)   │  0=value      │
│              │                 │               │  1=tombstone  │
└──────────────┴─────────────────┴───────────────┴───────────────┘
```

- `namespace_id`: Encodes `database.collection` as a compact integer
- `version` inverted (MaxUint64 - version) so newest version sorts first
- Enables MVCC: multiple versions of same document coexist, reader picks correct snapshot

---

## 6. Wire Protocol & MongoDB Compatibility

### 6.1 Protocol Support

Mammoth Engine implements **OP_MSG** (MongoDB 3.6+), the modern MongoDB wire protocol. Legacy opcodes (OP_QUERY, OP_INSERT, etc.) are NOT supported — they were deprecated in MongoDB 5.1.

**OP_MSG structure:**

```
┌──────────────────────────────────────┐
│  MsgHeader (16 bytes)                │
│    messageLength : int32             │
│    requestID     : int32             │
│    responseTo    : int32             │
│    opCode        : int32 (2013)      │
├──────────────────────────────────────┤
│  flagBits : uint32                   │
│    bit 0: checksumPresent            │
│    bit 1: moreToCome                 │
│    bit 16: exhaustAllowed            │
├──────────────────────────────────────┤
│  Sections[]                          │
│    Kind 0: Body (single BSON doc)    │
│    Kind 1: Document Sequence         │
├──────────────────────────────────────┤
│  Checksum (optional, CRC-32C)        │
└──────────────────────────────────────┘
```

### 6.2 Supported Commands

**CRUD:**
- `find`, `insert`, `update`, `delete`
- `findAndModify`, `getMore`, `killCursors`

**Aggregation:**
- `aggregate`, `count`, `distinct`
- `mapReduce` (deprecated, minimal support for compatibility)

**Index:**
- `createIndexes`, `dropIndexes`, `listIndexes`

**Admin:**
- `create`, `drop`, `listCollections`, `listDatabases`
- `renameCollection`, `collMod`, `collStats`, `dbStats`
- `serverStatus`, `ping`, `buildInfo`, `hostInfo`
- `currentOp`, `killOp`

**Auth:**
- `authenticate`, `saslStart`, `saslContinue`
- `createUser`, `updateUser`, `dropUser`, `usersInfo`
- `createRole`, `updateRole`, `dropRole`, `rolesInfo`

**Replication:**
- `replSetInitiate`, `replSetGetStatus`, `replSetGetConfig`
- `replSetStepDown`, `replSetFreeze`
- `isMaster` / `hello`

**Transaction:**
- `startTransaction`, `commitTransaction`, `abortTransaction`

**Diagnostic:**
- `explain`, `validate`, `profile`

### 6.3 Compatibility Target

- **Minimum MongoDB compatibility**: 6.0 API behavior
- **Driver compatibility**: Official MongoDB drivers for Go, Node.js, Python, Java, PHP, Rust, C#, Ruby
- **Tool compatibility**: mongosh, Compass, mongodump/mongorestore, Atlas CLI
- **ODM compatibility**: Mongoose (Node.js), Motor (Python), Spring Data MongoDB (Java)

---

## 7. Query Engine

### 7.1 Query Execution Pipeline

```
BSON Query Document
    │
    ▼
┌─────────────────┐
│  Query Parser    │  ← Converts BSON query operators to internal AST
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Query Planner   │  ← Enumerates candidate plans (index scan, coll scan)
│                  │     Considers: available indexes, selectivity estimates,
│                  │     covered queries, sort optimization
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Plan Optimizer  │  ← Predicate pushdown, index intersection,
│                  │     redundant stage elimination,
│                  │     sort-merge optimization
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Plan Executor   │  ← Volcano/iterator model.
│                  │     Each stage: Open() → Next() → Close()
│                  │     Lazy evaluation, streaming results.
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Result          │  ← BSON serialization, cursor management,
│  Serializer      │     batch sizing (101 docs first batch, 16MB max)
└─────────────────┘
```

### 7.2 Query Operators

**Comparison:** `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`

**Logical:** `$and`, `$or`, `$not`, `$nor`

**Element:** `$exists`, `$type`

**Evaluation:** `$regex`, `$expr`, `$mod`, `$text`, `$where` (sandboxed)

**Array:** `$all`, `$elemMatch`, `$size`

**Geospatial:** `$near`, `$nearSphere`, `$geoWithin`, `$geoIntersects`

**Bitwise:** `$bitsAllSet`, `$bitsAnySet`, `$bitsAllClear`, `$bitsAnyClear`

### 7.3 Update Operators

**Field:** `$set`, `$unset`, `$setOnInsert`, `$rename`, `$inc`, `$mul`, `$min`, `$max`, `$currentDate`

**Array:** `$push`, `$pull`, `$pop`, `$addToSet`, `$pullAll`, `$each`, `$sort`, `$slice`, `$position`

### 7.4 Aggregation Pipeline Stages

| Stage | Description | Priority |
|-------|-------------|----------|
| `$match` | Filter documents | v0.1 |
| `$project` | Reshape documents | v0.1 |
| `$group` | Group & aggregate | v0.1 |
| `$sort` | Sort results | v0.1 |
| `$limit` / `$skip` | Pagination | v0.1 |
| `$unwind` | Flatten arrays | v0.1 |
| `$lookup` | Left outer join | v0.2 |
| `$addFields` | Add computed fields | v0.2 |
| `$count` | Count documents | v0.2 |
| `$facet` | Multi-pipeline | v0.3 |
| `$bucket` / `$bucketAuto` | Histogram grouping | v0.3 |
| `$merge` / `$out` | Write results | v0.3 |
| `$graphLookup` | Recursive lookup | v0.4 |
| `$unionWith` | Union collections | v0.4 |
| `$replaceRoot` | Replace doc root | v0.2 |
| `$sample` | Random sampling | v0.2 |
| `$redact` | Field-level security | v0.3 |
| `$setWindowFields` | Window functions | v0.4 |

**Aggregation expressions:** Full MongoDB expression language support including `$add`, `$subtract`, `$multiply`, `$divide`, `$concat`, `$substr`, `$cond`, `$switch`, `$arrayElemAt`, `$map`, `$filter`, `$reduce`, date operators, string operators, type conversion operators.

---

## 8. Indexing

### 8.1 Index Types

| Type | Storage | Key Format | Use Case |
|------|---------|------------|----------|
| **Primary (_id)** | LSM-Tree (main) | `{ns_id}{_id}{version}` | Default unique index |
| **Single Field** | Separate LSM-Tree | `{ns_id}{idx_id}{field_val}{_id}` | Equality, range queries |
| **Compound** | Separate LSM-Tree | `{ns_id}{idx_id}{val1}{val2}...{_id}` | Multi-field queries |
| **Multikey** | Separate LSM-Tree | One entry per array element | Array field queries |
| **Text** | Inverted index | `{ns_id}{idx_id}{term}` → posting list | Full-text search |
| **Geospatial (2dsphere)** | S2 cell index | `{ns_id}{idx_id}{s2_cell_id}{_id}` | Location queries |
| **TTL** | Regular + background GC | Same as single field | Auto-expiring docs |
| **Unique** | Regular + constraint check | Same as single/compound | Uniqueness enforcement |
| **Partial** | Regular + filter predicate | Same as base type | Conditional indexing |
| **Wildcard** | Dynamic field detection | `{ns_id}{idx_id}{field_path}{val}{_id}` | Schema-less queries |
| **Hashed** | Separate LSM-Tree | `{ns_id}{idx_id}{hash(val)}{_id}` | Equality only, shard keys |

### 8.2 Index Selection (Query Planner)

1. Enumerate all indexes that match query predicates
2. Estimate selectivity for each candidate:
   - Equality predicates: Use index statistics (distinct count)
   - Range predicates: Use min/max bounds + histogram
   - No statistics: Fall back to heuristic (1/3 for range, 1/10 for equality)
3. Consider compound index prefix matching
4. Score candidates: `score = selectivity × covered_bonus × sort_bonus`
5. If multiple candidates score similarly: run trial execution (first 100 docs) and pick winner

### 8.3 Index Build

- **Foreground**: Blocks writes, fastest build. Default for empty collections.
- **Background**: Incremental, allows concurrent writes. Uses a hybrid approach:
  1. Scan existing documents, build in temporary LSM-Tree
  2. Replay WAL entries that occurred during scan
  3. Atomic swap into live index catalog

---

## 9. Transaction Support (ACID)

### 9.1 MVCC Implementation

Every write creates a new version of the document. Versions are ordered by `txn_id` (monotonically increasing uint64).

**Version visibility rules:**
- A transaction `T` sees version `V` if:
  - `V.txn_id < T.start_txn_id` (version created before T started)
  - AND `V` is committed (not aborted, not in-progress)
  - AND no newer committed version exists with `txn_id < T.start_txn_id`

**Transaction lifecycle:**

```
BEGIN                            COMMIT / ABORT
  │                                │
  ▼                                ▼
┌──────────┐    ┌──────────┐    ┌──────────────┐
│ Allocate │───▶│ Execute  │───▶│ Validate &   │
│ txn_id   │    │ Reads &  │    │ Write to WAL │
│ snapshot │    │ Writes   │    │ (atomic)     │
└──────────┘    │ (buffer) │    └──────────────┘
                └──────────┘
                Writes buffered in memory.
                On commit: atomic WAL write + memtable insert.
                On abort: discard buffer, no I/O.
```

### 9.2 Isolation Levels

| Level | Behavior | Default |
|-------|----------|---------|
| **Snapshot Isolation** | Each transaction reads from its start-time snapshot. Write-write conflicts detected at commit. | ✅ Yes |
| **Read Committed** | Each read sees the latest committed data at read time. | No |
| **Serializable** | Full conflict detection (read-write + write-read + write-write). SSI (Serializable Snapshot Isolation). | No |

### 9.3 Conflict Detection

- **Write-write conflicts**: Two transactions writing to the same document. First committer wins, second aborts and retries.
- **Detection mechanism**: At commit time, check if any document in the write set was modified by another committed transaction since our snapshot.

### 9.4 Garbage Collection

- Background goroutine periodically scans for old versions
- Safe to delete version `V` if:
  - A newer committed version exists
  - No active transaction has `start_txn_id <= V.txn_id`
- GC interval configurable (default: 60 seconds)
- Low watermark: `min(all_active_txn.start_txn_id)` — nothing below this can be seen

---

## 10. Replication (Raft)

### 10.1 Replica Set Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Node 1        │     │   Node 2        │     │   Node 3        │
│   (PRIMARY)     │◄───▶│   (SECONDARY)   │◄───▶│   (SECONDARY)   │
│                 │     │                 │     │                 │
│  Accepts reads  │     │  Accepts reads  │     │  Accepts reads  │
│  Accepts writes │     │  (configurable) │     │  (configurable) │
│                 │     │                 │     │                 │
│  Raft LEADER    │     │  Raft FOLLOWER  │     │  Raft FOLLOWER  │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         ▲                       ▲                       ▲
         │          Raft RPC (gRPC)                      │
         └───────────────────────┴───────────────────────┘
```

### 10.2 Raft Protocol

Full implementation of the Raft consensus algorithm:

- **Leader Election**: Randomized election timeout (150-300ms default), pre-vote extension
- **Log Replication**: Batched append entries, pipelining for throughput
- **Safety**: Leader completeness, election restriction, commitment rules
- **Membership Changes**: Joint consensus for safe add/remove of nodes
- **Snapshot Transfer**: Chunked transfer for large state, resume on failure
- **Leader Lease**: Time-based lease for fast local reads on leader (optional)

### 10.3 Operation Log (Oplog)

The Raft log serves as the oplog. Each entry contains:

```
OplogEntry {
    term      : uint64          // Raft term
    index     : uint64          // Raft log index
    timestamp : Timestamp       // MongoDB-compatible timestamp
    op_type   : insert | update | delete | command
    namespace : string          // "db.collection"
    document  : BSON            // For insert: full doc. For update: change doc.
    filter    : BSON            // For update/delete: query filter
    txn_id    : uint64          // Transaction identifier (0 for single-doc ops)
}
```

### 10.4 Read Preferences

| Preference | Behavior |
|------------|----------|
| `primary` | Always read from leader. Strongest consistency. |
| `primaryPreferred` | Read from leader; fall back to follower if unavailable. |
| `secondary` | Read from followers only. May see stale data. |
| `secondaryPreferred` | Read from followers; fall back to leader. |
| `nearest` | Read from node with lowest network latency. |

### 10.5 Write Concern

| Level | Behavior |
|-------|----------|
| `w: 1` | Acknowledged after leader persists (default) |
| `w: "majority"` | Acknowledged after majority of nodes persist |
| `w: N` | Acknowledged after N nodes persist |
| `j: true` | Acknowledged after WAL fsync (durable) |

---

## 11. Sharding & Horizontal Scaling

### 11.1 Sharded Cluster Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    Config Replica Set                      │
│            (shard metadata, chunk map, routing)            │
│              3+ nodes with Raft consensus                  │
└──────────────────────────┬───────────────────────────────┘
                           │
┌──────────────────────────▼───────────────────────────────┐
│                    Router (built into binary)              │
│              Routes queries to correct shard(s)            │
│              Merges results from multiple shards           │
└─────┬─────────────────┬──────────────────┬───────────────┘
      │                 │                  │
      ▼                 ▼                  ▼
┌──────────┐     ┌──────────┐     ┌──────────────┐
│ Shard 1  │     │ Shard 2  │     │ Shard 3      │
│ (Replica │     │ (Replica │     │ (Replica     │
│  Set)    │     │  Set)    │     │  Set)        │
└──────────┘     └──────────┘     └──────────────┘
```

**Key difference from MongoDB**: No separate `mongos` process. Router is built into every Mammoth node. Any node can route queries.

### 11.2 Shard Key Strategies

| Strategy | Pros | Cons |
|----------|------|------|
| **Range-based** | Efficient range scans, locality | Hotspots on monotonic keys |
| **Hash-based** | Uniform distribution | No range scan optimization |
| **Zone-based** | Geographic locality | Manual configuration |

### 11.3 Chunk Management

- Default chunk size: 128MB (configurable)
- Auto-split: When chunk exceeds threshold, split at median key
- Auto-balance: Background balancer moves chunks between shards for even distribution
- Migration: Chunk data streamed to destination shard, atomic metadata update on config server

---

## 12. Security

### 12.1 Authentication

| Method | Description |
|--------|-------------|
| **SCRAM-SHA-256** | Default. Challenge-response, password-based. MongoDB compatible. |
| **x509** | TLS certificate-based authentication. |
| **Internal** | Keyfile-based authentication between replica set members. |

### 12.2 Authorization (RBAC)

Built-in roles matching MongoDB:

- `read`, `readWrite` (per database)
- `dbAdmin`, `userAdmin` (per database)
- `clusterAdmin`, `clusterManager`, `clusterMonitor`
- `root` (superuser)
- Custom roles with fine-grained privilege grants

### 12.3 Encryption

| Layer | Implementation |
|-------|---------------|
| **In Transit** | TLS 1.3 for all connections (client, inter-node). Auto-generated self-signed certs for dev. |
| **At Rest** | AES-256-GCM encryption of SSTable files and WAL segments. Key rotation support. |
| **Field Level** | Client-side field-level encryption (CSFLE) compatible with MongoDB FLE drivers. |

### 12.4 Audit Log

- Configurable audit log for security-relevant operations
- JSON format, rotatable
- Events: auth success/failure, CRUD on sensitive collections, admin commands, schema changes

---

## 13. Observability

### 13.1 Metrics (Prometheus)

Exposed at `GET /metrics` endpoint:

**Storage:**
- `mammoth_storage_write_bytes_total` — Total bytes written
- `mammoth_storage_read_bytes_total` — Total bytes read
- `mammoth_storage_sstable_count{level}` — SSTable count per level
- `mammoth_storage_compaction_duration_seconds` — Compaction time histogram
- `mammoth_storage_memtable_size_bytes` — Current memtable size
- `mammoth_storage_bloom_filter_hit_total` — Bloom filter true negatives
- `mammoth_storage_bloom_filter_miss_total` — Bloom filter false positives
- `mammoth_storage_block_cache_hit_ratio` — Block cache hit rate

**Query:**
- `mammoth_query_duration_seconds{operation}` — Query latency histogram
- `mammoth_query_total{operation, status}` — Query count by operation and status
- `mammoth_query_slow_total` — Queries exceeding slow threshold
- `mammoth_query_active_cursors` — Open cursor count

**Replication:**
- `mammoth_raft_term` — Current Raft term
- `mammoth_raft_commit_index` — Committed log index
- `mammoth_raft_apply_index` — Applied log index
- `mammoth_raft_replication_lag_seconds` — Follower lag

**Connections:**
- `mammoth_connections_current` — Active connections
- `mammoth_connections_total` — Total connections since start

### 13.2 Structured Logging

JSON-formatted logs with configurable levels:

```json
{
  "ts": "2025-01-15T10:30:00.123Z",
  "level": "info",
  "component": "storage.compaction",
  "msg": "compaction completed",
  "level_from": 0,
  "level_to": 1,
  "files_in": 4,
  "files_out": 1,
  "bytes_in": 268435456,
  "bytes_out": 201326592,
  "duration_ms": 1523
}
```

### 13.3 Health Endpoint

`GET /health` returns:

```json
{
  "status": "ok",
  "version": "0.1.0",
  "uptime_seconds": 86400,
  "replication": {
    "role": "primary",
    "term": 5,
    "members": 3,
    "healthy": 3
  },
  "storage": {
    "data_size_bytes": 1073741824,
    "index_size_bytes": 268435456,
    "sstable_count": 42,
    "memtable_size_bytes": 33554432
  }
}
```

---

## 14. Embedded Mode

### 14.1 API Design

```go
import "github.com/mammothengine/mammoth/embed"

// Open a local database (no network server)
db, err := embed.Open("/path/to/data", &embed.Options{
    CacheSize:    256 * 1024 * 1024, // 256MB block cache
    WALSync:      embed.SyncFull,     // fsync on every write
    Compression:  embed.Snappy,
    EncryptionKey: key,               // nil for no encryption
})
defer db.Close()

// Get a collection
users := db.Collection("mydb", "users")

// Insert
id, err := users.InsertOne(bson.M{
    "name": "Ersin",
    "email": "ersin@ecostack.dev",
    "tags": []string{"go", "infrastructure"},
})

// Find
cursor, err := users.Find(bson.M{
    "tags": bson.M{"$in": []string{"go"}},
})
defer cursor.Close()

for cursor.Next() {
    var user User
    cursor.Decode(&user)
}

// Aggregate
results, err := users.Aggregate([]bson.M{
    {"$match": bson.M{"tags": "go"}},
    {"$group": bson.M{"_id": "$city", "count": bson.M{"$sum": 1}}},
    {"$sort": bson.M{"count": -1}},
})

// Transactions
err = db.Transaction(func(tx *embed.Tx) error {
    tx.Collection("mydb", "accounts").UpdateOne(
        bson.M{"_id": from},
        bson.M{"$inc": bson.M{"balance": -amount}},
    )
    tx.Collection("mydb", "accounts").UpdateOne(
        bson.M{"_id": to},
        bson.M{"$inc": bson.M{"balance": amount}},
    )
    return nil // commit; return error to abort
})
```

### 14.2 Embedded Use Cases

- **Edge/IoT**: Run on Raspberry Pi, ARM devices, embedded Linux
- **Desktop Applications**: Local data storage, offline-first apps
- **Testing**: In-process database for unit/integration tests
- **CLI Tools**: Data processing pipelines with document queries
- **Serverless Functions**: Cold-start friendly (no TCP connection overhead)

---

## 15. CLI & Administration

### 15.1 CLI Commands

```bash
# Start server
mammoth server --config /etc/mammoth/config.toml

# Start with inline config
mammoth server --port 27017 --data-dir /var/lib/mammoth --replica-set rs0

# Replica set management
mammoth rs init --members node1:27017,node2:27017,node3:27017
mammoth rs status
mammoth rs add node4:27017
mammoth rs remove node3:27017
mammoth rs stepdown

# Sharding
mammoth shard enable --database mydb
mammoth shard add-shard rs1/node1:27017,node2:27017
mammoth shard key --collection mydb.users --key '{"email": "hashed"}'
mammoth shard status

# Backup & Restore
mammoth backup --output /backup/2025-01-15.mammoth
mammoth restore --input /backup/2025-01-15.mammoth

# User management
mammoth user create --username admin --role root
mammoth user list
mammoth user delete --username olduser

# Diagnostics
mammoth status
mammoth top
mammoth stats --collection mydb.users
mammoth compact --force
mammoth validate --collection mydb.users

# Version
mammoth version
```

### 15.2 Configuration File

```toml
# /etc/mammoth/config.toml

[server]
bind = "0.0.0.0"
port = 27017
max_connections = 10000

[storage]
data_dir = "/var/lib/mammoth/data"
engine = "lsm"                          # "lsm" (default)
wal_sync = "full"                       # "full" | "batch" (every 100ms) | "none"
memtable_size = "64MB"
block_cache_size = "512MB"
compression = "snappy"                  # "snappy" | "lz4" | "zstd" | "none"
encryption_key_file = ""                # Path to encryption key (empty = disabled)

[storage.compaction]
strategy = "leveled"                    # "leveled" | "size-tiered" | "fifo"
l0_compaction_trigger = 4
max_levels = 7
level_size_multiplier = 10
max_compaction_concurrency = 2

[replication]
enabled = false
replica_set = ""                        # Replica set name
members = []                            # ["node1:27018", "node2:27018"]
election_timeout = "300ms"
heartbeat_interval = "100ms"
snapshot_interval = "10m"

[sharding]
enabled = false
config_servers = []                     # ["cfg1:27019", "cfg2:27019", "cfg3:27019"]
chunk_size = "128MB"
balancer_enabled = true

[security]
auth_enabled = false                    # Enable authentication
tls_enabled = false
tls_cert_file = ""
tls_key_file = ""
tls_ca_file = ""
audit_log = ""                          # Path to audit log (empty = disabled)

[observability]
metrics_enabled = true
metrics_port = 9090
log_level = "info"                      # "debug" | "info" | "warn" | "error"
log_format = "json"                     # "json" | "text"
slow_query_threshold = "100ms"
profiling_level = 0                     # 0=off, 1=slow, 2=all

[admin]
enabled = true
port = 8080                             # Admin web UI port
```

### 15.3 Admin Web UI

Built-in SPA (embedded in binary via `embed.FS`):

- **Dashboard**: Server stats, memory, connections, ops/sec, replication lag
- **Collections**: Browse, query, create, drop
- **Indexes**: View, create, drop, usage stats
- **Query Playground**: Interactive query editor with syntax highlighting
- **Users & Roles**: Manage authentication and authorization
- **Replica Set**: Member status, lag, force election
- **Profiler**: Slow query log viewer
- **Logs**: Real-time structured log viewer

---

## 16. Client SDKs

### 16.1 Strategy

**Phase 1**: Full MongoDB wire protocol compatibility means all existing MongoDB drivers work immediately. No custom SDK needed.

**Phase 2**: Native Mammoth SDK for Go with embedded mode support and Mammoth-specific features.

**Phase 3**: Thin Mammoth SDKs for other languages exposing Mammoth-specific features (admin, embedded mode via FFI, etc.).

### 16.2 Verified Driver Compatibility

| Language | Driver | Status |
|----------|--------|--------|
| Go | `go.mongodb.org/mongo-driver` | Primary target |
| Node.js | `mongodb` (npm) | Primary target |
| Python | `pymongo` | Primary target |
| Java | `mongodb-driver-sync` | Verified |
| PHP | `mongodb/mongodb` | Verified |
| Rust | `mongodb` (crate) | Verified |
| C# | `MongoDB.Driver` | Verified |
| Ruby | `mongo` (gem) | Verified |

---

## 17. Performance Targets

### 17.1 Single-Node Benchmarks (Target)

Hardware baseline: 8 vCPU, 32GB RAM, NVMe SSD.

| Operation | Target | MongoDB 7.0 Reference |
|-----------|--------|----------------------|
| Single insert | < 100μs (p50) | ~150μs |
| Batch insert (1000 docs) | < 5ms | ~8ms |
| Point read (_id lookup) | < 50μs (p50) | ~80μs |
| Range scan (1000 docs) | < 2ms | ~3ms |
| Secondary index lookup | < 100μs (p50) | ~120μs |
| Aggregation (simple group) | < 10ms (10K docs) | ~15ms |
| Write throughput | > 100K ops/sec | ~80K ops/sec |
| Read throughput | > 200K ops/sec | ~150K ops/sec |
| Memory baseline | < 50MB | ~400MB+ |
| Binary size | < 30MB | N/A (multi-process) |

### 17.2 Benchmark Suite

Standard benchmarks run on every release:

- **YCSB** (Yahoo Cloud Serving Benchmark): Workloads A-F
- **Custom document workload**: Mixed CRUD with various document sizes (1KB, 10KB, 100KB, 1MB)
- **Aggregation benchmark**: Pipeline complexity scaling
- **Concurrent benchmark**: 1, 10, 100, 1000 concurrent clients
- **Recovery benchmark**: WAL replay time, snapshot restore time

---

## 18. Deployment & Distribution

### 18.1 Binary Builds

| OS | Arch | Format |
|----|------|--------|
| Linux | amd64, arm64 | Static binary, .deb, .rpm, .apk |
| macOS | amd64 (Intel), arm64 (Apple Silicon) | Binary, Homebrew formula |
| Windows | amd64 | Binary, .msi installer |

### 18.2 Container Images

```dockerfile
FROM scratch
COPY mammoth /mammoth
EXPOSE 27017 8080 9090
ENTRYPOINT ["/mammoth", "server"]
```

- **Scratch-based**: Smallest possible image (< 30MB)
- **Alpine-based**: For debugging (with shell)
- **Distroless**: For production security

Published to: Docker Hub, GitHub Container Registry.

### 18.3 Package Managers

```bash
# Homebrew
brew install mammothengine/tap/mammoth

# APT
apt install mammoth-engine

# Go install (embedded mode library)
go get github.com/mammothengine/mammoth/embed@latest
```

---

## 19. Roadmap & Milestones

### v0.1.0 — Foundation (Storage Engine)

- [ ] LSM-Tree storage engine (memtable, SSTable, WAL)
- [ ] Concurrent skip list memtable
- [ ] Leveled compaction
- [ ] Bloom filters
- [ ] Block cache (LRU)
- [ ] Snappy compression
- [ ] Basic CRUD: Put, Get, Delete, Scan
- [ ] BSON encoder/decoder (pure Go)
- [ ] Internal binary format (MBF)
- [ ] ObjectID generation
- [ ] Basic unit + integration tests

### v0.2.0 — Query Engine & Wire Protocol

- [ ] MongoDB OP_MSG wire protocol
- [ ] CRUD commands: find, insert, update, delete
- [ ] Query parser (comparison, logical, element operators)
- [ ] Query planner (collection scan, index scan)
- [ ] Cursor management (getMore, killCursors)
- [ ] Primary index (_id)
- [ ] Secondary indexes (single field, compound)
- [ ] findAndModify
- [ ] mongosh compatibility

### v0.3.0 — Aggregation & Transactions

- [ ] Aggregation pipeline (match, project, group, sort, limit, skip, unwind)
- [ ] MVCC transaction support
- [ ] Multi-document transactions
- [ ] Snapshot isolation
- [ ] Update operators ($set, $inc, $push, etc.)
- [ ] $lookup (aggregation join)
- [ ] Multikey indexes
- [ ] TTL indexes

### v0.4.0 — Replication

- [ ] Raft consensus implementation
- [ ] Leader election & log replication
- [ ] Oplog
- [ ] Replica set commands (initiate, status, stepDown)
- [ ] Read preferences
- [ ] Write concern
- [ ] Snapshot transfer

### v0.5.0 — Security & Observability

- [ ] SCRAM-SHA-256 authentication
- [ ] RBAC (role-based access control)
- [ ] TLS for all connections
- [ ] Encryption at rest (AES-256-GCM)
- [ ] Prometheus metrics endpoint
- [ ] Structured JSON logging
- [ ] Slow query profiler
- [ ] Health endpoint

### v0.6.0 — Advanced Features

- [ ] Text indexes (full-text search)
- [ ] Geospatial indexes (2dsphere)
- [ ] Change streams
- [ ] Schema validation (JSON Schema)
- [ ] Capped collections
- [ ] GridFS-compatible file storage
- [ ] Partial indexes, wildcard indexes
- [ ] Embedded mode (Go library API)

### v0.7.0 — Sharding

- [ ] Config server (metadata store)
- [ ] Router (query routing)
- [ ] Hash-based sharding
- [ ] Range-based sharding
- [ ] Auto-split & auto-balance
- [ ] Chunk migration

### v0.8.0 — Admin & Ecosystem

- [ ] Admin web UI (SPA)
- [ ] CLI tools (backup, restore, user management)
- [ ] Audit logging
- [ ] YCSB benchmark suite
- [ ] Comprehensive documentation
- [ ] Docker images & Helm charts

### v1.0.0 — Production Ready

- [ ] Full MongoDB 6.0 compatibility test suite pass
- [ ] Jepsen testing for consistency guarantees
- [ ] Performance benchmarks published
- [ ] Security audit completed
- [ ] Stable API, no breaking changes
- [ ] LTS release with 2-year support commitment

---

## 20. References & Inspirations

### Academic Papers

- "Bigtable: A Distributed Storage System for Structured Data" (Google, 2006)
- "The Log-Structured Merge-Tree (LSM-Tree)" (O'Neil et al., 1996)
- "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout, 2014) — Raft
- "A Critique of ANSI SQL Isolation Levels" (Berenson et al., 1995)
- "Serializable Snapshot Isolation in PostgreSQL" (Ports & Grittner, 2012)
- "WiscKey: Separating Keys from Values in SSD-Conscious Storage" (Lu et al., 2016)
- "Monkey: Optimal Navigable Key-Value Store" (Dayan et al., 2017) — Bloom filter tuning

### Implementation References

- LevelDB design document (Google)
- RocksDB Wiki (Facebook/Meta)
- MongoDB wire protocol specification
- BSON specification (bsonspec.org)
- etcd Raft implementation (Go reference)
- S2 Geometry Library (Google)

### Existing Projects (for competitive analysis, NOT code reuse)

- MongoDB (SSPL) — the incumbent
- FerretDB (Apache 2.0) — MongoDB-compatible proxy over Postgres
- TiKV (Apache 2.0) — Raft-based KV store in Rust
- CockroachDB (BSL) — distributed SQL with Raft
- Pebble (BSD) — Go LSM-Tree engine (by CockroachDB team)
- BadgerDB (Apache 2.0) — Go KV store (WiscKey-inspired)
- BoltDB (MIT) — Go B+Tree embedded database

---

*This specification is a living document. It will evolve as implementation progresses and design decisions are validated through benchmarking and real-world testing.*

**Built with #NOFORKANYMORE philosophy by ECOSTACK TECHNOLOGY OÜ.**
