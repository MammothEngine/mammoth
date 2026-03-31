# Architecture Overview

Technical architecture documentation for Mammoth Engine.

## Table of Contents

- [System Architecture](#system-architecture)
- [Storage Engine](#storage-engine)
- [BSON Implementation](#bson-implementation)
- [Wire Protocol](#wire-protocol)
- [Query Processing](#query-processing)
- [Indexing](#indexing)
- [Replication](#replication)
- [Sharding](#sharding)
- [Transactions](#transactions)
- [Security](#security)
- [Performance Characteristics](#performance-characteristics)

## System Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Client Layer                          │
│              (MongoDB Drivers, mongosh, etc.)               │
└──────────────────────┬──────────────────────────────────────┘
                       │ MongoDB Wire Protocol
┌──────────────────────┴──────────────────────────────────────┐
│                     Network Layer                            │
│              TCP Server, Connection Pool                     │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────────┐
│                    Protocol Handler                          │
│     Command Parsing, Session Management, Auth Check         │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────────┐
│                   Query Processor                            │
│     Planner, Optimizer, Executor, Aggregation               │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────────┐
│                   Storage Engine                             │
│   MemTable → WAL → SSTable → Compaction → Manifest          │
└─────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Description |
|-----------|-------------|
| **Wire Protocol** | MongoDB-compatible network protocol implementation |
| **BSON** | Binary JSON encoding/decoding |
| **Query Parser** | Filter, projection, sort parsing |
| **Query Planner** | Cost-based plan selection |
| **Query Executor** | Plan execution, cursor management |
| **Index Catalog** | Index metadata and maintenance |
| **Storage Engine** | LSM-tree based persistent storage |
| **Replication** | Raft consensus for replica sets |
| **Sharding** | Range/hash-based data partitioning |

## Storage Engine

### LSM-Tree Architecture

```
┌─────────────────────────────────────────────────────┐
│                    MemTable                          │
│              (In-Memory Skip List)                   │
│  ┌─────┐  ┌─────┐  ┌─────┐  ┌─────┐  ┌─────┐     │
│  │ k1  │→ │ k2  │→ │ k3  │→ │ k4  │→ │ k5  │→ ...│
│  └─────┘  └─────┘  └─────┘  └─────┘  └─────┘     │
└────────┬────────────────────────────────────────────┘
         │ Flush (when full)
┌────────┴────────────────────────────────────────────┐
│                    WAL                               │
│            (Write-Ahead Log)                         │
│   append-only log for durability                     │
└────────┬────────────────────────────────────────────┘
         │ Background flush
┌────────┴────────────────────────────────────────────┐
│              SSTable Files (L0-L6)                   │
│  ┌──────────────┐                                    │
│  │    Level 0   │  ← New files from MemTable         │
│  └──────────────┘                                    │
│  ┌──────────────┐                                    │
│  │    Level 1   │  ← Compacted from L0               │
│  └──────────────┘                                    │
│  ┌──────────────┐                                    │
│  │    Level 2   │  ← Compacted from L1               │
│  └──────────────┘                                    │
│         ...                                          │
└─────────────────────────────────────────────────────┘
```

### Write Path

1. **Client Write** → Received by wire protocol handler
2. **WAL Append** → Write to write-ahead log (fsync for durability)
3. **MemTable Insert** → Add to in-memory skip list
4. **Ack Response** → Return success to client
5. **Background Flush** → When MemTable full, flush to SSTable (L0)

### Read Path

1. **Client Read** → Received by wire protocol handler
2. **MemTable Check** → Search in-memory structure first
3. **Block Cache Check** → Check if data in cache
4. **SSTable Search** → Binary search through levels (L0→L6)
5. **Bloom Filter Check** → Skip SSTables that definitely don't have key
6. **Return Result** → Return value to client

### Compaction Strategy

Mammoth uses **Leveled Compaction**:

- **L0**: Recently flushed files (may overlap key ranges)
- **L1-L6**: Sorted files with non-overlapping key ranges
- **Compaction Triggers**: Size threshold reached or manual trigger
- **Compaction Process**: Merge files from level N into level N+1

```
Compaction Flow:
┌─────────┐     ┌─────────┐     ┌─────────┐
│ L0 File │  →  │ L1 File │  →  │ L2 File │
│ (1 MB)  │     │ (10 MB) │     │ (100 MB)│
└─────────┘     └─────────┘     └─────────┘
```

## BSON Implementation

### BSON Document Structure

```
Document:
┌────────────────┬──────────────┬─────────────┬───────┐
│ int32: size    │ elements     │ 0x00 (null) │ total │
│ (4 bytes)      │              │ terminator  │ size  │
└────────────────┴──────────────┴─────────────┴───────┘

Element:
┌──────────┬────────────────┬──────────┐
│ byte:    │ cstring:       │ value    │
│ type     │ field name     │ (typed)  │
└──────────┴────────────────┴──────────┘

Type Byte Values:
0x01 - Double (8 bytes)
0x02 - String (4-byte length + cstring)
0x03 - Document (embedded document)
0x04 - Array
0x05 - Binary
0x07 - ObjectId (12 bytes)
0x08 - Boolean (1 byte)
0x09 - UTC DateTime (8 bytes)
0x0A - Null
0x0B - Regex
0x10 - Int32 (4 bytes)
0x11 - Timestamp (8 bytes)
0x12 - Int64 (8 bytes)
0x13 - Decimal128 (16 bytes)
```

### BSON Encoding Performance

- Zero-allocation path for common operations
- Object pooling for document reuse
- Fast path for primitive types
- Skip-list based key lookup (O(log n))

## Wire Protocol

### Message Structure

MongoDB Wire Protocol (OP_MSG):

```
┌────────────────┬──────────────┬──────────────┬─────────────┐
│ int32: msgLen  │ int32: reqID │ int32: resp │ int32: op   │
│                │              │ To          │ Code        │
├────────────────┼──────────────┼──────────────┼─────────────┤
│ bitFlags:      │ sections[]   │ checksum (  │             │
│ flags          │              │ optional)   │             │
└────────────────┴──────────────┴──────────────┴─────────────┘

Section Types:
0 - Body (single BSON document)
1 - Document Sequence (multiple BSON documents)
```

### Command Processing Flow

```
Client Request
      ↓
OP_MSG Parse
      ↓
Command Dispatcher
      ↓
┌─────┴─────┬─────────┬─────────┬─────────┐
↓           ↓         ↓         ↓         ↓
find      insert   update   delete    aggregate
      ↓
Handler Execution
      ↓
BSON Response
      ↓
OP_MSG Encode
      ↓
Client Response
```

### Supported Operations

| Category | Operations |
|----------|-----------|
| **CRUD** | find, insert, update, delete, count, distinct |
| **Bulk** | insertMany, updateMany, deleteMany, bulkWrite |
| **Index** | createIndexes, dropIndexes, listIndexes |
| **Admin** | serverStatus, dbStats, collStats, validate |
| **Auth** | authenticate, createUser, dropUser, grantRoles |
| **Repl** | replSetInitiate, replSetStatus, replSetReconfig |
| **Shard** | enableSharding, shardCollection, balancerControl |
| **Tx** | startTransaction, commitTransaction, abortTransaction |

## Query Processing

### Query Planner Architecture

```
Query
  ↓
Parser → Filter AST
  ↓
Index Selector → Candidate Indexes
  ↓
Cost Estimator → Cost for each candidate
  ↓
Plan Selector → Best Plan
  ↓
Executor → Results
```

### Plan Types

1. **COLLSCAN** - Full collection scan
2. **IXSCAN** - Index scan
3. **FETCH** - Document retrieval after index
4. **PROJECTION** - Field filtering
5. **SORT** - In-memory or index sort
6. **LIMIT** - Result limiting
7. **SKIP** - Result skipping
8. **AGGREGATE** - Pipeline stages

### Optimization Techniques

- **Index Selection**: Cost-based using statistics
- **Predicate Pushdown**: Apply filters early
- **Projection Pushdown**: Fetch only needed fields
- **Sort Elimination**: Use index order when possible
- **Limit Pushdown**: Stop early when limit specified

## Indexing

### Index Types

| Type | Structure | Use Case |
|------|-----------|----------|
| **Single Field** | B-tree | Equality, range queries |
| **Compound** | B-tree | Multi-field queries |
| **Multikey** | B-tree | Array field queries |
| **Text** | Inverted index | Full-text search |
| **Hashed** | Hash table | Sharding distribution |
| **Geospatial** | R-tree | Location queries |
| **Wildcard** | B-tree | Dynamic schema |
| **TTL** | B-tree + expire | Auto-expiration |

### Index Storage

```
Index Key Format:
┌────────────────┬────────────────┬────────────────┐
│ Namespace      │ Index Name     │ Encoded Values │ Document ID │
│ Prefix         │ Separator      │ (typed)        │ (12 bytes)  │
└────────────────┴────────────────┴────────────────┴─────────────┘

Key Encoding:
- Null byte (0x00) - null
- False (0x01), True (0x02) - booleans
- Type tag + bytes - numbers (sortable encoding)
- Length + UTF-8 bytes - strings
```

### Index Maintenance

- **Insertion**: Add index entry for each indexed field
- **Update**: Remove old entries, add new entries
- **Deletion**: Remove all index entries for document
- **Build**: Scan collection and build index incrementally

## Replication

### Raft Consensus

```
┌─────────┐          ┌─────────┐          ┌─────────┐
│ Node 1  │ ←──────→ │ Node 2  │ ←──────→ │ Node 3  │
│ Leader  │   RPC    │ Follower│   RPC    │ Follower│
└─────────┘          └─────────┘          └─────────┘
       │                  │                  │
       └──────────────────┼──────────────────┘
                          │
                    ┌─────┴─────┐
                    │   Log     │
                    │  Entries  │
                    └───────────┘
```

### Replication Flow

1. **Client Write** → Sent to Leader
2. **Append Entry** → Add to leader's log
3. **Replicate** → Send to followers
4. **Ack** → Followers acknowledge
5. **Commit** → Majority acks received
6. **Apply** → Apply to state machine
7. **Respond** → Return to client

### Log Structure

```
Log Entry:
┌──────────┬──────────┬──────────┬────────────┐
│ Index    │ Term     │ Command  │ Timestamp  │
│ (uint64) │ (uint64) │ (bytes)  │ (int64)    │
└──────────┴──────────┴──────────┴────────────┘

Committed entries are applied to the storage engine.
```

## Sharding

### Architecture

```
                    ┌─────────────┐
                    │   Client    │
                    └──────┬──────┘
                           │
                    ┌──────┴──────┐
                    │  Router     │
                    │  (mongos)   │
                    └──────┬──────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
   ┌────┴────┐       ┌────┴────┐       ┌────┴────┐
   │ Config  │       │ Config  │       │ Config  │
   │ Server  │       │ Server  │       │ Server  │
   └─────────┘       └─────────┘       └─────────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
   ┌────┴────┐       ┌────┴────┐       ┌────┴────┐
   │ Shard 1 │       │ Shard 2 │       │ Shard 3 │
   │ (RS)    │       │ (RS)    │       │ (RS)    │
   └─────────┘       └─────────┘       └─────────┘
```

### Chunk Management

```
Chunk Structure:
┌──────────────┬──────────────┬──────────────┬──────────────┐
│ Namespace    │ Min Key      │ Max Key      │ Shard ID     │
│ (string)     │ (bson)       │ (bson)       │ (string)     │
└──────────────┴──────────────┴──────────────┴──────────────┘

Balancer Process:
- Monitors chunk distribution
- Moves chunks from overloaded shards
- Runs during maintenance window
- Respects chunk size (default 64MB)
```

### Shard Key Selection

**Hashed Sharding**:
- Use: Even distribution, random access
- Syntax: `{ _id: "hashed" }`
- Pros: Write distribution
- Cons: Range queries inefficient

**Range Sharding**:
- Use: Time-series, ordered data
- Syntax: `{ timestamp: 1 }`
- Pros: Range queries efficient
- Cons: Hot spotting

## Transactions

### ACID Guarantees

- **Atomicity**: All-or-nothing operations
- **Consistency**: Constraints always satisfied
- **Isolation**: Snapshot isolation
- **Durability**: Committed data survives crashes

### Transaction Flow

```
Begin Transaction
      ↓
┌──────────────────────────────────────┐
│ Operations in Session Context        │
│ - Writes accumulate in transaction   │
│ - Reads see snapshot at start        │
└──────────────────────────────────────┘
      ↓
Commit / Abort
      ↓
Commit: Apply all changes atomically
Abort: Discard all changes
```

### Snapshot Isolation

- Reads see consistent snapshot
- Writes don't conflict until commit
- Conflicts resolved at commit time
- MVCC for concurrent transactions

## Security

### Authentication

**SCRAM-SHA-256**:
```
Client                           Server
   │                                │
   │── Client First Message ───────→│
   │                                │
   │←── Server First Message ───────│
   │                                │
   │── Client Final Message ───────→│
   │                                │
   │←── Server Final Message ───────│
```

**x.509 Certificate**:
- Client certificate authentication
- Mutual TLS verification
- Certificate-based identity

### Authorization

**RBAC Model**:
- Users have roles
- Roles have privileges
- Privileges grant actions on resources

**Built-in Roles**:
- `read` - Read-only access
- `readWrite` - Read and write
- `dbAdmin` - Database administration
- `userAdmin` - User management
- `clusterAdmin` - Cluster-wide administration

### Encryption

- **TLS/SSL**: Transport encryption
- **Encryption at Rest**: SSTable encryption
- **Field-Level Encryption**: Per-field encryption

## Performance Characteristics

### Throughput

| Operation | Single Node | 3-Node Replica |
|-----------|-------------|----------------|
| **Write** | ~50K ops/sec | ~30K ops/sec |
| **Read** | ~100K ops/sec | ~100K ops/sec |
| **Bulk Load** | ~200MB/sec | ~150MB/sec |
| **Range Scan** | ~50MB/sec | ~50MB/sec |

### Latency

| Operation | P50 | P99 |
|-----------|-----|-----|
| **Point Read** | 0.5ms | 2ms |
| **Point Write** | 1ms | 5ms |
| **Range Query (100 docs)** | 2ms | 10ms |
| **Index Lookup** | 1ms | 3ms |
| **Aggregation** | 10ms | 100ms |

### Scalability

| Metric | Capacity |
|--------|----------|
| **Max Database Size** | Unlimited (sharded) |
| **Max Collection Size** | Unlimited (sharded) |
| **Max Document Size** | 16 MB |
| **Max Indexes** | 64 per collection |
| **Max Shard Count** | 1000 |
| **Max Chunk Size** | 64 MB |

### Memory Usage

| Component | Memory |
|-----------|--------|
| **MemTable (per table)** | 64MB default |
| **Block Cache** | 256MB default |
| **Query Plan Cache** | 1000 entries |
| **Index Cache** | Auto-managed |
| **Connection Overhead** | ~1MB per connection |

## Design Principles

1. **Durability First**: WAL fsync before acknowledging writes
2. **Read Optimization**: Aggressive caching, bloom filters
3. **Write Amplification Trade-off**: Compaction optimizes reads
4. **Horizontal Scalability**: Sharding for unlimited scale
5. **Fault Tolerance**: Raft consensus for availability
6. **MongoDB Compatibility**: Drop-in replacement

## Comparison with MongoDB

| Feature | Mammoth | MongoDB |
|---------|---------|---------|
| Storage Engine | LSM-tree | B-tree (WiredTiger) |
| Replication | Raft | Custom consensus |
| Sharding | Yes | Yes |
| Transactions | Yes | Yes |
| Aggregation | Basic | Full feature set |
| Change Streams | Yes | Yes |
| Text Search | Basic | Atlas Search |
| Geospatial | Basic | Full feature set |

