# Mammoth Engine — IMPLEMENTATION GUIDE

> Technical implementation details for building Mammoth Engine.
> This document bridges SPECIFICATION.md (what to build) and TASKS.md (task breakdown).

**Version:** 0.1.0-draft
**Prerequisite:** Read SPECIFICATION.md first.

---

## Table of Contents

1. [Development Environment](#1-development-environment)
2. [Storage Engine Implementation](#2-storage-engine-implementation)
3. [BSON Codec Implementation](#3-bson-codec-implementation)
4. [Mammoth Binary Format (MBF)](#4-mammoth-binary-format-mbf)
5. [Wire Protocol Implementation](#5-wire-protocol-implementation)
6. [Query Engine Implementation](#6-query-engine-implementation)
7. [Index Implementation](#7-index-implementation)
8. [Transaction Manager Implementation](#8-transaction-manager-implementation)
9. [Replication (Raft) Implementation](#9-replication-raft-implementation)
10. [Sharding Implementation](#10-sharding-implementation)
11. [Server & Networking](#11-server--networking)
12. [Embedded Mode Implementation](#12-embedded-mode-implementation)
13. [Admin Web UI](#13-admin-web-ui)
14. [Security Implementation](#14-security-implementation)
15. [Observability Implementation](#15-observability-implementation)
16. [Testing Strategy](#16-testing-strategy)
17. [Build & Release Pipeline](#17-build--release-pipeline)
18. [Critical Implementation Notes](#18-critical-implementation-notes)

---

## 1. Development Environment

### 1.1 Prerequisites

```
Go 1.23+          — Primary language (use latest stable)
Make              — Build orchestration
Git               — Version control
Docker            — Integration testing, container builds
```

No other tools required. Zero CGo, zero external build dependencies.

### 1.2 Project Bootstrap

```bash
mkdir mammoth && cd mammoth
go mod init github.com/mammothengine/mammoth
```

### 1.3 Go Module Strategy

**Single module, zero dependencies.** The entire project is one `go.mod` with only the standard library. This is non-negotiable — it's the core value proposition.

Implications:
- Compression (Snappy, LZ4, Zstd) must be implemented in pure Go
- Cryptography uses `crypto/*` from stdlib
- No `google.golang.org/grpc` — implement gRPC-like protocol with raw TCP + protobuf-like encoding (or use stdlib `net/rpc` for internal communication)
- No `prometheus/client_golang` — implement Prometheus text exposition format manually (it's trivial)

### 1.4 Code Style & Conventions

```
- Package names: short, lowercase, no underscores (e.g., `memtable`, `sstable`, `wal`)
- Error handling: Wrap with context using fmt.Errorf("component: operation: %w", err)
- Interfaces: Define at consumption site, not at implementation site
- Concurrency: Prefer channels for coordination, sync.Mutex for shared state
- Context: All public APIs accept context.Context as first parameter
- Naming: Use domain terminology (memtable, sstable, compaction, bloom) not generic names
- Comments: Every exported type/function has a doc comment starting with the name
- Files: One primary type per file, filename matches type (e.g., skiplist.go for SkipList)
```

---

## 2. Storage Engine Implementation

### 2.1 Architecture

The storage engine is the heart of Mammoth. It's a standalone key-value store that the upper layers (document model, query engine) build upon.

```
pkg/engine/
├── engine.go           # Engine interface & main implementation
├── options.go          # Configuration options
├── batch.go            # Write batch (atomic multi-key writes)
├── snapshot.go         # Read snapshot (MVCC point-in-time view)
├── iterator.go         # Iterator interface for range scans
├── wal/
│   ├── wal.go          # Write-ahead log manager
│   ├── segment.go      # Single WAL segment file
│   ├── reader.go       # WAL replay for recovery
│   └── record.go       # WAL record encoding/decoding
├── memtable/
│   ├── memtable.go     # Memtable wrapper (active + immutables)
│   ├── skiplist.go     # Concurrent skip list implementation
│   ├── arena.go        # Arena allocator for skip list nodes
│   └── iterator.go     # Memtable iterator
├── sstable/
│   ├── writer.go       # SSTable file writer
│   ├── reader.go       # SSTable file reader
│   ├── block.go        # Data block encoding/decoding
│   ├── index.go        # Index block
│   ├── footer.go       # SSTable footer
│   ├── iterator.go     # SSTable iterator
│   └── merge.go        # Multi-SSTable merge iterator
├── compaction/
│   ├── compaction.go   # Compaction orchestrator
│   ├── leveled.go      # Leveled compaction strategy
│   ├── sizetiered.go   # Size-tiered compaction strategy
│   ├── fifo.go         # FIFO compaction strategy
│   └── picker.go       # Compaction file picker
├── bloom/
│   ├── filter.go       # Bloom filter implementation
│   └── hash.go         # Murmur3 hash for bloom filter
├── cache/
│   ├── lru.go          # Sharded LRU cache
│   └── cache.go        # Block cache interface
├── compression/
│   ├── snappy.go       # Snappy compression (pure Go)
│   ├── lz4.go          # LZ4 compression (pure Go)
│   └── zstd.go         # Zstd compression (pure Go)
└── manifest/
    ├── manifest.go     # Version manifest (tracks SSTable files per level)
    └── version.go      # Version set (current + historical states)
```

### 2.2 Engine Interface

```go
// pkg/engine/engine.go

type Engine interface {
    // Basic operations
    Put(key, value []byte) error
    Get(key []byte) ([]byte, error)
    Delete(key []byte) error

    // Batch operations (atomic)
    NewBatch() Batch
    Write(batch Batch) error

    // Snapshots (MVCC)
    NewSnapshot() Snapshot

    // Iteration
    NewIterator(opts *IteratorOptions) Iterator

    // Lifecycle
    Close() error
    Flush() error                    // Force memtable flush
    Compact(level int) error         // Force compaction

    // Stats
    Stats() EngineStats
}

type Batch interface {
    Put(key, value []byte)
    Delete(key []byte)
    Count() int
    Reset()
}

type Snapshot interface {
    Get(key []byte) ([]byte, error)
    NewIterator(opts *IteratorOptions) Iterator
    Release()
}

type Iterator interface {
    SeekToFirst()
    SeekToLast()
    Seek(key []byte)
    Next()
    Prev()
    Valid() bool
    Key() []byte
    Value() []byte
    Error() error
    Close()
}
```

### 2.3 Skip List Implementation

The memtable uses a concurrent skip list — this is the most performance-critical data structure in the entire engine.

```go
// pkg/engine/memtable/skiplist.go

const (
    maxHeight    = 20          // Max skip list height (supports ~10^8 entries)
    branchFactor = 4           // 1/4 probability of height increase
)

type SkipList struct {
    head     *node
    height   atomic.Int32      // Current max height
    size     atomic.Int64      // Total bytes (keys + values)
    count    atomic.Int64      // Number of entries
    arena    *Arena             // Arena allocator for nodes
}

type node struct {
    key      []byte
    value    []byte
    tower    [maxHeight]atomic.Pointer[node]  // Forward pointers
    height   int
}
```

**Implementation notes:**

- **Arena allocator**: Pre-allocate large byte slices (e.g., 64MB) and sub-allocate nodes from them. This eliminates GC pressure from millions of small allocations. When memtable becomes immutable, the entire arena is freed at once.

- **Lock-free reads**: Reads use `atomic.Pointer.Load()` to traverse — no locks needed. This is safe because skip list inserts only add new nodes (never modify existing).

- **Write locking**: A single `sync.Mutex` guards writes. This is acceptable because WAL is the write bottleneck, not the memtable. Alternatively, use CAS-based lock-free insertion for higher write concurrency.

- **Random height generation**: Use `math/rand/v2` with thread-local source. Height `h` has probability `(1/branchFactor)^h`.

- **Comparison**: Keys are compared as raw bytes using `bytes.Compare`. The key encoding (see Section 5.4 of SPEC) ensures correct sort order for composite keys.

### 2.4 WAL Implementation

```go
// pkg/engine/wal/wal.go

type WAL struct {
    dir         string
    mu          sync.Mutex
    current     *Segment       // Active segment being written to
    segments    []*Segment     // All segments (for recovery)
    seqNum      atomic.Uint64  // Monotonic sequence number
    syncMode    SyncMode       // Full, Batch, None
    batchTicker *time.Ticker   // For batch sync mode
}

type SyncMode int

const (
    SyncFull  SyncMode = iota  // fsync every write (safest, slowest)
    SyncBatch                   // fsync every N ms (balanced)
    SyncNone                    // OS decides when to flush (fastest, risk of data loss)
)
```

**WAL record format** (binary):

```
[CRC-32C: 4 bytes][Length: 4 bytes][Type: 1 byte][SeqNo: 4 bytes][Payload: variable]

Type values:
  0x01 = Full record (fits in one block)
  0x02 = First fragment
  0x03 = Middle fragment
  0x04 = Last fragment

Payload format:
  [OpType: 1 byte][KeyLen: 4 bytes][Key: variable][ValLen: 4 bytes][Value: variable]

OpType values:
  0x01 = Put
  0x02 = Delete
  0x03 = BatchStart (marks beginning of atomic batch)
  0x04 = BatchEnd (marks end of atomic batch, commit point)
```

**Critical implementation details:**

- **Block alignment**: WAL is divided into 32KB blocks. Records are aligned to block boundaries. If a record doesn't fit in the remaining block space, pad with zeros and start fresh.

- **CRC validation**: Every record has CRC-32C (Castagnoli). On recovery, corrupted records at the tail are truncated (power loss during write). Corruption in the middle means unrecoverable data loss — log an error and stop.

- **Segment rotation**: When a segment reaches 64MB, close it and create a new one. Old segments are deleted after their contents are flushed to SSTables.

- **Recovery**: On startup, replay all WAL segments in order. For each record, re-insert into memtable. Batch records between BatchStart/BatchEnd are atomic — if BatchEnd is missing, discard the partial batch.

- **fsync strategy**: `SyncFull` calls `file.Sync()` after every write. `SyncBatch` buffers writes and calls `file.Sync()` every 100ms (configurable). Trade-off: `SyncFull` guarantees zero data loss but adds ~2ms per write on HDD, ~50μs on NVMe.

### 2.5 SSTable Writer

```go
// pkg/engine/sstable/writer.go

type Writer struct {
    file        *os.File
    offset      uint64
    blockSize   int           // Target data block size (default 4KB)
    compression Compression
    bloom       *bloom.Filter
    index       []indexEntry  // Block index entries
    dataBlock   *blockBuilder // Current data block being built
    props       tableProperties
}

type indexEntry struct {
    lastKey []byte           // Last key in the data block
    offset  uint64           // Byte offset of the data block
    size    uint32           // Size of the data block
}
```

**Write flow:**

1. `Add(key, value)` — append to current data block builder
2. When block exceeds `blockSize`:
   a. Compress the block
   b. Write compressed block to file
   c. Add key to bloom filter
   d. Record index entry (lastKey → offset)
   e. Start new data block
3. `Finish()`:
   a. Flush last data block
   b. Write bloom filter as meta block
   c. Write index block
   d. Write footer (offsets + magic number 0x4D414D4D "MAMM")

**Data block encoding** (prefix compression):

```
For each key-value pair in block:
  [shared_prefix_len: varint]    ← bytes shared with previous key
  [unshared_len: varint]         ← bytes NOT shared
  [value_len: varint]
  [unshared_key_bytes: variable]
  [value_bytes: variable]

Restart points every 16 entries:
  [restart_offset_0: uint32]
  [restart_offset_1: uint32]
  ...
  [num_restarts: uint32]
```

Prefix compression typically saves 30-50% for keys with common prefixes (which our namespace-encoded keys have).

### 2.6 SSTable Reader

```go
// pkg/engine/sstable/reader.go

type Reader struct {
    file     *os.File
    fileSize int64
    footer   Footer
    index    []indexEntry        // Loaded into memory on open
    bloom    *bloom.Filter       // Loaded into memory on open
    cache    *cache.BlockCache   // Shared block cache
}
```

**Read flow (point lookup):**

1. Check bloom filter → if negative, key definitely not in this SSTable, return immediately
2. Binary search index entries to find candidate data block
3. Check block cache for the block → if hit, use cached block
4. Read block from disk, decompress, put in cache
5. Binary search within block using restart points
6. If found, return value; otherwise, key not in this SSTable

### 2.7 Compaction — Leveled Strategy

```go
// pkg/engine/compaction/leveled.go

type LeveledCompaction struct {
    mu              sync.Mutex
    manifest        *manifest.Manifest
    engine          *Engine
    maxLevelBytes   [maxLevels]int64  // Max bytes per level
    scoreThreshold  float64           // Trigger when score > 1.0
    running         atomic.Bool
}
```

**Compaction scoring:**

```
Level 0 score = num_files / l0_compaction_trigger (default 4)
Level N score = level_bytes / max_level_bytes[N]

max_level_bytes[1] = 256MB (configurable base)
max_level_bytes[N] = max_level_bytes[N-1] * level_size_multiplier (default 10)

So: L1=256MB, L2=2.56GB, L3=25.6GB, L4=256GB, L5=2.56TB, L6=25.6TB
```

**Compaction process:**

1. Pick level with highest score > 1.0
2. For L0: Pick all L0 files + overlapping L1 files
3. For L1+: Pick one file from L(N) + overlapping files from L(N+1)
4. Create merge iterator over selected files
5. Write new SSTable(s) for L(N+1) — split at size boundary (64MB per file)
6. During merge: drop tombstones if at bottom level, drop old MVCC versions if below GC watermark
7. Update manifest atomically (add new files, remove old files)
8. Delete old SSTable files

**Concurrency**: Max 2 concurrent compactions (configurable). L0→L1 compaction has priority because L0 affects read performance (all L0 files must be checked).

### 2.8 Bloom Filter

```go
// pkg/engine/bloom/filter.go

type Filter struct {
    bits     []byte          // Bit array
    numHash  uint32          // Number of hash functions (k)
    numBits  uint32          // Total bits (m)
}

// Optimal parameters for 1% false positive rate:
// m = -n * ln(p) / (ln(2)^2)  ≈ 10 bits per key
// k = (m/n) * ln(2)           ≈ 7 hash functions
```

**Hash strategy**: Use Murmur3 to generate two 64-bit hashes (h1, h2). Derive k hash values as `h(i) = h1 + i*h2` (Kirsch-Mitzenmacher optimization). This gives k independent hashes with only one hash computation.

### 2.9 Block Cache (Sharded LRU)

```go
// pkg/engine/cache/lru.go

type ShardedLRU struct {
    shards    [numShards]*lruShard
    shardMask uint64
}

type lruShard struct {
    mu       sync.RWMutex
    capacity int64
    used     int64
    items    map[uint64]*entry     // hash → entry
    head     *entry                // Most recently used
    tail     *entry                // Least recently used
}

const numShards = 16  // Reduces lock contention

func shardIndex(key uint64) int {
    return int(key & (numShards - 1))
}
```

Cache key: `sstableID << 32 | blockOffset`

### 2.10 Compression — Pure Go Implementations

**Snappy** (default):
- Port of Google Snappy algorithm
- Block format: literal and copy operations
- No external package — implement the core algorithm (~500 lines)
- Focus on fast decompression (more important than compression ratio for reads)

**LZ4**:
- LZ4 block compression format
- Faster than Snappy for decompression, slightly better ratio
- ~600 lines pure Go

**Zstd**:
- Most complex — Huffman coding + FSE (Finite State Entropy) + LZ77
- Use for cold data (L3+ levels) where compression ratio matters more than speed
- ~3000 lines pure Go (or start with Snappy/LZ4 only, add Zstd in later milestone)

**Recommendation**: Implement Snappy first (simplest, good enough). Add LZ4 in v0.2. Add Zstd in v0.4.

### 2.11 Manifest (Version Management)

The manifest tracks which SSTable files exist at each level. It enables atomic state transitions during compaction and crash recovery.

```go
// pkg/engine/manifest/manifest.go

type Manifest struct {
    mu          sync.RWMutex
    current     *Version
    log         *os.File        // Append-only manifest log file
    nextFileNum atomic.Uint64   // Next SSTable file number
}

type Version struct {
    levels [maxLevels][]*FileMetadata
}

type FileMetadata struct {
    fileNum    uint64
    fileSize   int64
    smallestKey []byte
    largestKey  []byte
    entryCount uint64
    level      int
}
```

**Manifest log format:**

```
Each entry is:
  [Length: varint][Type: uint8][Payload: variable]

Types:
  0x01 = AddFile { level, fileNum, fileSize, smallestKey, largestKey }
  0x02 = RemoveFile { level, fileNum }
  0x03 = CompactionPointer { level, key }  // Resume compaction after restart
  0x04 = NextFileNumber { num }
  0x05 = LogNumber { num }                 // Current WAL segment number
```

On startup: Replay manifest log to reconstruct current version. Periodically snapshot the full version to a new manifest file (compact the manifest log itself).

---

## 3. BSON Codec Implementation

### 3.1 Architecture

```
pkg/bson/
├── bson.go         # Public types: Document, Array, Value, ObjectID
├── types.go        # BSON type constants and type enum
├── encoder.go      # Go values → BSON bytes
├── decoder.go      # BSON bytes → Go values
├── objectid.go     # ObjectID generation & parsing
├── raw.go          # Raw BSON manipulation (zero-copy access)
├── marshal.go      # Struct marshaling (reflection-based)
├── unmarshal.go    # Struct unmarshaling (reflection-based)
└── compare.go      # BSON value comparison (for sorting, indexing)
```

### 3.2 Core Types

```go
// pkg/bson/bson.go

// Document is an ordered map of string → Value
type Document struct {
    keys   []string
    values []Value
    index  map[string]int  // fast lookup by key name
}

// Value represents any BSON value
type Value struct {
    Type BSONType
    Data []byte    // Raw BSON-encoded value (zero-copy friendly)
}

// ObjectID is a 12-byte unique identifier
type ObjectID [12]byte

// Common constructors
func D(pairs ...any) Document      // bson.D("name", "Ersin", "age", 30)
func M(m map[string]any) Document  // From Go map (unordered)
func A(values ...any) Array        // bson.A(1, 2, 3)
```

### 3.3 Encoding Strategy

BSON encoding must be **correct** and **fast**. Two modes:

**Reflection-based** (convenient, slower):
```go
type User struct {
    Name string `bson:"name"`
    Age  int    `bson:"age"`
}
user := User{Name: "Ersin", Age: 30}
bytes, err := bson.Marshal(user)
```

**Direct builder** (fast, no reflection):
```go
doc := bson.NewDocument()
doc.Append("name", bson.String("Ersin"))
doc.Append("age", bson.Int32(30))
bytes := doc.Encode()
```

For wire protocol hot path, use direct builder. Reflection-based is for user-facing embedded mode API.

### 3.4 ObjectID Generation

```go
// pkg/bson/objectid.go

var (
    objectIDCounter atomic.Uint32   // Initialized to random value
    processRandom   [5]byte         // Generated once at startup
)

func NewObjectID() ObjectID {
    var id ObjectID
    // Bytes 0-3: Unix timestamp (seconds, big-endian)
    binary.BigEndian.PutUint32(id[0:4], uint32(time.Now().Unix()))
    // Bytes 4-8: Random value (per-process, generated at startup)
    copy(id[4:9], processRandom[:])
    // Bytes 9-11: Incrementing counter (big-endian)
    c := objectIDCounter.Add(1)
    id[9] = byte(c >> 16)
    id[10] = byte(c >> 8)
    id[11] = byte(c)
    return id
}
```

### 3.5 BSON Comparison

Comparison order follows MongoDB spec for index sorting:

```
MinKey < Null < Numbers (int32/int64/double/decimal128 compared numerically)
  < String (UTF-8 byte order) < Document < Array < Binary
  < ObjectID (byte order) < Boolean (false < true) < DateTime
  < Timestamp < Regex < MaxKey
```

This comparison function is used by the query engine, index lookup, and aggregation sort.

---

## 4. Mammoth Binary Format (MBF)

### 4.1 Design Goals

- Faster random field access than BSON (O(1) via field index vs O(n) scan)
- Smaller storage footprint (varint encoding, no redundant size prefixes)
- MVCC metadata embedded in format

### 4.2 Implementation

```go
// pkg/document/mbf.go

type MBFDocument struct {
    raw []byte    // Complete MBF-encoded document
}

// Encode converts BSON document to MBF for storage
func Encode(doc bson.Document) []byte {
    // 1. Count fields
    // 2. Build field index (sorted by FNV-1a hash of field name)
    // 3. Write header: magic(2) + version(1) + flags(1) + field_count(varint) + total_size(varint)
    // 4. Write field index: [{name_hash: uint32, offset: varint}, ...]
    // 5. Write field data: [{type(1), name_len(varint), name, val_len(varint), value}, ...]
    // 6. Write CRC-32C checksum
}

// Decode converts MBF back to BSON for wire protocol response
func Decode(data []byte) bson.Document {
    // Reverse of Encode
}

// GetField provides O(1) field access without full decode
func (d *MBFDocument) GetField(name string) (bson.Value, bool) {
    hash := fnv1a(name)
    // Binary search field index by hash
    // On match, jump to offset, decode single field
}
```

### 4.3 MBF vs BSON Size Comparison

Example document: `{"name": "Ersin", "age": 30, "tags": ["go", "rust", "php"]}`

```
BSON: 73 bytes (4-byte sizes everywhere, type prefixes, null terminators)
MBF:  ~58 bytes (varint sizes, compact header, field index)
Savings: ~20% (varies by document structure)
```

For large documents with many fields, MBF savings are more significant due to O(1) field access eliminating the need to scan all fields.

---

## 5. Wire Protocol Implementation

### 5.1 Architecture

```
pkg/wire/
├── wire.go         # Protocol constants, message types
├── message.go      # OP_MSG structure
├── header.go       # MsgHeader (16 bytes)
├── section.go      # Section Kind 0 (body) and Kind 1 (document sequence)
├── reader.go       # Read OP_MSG from net.Conn
├── writer.go       # Write OP_MSG to net.Conn
├── command.go      # Command dispatch (find, insert, etc.)
└── handler.go      # Command handler interface
```

### 5.2 OP_MSG Implementation

```go
// pkg/wire/message.go

type OpMsg struct {
    Header    MsgHeader
    FlagBits  uint32
    Sections  []Section
    Checksum  uint32     // Optional CRC-32C
}

type MsgHeader struct {
    MessageLength int32
    RequestID     int32
    ResponseTo    int32
    OpCode        int32  // Always 2013 for OP_MSG
}

type Section interface {
    Kind() byte
}

type BodySection struct {    // Kind 0
    Document bson.Document
}

type DocSequenceSection struct {  // Kind 1
    Size       int32
    Identifier string
    Documents  []bson.Document
}
```

### 5.3 Connection Handling

```go
// pkg/server/mongo/server.go

type Server struct {
    listener  net.Listener
    handler   wire.CommandHandler
    wg        sync.WaitGroup
    ctx       context.Context
    cancel    context.CancelFunc
    connCount atomic.Int64
}

func (s *Server) acceptLoop() {
    for {
        conn, err := s.listener.Accept()
        if err != nil {
            if s.ctx.Err() != nil {
                return // Server shutting down
            }
            continue
        }
        s.wg.Add(1)
        go s.handleConnection(conn)
    }
}

func (s *Server) handleConnection(conn net.Conn) {
    defer s.wg.Done()
    defer conn.Close()
    s.connCount.Add(1)
    defer s.connCount.Add(-1)

    reader := wire.NewReader(conn)
    writer := wire.NewWriter(conn)

    for {
        msg, err := reader.ReadMessage()
        if err != nil {
            return // Connection closed or error
        }

        response := s.handler.Handle(s.ctx, msg)
        if err := writer.WriteMessage(response); err != nil {
            return
        }
    }
}
```

### 5.4 Command Dispatch

```go
// pkg/wire/command.go

type CommandHandler interface {
    Handle(ctx context.Context, msg *OpMsg) *OpMsg
}

type Dispatcher struct {
    handlers map[string]CommandFunc
    engine   *engine.Engine
    catalog  *catalog.Catalog
    txnMgr   *txn.Manager
}

func (d *Dispatcher) Handle(ctx context.Context, msg *OpMsg) *OpMsg {
    body := msg.Sections[0].(*BodySection).Document
    // First key in body document is the command name
    cmdName := body.Keys()[0]

    handler, ok := d.handlers[cmdName]
    if !ok {
        return errorResponse(msg.Header.RequestID, fmt.Sprintf("unknown command: %s", cmdName))
    }

    return handler(ctx, msg)
}

// Registration
func (d *Dispatcher) init() {
    d.handlers = map[string]CommandFunc{
        "find":            d.handleFind,
        "insert":          d.handleInsert,
        "update":          d.handleUpdate,
        "delete":          d.handleDelete,
        "aggregate":       d.handleAggregate,
        "findAndModify":   d.handleFindAndModify,
        "getMore":         d.handleGetMore,
        "killCursors":     d.handleKillCursors,
        "createIndexes":   d.handleCreateIndexes,
        "dropIndexes":     d.handleDropIndexes,
        "listIndexes":     d.handleListIndexes,
        "create":          d.handleCreate,
        "drop":            d.handleDrop,
        "listCollections": d.handleListCollections,
        "listDatabases":   d.handleListDatabases,
        "serverStatus":    d.handleServerStatus,
        "ping":            d.handlePing,
        "hello":           d.handleHello,
        "isMaster":        d.handleIsMaster,
        // ... more commands
    }
}
```

### 5.5 MongoDB Handshake

When a client connects, the first command is typically `hello` (or legacy `isMaster`). The response must include:

```go
func (d *Dispatcher) handleHello(ctx context.Context, msg *OpMsg) *OpMsg {
    return okResponse(msg.Header.RequestID, bson.D(
        "ismaster", true,
        "maxBsonObjectSize", 16*1024*1024,     // 16MB
        "maxMessageSizeBytes", 48*1024*1024,    // 48MB
        "maxWriteBatchSize", 100000,
        "localTime", time.Now(),
        "minWireVersion", 0,
        "maxWireVersion", 21,                   // MongoDB 7.0
        "readOnly", false,
        "ok", 1.0,
    ))
}
```

The `maxWireVersion` determines which features the client driver will attempt to use. Target wire version 21 (MongoDB 7.0 compat).

---

## 6. Query Engine Implementation

### 6.1 Architecture

```
pkg/query/
├── parser/
│   ├── parser.go       # BSON query → QueryAST
│   ├── ast.go          # AST node types
│   ├── operators.go    # Operator implementations ($gt, $in, $regex, etc.)
│   └── update.go       # Update operator parsing ($set, $inc, etc.)
├── planner/
│   ├── planner.go      # Generate candidate plans
│   ├── plan.go         # Plan node types (CollScan, IndexScan, etc.)
│   ├── cost.go         # Cost estimation model
│   └── stats.go        # Collection/index statistics
├── optimizer/
│   ├── optimizer.go    # Plan optimization passes
│   ├── predicate.go    # Predicate pushdown
│   └── index.go        # Index selection heuristics
├── executor/
│   ├── executor.go     # Plan execution engine
│   ├── scan.go         # CollectionScan, IndexScan executors
│   ├── filter.go       # Filter executor (apply predicates)
│   ├── sort.go         # Sort executor (in-memory + external)
│   ├── limit.go        # Limit/Skip executors
│   ├── project.go      # Projection executor
│   └── cursor.go       # Cursor management (getMore support)
└── aggregation/
    ├── pipeline.go     # Pipeline orchestrator
    ├── match.go        # $match stage
    ├── group.go        # $group stage
    ├── sort.go         # $sort stage
    ├── project.go      # $project stage
    ├── unwind.go       # $unwind stage
    ├── lookup.go       # $lookup stage
    ├── limit.go        # $limit / $skip stages
    ├── count.go        # $count stage
    ├── addfields.go    # $addFields stage
    ├── facet.go        # $facet stage
    ├── bucket.go       # $bucket / $bucketAuto stages
    ├── merge.go        # $merge / $out stages
    ├── expression.go   # Expression evaluator ($add, $concat, $cond, etc.)
    └── accumulator.go  # Group accumulators ($sum, $avg, $min, $max, etc.)
```

### 6.2 Query AST

```go
// pkg/query/parser/ast.go

type QueryNode interface {
    Evaluate(doc bson.Document) bool
}

type ComparisonNode struct {
    Field    string     // Dot-notation field path (e.g., "address.city")
    Operator CompOp    // Eq, Ne, Gt, Gte, Lt, Lte
    Value    bson.Value
}

type LogicalNode struct {
    Operator LogOp      // And, Or, Not, Nor
    Children []QueryNode
}

type InNode struct {
    Field  string
    Values []bson.Value
    Negate bool          // true for $nin
}

type ExistsNode struct {
    Field  string
    Exists bool
}

type RegexNode struct {
    Field   string
    Pattern string
    Options string      // "i", "m", "s", "x"
}

type ElemMatchNode struct {
    Field    string
    SubQuery QueryNode
}
```

### 6.3 Plan Nodes (Volcano Model)

```go
// pkg/query/executor/executor.go

type PlanNode interface {
    Open(ctx context.Context) error
    Next() (bson.Document, error)  // Returns nil, nil when exhausted
    Close() error
    Explain() ExplainNode
}

// Collection scan — reads every document
type CollScanNode struct {
    collection *catalog.Collection
    iterator   engine.Iterator
    filter     QueryNode      // Optional in-place filter
}

// Index scan — reads index entries, then fetches documents
type IndexScanNode struct {
    index     *index.Index
    bounds    IndexBounds     // Start key, end key, inclusive flags
    direction int             // 1 = forward, -1 = reverse
    filter    QueryNode       // Residual filter (predicates not covered by index)
}

// Filter — applies predicate to child node output
type FilterNode struct {
    child  PlanNode
    filter QueryNode
}

// Sort — in-memory sort (external sort if > 100MB)
type SortNode struct {
    child    PlanNode
    sortSpec []SortField
    limit    int64           // Optimization: if limit is small, use top-K heap
}

// Limit / Skip
type LimitNode struct {
    child PlanNode
    limit int64
    skip  int64
}

// Project — field selection and reshaping
type ProjectNode struct {
    child      PlanNode
    projection ProjectionSpec
}
```

### 6.4 External Sort

When sort data exceeds 100MB (configurable), spill to disk:

1. Read chunks from child node (100MB each)
2. Sort each chunk in memory (using Go's `slices.SortFunc`)
3. Write sorted chunks to temporary SSTable files
4. Merge sorted chunks using merge iterator (k-way merge)
5. Clean up temporary files after query completes

### 6.5 Cursor Management

```go
// pkg/query/executor/cursor.go

type CursorManager struct {
    mu      sync.RWMutex
    cursors map[int64]*Cursor
    nextID  atomic.Int64
    timeout time.Duration     // Default: 10 minutes
}

type Cursor struct {
    id        int64
    plan      PlanNode
    namespace string
    created   time.Time
    lastUsed  time.Time
    batchSize int32
}
```

First `find` returns up to 101 documents (or 16MB, whichever is smaller). Subsequent `getMore` requests use the cursor ID to continue iteration.

---

## 7. Index Implementation

### 7.1 Architecture

Each secondary index is a separate LSM-Tree instance. Index entries map encoded field values to document IDs.

```go
// pkg/index/manager.go

type Manager struct {
    mu       sync.RWMutex
    indexes  map[string]*Index                 // indexName → Index
    engines  map[string]*engine.Engine         // indexName → dedicated LSM-Tree
    catalog  *catalog.Catalog
}

type Index struct {
    Name       string
    Namespace  string            // "db.collection"
    KeyFields  []IndexField      // [{field: "email", order: 1}, ...]
    Unique     bool
    Sparse     bool
    Partial    *bson.Document    // Partial filter expression
    TTL        time.Duration     // 0 = no TTL
    IndexType  IndexType         // Regular, Text, Geo, Hashed, Wildcard
    engine     *engine.Engine    // Dedicated LSM-Tree for this index
}

type IndexField struct {
    Field string
    Order int    // 1 = ascending, -1 = descending
}
```

### 7.2 Index Key Encoding

Index keys must sort correctly as raw bytes. Use **order-preserving encoding**:

```go
// pkg/index/encoding.go

// Encode a BSON value to bytes that sort correctly
func EncodeIndexKey(val bson.Value, ascending bool) []byte {
    var buf []byte

    // Type prefix (1 byte) — ensures cross-type sort order matches MongoDB
    buf = append(buf, typeOrderByte(val.Type))

    switch val.Type {
    case bson.TypeInt32:
        // Flip sign bit for correct signed integer byte ordering
        v := val.Int32()
        buf = appendUint32(buf, uint32(v) ^ (1 << 31))

    case bson.TypeInt64:
        v := val.Int64()
        buf = appendUint64(buf, uint64(v) ^ (1 << 63))

    case bson.TypeDouble:
        // IEEE 754 float to sortable bytes
        v := val.Double()
        bits := math.Float64bits(v)
        if v >= 0 {
            bits ^= (1 << 63)          // Flip sign bit
        } else {
            bits = ^bits               // Flip all bits for negative
        }
        buf = appendUint64(buf, bits)

    case bson.TypeString:
        // UTF-8 bytes + null terminator
        buf = append(buf, val.StringBytes()...)
        buf = append(buf, 0x00)

    case bson.TypeObjectID:
        buf = append(buf, val.ObjectID()[:]...)

    case bson.TypeNull:
        // No additional bytes needed — type prefix is sufficient

    // ... more types
    }

    // For descending order, flip all bits
    if !ascending {
        for i := range buf {
            buf[i] = ^buf[i]
        }
    }

    return buf
}
```

### 7.3 Compound Index Key

For compound index `{a: 1, b: -1}`:

```
Key = EncodeIndexKey(a, asc=true) + EncodeIndexKey(b, asc=false) + DocumentID
```

Separator between fields not needed because each field encoding is self-delimiting (null-terminated strings, fixed-size numbers).

### 7.4 Multikey Index

For document `{tags: ["go", "rust", "php"]}` with index on `tags`:

Insert 3 index entries:
```
Key1 = "go"    + docID → empty value
Key2 = "rust"  + docID → empty value
Key3 = "php"   + docID → empty value
```

### 7.5 Text Index (Inverted Index)

```go
// pkg/index/text/text.go

type TextIndex struct {
    engine     *engine.Engine  // LSM-Tree for term → posting list
    analyzer   *Analyzer       // Tokenizer + stemmer + stop words
    weights    map[string]float64  // Field → weight multiplier
}

type Analyzer struct {
    tokenizer  Tokenizer       // Unicode word boundary tokenizer
    stemmer    Stemmer         // Porter stemmer (English), language-aware
    stopWords  map[string]bool // Language-specific stop words
    language   string          // "en", "tr", etc.
}

// Posting list entry
type Posting struct {
    DocID     ObjectID
    Score     float32      // TF-IDF score
    Positions []uint32     // Word positions (for phrase queries)
}
```

**Key format**: `{ns_id}{idx_id}{term_bytes}` → `{posting_list_encoded}`

**Query**: `$text: {$search: "document database"}` →
1. Tokenize search terms
2. Look up each term in inverted index
3. Intersect posting lists
4. Score by TF-IDF
5. Sort by score descending

### 7.6 TTL Index Background Worker

```go
// pkg/index/ttl.go

func (m *Manager) startTTLWorker(ctx context.Context) {
    ticker := time.NewTicker(60 * time.Second) // Check every 60s
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            m.cleanExpiredDocuments()
        }
    }
}

func (m *Manager) cleanExpiredDocuments() {
    for _, idx := range m.TTLIndexes() {
        now := time.Now()
        expireBefore := now.Add(-idx.TTL)

        // Scan index for entries with datetime < expireBefore
        iter := idx.engine.NewIterator(&engine.IteratorOptions{
            UpperBound: encodeDateTime(expireBefore),
        })
        defer iter.Close()

        for iter.SeekToFirst(); iter.Valid(); iter.Next() {
            docID := extractDocID(iter.Key())
            // Delete document from main collection
            // Delete all index entries for this document
        }
    }
}
```

---

## 8. Transaction Manager Implementation

### 8.1 Architecture

```
pkg/txn/
├── manager.go      # Transaction lifecycle management
├── transaction.go  # Single transaction state
├── mvcc.go         # Version visibility rules
├── conflict.go     # Write-write conflict detection
├── gc.go           # Old version garbage collection
└── watermark.go    # Low watermark tracking
```

### 8.2 Transaction Manager

```go
// pkg/txn/manager.go

type Manager struct {
    mu           sync.RWMutex
    nextTxnID    atomic.Uint64
    active       map[uint64]*Transaction  // Active transactions
    committed    *CommitLog               // Ordered list of committed txn IDs
    watermark    atomic.Uint64            // Lowest active txn ID (GC boundary)
}

func (m *Manager) Begin() *Transaction {
    txnID := m.nextTxnID.Add(1)
    snapshot := m.watermark.Load()

    txn := &Transaction{
        id:        txnID,
        snapshot:  snapshot,
        readSet:   make(map[string]uint64),   // key → version read
        writeSet:  make(map[string][]byte),   // key → new value
        status:    TxnActive,
        startTime: time.Now(),
    }

    m.mu.Lock()
    m.active[txnID] = txn
    m.updateWatermark()
    m.mu.Unlock()

    return txn
}

func (m *Manager) Commit(txn *Transaction) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    // Conflict detection: check if any key in write set
    // was modified by another committed txn since our snapshot
    for key, readVersion := range txn.readSet {
        currentVersion := m.getLatestCommittedVersion(key)
        if currentVersion > txn.snapshot {
            txn.status = TxnAborted
            delete(m.active, txn.id)
            return ErrWriteConflict
        }
    }

    // Write to WAL atomically
    batch := m.engine.NewBatch()
    for key, value := range txn.writeSet {
        encodedKey := encodeVersionedKey(key, txn.id)
        batch.Put(encodedKey, value)
    }
    if err := m.engine.Write(batch); err != nil {
        txn.status = TxnAborted
        delete(m.active, txn.id)
        return err
    }

    // Mark as committed
    txn.status = TxnCommitted
    m.committed.Add(txn.id)
    delete(m.active, txn.id)
    m.updateWatermark()

    return nil
}
```

### 8.3 MVCC Read Logic

```go
// pkg/txn/mvcc.go

func (m *Manager) Read(txn *Transaction, key string) ([]byte, error) {
    // Create iterator that scans versions of this key
    prefix := encodeKeyPrefix(key)
    iter := m.engine.NewIterator(&engine.IteratorOptions{
        Prefix: prefix,
    })
    defer iter.Close()

    // Versions are sorted newest-first (inverted version number in key)
    for iter.SeekToFirst(); iter.Valid(); iter.Next() {
        version := extractVersion(iter.Key())

        // Skip versions from our own uncommitted writes (read-your-writes)
        if version == txn.id {
            txn.readSet[key] = version
            return iter.Value(), nil
        }

        // Skip versions newer than our snapshot
        if version > txn.snapshot && !m.isCommitted(version) {
            continue
        }

        // Skip versions from transactions that were active when we started
        if version > txn.snapshot {
            continue
        }

        // This version is visible to us
        txn.readSet[key] = version
        return iter.Value(), nil
    }

    return nil, ErrKeyNotFound
}
```

### 8.4 Garbage Collection

```go
// pkg/txn/gc.go

func (m *Manager) startGC(ctx context.Context) {
    ticker := time.NewTicker(m.gcInterval) // Default: 60s
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            m.collectGarbage()
        }
    }
}

func (m *Manager) collectGarbage() {
    watermark := m.watermark.Load()

    // For each key with multiple versions:
    // Keep the latest committed version visible to watermark
    // Delete all older versions
    //
    // Implementation: scan engine with special GC iterator
    // that identifies multi-version keys and removes old ones
}
```

---

## 9. Replication (Raft) Implementation

### 9.1 Architecture

```
pkg/replication/
├── raft/
│   ├── raft.go         # Core Raft state machine
│   ├── log.go          # Raft log (stored in dedicated LSM-Tree)
│   ├── state.go        # Persistent state (currentTerm, votedFor)
│   ├── transport.go    # RPC transport (TCP-based, custom protocol)
│   ├── election.go     # Leader election logic
│   ├── replication.go  # Log replication to followers
│   ├── snapshot.go     # Snapshot creation and transfer
│   ├── config.go       # Raft configuration
│   └── prevote.go      # Pre-vote extension (prevents disruption)
├── oplog/
│   ├── oplog.go        # Operation log (wraps Raft log entries)
│   ├── entry.go        # Oplog entry types
│   └── applier.go      # Apply oplog entries to storage engine
├── replica_set.go      # Replica set management
└── read_preference.go  # Read routing logic
```

### 9.2 Raft State Machine

```go
// pkg/replication/raft/raft.go

type Raft struct {
    mu sync.Mutex

    // Persistent state (saved to disk before responding to RPCs)
    currentTerm uint64
    votedFor    uint64      // 0 = none
    log         *Log

    // Volatile state (all servers)
    commitIndex uint64
    lastApplied uint64
    state       NodeState   // Follower, Candidate, Leader

    // Volatile state (leaders only)
    nextIndex   map[uint64]uint64   // peer → next log index to send
    matchIndex  map[uint64]uint64   // peer → highest replicated index

    // Configuration
    id          uint64
    peers       map[uint64]string   // peer ID → address
    transport   Transport

    // Channels
    applyCh     chan ApplyMsg        // Committed entries sent here
    electTimer  *time.Timer          // Election timeout (randomized)
    heartbeat   *time.Ticker         // Leader heartbeat interval

    // Apply callback — connects Raft to storage engine
    applyFunc   func(entry LogEntry) error
}

type NodeState int

const (
    Follower  NodeState = iota
    Candidate
    Leader
)

type LogEntry struct {
    Term    uint64
    Index   uint64
    Type    EntryType     // Normal, Config, Noop
    Data    []byte        // Serialized oplog entry
}
```

### 9.3 Transport Protocol

Custom TCP-based protocol (not gRPC — zero dependency):

```
Message format:
  [MsgType: uint8][PayloadLen: uint32][Payload: variable]

Message types:
  0x01 = RequestVote
  0x02 = RequestVoteResponse
  0x03 = AppendEntries
  0x04 = AppendEntriesResponse
  0x05 = InstallSnapshot
  0x06 = InstallSnapshotResponse
  0x07 = PreVote
  0x08 = PreVoteResponse
```

Connection management: Persistent TCP connections between peers. Reconnect on failure with exponential backoff. Heartbeats double as keepalive.

### 9.4 Snapshot Transfer

For new nodes or nodes that fell far behind:

1. Leader creates snapshot of current storage state
2. Chunks snapshot into 1MB pieces
3. Sends chunks via InstallSnapshot RPC
4. Follower writes chunks to temporary file
5. On complete: verify checksum, replace local data, update Raft state

---

## 10. Sharding Implementation

### 10.1 Architecture

```
pkg/sharding/
├── router/
│   ├── router.go       # Query routing to shards
│   ├── catalog.go      # Shard metadata cache
│   └── merge.go        # Result merging from multiple shards
├── chunk/
│   ├── chunk.go        # Chunk metadata
│   ├── split.go        # Chunk splitting logic
│   └── range.go        # Key range management
├── balancer/
│   ├── balancer.go     # Auto-balance orchestrator
│   ├── policy.go       # Balance policy (even distribution)
│   └── migrator.go     # Chunk migration between shards
└── config/
    ├── server.go       # Config server (Raft cluster for metadata)
    └── schema.go       # Sharding metadata schema
```

### 10.2 Router Integration

Unlike MongoDB's separate `mongos`, the router is built into every Mammoth node:

```go
// pkg/sharding/router/router.go

type Router struct {
    localShard   string                         // This node's shard name
    configClient *config.Client                 // Connection to config servers
    catalog      *ShardCatalog                  // Cached routing table
    refresher    *time.Ticker                   // Periodic catalog refresh
}

func (r *Router) Route(ctx context.Context, ns string, query bson.Document) ([]ShardTarget, error) {
    shardKey := r.catalog.GetShardKey(ns)
    if shardKey == "" {
        // Unsharded collection — route to primary shard
        return []ShardTarget{{Shard: r.catalog.PrimaryShard(ns)}}, nil
    }

    // Extract shard key value from query
    keyValue := extractShardKeyValue(query, shardKey)

    if keyValue != nil {
        // Targeted query — route to specific shard(s)
        chunks := r.catalog.FindChunks(ns, keyValue)
        return chunksToTargets(chunks), nil
    }

    // Scatter-gather — query all shards
    return r.catalog.AllShards(ns), nil
}
```

---

## 11. Server & Networking

### 11.1 Main Entry Point

```go
// cmd/mammoth/main.go

func main() {
    cfg := config.Load()            // TOML config + CLI flags

    // Initialize storage engine
    eng, err := engine.Open(cfg.Storage)

    // Initialize catalog
    cat := catalog.New(eng)

    // Initialize transaction manager
    txnMgr := txn.NewManager(eng)

    // Initialize index manager
    idxMgr := index.NewManager(eng, cat)

    // Initialize query engine
    queryEng := query.NewEngine(eng, cat, idxMgr, txnMgr)

    // Initialize replication (if enabled)
    var raftNode *replication.Node
    if cfg.Replication.Enabled {
        raftNode = replication.New(cfg.Replication, eng)
        raftNode.Start()
    }

    // Start MongoDB wire protocol server
    mongoServer := mongo.NewServer(cfg.Server, queryEng)
    go mongoServer.ListenAndServe()

    // Start HTTP API server (metrics, health, admin)
    httpServer := http.NewServer(cfg.Admin, queryEng)
    go httpServer.ListenAndServe()

    // Wait for shutdown signal
    sig := make(chan os.Signal, 1)
    signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
    <-sig

    // Graceful shutdown
    mongoServer.Shutdown()
    httpServer.Shutdown()
    if raftNode != nil {
        raftNode.Stop()
    }
    txnMgr.Close()
    eng.Close()
}
```

### 11.2 Graceful Shutdown Sequence

1. Stop accepting new connections
2. Wait for in-flight queries to complete (timeout: 30s)
3. Close all cursors
4. Stop Raft (if leader: transfer leadership first)
5. Flush memtable to SSTable
6. Sync WAL
7. Close storage engine
8. Exit

---

## 12. Embedded Mode Implementation

### 12.1 Public API

```go
// embed/mammoth.go

package embed

// Open creates or opens a Mammoth database at the given path
func Open(path string, opts *Options) (*DB, error)

// DB represents an embedded Mammoth database
type DB struct {
    engine  *engine.Engine
    catalog *catalog.Catalog
    txnMgr  *txn.Manager
    idxMgr  *index.Manager
    queryEng *query.Engine
}

func (db *DB) Collection(database, collection string) *Collection
func (db *DB) Transaction(fn func(tx *Tx) error) error
func (db *DB) Close() error

// Collection provides document operations
type Collection struct {
    db        *DB
    namespace string    // "database.collection"
}

func (c *Collection) InsertOne(doc any) (ObjectID, error)
func (c *Collection) InsertMany(docs []any) ([]ObjectID, error)
func (c *Collection) FindOne(filter any) *SingleResult
func (c *Collection) Find(filter any, opts ...FindOption) (*Cursor, error)
func (c *Collection) UpdateOne(filter, update any) (*UpdateResult, error)
func (c *Collection) UpdateMany(filter, update any) (*UpdateResult, error)
func (c *Collection) DeleteOne(filter any) (*DeleteResult, error)
func (c *Collection) DeleteMany(filter any) (*DeleteResult, error)
func (c *Collection) Aggregate(pipeline []any) (*Cursor, error)
func (c *Collection) CreateIndex(keys any, opts ...IndexOption) (string, error)
func (c *Collection) CountDocuments(filter any) (int64, error)
```

### 12.2 API Compatibility Strategy

The embedded mode API mirrors the official MongoDB Go driver API as closely as possible. Goal: users can swap `go.mongodb.org/mongo-driver` imports for `github.com/mammothengine/mammoth/embed` with minimal code changes.

---

## 13. Admin Web UI

### 13.1 Technology

- **Frontend**: Alpine.js + Tailwind CSS (CDN) + CodeMirror 6 (embedded)
- **Delivery**: SPA embedded in Go binary via `embed.FS`
- **API**: REST endpoints served by the HTTP server

Consistent with DataBrowse's approach — proven stack, zero build step for frontend.

### 13.2 Embedded Assets

```go
// pkg/admin/admin.go

//go:embed dist/*
var staticFiles embed.FS

func Handler() http.Handler {
    return http.FileServer(http.FS(staticFiles))
}
```

### 13.3 REST API Endpoints

```
GET    /api/v1/status                    — Server status & stats
GET    /api/v1/databases                 — List databases
GET    /api/v1/databases/:db/collections — List collections
GET    /api/v1/databases/:db/collections/:coll/documents — Query documents
POST   /api/v1/databases/:db/collections/:coll/documents — Insert document
PUT    /api/v1/databases/:db/collections/:coll/documents/:id — Update document
DELETE /api/v1/databases/:db/collections/:coll/documents/:id — Delete document
GET    /api/v1/databases/:db/collections/:coll/indexes — List indexes
POST   /api/v1/databases/:db/collections/:coll/indexes — Create index
GET    /api/v1/replica-set/status        — Replica set status
GET    /api/v1/profiler                  — Slow query log
GET    /api/v1/logs                      — Structured logs (SSE stream)
GET    /metrics                          — Prometheus metrics
GET    /health                           — Health check
```

---

## 14. Security Implementation

### 14.1 SCRAM-SHA-256

```go
// pkg/auth/scram/scram.go

// Implements SCRAM-SHA-256 as per RFC 7677
// Used in MongoDB's SASL authentication flow

type Server struct {
    credentials CredentialStore    // username → storedKey, serverKey, salt, iterations
}

// Authentication flow:
// 1. Client sends saslStart with mechanism "SCRAM-SHA-256"
// 2. Server responds with server-first-message (salt, iteration count)
// 3. Client sends saslContinue with client-final-message (proof)
// 4. Server verifies proof, responds with server-final-message
// 5. Client verifies server signature
```

### 14.2 Encryption at Rest

```go
// pkg/crypto/encryption.go

// Each SSTable file and WAL segment is encrypted with AES-256-GCM
// Key hierarchy:
//   Master Key (from config/keyfile)
//     └── Data Encryption Key (DEK) per file, encrypted with master key
//         └── AES-256-GCM encrypts file blocks

type Encryptor struct {
    masterKey [32]byte
}

func (e *Encryptor) EncryptBlock(plaintext []byte) ([]byte, error) {
    // Generate random 12-byte nonce
    // Encrypt with AES-256-GCM
    // Return: [nonce(12)][ciphertext][tag(16)]
}

func (e *Encryptor) DecryptBlock(data []byte) ([]byte, error) {
    // Extract nonce, ciphertext, tag
    // Decrypt and verify with AES-256-GCM
}
```

---

## 15. Observability Implementation

### 15.1 Prometheus Metrics

Implement Prometheus text exposition format manually (trivial, no dependency needed):

```go
// pkg/metrics/prometheus.go

type Registry struct {
    mu       sync.RWMutex
    counters map[string]*Counter
    gauges   map[string]*Gauge
    histos   map[string]*Histogram
}

func (r *Registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
    w.Header().Set("Content-Type", "text/plain; version=0.0.4")
    r.mu.RLock()
    defer r.mu.RUnlock()

    for name, counter := range r.counters {
        fmt.Fprintf(w, "# TYPE %s counter\n%s %d\n", name, name, counter.Value())
    }
    // ... gauges, histograms
}
```

### 15.2 Structured Logging

```go
// pkg/logger/logger.go

type Logger struct {
    output    io.Writer
    level     Level
    component string
}

func (l *Logger) Info(msg string, fields ...Field) {
    if l.level > LevelInfo { return }
    entry := map[string]any{
        "ts":        time.Now().UTC().Format(time.RFC3339Nano),
        "level":     "info",
        "component": l.component,
        "msg":       msg,
    }
    for _, f := range fields {
        entry[f.Key] = f.Value
    }
    json.NewEncoder(l.output).Encode(entry)
}
```

---

## 16. Testing Strategy

### 16.1 Test Structure

```
Tests follow Go convention: *_test.go files alongside source.

Levels:
1. Unit tests       — every package, every exported function
2. Integration tests — cross-package interactions
3. End-to-end tests  — full server with MongoDB driver client
4. Compatibility tests — MongoDB test suite subset
5. Benchmark tests   — performance regression detection
6. Chaos tests       — network partitions, crashes, disk full
```

### 16.2 Key Test Categories

**Storage engine:**
- Memtable: insert, get, delete, iterator, concurrent access
- WAL: write, read, recovery after crash, corruption handling
- SSTable: write, read, bloom filter accuracy, compression
- Compaction: leveled merge, tombstone GC, concurrent compaction
- Full engine: CRUD, batch, snapshot, iterator over levels

**BSON codec:**
- Encode/decode round-trip for all types
- Comparison ordering correctness
- Edge cases: empty docs, nested docs, large arrays, max sizes

**Wire protocol:**
- OP_MSG parsing and serialization
- Command dispatch for all supported commands
- Connection lifecycle (handshake, keepalive, close)
- Error responses for invalid commands

**Query engine:**
- Query parser: all operators, nested conditions, dot notation
- Query planner: index selection, collection scan fallback
- Executor: sort, limit, skip, projection
- Aggregation: each stage independently + pipeline combinations
- Cursor management: getMore, timeout, killCursors

**Transactions:**
- MVCC snapshot isolation
- Write-write conflict detection
- Concurrent transactions
- Recovery after crash during commit

**Replication:**
- Leader election (normal, network partition, split brain)
- Log replication correctness
- Snapshot transfer
- Membership changes

### 16.3 MongoDB Compatibility Test Runner

```go
// test/compat/runner_test.go

// Run a subset of MongoDB's official CRUD specification tests
// These are YAML-based test definitions from:
// https://github.com/mongodb/specifications/tree/master/source/crud/tests

func TestMongoDBCompatCRUD(t *testing.T) {
    server := startTestServer(t)
    defer server.Stop()

    client := connectMongoDriver(t, server.Addr())

    // Run each test case from YAML files
    for _, tc := range loadTestCases("crud") {
        t.Run(tc.Description, func(t *testing.T) {
            tc.Run(t, client)
        })
    }
}
```

---

## 17. Build & Release Pipeline

### 17.1 Makefile

```makefile
.PHONY: build test bench lint release

VERSION := $(shell git describe --tags --always)
LDFLAGS := -ldflags "-s -w -X main.version=$(VERSION)"

build:
	CGO_ENABLED=0 go build $(LDFLAGS) -o bin/mammoth ./cmd/mammoth

test:
	go test -race -count=1 ./...

bench:
	go test -bench=. -benchmem ./pkg/engine/...
	go test -bench=. -benchmem ./pkg/bson/...

lint:
	go vet ./...
	staticcheck ./...

release:
	# Linux amd64
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build $(LDFLAGS) -o dist/mammoth-linux-amd64 ./cmd/mammoth
	# Linux arm64
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build $(LDFLAGS) -o dist/mammoth-linux-arm64 ./cmd/mammoth
	# macOS amd64
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build $(LDFLAGS) -o dist/mammoth-darwin-amd64 ./cmd/mammoth
	# macOS arm64 (Apple Silicon)
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build $(LDFLAGS) -o dist/mammoth-darwin-arm64 ./cmd/mammoth
	# Windows amd64
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build $(LDFLAGS) -o dist/mammoth-windows-amd64.exe ./cmd/mammoth

docker:
	docker build -t mammothengine/mammoth:$(VERSION) .
	docker build -t mammothengine/mammoth:latest .

clean:
	rm -rf bin/ dist/
```

### 17.2 Dockerfile

```dockerfile
# Build stage
FROM golang:1.23-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags "-s -w" -o mammoth ./cmd/mammoth

# Production stage
FROM scratch
COPY --from=builder /build/mammoth /mammoth
EXPOSE 27017 8080 9090
VOLUME /data
ENTRYPOINT ["/mammoth", "server", "--data-dir", "/data"]
```

### 17.3 GitHub Actions CI

```yaml
# .github/workflows/ci.yml
name: CI
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: '1.23' }
      - run: go test -race -coverprofile=coverage.out ./...
      - run: go vet ./...
  
  bench:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: '1.23' }
      - run: go test -bench=. -benchmem ./pkg/engine/... | tee bench.txt

  release:
    if: startsWith(github.ref, 'refs/tags/v')
    needs: [test]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: '1.23' }
      - run: make release
      - uses: softprops/action-gh-release@v1
        with:
          files: dist/*
```

---

## 18. Critical Implementation Notes

### 18.1 Common Pitfalls

1. **Byte comparison vs semantic comparison**: BSON type ordering matters. `int32(1)` must equal `int64(1)` in queries but they have different byte representations. Index key encoding must normalize numeric types.

2. **Dot notation in nested documents**: `"address.city"` must traverse nested documents. This affects query parsing, update operators, index key extraction, and projection. Build a robust `fieldPath` utility early.

3. **Array semantics**: MongoDB array matching is complex. `{"tags": "go"}` matches `{"tags": ["go", "rust"]}`. `$elemMatch` vs implicit array matching. This is one of the most bug-prone areas.

4. **Cursor lifecycle**: Cursors hold references to storage engine iterators. Must handle: timeout, explicit kill, client disconnect, server restart. Leaked cursors = leaked file descriptors and memory.

5. **WAL ordering in distributed mode**: With Raft, the Raft log replaces the local WAL for replicated data. Single-node mode uses WAL directly. The abstraction must handle both.

6. **Compaction + MVCC interaction**: Compaction must not delete versions that active transactions might need. The GC watermark must be consulted during compaction's merge phase.

7. **Memory pressure**: Large scans can OOM if not bounded. Implement memory accounting: track allocations per query, abort if exceeding limit (default 100MB per query).

8. **fsync semantics**: `os.File.Sync()` behavior varies by OS and filesystem. On Linux ext4 with `data=ordered`, metadata updates require `sync_file_range` + `fdatasync`. Test on real hardware, not just in-memory filesystems.

### 18.2 Performance-Critical Paths

These code paths will be in the hot path for every operation. Optimize aggressively:

1. **BSON decode** — avoid allocations, use zero-copy where possible
2. **Key comparison** — `bytes.Compare` is already well-optimized in Go, but custom comparison for known key formats can be faster
3. **Bloom filter probe** — single cache line access if bits fit in CPU cache
4. **Memtable lookup** — skip list traversal, atomic pointer loads
5. **Block cache lookup** — sharded LRU with RWMutex
6. **Connection read/write** — minimize syscalls with buffered I/O (`bufio.Reader/Writer`)

### 18.3 Implementation Order

The recommended order maximizes testability at each step:

```
Phase 1: Foundation (v0.1)
  1. pkg/bson           — Need this for everything
  2. pkg/engine/wal     — Durability foundation
  3. pkg/engine/memtable — In-memory operations
  4. pkg/engine/bloom   — Needed by SSTable
  5. pkg/engine/sstable — On-disk storage
  6. pkg/engine/cache   — Read performance
  7. pkg/engine/compression — Snappy first
  8. pkg/engine/manifest — Version tracking
  9. pkg/engine/compaction — Leveled strategy
  10. pkg/engine (integration) — Full engine API

Phase 2: Document Layer (v0.2)
  11. pkg/document       — MBF format
  12. pkg/catalog         — Database/collection metadata
  13. pkg/wire            — OP_MSG protocol
  14. pkg/query/parser    — Query AST
  15. pkg/query/executor  — Basic scan + filter
  16. pkg/server/mongo    — TCP server
  17. pkg/index (basic)   — Primary + single field
  18. End-to-end: mongosh can connect, insert, find

Phase 3: Query Power (v0.3)
  19. pkg/query/planner   — Index selection
  20. pkg/query/optimizer — Predicate pushdown
  21. pkg/query/aggregation — Pipeline stages
  22. pkg/index (compound, multikey, TTL)
  23. pkg/txn             — MVCC transactions

Phase 4: Distribution (v0.4)
  24. pkg/replication/raft — Raft consensus
  25. pkg/replication/oplog — Operation log
  26. pkg/auth/scram       — Authentication
  27. pkg/auth/rbac        — Authorization

Phase 5: Production (v0.5+)
  28. pkg/sharding         — Horizontal scaling
  29. pkg/admin            — Web UI
  30. pkg/crypto           — Encryption at rest
  31. pkg/index (text, geo) — Advanced indexes
  32. embed/               — Embedded mode public API
```

---

*This implementation guide is a companion to SPECIFICATION.md. Together they provide the complete blueprint for building Mammoth Engine from scratch.*

**Built with #NOFORKANYMORE philosophy by ECOSTACK TECHNOLOGY OÜ.**
