# Mammoth Engine — TASKS

> Granular task breakdown derived from SPECIFICATION.md and IMPLEMENTATION.md.
> Each task is atomic, testable, and independently mergeable.

---

## Phase 1: Foundation — Storage Engine (v0.1.0)

### 1.1 BSON Codec
- [ ] Define BSON type constants and type enum (`pkg/bson/types.go`)
- [ ] Implement `ObjectID` generation (12-byte, timestamp + random + counter)
- [ ] Implement `ObjectID` parsing (hex string ↔ bytes, JSON marshaling)
- [ ] Implement BSON `Document` type (ordered map with fast key lookup)
- [ ] Implement BSON `Value` type (tagged union for all BSON types)
- [ ] Implement BSON encoder (Document → `[]byte`)
- [ ] Implement BSON decoder (`[]byte` → Document, zero-copy raw access)
- [ ] Implement BSON comparison function (cross-type ordering per MongoDB spec)
- [ ] Implement struct marshaling (Go struct → BSON via reflection + struct tags)
- [ ] Implement struct unmarshaling (BSON → Go struct via reflection)
- [ ] Implement convenience constructors: `bson.D()`, `bson.M()`, `bson.A()`
- [ ] Write comprehensive BSON round-trip tests (all types, edge cases)
- [ ] Benchmark BSON encode/decode vs raw JSON baseline

### 1.2 Write-Ahead Log (WAL)
- [ ] Define WAL record format (header: CRC-32C + length + type + seqno)
- [ ] Implement CRC-32C (Castagnoli) checksum function
- [ ] Implement WAL segment file writer (32KB block alignment)
- [ ] Implement WAL record types (Full, First, Middle, Last fragments)
- [ ] Implement WAL operation types (Put, Delete, BatchStart, BatchEnd)
- [ ] Implement WAL segment rotation (close at 64MB, create new)
- [ ] Implement WAL replay/recovery (read all segments, rebuild memtable)
- [ ] Implement sync modes (Full fsync, Batch 100ms, None)
- [ ] Handle corruption detection (CRC mismatch → truncate tail)
- [ ] Handle incomplete batch recovery (discard partial batch without BatchEnd)
- [ ] Implement old segment cleanup (delete after flush to SSTable)
- [ ] Write WAL tests: write/read round-trip, crash recovery simulation, corruption

### 1.3 Memtable (Concurrent Skip List)
- [ ] Implement arena allocator (pre-allocated byte slab, bump allocation)
- [ ] Implement skip list node structure (key, value, atomic tower pointers)
- [ ] Implement random height generation (1/4 probability, max height 20)
- [ ] Implement skip list `Put` (insert or update, mutex-guarded writes)
- [ ] Implement skip list `Get` (lock-free read via atomic pointer loads)
- [ ] Implement skip list `Delete` (tombstone marker)
- [ ] Implement skip list `Iterator` (SeekToFirst, SeekToLast, Seek, Next, Prev)
- [ ] Track skip list size (atomic counter for bytes + entry count)
- [ ] Implement memtable wrapper (active + immutable list management)
- [ ] Implement memtable rotation (freeze active → immutable, create new active)
- [ ] Write concurrent access tests (multiple goroutines read/write simultaneously)
- [ ] Benchmark skip list operations vs Go `sync.Map` baseline

### 1.4 Bloom Filter
- [ ] Implement Murmur3 128-bit hash function (pure Go)
- [ ] Implement Kirsch-Mitzenmacher double-hashing (k hashes from 2 base hashes)
- [ ] Implement bloom filter `Add` (set k bits)
- [ ] Implement bloom filter `MayContain` (test k bits)
- [ ] Implement optimal parameter calculation (10 bits/key, 7 hashes for 1% FPR)
- [ ] Implement bloom filter serialization (for SSTable meta block)
- [ ] Write false positive rate validation test (insert N keys, measure FPR)

### 1.5 SSTable
- [ ] Define SSTable file format (data blocks + meta blocks + index block + footer)
- [ ] Define footer format (48 bytes, magic number 0x4D414D4D "MAMM")
- [ ] Implement data block builder (prefix compression, restart points every 16 entries)
- [ ] Implement varint encoding/decoding (for compact size prefixes)
- [ ] Implement SSTable `Writer` (buffered data blocks → compressed → file)
- [ ] Implement SSTable `Writer.Add(key, value)` with block flushing
- [ ] Implement SSTable `Writer.Finish()` (flush last block, write bloom + index + footer)
- [ ] Implement SSTable `Reader.Open()` (read footer, load index + bloom into memory)
- [ ] Implement SSTable `Reader.Get(key)` (bloom → index binary search → block read)
- [ ] Implement SSTable block reader (decompress, prefix-expand, binary search restarts)
- [ ] Implement SSTable `Iterator` (sequential scan across blocks)
- [ ] Implement merge iterator (k-way merge of multiple SSTable iterators)
- [ ] Write SSTable round-trip tests (write N entries, read back all)
- [ ] Benchmark SSTable read performance (point lookup, range scan)

### 1.6 Compression (Snappy — Phase 1)
- [ ] Implement Snappy block compression (literal + copy operations, pure Go)
- [ ] Implement Snappy decompression
- [ ] Implement compression interface (Compress/Decompress with algorithm selection)
- [ ] Write compression round-trip tests with various data patterns
- [ ] Benchmark compression ratio and speed

### 1.7 Block Cache (Sharded LRU)
- [ ] Implement LRU doubly-linked list (insert at head, evict from tail)
- [ ] Implement single LRU shard (mutex + map + linked list)
- [ ] Implement sharded LRU cache (16 shards, shard by key hash)
- [ ] Implement cache `Get` (RLock, lookup, promote to head)
- [ ] Implement cache `Put` (Lock, insert, evict if over capacity)
- [ ] Implement cache size tracking (bytes used vs capacity)
- [ ] Write cache tests: hit/miss, eviction order, concurrent access

### 1.8 Manifest (Version Management)
- [ ] Define manifest log entry format (AddFile, RemoveFile, NextFileNumber, etc.)
- [ ] Implement `Version` struct (SSTable files per level)
- [ ] Implement `FileMetadata` (fileNum, size, key range, entry count)
- [ ] Implement manifest log writer (append entries atomically)
- [ ] Implement manifest log reader (replay to reconstruct current version)
- [ ] Implement manifest snapshot (compact manifest log periodically)
- [ ] Implement atomic version swap (install new version after compaction)
- [ ] Write manifest recovery tests (crash during compaction, manifest corruption)

### 1.9 Compaction (Leveled)
- [ ] Implement compaction scoring (L0 file count, L1+ byte ratio)
- [ ] Implement file picker (select L(N) file + overlapping L(N+1) files)
- [ ] Implement compaction merge (k-way merge → new SSTable files)
- [ ] Implement tombstone handling (drop tombstones at bottom level)
- [ ] Implement compaction concurrency control (max 2 concurrent, L0→L1 priority)
- [ ] Implement size-based SSTable splitting during compaction (64MB per file)
- [ ] Implement compaction background goroutine (trigger on score > 1.0)
- [ ] Implement rate limiting (prevent compaction from starving foreground I/O)
- [ ] Write compaction correctness tests (verify all data survives compaction)
- [ ] Write compaction stress test (high write rate + concurrent reads)

### 1.10 Engine Integration
- [ ] Implement `Engine.Open()` (recover WAL, load manifest, start background goroutines)
- [ ] Implement `Engine.Put()` (WAL write → memtable insert)
- [ ] Implement `Engine.Get()` (memtable → immutables → L0 → L1..N)
- [ ] Implement `Engine.Delete()` (tombstone write)
- [ ] Implement `Engine.NewBatch()` and `Engine.Write(batch)` (atomic multi-key writes)
- [ ] Implement `Engine.NewSnapshot()` (MVCC snapshot for consistent reads)
- [ ] Implement `Engine.NewIterator()` (merge iterator across all levels)
- [ ] Implement `Engine.Flush()` (force memtable flush)
- [ ] Implement `Engine.Close()` (flush, sync, cleanup)
- [ ] Implement `Engine.Stats()` (file counts, sizes, cache hit ratio, etc.)
- [ ] Implement memtable flush trigger (background goroutine, size threshold)
- [ ] Write full engine integration tests (CRUD, batch, snapshot, concurrent access)
- [ ] Write crash recovery integration test (kill process, verify data integrity)
- [ ] Run initial YCSB-style benchmark (workload A: 50/50 read/write)

---

## Phase 2: Document Layer & Wire Protocol (v0.2.0)

### 2.1 Mammoth Binary Format (MBF)
- [ ] Define MBF document layout (header + field index + field data + checksum)
- [ ] Implement FNV-1a hash for field name hashing
- [ ] Implement MBF encoder (BSON Document → MBF bytes)
- [ ] Implement MBF decoder (MBF bytes → BSON Document)
- [ ] Implement `GetField` direct access (binary search on field index, O(1) access)
- [ ] Write MBF round-trip tests and size comparison vs BSON

### 2.2 Catalog & Metadata
- [ ] Define catalog storage format (reserved namespace for metadata documents)
- [ ] Implement `Catalog.CreateDatabase(name)`
- [ ] Implement `Catalog.DropDatabase(name)`
- [ ] Implement `Catalog.ListDatabases()`
- [ ] Implement `Catalog.CreateCollection(db, name, options)`
- [ ] Implement `Catalog.DropCollection(db, name)`
- [ ] Implement `Catalog.ListCollections(db)`
- [ ] Implement namespace ID assignment (compact varint for "db.collection")
- [ ] Implement collection metadata (document count, storage size, index list)
- [ ] Write catalog persistence tests (restart and verify metadata)

### 2.3 Storage Key Encoding
- [ ] Implement namespace-aware key encoding (`{ns_id}{doc_id}{version}{type}`)
- [ ] Implement version inversion (MaxUint64 - version for newest-first sort)
- [ ] Implement key decoding (extract namespace, doc_id, version)
- [ ] Write key ordering tests (verify sort correctness)

### 2.4 Document CRUD Layer
- [ ] Implement `InsertOne` (generate _id if missing, encode MBF, write to engine)
- [ ] Implement `InsertMany` (batch write for multiple documents)
- [ ] Implement `FindOne` (query parse → plan → execute → return first result)
- [ ] Implement `Find` (query → cursor with batch iteration)
- [ ] Implement `UpdateOne` (find doc → apply update operators → write new version)
- [ ] Implement `UpdateMany` (scan + batch update)
- [ ] Implement `DeleteOne` (find doc → write tombstone)
- [ ] Implement `DeleteMany` (scan + batch delete)
- [ ] Implement `FindAndModify` (atomic find + update/delete + return)
- [ ] Implement dot-notation field path traversal (nested document access)
- [ ] Write CRUD integration tests with real engine backend

### 2.5 Wire Protocol
- [ ] Implement `MsgHeader` read/write (16 bytes: length, requestID, responseTo, opCode)
- [ ] Implement OP_MSG reader (parse flagBits, sections, optional checksum)
- [ ] Implement OP_MSG writer (serialize response with body section)
- [ ] Implement Section Kind 0 (body) read/write
- [ ] Implement Section Kind 1 (document sequence) read/write
- [ ] Implement command extraction (first key in body document = command name)
- [ ] Implement error response builder (MongoDB error format with code + message)
- [ ] Implement `ok` response builder (standard success response)
- [ ] Write wire protocol parsing tests with captured MongoDB traffic

### 2.6 MongoDB Server
- [ ] Implement TCP listener with goroutine-per-connection model
- [ ] Implement connection handler (read OP_MSG → dispatch → write response)
- [ ] Implement command dispatcher (route command name to handler function)
- [ ] Implement `hello` / `isMaster` command (driver handshake)
- [ ] Implement `ping` command
- [ ] Implement `buildInfo` command
- [ ] Implement `serverStatus` command
- [ ] Implement `find` command handler
- [ ] Implement `insert` command handler
- [ ] Implement `update` command handler
- [ ] Implement `delete` command handler
- [ ] Implement `findAndModify` command handler
- [ ] Implement `listDatabases` command handler
- [ ] Implement `listCollections` command handler
- [ ] Implement `create` command handler (create collection)
- [ ] Implement `drop` command handler (drop collection)
- [ ] Implement cursor management (allocate cursor ID, track open cursors)
- [ ] Implement `getMore` command handler (continue cursor iteration)
- [ ] Implement `killCursors` command handler
- [ ] Implement cursor timeout (10 min default, background cleanup goroutine)
- [ ] Implement connection limit enforcement (configurable max_connections)
- [ ] Implement graceful shutdown (drain connections, flush, close)
- [ ] **MILESTONE TEST**: Connect with `mongosh`, run insert + find successfully

### 2.7 Query Parser
- [ ] Implement query parser entry point (BSON document → QueryAST)
- [ ] Implement comparison operators ($eq, $ne, $gt, $gte, $lt, $lte)
- [ ] Implement `$in` / `$nin` operators
- [ ] Implement logical operators ($and, $or, $not, $nor)
- [ ] Implement `$exists` operator
- [ ] Implement `$type` operator
- [ ] Implement `$regex` operator
- [ ] Implement implicit `$and` (multiple fields in query document)
- [ ] Implement implicit equality (field: value without $eq)
- [ ] Implement dot-notation parsing for nested field queries
- [ ] Implement array matching semantics ({"tags": "go"} matches array field)
- [ ] Implement `$elemMatch` operator
- [ ] Implement `$size` operator (array length)
- [ ] Implement `$all` operator
- [ ] Write parser tests for all operators with edge cases

### 2.8 Update Operators
- [ ] Implement `$set` (set field value, supports dot notation)
- [ ] Implement `$unset` (remove field)
- [ ] Implement `$inc` (increment numeric field)
- [ ] Implement `$mul` (multiply numeric field)
- [ ] Implement `$min` / `$max` (conditional update)
- [ ] Implement `$rename` (rename field)
- [ ] Implement `$currentDate` (set to current date/timestamp)
- [ ] Implement `$setOnInsert` (set only during upsert insert)
- [ ] Implement `$push` (append to array)
- [ ] Implement `$pull` (remove from array by condition)
- [ ] Implement `$pop` (remove first/last array element)
- [ ] Implement `$addToSet` (append unique to array)
- [ ] Implement `$pullAll` (remove multiple values from array)
- [ ] Implement `$each`, `$sort`, `$slice`, `$position` (array modifiers)
- [ ] Implement upsert logic (insert if not found)
- [ ] Write update operator tests with before/after document verification

### 2.9 Primary Index
- [ ] Implement automatic `_id` index (unique, on every collection)
- [ ] Implement `_id` lookup via storage engine key encoding
- [ ] Implement duplicate `_id` detection on insert
- [ ] Write primary index tests (insert, find by _id, duplicate rejection)

### 2.10 Secondary Indexes (Basic)
- [ ] Implement index key encoding (order-preserving, type-aware)
- [ ] Implement index key encoding for: int32, int64, double, string, objectId, null, bool, datetime
- [ ] Implement descending key encoding (bit flip)
- [ ] Implement single-field index (dedicated LSM-Tree per index)
- [ ] Implement compound index (multi-field key encoding)
- [ ] Implement index `CreateIndex` (background scan + build)
- [ ] Implement index `DropIndex`
- [ ] Implement `listIndexes` command handler
- [ ] Implement `createIndexes` command handler
- [ ] Implement `dropIndexes` command handler
- [ ] Implement index maintenance on insert (add index entries)
- [ ] Implement index maintenance on update (remove old entries, add new)
- [ ] Implement index maintenance on delete (remove index entries)
- [ ] Implement unique index constraint enforcement
- [ ] Write index tests: create, query via index, unique constraint, compound

---

## Phase 3: Query Power & Transactions (v0.3.0)

### 3.1 Query Planner
- [ ] Implement candidate plan enumeration (collection scan + each matching index)
- [ ] Implement index matching (which indexes satisfy query predicates)
- [ ] Implement compound index prefix matching
- [ ] Implement selectivity estimation (heuristic-based for v0.3)
- [ ] Implement plan scoring (selectivity × covered bonus × sort bonus)
- [ ] Implement `explain` command (return plan details without executing)
- [ ] Write planner tests (verify correct index selection for various queries)

### 3.2 Query Optimizer
- [ ] Implement predicate pushdown (push filters closer to scan)
- [ ] Implement redundant filter elimination
- [ ] Implement sort optimization (use index order when available)
- [ ] Implement covered query detection (all fields in projection covered by index)
- [ ] Implement limit pushdown (propagate limit to scan level)

### 3.3 Aggregation Pipeline
- [ ] Implement pipeline executor (chain stage iterators)
- [ ] Implement `$match` stage (reuses query filter executor)
- [ ] Implement `$project` stage (field inclusion/exclusion, computed fields)
- [ ] Implement `$addFields` stage (add computed fields, keep existing)
- [ ] Implement `$group` stage (hash-based grouping in memory)
- [ ] Implement group accumulators: `$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, `$addToSet`, `$count`
- [ ] Implement `$sort` stage (in-memory sort, external sort if > 100MB)
- [ ] Implement `$limit` stage
- [ ] Implement `$skip` stage
- [ ] Implement `$unwind` stage (flatten arrays into individual documents)
- [ ] Implement `$count` stage
- [ ] Implement `$replaceRoot` stage
- [ ] Implement `$sample` stage (random document selection)
- [ ] Implement `$lookup` stage (left outer join via nested scan)
- [ ] Implement expression evaluator for computed fields ($add, $subtract, $multiply, $divide, $concat, $substr, $cond, $switch, $ifNull)
- [ ] Implement date expressions ($year, $month, $dayOfMonth, $hour, etc.)
- [ ] Implement string expressions ($toLower, $toUpper, $trim, $split, etc.)
- [ ] Implement type conversion expressions ($toInt, $toString, $toDate, etc.)
- [ ] Implement `aggregate` command handler
- [ ] Implement `count` command handler
- [ ] Implement `distinct` command handler
- [ ] Write aggregation tests for each stage individually
- [ ] Write aggregation pipeline combination tests

### 3.4 Advanced Indexes
- [ ] Implement multikey index (one entry per array element)
- [ ] Implement multikey index detection (auto-detect array fields during insert)
- [ ] Implement TTL index (store datetime index + background expiration)
- [ ] Implement TTL background worker (60s interval cleanup goroutine)
- [ ] Implement partial index (filter expression evaluated during maintenance)
- [ ] Implement sparse index (skip documents where indexed field is missing)
- [ ] Write tests for each advanced index type

### 3.5 Transaction Manager (MVCC)
- [ ] Implement transaction ID generator (monotonically increasing uint64)
- [ ] Implement `Begin()` (allocate txn_id, capture snapshot watermark)
- [ ] Implement transaction read (version visibility check per MVCC rules)
- [ ] Implement transaction write (buffer writes in memory)
- [ ] Implement `Commit()` (conflict detection → WAL write → memtable insert)
- [ ] Implement `Abort()` (discard write buffer)
- [ ] Implement write-write conflict detection (check write set vs committed versions)
- [ ] Implement watermark tracking (min active txn_id)
- [ ] Implement GC goroutine (delete old versions below watermark)
- [ ] Implement `startTransaction` / `commitTransaction` / `abortTransaction` commands
- [ ] Implement multi-document transaction across collections
- [ ] Implement read-your-own-writes within a transaction
- [ ] Write transaction isolation tests (concurrent txns see correct snapshots)
- [ ] Write conflict detection tests (simultaneous updates to same document)
- [ ] Write transaction recovery tests (crash during commit)

### 3.6 Sort & Pagination
- [ ] Implement external sort (spill to disk when sort data > 100MB)
- [ ] Implement sort with limit optimization (top-K heap sort)
- [ ] Implement proper skip/limit with cursor-based pagination

---

## Phase 4: Replication (v0.4.0)

### 4.1 Raft Core
- [ ] Implement Raft persistent state (currentTerm, votedFor, log — stored in LSM-Tree)
- [ ] Implement Raft log (append, truncate, get entry by index, last index/term)
- [ ] Implement Follower state (accept AppendEntries, grant votes)
- [ ] Implement Candidate state (request votes, handle responses, timeout)
- [ ] Implement Leader state (send AppendEntries, track nextIndex/matchIndex)
- [ ] Implement election timeout (randomized 150-300ms)
- [ ] Implement heartbeat (leader sends empty AppendEntries at interval)
- [ ] Implement RequestVote RPC (term check, log up-to-date check)
- [ ] Implement AppendEntries RPC (log consistency check, append, commit update)
- [ ] Implement commit advancement (leader: majority matchIndex)
- [ ] Implement log application (apply committed entries to state machine)
- [ ] Implement Pre-Vote extension (prevent disruption from partitioned nodes)
- [ ] Write Raft tests: basic election, re-election, log replication, safety properties

### 4.2 Raft Transport
- [ ] Implement TCP-based RPC transport (persistent connections between peers)
- [ ] Implement message serialization (custom binary format, no protobuf dependency)
- [ ] Implement connection pooling and reconnection with exponential backoff
- [ ] Implement request pipelining (send multiple AppendEntries without waiting)
- [ ] Write transport tests: message round-trip, connection failure, reconnection

### 4.3 Raft Snapshot
- [ ] Implement snapshot creation (serialize current state to file)
- [ ] Implement InstallSnapshot RPC (chunked transfer to slow follower)
- [ ] Implement snapshot application (replace local state, truncate log)
- [ ] Implement snapshot scheduling (periodic or on log size threshold)
- [ ] Write snapshot tests: create, transfer, apply, resume on failure

### 4.4 Raft Membership Changes
- [ ] Implement AddNode via joint consensus
- [ ] Implement RemoveNode via joint consensus
- [ ] Implement configuration change safety (one change at a time)
- [ ] Write membership change tests: add node, remove node, leader change during reconfig

### 4.5 Oplog & Replica Set
- [ ] Implement oplog entry format (term, index, timestamp, op_type, namespace, document)
- [ ] Implement oplog applier (apply entries to storage engine)
- [ ] Implement replica set initialization (`replSetInitiate` command)
- [ ] Implement `replSetGetStatus` command
- [ ] Implement `replSetGetConfig` command
- [ ] Implement `replSetStepDown` command
- [ ] Implement read preference routing (primary, secondary, nearest)
- [ ] Implement write concern acknowledgment (w:1, w:majority, w:N, j:true)
- [ ] Write end-to-end replication tests (3-node cluster, write on primary, read on secondary)
- [ ] Write failover tests (kill primary, verify new leader elected, data intact)

---

## Phase 5: Security & Observability (v0.5.0)

### 5.1 Authentication
- [ ] Implement credential storage (username → storedKey, serverKey, salt, iterations)
- [ ] Implement SCRAM-SHA-256 server-side (RFC 7677)
- [ ] Implement `saslStart` / `saslContinue` command handlers
- [ ] Implement `createUser` / `updateUser` / `dropUser` / `usersInfo` commands
- [ ] Implement authentication enforcement (reject unauthenticated commands when auth enabled)
- [ ] Write auth tests: successful login, wrong password, user management

### 5.2 Authorization (RBAC)
- [ ] Implement privilege model (action + resource)
- [ ] Implement built-in roles (read, readWrite, dbAdmin, userAdmin, root, etc.)
- [ ] Implement custom role creation (`createRole`, `updateRole`, `dropRole`)
- [ ] Implement authorization check on every command
- [ ] Write RBAC tests: privilege enforcement, role inheritance

### 5.3 TLS
- [ ] Implement TLS configuration loading (cert, key, CA files)
- [ ] Implement TLS listener for MongoDB protocol
- [ ] Implement TLS for inter-node communication (replication)
- [ ] Implement auto-generated self-signed certs for development mode
- [ ] Write TLS tests: encrypted connection, certificate validation

### 5.4 Encryption at Rest
- [ ] Implement key loading from file
- [ ] Implement AES-256-GCM block encryption/decryption
- [ ] Integrate encryption into SSTable writer (encrypt blocks before write)
- [ ] Integrate encryption into SSTable reader (decrypt blocks after read)
- [ ] Integrate encryption into WAL (encrypt records)
- [ ] Implement key rotation support
- [ ] Write encryption tests: encrypted write/read, key rotation, wrong key rejection

### 5.5 Prometheus Metrics
- [ ] Implement Counter, Gauge, Histogram metric types
- [ ] Implement Prometheus text exposition format serializer
- [ ] Implement `/metrics` HTTP endpoint
- [ ] Register storage metrics (write/read bytes, SSTable count, compaction duration, etc.)
- [ ] Register query metrics (duration histogram, count by operation/status)
- [ ] Register replication metrics (term, commit index, lag)
- [ ] Register connection metrics (current, total)
- [ ] Write metrics endpoint test (verify format, verify values update)

### 5.6 Structured Logging
- [ ] Implement JSON logger (timestamp, level, component, message, fields)
- [ ] Implement log levels (debug, info, warn, error)
- [ ] Implement component-scoped loggers (storage, query, replication, auth, etc.)
- [ ] Implement log rotation (by size or time)
- [ ] Add structured logging to all major code paths

### 5.7 Health & Diagnostics
- [ ] Implement `/health` endpoint (server status, replication state, storage stats)
- [ ] Implement slow query profiler (log queries exceeding threshold)
- [ ] Implement `profile` command (set profiling level: off, slow, all)
- [ ] Implement `currentOp` command (list active operations)
- [ ] Implement `killOp` command (cancel running operation)
- [ ] Implement `validate` command (check collection/index integrity)
- [ ] Implement audit log (security events in JSON format)

---

## Phase 6: Advanced Features (v0.6.0)

### 6.1 Text Index (Full-Text Search)
- [ ] Implement Unicode word boundary tokenizer
- [ ] Implement Porter stemmer (English)
- [ ] Implement Turkish stemmer
- [ ] Implement stop word lists (English, Turkish)
- [ ] Implement inverted index storage (term → posting list in LSM-Tree)
- [ ] Implement TF-IDF scoring
- [ ] Implement `$text` query operator with `$search`
- [ ] Implement `$meta: "textScore"` projection
- [ ] Write text search tests (tokenization, stemming, scoring, multi-language)

### 6.2 Geospatial Index (2dsphere)
- [ ] Implement S2 cell ID computation (pure Go, no external library)
- [ ] Implement S2 cell covering for regions
- [ ] Implement geospatial index storage (S2 cell ID → document ID)
- [ ] Implement `$near` / `$nearSphere` query operators
- [ ] Implement `$geoWithin` query operator (polygon, circle)
- [ ] Implement `$geoIntersects` query operator
- [ ] Implement GeoJSON parsing (Point, LineString, Polygon)
- [ ] Write geospatial tests (point queries, region queries, distance sorting)

### 6.3 Change Streams
- [ ] Implement change event format (operationType, fullDocument, ns, documentKey)
- [ ] Implement oplog tailing for real-time change detection
- [ ] Implement `$changeStream` aggregation stage
- [ ] Implement resume token for reconnection
- [ ] Implement change stream filtering (match on operationType, namespace)
- [ ] Write change stream tests (insert/update/delete events, resume after disconnect)

### 6.4 Schema Validation
- [ ] Implement JSON Schema validator (subset: type, required, properties, items, enum, min/max)
- [ ] Implement `collMod` with validator option
- [ ] Implement validation action (error vs warn)
- [ ] Implement validation level (strict vs moderate)
- [ ] Write schema validation tests (valid/invalid documents, action/level combos)

### 6.5 Capped Collections
- [ ] Implement size-limited collection (auto-delete oldest when size exceeded)
- [ ] Implement document count limit
- [ ] Implement insertion order guarantee (natural sort = insertion order)
- [ ] Implement restriction enforcement (no deletes, no updates that increase size)
- [ ] Write capped collection tests

### 6.6 GridFS
- [ ] Implement `fs.files` metadata collection (filename, length, chunkSize, md5)
- [ ] Implement `fs.chunks` data collection (files_id, n, data)
- [ ] Implement file upload (split into 255KB chunks)
- [ ] Implement file download (stream chunks in order)
- [ ] Implement file delete (remove metadata + all chunks)
- [ ] Write GridFS tests (upload, download, large files, concurrent access)

### 6.7 Wildcard Index
- [ ] Implement dynamic field path extraction (recursively traverse document)
- [ ] Implement wildcard index maintenance (index all fields or path-filtered)
- [ ] Implement wildcard index query matching
- [ ] Write wildcard index tests

### 6.8 Embedded Mode
- [ ] Implement public API package (`embed/mammoth.go`)
- [ ] Implement `Open(path, opts)` → `*DB`
- [ ] Implement `DB.Collection(db, coll)` → `*Collection`
- [ ] Implement all Collection methods (InsertOne, Find, UpdateOne, etc.)
- [ ] Implement `DB.Transaction(fn)` → error
- [ ] Implement `DB.Close()` (clean shutdown)
- [ ] Ensure no server process starts in embedded mode
- [ ] Write embedded mode API tests (mirror server-mode tests)
- [ ] Write embedded mode concurrency tests (multiple goroutines)

---

## Phase 7: Sharding (v0.7.0)

### 7.1 Config Server
- [ ] Implement config server metadata storage (databases, shards, chunks)
- [ ] Implement config server as Raft cluster (3-node, reuses Raft impl)
- [ ] Implement chunk metadata CRUD (create, split, migrate)
- [ ] Implement shard registration and deregistration
- [ ] Implement sharding enable for database/collection

### 7.2 Router
- [ ] Implement routing table cache (shard key → chunk ranges → shard addresses)
- [ ] Implement targeted query routing (extract shard key from query)
- [ ] Implement scatter-gather routing (query all shards when no shard key)
- [ ] Implement result merging (combine results from multiple shards)
- [ ] Implement distributed sort (merge sort across shard results)
- [ ] Implement distributed aggregation (push $match/$sort to shards, merge locally)
- [ ] Implement routing table refresh (periodic + on stale config detection)

### 7.3 Shard Key Strategies
- [ ] Implement hash-based shard key (MD5 hash of field value)
- [ ] Implement range-based shard key (raw field value ranges)
- [ ] Implement shard key extraction from documents
- [ ] Implement shard key validation (immutable after insert)

### 7.4 Chunk Management
- [ ] Implement chunk splitting (split at median key when chunk > 128MB)
- [ ] Implement auto-split trigger (check after inserts)
- [ ] Implement chunk migration (stream data to destination shard)
- [ ] Implement migration coordination (lock chunk, copy, update config, unlock)
- [ ] Implement auto-balancer (background goroutine, even chunk distribution)
- [ ] Write sharding end-to-end tests (insert, query, split, balance, migrate)

---

## Phase 8: Admin & Ecosystem (v0.8.0)

### 8.1 Admin Web UI
- [ ] Design admin UI layout (sidebar navigation, main content area)
- [ ] Implement dashboard page (server stats, charts, connections graph)
- [ ] Implement databases/collections browser
- [ ] Implement document query playground (CodeMirror 6 editor)
- [ ] Implement index management page
- [ ] Implement user management page
- [ ] Implement replica set status page
- [ ] Implement slow query profiler viewer
- [ ] Implement real-time log viewer (SSE stream)
- [ ] Embed UI assets in Go binary via `embed.FS`
- [ ] Implement REST API endpoints for all UI pages

### 8.2 CLI Tools
- [ ] Implement `mammoth backup` (consistent snapshot export)
- [ ] Implement `mammoth restore` (import from backup)
- [ ] Implement `mammoth user create/list/delete`
- [ ] Implement `mammoth compact --force`
- [ ] Implement `mammoth validate --collection`
- [ ] Implement `mammoth top` (real-time operation monitor)
- [ ] Implement `mammoth stats` (collection statistics)
- [ ] Implement `mammoth version`

### 8.3 Configuration
- [ ] Implement TOML config file parser (pure Go, simple subset)
- [ ] Implement CLI flag parsing (stdlib `flag` package)
- [ ] Implement config merging (file defaults → CLI overrides → env var overrides)
- [ ] Implement config validation (required fields, valid ranges)
- [ ] Implement config hot-reload for non-critical settings (log level, slow query threshold)

### 8.4 Documentation
- [ ] Write README.md (overview, quick start, feature list)
- [ ] Write installation guide (binary, Docker, Homebrew, package managers)
- [ ] Write configuration reference
- [ ] Write migration guide (MongoDB → Mammoth Engine)
- [ ] Write embedded mode tutorial
- [ ] Write replication setup guide
- [ ] Write sharding setup guide
- [ ] Write security hardening guide
- [ ] Write performance tuning guide
- [ ] Generate API documentation (Go doc)

### 8.5 Benchmarks & Testing
- [ ] Implement YCSB benchmark runner (workloads A-F)
- [ ] Run YCSB comparison: Mammoth vs MongoDB 7.0
- [ ] Run memory usage comparison: Mammoth vs MongoDB
- [ ] Run startup time comparison: Mammoth vs MongoDB
- [ ] Run MongoDB CRUD specification compatibility tests
- [ ] Run MongoDB aggregation specification compatibility tests
- [ ] Run MongoDB transaction specification compatibility tests
- [ ] Consider Jepsen testing setup for consistency verification

### 8.6 Distribution
- [ ] Set up GitHub Actions CI (test, lint, benchmark)
- [ ] Set up GitHub Actions release pipeline (multi-arch binaries)
- [ ] Create Dockerfile (scratch-based, < 30MB)
- [ ] Create Docker Compose example (3-node replica set)
- [ ] Create Helm chart for Kubernetes deployment
- [ ] Set up Homebrew tap (`mammothengine/tap/mammoth`)
- [ ] Create .deb package build
- [ ] Create .rpm package build
- [ ] Publish Docker images to Docker Hub + GHCR

---

## v1.0.0 Checklist — Production Ready

- [ ] All Phase 1-8 tasks completed
- [ ] MongoDB 6.0 CRUD specification tests pass
- [ ] MongoDB 6.0 aggregation specification tests pass
- [ ] MongoDB 6.0 transaction specification tests pass
- [ ] YCSB benchmarks meet or exceed performance targets
- [ ] 3-node replica set failover completes within 5 seconds
- [ ] Zero data loss on single-node crash (WAL fsync mode)
- [ ] Security audit completed (auth, TLS, encryption at rest)
- [ ] Jepsen test results published
- [ ] Memory baseline < 50MB
- [ ] Binary size < 30MB
- [ ] Documentation complete
- [ ] Docker images published
- [ ] Homebrew formula published
- [ ] GitHub releases with multi-arch binaries
- [ ] CHANGELOG.md maintained
- [ ] Stable API — no breaking changes commitment

---

*Task count: ~350+ individual tasks across 8 phases.*
*Estimated timeline: 12-18 months for v1.0.0 (solo developer, full-time).*

**Built with #NOFORKANYMORE philosophy by ECOSTACK TECHNOLOGY OÜ.**
