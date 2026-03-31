# Mammoth Engine API Reference

Complete API reference for Mammoth Engine's MongoDB-compatible wire protocol implementation.

## Table of Contents

- [Connection Commands](#connection-commands)
- [Database Commands](#database-commands)
- [Collection Commands](#collection-commands)
- [CRUD Operations](#crud-operations)
- [Index Commands](#index-commands)
- [Aggregation](#aggregation)
- [Replication Commands](#replication-commands)
- [Sharding Commands](#sharding-commands)
- [Transaction Commands](#transaction-commands)
- [Admin Commands](#admin-commands)

## Connection Commands

### handshake
Initial connection handshake.

```javascript
db.adminCommand({
  "hello": 1,
  "client": {
    "driver": { "name": "mongo-go-driver", "version": "1.12.0" }
  }
})
```

**Response:**
```javascript
{
  "isWritablePrimary": true,
  "secondary": false,
  "maxBsonObjectSize": 16777216,
  "maxMessageSizeBytes": 48000000,
  "maxWriteBatchSize": 100000,
  "logicalSessionTimeoutMinutes": 30,
  "connectionId": 1,
  "minWireVersion": 0,
  "maxWireVersion": 21,
  "readOnly": false,
  "ok": 1
}
```

### isMaster
Check if node is primary.

```javascript
db.isMaster()
```

**Response:**
```javascript
{
  "ismaster": true,
  "secondary": false,
  "hosts": ["localhost:27017"],
  "setName": "rs0",
  "ok": 1
}
```

## Database Commands

### createDatabase
Create a new database.

```javascript
db.adminCommand({ "createDatabase": "mydb" })
```

### dropDatabase
Drop the current database.

```javascript
db.dropDatabase()
```

### listDatabases
List all databases.

```javascript
db.adminCommand({ "listDatabases": 1 })
```

**Response:**
```javascript
{
  "databases": [
    { "name": "admin", "sizeOnDisk": 8192 },
    { "name": "mydb", "sizeOnDisk": 16384 }
  ],
  "totalSize": 24576,
  "ok": 1
}
```

## Collection Commands

### create
Create a new collection.

```javascript
db.createCollection("mycollection", {
  capped: false,
  size: 100000,
  max: 1000,
  validator: { $jsonSchema: { ... } }
})
```

### drop
Drop a collection.

```javascript
db.mycollection.drop()
```

### listCollections
List all collections in database.

```javascript
db.getCollectionNames()
// or
db.listCollections()
```

### collStats
Get collection statistics.

```javascript
db.mycollection.stats()
```

**Response:**
```javascript
{
  "ns": "mydb.mycollection",
  "count": 1000,
  "size": 16384,
  "avgObjSize": 16,
  "storageSize": 20480,
  "capped": false,
  "ok": 1
}
```

## CRUD Operations

### insert
Insert documents into collection.

```javascript
db.mycollection.insertOne({ name: "John", age: 30 })
db.mycollection.insertMany([
  { name: "Jane", age: 25 },
  { name: "Bob", age: 35 }
])
```

### find
Query documents.

```javascript
// Find all
db.mycollection.find()

// Find with filter
db.mycollection.find({ age: { $gte: 25 } })

// Find with projection
db.mycollection.find(
  { status: "active" },
  { name: 1, email: 1 }
)

// Find with sort and limit
db.mycollection.find()
  .sort({ age: -1 })
  .skip(10)
  .limit(20)
```

**Supported Operators:**
- Comparison: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`
- Logical: `$and`, `$or`, `$nor`, `$not`
- Element: `$exists`, `$type`
- Array: `$all`, `$elemMatch`, `$size`
- Text: `$text` (full-text search)
- Geospatial: `$near`, `$geoWithin`, `$geoIntersects`

### update
Update documents.

```javascript
// Update one
db.mycollection.updateOne(
  { _id: ObjectId("...") },
  { $set: { status: "inactive" } }
)

// Update many
db.mycollection.updateMany(
  { status: "pending" },
  { $set: { status: "active" } }
)

// Upsert
db.mycollection.updateOne(
  { email: "new@example.com" },
  { $set: { name: "New User" } },
  { upsert: true }
)
```

**Update Operators:**
- `$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`
- `$rename`, `$currentDate`
- `$addToSet`, `$pop`, `$pull`, `$push`, `$pullAll`

### delete
Delete documents.

```javascript
// Delete one
db.mycollection.deleteOne({ _id: ObjectId("...") })

// Delete many
db.mycollection.deleteMany({ status: "inactive" })
```

### count
Count documents.

```javascript
db.mycollection.countDocuments({ status: "active" })
db.mycollection.estimatedDocumentCount()
```

### distinct
Get distinct values.

```javascript
db.mycollection.distinct("status")
```

## Index Commands

### createIndexes
Create one or more indexes.

```javascript
db.mycollection.createIndex({ email: 1 }, { unique: true })
db.mycollection.createIndex({ name: "text" })
db.mycollection.createIndex({ location: "2dsphere" })
db.mycollection.createIndex({ "$**": 1 })  // Wildcard index
```

**Index Types:**
- Single field: `{ field: 1 }` or `{ field: -1 }`
- Compound: `{ field1: 1, field2: -1 }`
- Multikey (arrays): Automatic for array fields
- Text: `{ field: "text" }`
- Hashed: `{ field: "hashed" }`
- Geospatial: `{ location: "2dsphere" }`
- Wildcard: `{ "$**": 1 }`
- TTL: `{ expireAt: 1 }` with `expireAfterSeconds: 0`

### dropIndexes
Drop indexes.

```javascript
db.mycollection.dropIndex("idx_email_1")
db.mycollection.dropIndexes()  // Drop all indexes
```

### listIndexes
List all indexes.

```javascript
db.mycollection.getIndexes()
```

## Aggregation

### aggregate
Run aggregation pipeline.

```javascript
db.mycollection.aggregate([
  { $match: { status: "active" } },
  { $group: { _id: "$category", total: { $sum: "$amount" } } },
  { $sort: { total: -1 } },
  { $limit: 10 }
])
```

**Pipeline Stages:**
- `$match` - Filter documents
- `$project` - Reshape documents
- `$group` - Group by key
- `$sort` - Sort documents
- `$limit` - Limit results
- `$skip` - Skip documents
- `$unwind` - Deconstruct arrays
- `$lookup` - Left outer join
- `$facet` - Multi-sub-pipelines

**Aggregation Operators:**
- Accumulators: `$sum`, `$avg`, `$min`, `$max`, `$count`, `$first`, `$last`
- Arithmetic: `$add`, `$subtract`, `$multiply`, `$divide`
- String: `$concat`, `$substr`, `$toLower`, `$toUpper`
- Date: `$year`, `$month`, `$dayOfMonth`, `$hour`, `$minute`, `$second`

## Replication Commands

### replSetInitiate
Initialize replica set.

```javascript
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "node1:27017" },
    { _id: 1, host: "node2:27017" },
    { _id: 2, host: "node3:27017" }
  ]
})
```

### replSetStatus
Get replica set status.

```javascript
rs.status()
```

**Response:**
```javascript
{
  "set": "rs0",
  "date": ISODate("2024-01-01T00:00:00Z"),
  "myState": 1,
  "term": 1,
  "members": [
    {
      "_id": 0,
      "name": "node1:27017",
      "health": 1,
      "state": 1,
      "stateStr": "PRIMARY",
      "optime": { "ts": Timestamp(1704067200, 1) }
    }
  ],
  "ok": 1
}
```

### replSetStepDown
Step down as primary.

```javascript
rs.stepDown(60)  // Step down for 60 seconds
```

### replSetReconfig
Reconfigure replica set.

```javascript
var cfg = rs.conf()
cfg.members.push({ _id: 3, host: "node4:27017" })
rs.reconfig(cfg)
```

## Sharding Commands

### enableSharding
Enable sharding on database.

```javascript
sh.enableSharding("mydb")
```

### shardCollection
Shard a collection.

```javascript
sh.shardCollection("mydb.mycollection", { _id: "hashed" })
sh.shardCollection("mydb.orders", { orderDate: 1 }, true)  // Range sharding
```

### shardingStatus
Get sharding status.

```javascript
sh.status()
```

### balancerStart/Stop
Control balancer.

```javascript
sh.startBalancer()
sh.stopBalancer()
sh.setBalancerState(false)
```

### addShard
Add new shard.

```javascript
sh.addShard("rs1/node1:27018,node2:27018,node3:27018")
```

## Transaction Commands

### startTransaction
Start a multi-document transaction.

```javascript
session = db.getMongo().startSession()
session.startTransaction({
  readConcern: { level: "snapshot" },
  writeConcern: { w: "majority" }
})
```

### commitTransaction
Commit transaction.

```javascript
session.commitTransaction()
```

### abortTransaction
Abort transaction.

```javascript
session.abortTransaction()
```

**Transaction Example:**
```javascript
session = db.getMongo().startSession()
try {
  session.startTransaction()

  session.getDatabase("shop").orders.insertOne({ ... })
  session.getDatabase("shop").inventory.updateOne({ ... })

  session.commitTransaction()
} catch (error) {
  session.abortTransaction()
  throw error
} finally {
  session.endSession()
}
```

## Admin Commands

### serverStatus
Get server status.

```javascript
db.serverStatus()
```

### dbStats
Get database statistics.

```javascript
db.stats()
```

### collStats
Get collection statistics.

```javascript
db.runCommand({ collStats: "mycollection" })
```

### validate
Validate collection.

```javascript
db.mycollection.validate({ full: true })
```

### compact
Compact collection.

```javascript
db.runCommand({ compact: "mycollection" })
```

### reIndex
Rebuild indexes.

```javascript
db.mycollection.reIndex()
```

### setProfilingLevel
Set profiling level.

```javascript
db.setProfilingLevel(1, { slowms: 100 })  // Profile slow queries
```

### currentOp
Show current operations.

```javascript
db.currentOp()
```

### killOp
Kill operation.

```javascript
db.killOp(opid)
```

### explain
Get query execution plan.

```javascript
db.mycollection.find({ status: "active" }).explain("executionStats")
```

**Response:**
```javascript
{
  "queryPlanner": {
    "plannerVersion": 1,
    "namespace": "mydb.mycollection",
    "indexFilterSet": false,
    "parsedQuery": { ... },
    "winningPlan": {
      "stage": "FETCH",
      "inputStage": {
        "stage": "IXSCAN",
        "indexName": "idx_status_1",
        ...
      }
    }
  },
  "executionStats": {
    "executionSuccess": true,
    "nReturned": 100,
    "executionTimeMillis": 5,
    "totalKeysExamined": 100,
    "totalDocsExamined": 100,
    ...
  },
  "ok": 1
}
```

## Error Codes

Common error codes returned by Mammoth Engine:

| Code | Name | Description |
|------|------|-------------|
| 0 | OK | Success |
| 13 | Unauthorized | Authentication failed |
| 18 | AuthenticationFailed | Wrong credentials |
| 59 | CommandNotFound | Unknown command |
| 11000 | DuplicateKey | Unique constraint violation |
| 11600 | InterruptedAtShutdown | Server shutting down |
| 11602 | InterruptedDueToReplStateChange | Replication state changed |
| 13435 | NotMasterNoSlaveOk | Not primary and slaveOk not set |
| 13436 | NotMasterOrSecondary | Not in replica set |

## Data Types

Supported BSON data types:

- `Double` - 64-bit IEEE 754 floating point
- `String` - UTF-8 string
- `Object` - Embedded document
- `Array` - Array of values
- `Binary` - Binary data
- `ObjectId` - 12-byte ObjectId
- `Boolean` - true/false
- `Date` - UTC datetime
- `Null` - Null value
- `Regex` - Regular expression
- `Int32` - 32-bit signed integer
- `Int64` - 64-bit signed integer (Long)
- `Decimal128` - 128-bit IEEE 754-2008 decimal
- `Timestamp` - Internal MongoDB timestamp
- `MinKey` / `MaxKey` - Special comparison values

## Limits

| Limit | Value |
|-------|-------|
| Maximum document size | 16 MB |
| Maximum message size | 48 MB |
| Maximum write batch size | 100,000 |
| Maximum index key size | 1024 bytes |
| Maximum collection name length | 120 characters |
| Maximum database name length | 64 characters |
| Maximum field name length | No limit (recommended < 100) |
| Maximum nested depth | 100 levels |
| Maximum indexes per collection | 64 |
| Maximum compound index fields | 31 |
