# Tutorials

Step-by-step tutorials for common Mammoth Engine tasks.

## Table of Contents

- [Getting Started](#getting-started)
- [Basic CRUD Operations](#basic-crud-operations)
- [Working with Indexes](#working-with-indexes)
- [Aggregation Pipeline](#aggregation-pipeline)
- [Setting up Replica Set](#setting-up-replica-set)
- [Configuring Sharding](#configuring-sharding)
- [Using Transactions](#using-transactions)
- [Full-Text Search](#full-text-search)
- [Geospatial Queries](#geospatial-queries)
- [Backup and Restore](#backup-and-restore)
- [Performance Tuning](#performance-tuning)

## Getting Started

### Installation

```bash
# Download and install
wget https://github.com/mammothengine/mammoth/releases/latest/download/mammoth-linux-amd64
cp mammoth-linux-amd64 /usr/local/bin/mammoth
chmod +x /usr/local/bin/mammoth

# Create directories
mkdir -p /var/lib/mammoth /var/log/mammoth /etc/mammoth

# Create default config
cat > /etc/mammoth/mammoth.conf << 'EOF'
server:
  data_dir: "/var/lib/mammoth"

network:
  bind_address: "0.0.0.0"
  port: 27017

auth:
  enabled: false

logging:
  level: "info"
  file: "/var/log/mammoth/mammoth.log"
EOF
```

### Starting the Server

```bash
# Start in foreground
mammoth --config /etc/mammoth/mammoth.conf

# Start as daemon
mammoth --config /etc/mammoth/mammoth.conf --daemon

# Or use systemd
systemctl start mammoth
systemctl enable mammoth
```

### First Connection

```bash
# Connect using mongosh
mongosh mongodb://localhost:27017

# Or using any MongoDB driver
# Connection string: mongodb://localhost:27017
```

## Basic CRUD Operations

### Creating Documents

```javascript
// Insert one document
db.users.insertOne({
  name: "John Doe",
  email: "john@example.com",
  age: 30,
  createdAt: new Date()
})

// Insert multiple documents
db.users.insertMany([
  { name: "Jane Smith", email: "jane@example.com", age: 25 },
  { name: "Bob Wilson", email: "bob@example.com", age: 35 }
])
```

### Reading Documents

```javascript
// Find all users
db.users.find()

// Find with filter
db.users.find({ age: { $gte: 25 } })

// Find with projection (only return name and email)
db.users.find(
  { age: { $gte: 25 } },
  { name: 1, email: 1, _id: 0 }
)

// Find one document
db.users.findOne({ email: "john@example.com" })

// Sort results
db.users.find().sort({ age: -1 })  // Descending

// Limit and skip
db.users.find().skip(10).limit(20)

// Count documents
db.users.countDocuments({ age: { $gte: 25 } })
```

### Updating Documents

```javascript
// Update one document
db.users.updateOne(
  { email: "john@example.com" },
  { $set: { age: 31, lastUpdated: new Date() } }
)

// Update many documents
db.users.updateMany(
  { age: { $lt: 30 } },
  { $inc: { age: 1 } }  // Increment age by 1
)

// Upsert (insert if not exists)
db.users.updateOne(
  { email: "new@example.com" },
  { $set: { name: "New User", age: 20 } },
  { upsert: true }
)

// Array operations
db.users.updateOne(
  { email: "john@example.com" },
  { $push: { tags: "premium" } }
)
```

### Deleting Documents

```javascript
// Delete one document
db.users.deleteOne({ email: "john@example.com" })

// Delete many documents
db.users.deleteMany({ age: { $lt: 18 } })

// Delete all documents (use with caution!)
db.users.deleteMany({})
```

## Working with Indexes

### Creating Indexes

```javascript
// Single field index (ascending)
db.users.createIndex({ email: 1 }, { unique: true })

// Compound index
db.users.createIndex({ lastName: 1, firstName: 1 })

// Text index for full-text search
db.articles.createIndex({ content: "text" })

// Hashed index (for sharding)
db.users.createIndex({ _id: "hashed" })

// TTL index (auto-expire documents)
db.sessions.createIndex(
  { expireAt: 1 },
  { expireAfterSeconds: 0 }
)

// Geospatial index
db.locations.createIndex({ coordinates: "2dsphere" })

// Wildcard index (indexes all fields)
db.logs.createIndex({ "$**": 1 })
```

### Index Management

```javascript
// List all indexes
db.users.getIndexes()

// Drop an index
db.users.dropIndex("email_1")

// Drop all indexes except _id
db.users.dropIndexes()

// Explain query execution
db.users.find({ email: "john@example.com" }).explain("executionStats")
```

## Aggregation Pipeline

### Basic Aggregation

```javascript
// Count users by age group
db.users.aggregate([
  {
    $group: {
      _id: { $floor: { $divide: ["$age", 10] } },
      count: { $sum: 1 },
      avgAge: { $avg: "$age" }
    }
  },
  { $sort: { _id: 1 } }
])
```

### Joining Collections

```javascript
// Lookup (left join) example
db.orders.aggregate([
  {
    $lookup: {
      from: "customers",
      localField: "customerId",
      foreignField: "_id",
      as: "customer"
    }
  },
  {
    $unwind: "$customer"
  },
  {
    $project: {
      orderTotal: 1,
      customerName: "$customer.name",
      customerEmail: "$customer.email"
    }
  }
])
```

### Faceted Search

```javascript
db.products.aggregate([
  {
    $facet: {
      categories: [
        { $group: { _id: "$category", count: { $sum: 1 } } }
      ],
      priceRanges: [
        {
          $bucket: {
            groupBy: "$price",
            boundaries: [0, 50, 100, 500, 1000],
            default: "Other"
          }
        }
      ],
      products: [
        { $match: { status: "active" } },
        { $limit: 20 }
      ]
    }
  }
])
```

## Setting up Replica Set

### Initial Setup

```bash
# Create directories for three nodes
mkdir -p /data/{rs0-1,rs0-2,rs0-3}

# Start node 1
mammoth --dbpath /data/rs0-1 --port 27017 --replSet rs0

# Start node 2
mammoth --dbpath /data/rs0-2 --port 27018 --replSet rs0

# Start node 3
mammoth --dbpath /data/rs0-3 --port 27019 --replSet rs0
```

### Initialize Replica Set

```javascript
// Connect to first node
mongosh --port 27017

// Initialize replica set
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "localhost:27017", priority: 2 },
    { _id: 1, host: "localhost:27018" },
    { _id: 2, host: "localhost:27019", arbiterOnly: true }
  ]
})

// Check status
rs.status()
rs.isMaster()
```

### Adding Members

```javascript
// Add new member
rs.add("localhost:27020")

// Add member with configuration
rs.add({
  host: "localhost:27020",
  priority: 0,
  hidden: true,
  slaveDelay: 3600  // 1 hour delay
})

// Remove member
rs.remove("localhost:27020")
```

### Failover Testing

```javascript
// Check current primary
rs.isMaster()

// Step down primary (forces election)
rs.stepDown(60)

// Check new primary
rs.isMaster()
```

## Configuring Sharding

### Config Servers

```bash
# Start config servers (3 for production)
mkdir -p /data/config{1,2,3}

mammoth --configsvr --dbpath /data/config1 --port 27019
mammoth --configsvr --dbpath /data/config2 --port 27020
mammoth --configsvr --dbpath /data/config3 --port 27021
```

### Shard Servers

```bash
# Start shard servers (each a replica set)
mkdir -p /data/shard{1,2}

mammoth --shardsvr --dbpath /data/shard1 --port 27018 --replSet shard1
mammoth --shardsvr --dbpath /data/shard2 --port 27028 --replSet shard2
```

### Initialize Sharding

```javascript
// Connect to config server
mongosh --port 27019

// Initialize config server replica set
rs.initiate({
  _id: "configRS",
  members: [
    { _id: 0, host: "localhost:27019" },
    { _id: 1, host: "localhost:27020" },
    { _id: 2, host: "localhost:27021" }
  ]
})

// Initialize shard replica sets
// (Connect to each shard and run rs.initiate)
```

### Enable Sharding

```javascript
// Connect through mongos (or use mammoth router)
mongosh mongodb://localhost:27017

// Enable sharding on database
sh.enableSharding("mydb")

// Shard collection by hashed _id
sh.shardCollection("mydb.users", { _id: "hashed" })

// Shard by range (natural ordering)
sh.shardCollection("mydb.orders", { orderDate: 1 })

// Check sharding status
sh.status()
sh.isBalancerRunning()
```

## Using Transactions

### Multi-Document Transactions

```javascript
// Start session
session = db.getMongo().startSession()

try {
  // Start transaction
  session.startTransaction({
    readConcern: { level: "snapshot" },
    writeConcern: { w: "majority" }
  })

  // Perform operations within transaction
  session.getDatabase("shop").orders.insertOne({
    _id: ObjectId(),
    customer: "john",
    items: [...],
    total: 100.00
  })

  session.getDatabase("shop").inventory.updateMany(
    { sku: { $in: ["item1", "item2"] } },
    { $inc: { quantity: -1 } }
  )

  session.getDatabase("shop").customers.updateOne(
    { name: "john" },
    { $inc: { totalSpent: 100.00 } }
  )

  // Commit transaction
  session.commitTransaction()
  print("Transaction committed successfully")

} catch (error) {
  // Abort on error
  session.abortTransaction()
  print("Transaction aborted:", error)
  throw error

} finally {
  // Always end session
  session.endSession()
}
```

## Full-Text Search

### Creating Text Index

```javascript
// Single field text index
db.articles.createIndex({ content: "text" })

// Compound text index
db.articles.createIndex({
  title: "text",
  content: "text",
  tags: "text"
}, {
  weights: {
    title: 10,
    content: 5,
    tags: 2
  }
})
```

### Querying Text Index

```javascript
// Basic text search
db.articles.find({ $text: { $search: "mongodb database" } })

// Text search with relevance score
db.articles.aggregate([
  { $match: { $text: { $search: "mongodb" } } },
  { $addFields: { score: { $meta: "textScore" } } },
  { $sort: { score: -1 } },
  { $limit: 10 }
])

// Phrase search
db.articles.find({ $text: { $search: '"distributed database"' } })

// Exclude terms
db.articles.find({ $text: { $search: "database -sql" } })
```

## Geospatial Queries

### Creating Geospatial Index

```javascript
// 2dsphere index for Earth-like sphere
db.locations.createIndex({ coordinates: "2dsphere" })

// Insert location document
db.locations.insertOne({
  name: "Central Park",
  type: "park",
  coordinates: {
    type: "Point",
    coordinates: [-73.968285, 40.785091]  // [longitude, latitude]
  }
})
```

### Geospatial Queries

```javascript
// Find near a point
db.locations.find({
  coordinates: {
    $near: {
      $geometry: {
        type: "Point",
        coordinates: [-73.9857, 40.7484]
      },
      $maxDistance: 5000  // 5km in meters
    }
  }
})

// Find within polygon
db.locations.find({
  coordinates: {
    $geoWithin: {
      $geometry: {
        type: "Polygon",
        coordinates: [[
          [-74, 40.7],
          [-73.9, 40.7],
          [-73.9, 40.8],
          [-74, 40.8],
          [-74, 40.7]
        ]]
      }
    }
  }
})

// Find locations within radius (center sphere)
db.locations.find({
  coordinates: {
    $geoWithin: {
      $centerSphere: [[-73.9857, 40.7484], 5 / 3963.2]  // 5 miles
    }
  }
})
```

## Backup and Restore

### Logical Backup

```bash
# Export entire database
mongodump --host localhost:27017 --db mydb --out /backup/$(date +%Y%m%d)

# Export specific collection
mongodump --host localhost:27017 --db mydb --collection users --out /backup

# Export with query
mongodump --host localhost:27017 --db mydb --collection logs --query '{"date": {"$gte": {"$date": "2024-01-01T00:00:00Z"}}}'
```

### Logical Restore

```bash
# Restore database
mongorestore --host localhost:27017 --db mydb /backup/20240101/mydb

# Restore with drop (removes existing data)
mongorestore --host localhost:27017 --db mydb --drop /backup/mydb
```

### Physical Backup (Snapshot)

```bash
# Create snapshot using LVM (recommended for large datasets)
sudo lvcreate --size 10G --snapshot --name mammoth-snap /dev/vg0/mammoth

# Mount snapshot and copy
sudo mkdir -p /mnt/snap
sudo mount /dev/vg0/mammoth-snap /mnt/snap
cp -r /mnt/snap /backup/mammoth-$(date +%Y%m%d)

# Remove snapshot
sudo umount /mnt/snap
sudo lvremove /dev/vg0/mammoth-snap
```

### Point-in-Time Recovery

```bash
# Backup with oplog
gorilla backup --host localhost:27017 --out /backup/base --oplog

# Restore to specific point in time
gorilla restore --host localhost:27017 /backup/base --oplogReplay --oplogLimit "2024-01-15T10:00:00"
```

## Performance Tuning

### Analyzing Queries

```javascript
// Explain query execution
db.users.find({ email: "john@example.com" }).explain("executionStats")

// Get detailed stats
{
  "queryPlanner": { ... },
  "executionStats": {
    "executionSuccess": true,
    "nReturned": 1,
    "executionTimeMillis": 5,
    "totalKeysExamined": 1,
    "totalDocsExamined": 1,
    "executionStages": {
      "stage": "FETCH",
      "inputStage": {
        "stage": "IXSCAN",
        "indexName": "email_1"
      }
    }
  }
}
```

### Index Optimization

```javascript
// Find slow queries (requires profiling)
db.setProfilingLevel(1, { slowms: 100 })
db.system.profile.find().sort({ ts: -1 }).limit(10)

// Check index usage
db.users.aggregate([
  { $indexStats: {} }
])

// Current operations
db.currentOp({ "secs_running": { $gt: 10 } })
```

### Configuration Tuning

```yaml
# For high write throughput
storage:
  memtable_size: 268435456      # 256 MB
  memtable_count: 8
  wal_sync_interval: "1s"
  compression: false            # Disable for speed

# For read-heavy workloads
storage:
  cache_size: 8589934592        # 8 GB
  bloom_filter_enabled: true
  compression: true

# For large datasets
compaction:
  level0_threshold: 8
  background_threads: 4
```

### Monitoring Performance

```javascript
// Server status
db.serverStatus()

// Database stats
db.stats()

// Collection stats
db.users.stats()

// Current memory usage
db.serverStatus().mem

// Connection pool stats
db.serverStatus().connections

// Operation counters
db.serverStatus().opcounters
```

## Common Patterns

### Pagination

```javascript
// Efficient pagination using _id
db.users.find({ _id: { $gt: lastSeenId } })
  .sort({ _id: 1 })
  .limit(20)

// With filter
db.users.find({
  status: "active",
  _id: { $gt: lastSeenId }
})
.sort({ _id: 1 })
.limit(20)
```

### Schema Validation

```javascript
db.createCollection("users", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["email", "name"],
      properties: {
        email: {
          bsonType: "string",
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
        },
        name: {
          bsonType: "string",
          minLength: 1,
          maxLength: 100
        },
        age: {
          bsonType: "int",
          minimum: 0,
          maximum: 150
        }
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"
})
```

### TTL Documents

```javascript
// Auto-expire sessions after 1 hour
db.sessions.createIndex(
  { createdAt: 1 },
  { expireAfterSeconds: 3600 }
)

db.sessions.insertOne({
  userId: ObjectId(),
  token: "abc123",
  createdAt: new Date()
})
```

### Bulk Operations

```javascript
db.users.bulkWrite([
  { insertOne: { document: { name: "User1", email: "user1@example.com" } } },
  { insertOne: { document: { name: "User2", email: "user2@example.com" } } },
  { updateOne: {
    filter: { email: "john@example.com" },
    update: { $set: { lastLogin: new Date() } }
  }},
  { deleteOne: { filter: { status: "inactive" } } }
])
```
