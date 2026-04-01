// Documentation content stored as objects for easy management
export const docsNavigation = [
  {
    title: 'Getting Started',
    items: [
      { title: 'Introduction', slug: 'intro' },
      { title: 'Quick Start', slug: 'quickstart' },
      { title: 'Installation', slug: 'installation' },
    ],
  },
  {
    title: 'Core Concepts',
    items: [
      { title: 'Architecture', slug: 'architecture' },
      { title: 'Configuration', slug: 'configuration' },
      { title: 'Deployment', slug: 'deployment' },
    ],
  },
  {
    title: 'API Reference',
    items: [
      { title: 'Overview', slug: 'api-overview' },
      { title: 'CRUD Operations', slug: 'crud' },
      { title: 'Indexes', slug: 'indexes' },
      { title: 'Transactions', slug: 'transactions' },
      { title: 'Aggregation', slug: 'aggregation' },
    ],
  },
  {
    title: 'Advanced',
    items: [
      { title: 'Replication', slug: 'replication' },
      { title: 'Sharding', slug: 'sharding' },
      { title: 'Security', slug: 'security' },
      { title: 'Monitoring', slug: 'monitoring' },
    ],
  },
];

export const docsContent: Record<string, string> = {
  intro: `# Introduction

Mammoth Engine is a high-performance, MongoDB-compatible document database built in Go. It provides a lightweight, embeddable alternative to MongoDB while maintaining full wire protocol compatibility.

## Key Features

- **MongoDB Compatible**: Drop-in replacement for MongoDB applications
- **High Performance**: LSM-Tree storage engine with 50K+ writes/sec
- **ACID Transactions**: Multi-document transactions with snapshot isolation
- **Raft Replication**: Built-in consensus for high availability
- **Sharding**: Horizontal scaling across multiple nodes
- **Embeddable**: Use as a library or standalone server

## Use Cases

- **Edge Computing**: Lightweight database for IoT and edge devices
- **Microservices**: Dedicated database per service pattern
- **Testing**: Fast, isolated test databases
- **Caching**: Persistent cache layer with MongoDB query syntax
- **Analytics**: Efficient time-series data storage`,

  quickstart: `# Quick Start

Get Mammoth running in under a minute.

## Installation

\`\`\`bash
# Download latest release
curl -L https://github.com/MammothEngine/mammoth/releases/latest/download/mammoth-linux-amd64 -o mammoth
chmod +x mammoth

# Or build from source
go install github.com/MammothEngine/mammoth/cmd/mammoth@latest
\`\`\`

## Start Server

\`\`\`bash
# Basic start
mammoth serve --data-dir=./data

# With custom port
mammoth serve --data-dir=./data --port=27017

# With authentication
mammoth serve --data-dir=./data --auth=true
\`\`\`

## Connect with mongosh

\`\`\`bash
mongosh mongodb://localhost:27017
\`\`\`

## First Operations

\`\`\`javascript
// Create a database
use mydb

// Insert a document
db.users.insertOne({
  name: "John Doe",
  email: "john@example.com",
  age: 30
})

// Query documents
db.users.find({ age: { $gte: 25 } })

// Create an index
db.users.createIndex({ email: 1 }, { unique: true })
\`\`\`

## Health Check

\`\`\`bash
# HTTP health endpoint
curl http://localhost:8080/health

# Or use mongosh
db.adminCommand({ ping: 1 })
\`\`\``,

  installation: `# Installation

Multiple ways to install Mammoth Engine.

## Binary Releases

### Linux

\`\`\`bash
# AMD64
curl -L https://github.com/MammothEngine/mammoth/releases/latest/download/mammoth-linux-amd64 -o mammoth

# ARM64
curl -L https://github.com/MammothEngine/mammoth/releases/latest/download/mammoth-linux-arm64 -o mammoth

chmod +x mammoth
sudo mv mammoth /usr/local/bin/
\`\`\`

### macOS

\`\`\`bash
# AMD64
curl -L https://github.com/MammothEngine/mammoth/releases/latest/download/mammoth-darwin-amd64 -o mammoth

# ARM64 (Apple Silicon)
curl -L https://github.com/MammothEngine/mammoth/releases/latest/download/mammoth-darwin-arm64 -o mammoth

chmod +x mammoth
sudo mv mammoth /usr/local/bin/
\`\`\`

### Windows

\`\`\`powershell
# PowerShell (AMD64)
Invoke-WebRequest -Uri https://github.com/MammothEngine/mammoth/releases/latest/download/mammoth-windows-amd64.exe -OutFile mammoth.exe
\`\`\`

## Docker

\`\`\`bash
# Pull image
docker pull mammothengine/mammoth:latest

# Run container
docker run -d \\
  --name mammoth \\
  -p 27017:27017 \\
  -p 8080:8080 \\
  -v mammoth-data:/data \\
  mammothengine/mammoth:latest
\`\`\`

## Go Install

\`\`\`bash
# Install latest
go install github.com/MammothEngine/mammoth/cmd/mammoth@latest
\`\`\`

## Build from Source

\`\`\`bash
# Clone repository
git clone https://github.com/MammothEngine/mammoth.git
cd mammoth

# Build
make build

# Run tests
make test
\`\`\``,

  architecture: `# Architecture

Understanding Mammoth's design and components.

## Overview

Mammoth uses a layered architecture inspired by modern database systems:

- **Wire Protocol Layer**: Full MongoDB wire protocol compatibility
- **Query Engine**: MongoDB query language support with aggregation pipeline
- **Transaction Manager**: ACID transactions with snapshot isolation
- **Storage Engine**: LSM-Tree with SSTable files
- **Replication Layer**: Raft consensus algorithm

## Components

### Wire Protocol Layer
- Full MongoDB wire protocol compatibility
- Supports MongoDB drivers and tools
- Handles OP_QUERY, OP_MSG, and other operations

### Query Engine
- MongoDB query language support
- Aggregation pipeline framework
- Index selection and optimization

### Transaction Manager
- ACID transactions with snapshot isolation
- Multi-version concurrency control (MVCC)
- Deadlock detection and resolution

### Storage Engine
- Log-Structured Merge Tree (LSM-Tree)
- Sorted String Table (SSTable) files
- Write-Ahead Log (WAL) for durability
- Block-based compression`,

  configuration: `# Configuration

Configure Mammoth for your environment.

## Configuration File

\`\`\`yaml
# config.yaml
server:
  host: "0.0.0.0"
  port: 27017
  tls:
    enabled: false
    cert_file: "/path/to/cert.pem"
    key_file: "/path/to/key.pem"

storage:
  data_dir: "./data"
  max_memory: "1GB"
  compression: true

replication:
  enabled: true
  node_id: "node1"
  cluster_name: "mammoth-cluster"
  peers:
    - "node2:2380"
    - "node3:2380"

logging:
  level: "info"
  file: "/var/log/mammoth/mammoth.log"
\`\`\`

## Environment Variables

All config options can be set via environment variables:

\`\`\`bash
export MAMMOTH_SERVER_PORT=27017
export MAMMOTH_STORAGE_DATA_DIR=/data
export MAMMOTH_REPLICATION_ENABLED=true
\`\`\`

## Command Line Flags

\`\`\`bash
# Override config file settings
mammoth serve \\
  --config=/etc/mammoth/config.yaml \\
  --port=27018 \\
  --data-dir=/var/lib/mammoth \\
  --auth=true
\`\`\``,

  deployment: `# Deployment

Deploy Mammoth in production environments.

## Single Node

### Systemd Service

\`\`\`ini
[Unit]
Description=Mammoth Database
After=network.target

[Service]
Type=simple
User=mammoth
ExecStart=/usr/local/bin/mammoth serve --config=/etc/mammoth/config.yaml
Restart=always

[Install]
WantedBy=multi-user.target
\`\`\`

### Docker Compose

\`\`\`yaml
version: '3.8'
services:
  mammoth:
    image: mammothengine/mammoth:latest
    ports:
      - "27017:27017"
      - "8080:8080"
    volumes:
      - mammoth-data:/data
    restart: unless-stopped

volumes:
  mammoth-data:
\`\`\`

## High Availability

### 3-Node Cluster

Configure each node with replication settings pointing to the other two nodes for automatic failover.`,

  'api-overview': `# API Overview

Mammoth supports MongoDB-compatible APIs.

## Wire Protocol

Full compatibility with MongoDB wire protocol:
- **OP_QUERY**: Standard queries
- **OP_INSERT**: Insert operations
- **OP_UPDATE**: Update operations
- **OP_DELETE**: Delete operations
- **OP_MSG**: Modern message protocol
- **OP_REPLY**: Response format

## Supported Commands

### Database Commands

| Command | Description |
|---------|-------------|
| find | Query documents |
| insert | Insert documents |
| update | Update documents |
| delete | Delete documents |
| aggregate | Aggregation pipeline |
| createIndexes | Create indexes |
| count | Count documents |

### Driver Compatibility

Tested with official MongoDB drivers:
- **Node.js**: mongodb npm package
- **Python**: pymongo
- **Go**: mongo-driver
- **Java**: MongoDB Java Driver
- **C#**: MongoDB .NET Driver`,

  crud: `# CRUD Operations

Create, Read, Update, Delete operations.

## Create

### Insert One

\`\`\`javascript
db.users.insertOne({
  name: "Alice",
  email: "alice@example.com"
})
\`\`\`

### Insert Many

\`\`\`javascript
db.users.insertMany([
  { name: "Bob", email: "bob@example.com" },
  { name: "Charlie", email: "charlie@example.com" }
])
\`\`\`

## Read

### Find All

\`\`\`javascript
db.users.find()
db.users.find().toArray()
\`\`\`

### Find with Filter

\`\`\`javascript
db.users.find({ age: { $gte: 18 } })
db.users.findOne({ email: "alice@example.com" })
\`\`\`

## Update

\`\`\`javascript
db.users.updateOne(
  { _id: ObjectId("...") },
  { $set: { name: "New Name" } }
)
\`\`\`

## Delete

\`\`\`javascript
db.users.deleteOne({ _id: ObjectId("...") })
db.users.deleteMany({ status: "inactive" })
\`\`\``,

  indexes: `# Indexes

Indexes improve query performance.

## Creating Indexes

### Single Field Index

\`\`\`javascript
db.users.createIndex({ email: 1 })
db.users.createIndex({ createdAt: -1 })
\`\`\`

### Compound Index

\`\`\`javascript
db.users.createIndex({ status: 1, createdAt: -1 })
\`\`\`

### Unique Index

\`\`\`javascript
db.users.createIndex({ email: 1 }, { unique: true })
\`\`\`

### TTL Index

\`\`\`javascript
db.sessions.createIndex(
  { expiresAt: 1 },
  { expireAfterSeconds: 0 }
)
\`\`\`

## Managing Indexes

\`\`\`javascript
db.users.getIndexes()
db.users.dropIndex("email_1")
db.users.dropIndexes()
\`\`\``,

  transactions: `# Transactions

ACID transactions for data consistency.

## Using Transactions

### Basic Transaction

\`\`\`javascript
const session = db.getMongo().startSession()

try {
  session.startTransaction()

  const users = session.getDatabase("mydb").users
  const orders = session.getDatabase("mydb").orders

  users.updateOne({ _id: userId }, { $inc: { balance: -100 } })
  orders.insertOne({ userId: userId, amount: 100 })

  session.commitTransaction()
} catch (error) {
  session.abortTransaction()
  throw error
} finally {
  session.endSession()
}
\`\`\`

## Transaction Behaviors

- **Snapshot Isolation**: Consistent point-in-time view
- **Atomicity**: All operations succeed or all roll back
- **Durability**: Committed transactions survive crashes`,

  aggregation: `# Aggregation Pipeline

Powerful data processing framework.

## Basic Pipeline

\`\`\`javascript
db.orders.aggregate([
  { $match: { status: "completed" } },
  { $group: { _id: "$category", total: { $sum: "$amount" } } },
  { $sort: { total: -1 } }
])
\`\`\`

## Pipeline Stages

### $match

\`\`\`javascript
{ $match: { status: "active", age: { $gte: 18 } } }
\`\`\`

### $group

\`\`\`javascript
{
  $group: {
    _id: "$category",
    totalSales: { $sum: "$amount" },
    count: { $sum: 1 }
  }
}
\`\`\`

### $sort

\`\`\`javascript
{ $sort: { age: -1, name: 1 } }
\`\`\``,

  replication: `# Replication

High availability with Raft consensus.

## Architecture

Mammoth uses the Raft consensus algorithm for distributed consensus with automatic leader election and failover.

## Configuration

### 3-Node Cluster

Configure each node with:
- replication.enabled: true
- node_id: unique identifier
- peers: list of other nodes

## Failover

- Automatic leader election on failure
- Typically completes in under 3 seconds
- Clients automatically redirected to new leader

## Read Scaling

\`\`\`javascript
// Read from followers
db.getMongo().setReadPref("secondary")
db.collection.find()
\`\`\``,

  sharding: `# Sharding

Horizontal scaling across clusters.

## Architecture

Distribute data across multiple shards based on a shard key.

## Shard Key

\`\`\`javascript
// Enable sharding on database
sh.enableSharding("mydb")

// Shard collection
sh.shardCollection("mydb.users", { userId: 1 })
\`\`\`

## Strategies

**Hashed Sharding**: Even distribution
**Ranged Sharding**: Optimized for range queries`,

  security: `# Security

Secure your Mammoth deployment.

## Authentication

\`\`\`javascript
// Create admin user
use admin
db.createUser({
  user: "admin",
  pwd: "secure-password",
  roles: ["root"]
})
\`\`\`

## TLS/SSL

\`\`\`yaml
server:
  tls:
    enabled: true
    cert_file: "/etc/mammoth/server.crt"
    key_file: "/etc/mammoth/server.key"
\`\`\`

## Best Practices

- Enable authentication
- Use TLS for all connections
- Configure firewall rules
- Enable audit logging
- Regular security updates`,

  monitoring: `# Monitoring

Monitor Mammoth performance and health.

## Metrics

\`\`\`yaml
metrics:
  enabled: true
  port: 9100
  path: "/metrics"
\`\`\`

Access: curl http://localhost:9100/metrics

## Health Checks

\`\`\`bash
curl http://localhost:8080/health
curl http://localhost:8080/ready
\`\`\`

## Key Metrics

- mammoth_ops_total: Total operations
- mammoth_op_latency: Operation latency
- mammoth_storage_size: Storage size
- mammoth_replication_lag: Replication lag`,
};

export function getDocContent(slug: string): string {
  return docsContent[slug] || docsContent['intro'];
}