# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.1] - 2026-04-01

### Added
- Initial alpha release of Mammoth Engine
- MongoDB-compatible wire protocol implementation
- LSM-Tree storage engine with memtable and SSTable support
- BSON encoding/decoding
- CRUD operations (Insert, Find, Update, Delete)
- Single field and compound index support
- Aggregation pipeline framework
- Raft-based replication for high availability
- Sharding support for horizontal scaling
- ACID transactions with snapshot isolation
- TTL indexes for automatic document expiration
- Full-text search indexes
- Geospatial queries (2dsphere)
- Encryption support (AES-256-GCM)
- Wire protocol handler for MongoDB driver compatibility
- Session management
- User authentication framework
- Backup and restore utilities
- Change streams (initial implementation)
- Comprehensive benchmark suite (26 benchmarks)
- Chaos engineering tests for reliability validation

### Known Issues
- Query optimizer is basic, complex queries may not be fully optimized
- Aggregation operators are limited in this alpha release
- Some edge cases in concurrent transactions need further testing
- Full-text search relevancy scoring is basic

### Notes
This is an early alpha release intended for testing and feedback.
API stability is not guaranteed until v1.0.0.

[0.0.1]: https://github.com/mammothengine/mammoth/releases/tag/v0.0.1
