# Mammoth Engine Documentation

Welcome to Mammoth Engine documentation.

## Available Guides

### Getting Started
- [Deployment Guide](DEPLOYMENT.md) - Production deployment instructions
- [Configuration Guide](CONFIGURATION.md) - Complete configuration reference
- [Tutorials](TUTORIALS.md) - Step-by-step guides for common tasks

### API Reference
- [API Reference](API_REFERENCE.md) - Complete API documentation
- [Architecture Overview](ARCHITECTURE.md) - Technical architecture details

### Project Documentation
- [Main Repository](../)
- [Specification](../.project/SPECIFICATION.md)
- [Implementation](../.project/IMPLEMENTATION.md)

## Quick Links

| Topic | Document |
|-------|----------|
| Installation | [DEPLOYMENT.md](DEPLOYMENT.md) |
| Configuration | [CONFIGURATION.md](CONFIGURATION.md) |
| CRUD Operations | [TUTORIALS.md](TUTORIALS.md#basic-crud-operations) |
| Indexes | [TUTORIALS.md](TUTORIALS.md#working-with-indexes) |
| Replication | [TUTORIALS.md](TUTORIALS.md#setting-up-replica-set) |
| Sharding | [TUTORIALS.md](TUTORIALS.md#configuring-sharding) |
| Transactions | [TUTORIALS.md](TUTORIALS.md#using-transactions) |
| API Commands | [API_REFERENCE.md](API_REFERENCE.md) |

## Getting Started

See the [Deployment Guide](DEPLOYMENT.md) for comprehensive production deployment instructions.

```bash
# Quick start with Docker
docker-compose up -d

# Connect with mongosh
mongosh mongodb://localhost:27017
```

## Documentation Structure

```
docs/
├── README.md           # This file - documentation index
├── DEPLOYMENT.md       # Production deployment guide
├── CONFIGURATION.md    # Configuration reference
├── API_REFERENCE.md    # API documentation
├── TUTORIALS.md        # Step-by-step tutorials
└── ARCHITECTURE.md     # Architecture overview
```

## Contributing to Documentation

Documentation improvements are welcome! Please:
1. Ensure technical accuracy
2. Include working examples
3. Follow existing formatting style
4. Test all code snippets

## Version Information

This documentation is for Mammoth Engine v1.0.0.

Last updated: 2026-03-31
