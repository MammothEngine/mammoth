# Mammoth Engine Production Deployment Guide

## Overview

This guide covers deploying Mammoth Engine in production environments with Docker, systemd, monitoring, and best practices for high availability.

## Table of Contents

1. [Quick Start with Docker](#quick-start-with-docker)
2. [Production Configuration](#production-configuration)
3. [Docker Compose Deployment](#docker-compose-deployment)
4. [Systemd Service](#systemd-service)
5. [TLS/SSL Setup](#tlsssl-setup)
6. [Monitoring & Metrics](#monitoring--metrics)
7. [Backup & Restore](#backup--restore)
8. [Security Hardening](#security-hardening)
9. [Troubleshooting](#troubleshooting)

---

## Quick Start with Docker

### Single Node Deployment

```bash
# Pull the image
docker pull mammothengine/mammoth:latest

# Run with basic configuration
docker run -d \
  --name mammoth \
  -p 27017:27017 \
  -p 8080:8080 \
  -p 9100:9100 \
  -v mammoth-data:/data \
  mammothengine/mammoth:latest \
  mammoth serve --data-dir=/data --auth=true

# Check status
docker logs -f mammoth
```

### Docker Compose (Production)

```yaml
version: '3.8'

services:
  mammoth:
    image: mammothengine/mammoth:latest
    container_name: mammoth
    restart: unless-stopped
    ports:
      - "27017:27017"      # MongoDB wire protocol
      - "8080:8080"        # Health check
      - "9100:9100"        # Prometheus metrics
      - "8081:8081"        # Admin UI
    volumes:
      - mammoth-data:/data
      - ./mammoth.conf:/etc/mammoth/mammoth.conf:ro
      - ./tls:/etc/mammoth/tls:ro
    environment:
      - MAMMOTH_ENCRYPTION_KEY=${ENCRYPTION_KEY}
    command: >
      mammoth serve
      --config=/etc/mammoth/mammoth.conf
      --data-dir=/data
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
    ulimits:
      nofile:
        soft: 65536
        hard: 65536

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana:latest
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_PASSWORD}
    volumes:
      - grafana-data:/var/lib/grafana
      - ./grafana-dashboards:/etc/grafana/provisioning/dashboards:ro
    ports:
      - "3000:3000"

volumes:
  mammoth-data:
  grafana-data:
```

---

## Production Configuration

### TOML Configuration File

Create `mammoth.conf`:

```toml
# Server Configuration
[server]
port = 27017
bind = "0.0.0.0"
data-dir = "/data"
log-level = "info"

# Authentication
[server.auth]
enabled = true
require-auth = true

# TLS Configuration
[server.tls]
cert-file = "/etc/mammoth/tls/server.crt"
key-file = "/etc/mammoth/tls/server.key"
min-version = "1.2"

# Performance Tuning
[server.performance]
memtable-size = 67108864        # 64MB
max-wal-size = 134217728        # 128MB
compaction-workers = 2

# Monitoring
[server.metrics]
enabled = true
port = 9100
path = "/metrics"

# Health Check
[server.health]
enabled = true
port = 8080
path = "/health"

# Admin UI
[server.admin]
enabled = true
port = 8081

# Audit Logging
[server.audit]
enabled = true
path = "/var/log/mammoth/audit.log"
max-size = 100
max-backups = 10
max-age = 30

# Compression
[storage]
compression = "snappy"          # snappy, lz4, zstd, none

# Encryption at Rest
[storage.encryption]
enabled = true
key-file = "/etc/mammoth/encryption.key"

# Slow Query Logging
[server.slow-query]
enabled = true
threshold = "100ms"             # Log queries slower than 100ms
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MAMMOTH_ENCRYPTION_KEY` | Master encryption key for data at rest | - |
| `MAMMOTH_CONFIG` | Path to config file | - |
| `MAMMOTH_DATA_DIR` | Data directory | ./data |
| `MAMMOTH_LOG_LEVEL` | Log level (debug, info, warn, error) | info |

---

## Docker Compose Deployment

### 1. Create Project Directory

```bash
mkdir mammoth-production
cd mammoth-production
mkdir -p data tls config
```

### 2. Generate TLS Certificates

```bash
# Generate self-signed certificates (for testing)
docker run --rm -v $(pwd)/tls:/certs mammothengine/mammoth:latest \
  mammoth cert --self-signed --cert=/certs/server.crt --key=/certs/server.key

# Or use Let's Encrypt for production
certbot certonly --standalone -d db.example.com
cp /etc/letsencrypt/live/db.example.com/* ./tls/
```

### 3. Generate Encryption Key

```bash
openssl rand -base64 32 > config/encryption.key
```

### 4. Start Services

```bash
# Set environment
export ENCRYPTION_KEY=$(cat config/encryption.key)
export GRAFANA_PASSWORD=SecurePassword123

# Start
docker-compose up -d

# Verify
docker-compose ps
docker-compose logs -f mammoth
```

---

## Systemd Service

### Native Deployment

Create `/etc/systemd/system/mammoth.service`:

```ini
[Unit]
Description=Mammoth Engine Database
After=network.target
Wants=network.target

[Service]
Type=simple
User=mammoth
Group=mammoth
WorkingDirectory=/var/lib/mammoth

# Security
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/mammoth/data /var/log/mammoth
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true

# Resource Limits
LimitNOFILE=65536
LimitNPROC=4096

# Environment
Environment="MAMMOTH_ENCRYPTION_KEY_FILE=/etc/mammoth/encryption.key"
Environment="MAMMOTH_CONFIG=/etc/mammoth/mammoth.conf"

# Binary
ExecStart=/usr/local/bin/mammoth serve
ExecStop=/bin/kill -TERM $MAINPID
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### Enable Service

```bash
# Create user
useradd -r -s /bin/false mammoth

# Set permissions
mkdir -p /var/lib/mammoth/data /var/log/mammoth
chown -R mammoth:mammoth /var/lib/mammoth /var/log/mammoth

# Enable and start
systemctl daemon-reload
systemctl enable mammoth
systemctl start mammoth
systemctl status mammoth
```

---

## TLS/SSL Setup

### Generate Certificates

```bash
# Create CA
openssl req -new -x509 -days 3650 -keyout ca.key -out ca.crt -subj "/CN=Mammoth CA"

# Create server certificate
openssl req -new -keyout server.key -out server.csr -subj "/CN=mammoth.example.com"
openssl x509 -req -days 365 -in server.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out server.crt

# Set permissions
chmod 600 server.key
chmod 644 server.crt
```

### Client Certificate Authentication

```bash
# Create client certificate
openssl req -new -keyout client.key -out client.csr -subj "/CN=client1"
openssl x509 -req -days 365 -in client.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out client.crt

# Connect with certificate
mongosh --tls --tlsCertificateKeyFile client.pem \
  --tlsCAFile ca.crt mongodb://mammoth.example.com:27017
```

---

## Monitoring & Metrics

### Prometheus Configuration

Create `prometheus.yml`:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'mammoth'
    static_configs:
      - targets: ['mammoth:9100']
    metrics_path: /metrics
```

### Key Metrics

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `mammoth_requests_total` | Total requests | > 10000/min |
| `mammoth_request_duration_seconds` | Request latency | p99 > 100ms |
| `mammoth_storage_size_bytes` | Database size | > 80% disk |
| `mammoth_memtable_size_bytes` | Memtable size | > 64MB |
| `mammoth_compactions_total` | Compactions | failed > 0 |

### Health Check Endpoint

```bash
# Health check
curl http://localhost:8080/health

# Expected response:
# {"status":"ok","uptime":"1h30m","version":"0.9.0"}
```

---

## Backup & Restore

### Automated Backup

```bash
#!/bin/bash
# backup.sh - Run daily via cron

BACKUP_DIR=/backups/mammoth
DATE=$(date +%Y%m%d_%H%M%S)
RETENTION_DAYS=7

# Create backup
mkdir -p $BACKUP_DIR
mammoth backup --output=$BACKUP_DIR/backup_$DATE.tar.gz

# Upload to S3 (optional)
aws s3 cp $BACKUP_DIR/backup_$DATE.tar.gz s3://mammoth-backups/

# Clean old backups
find $BACKUP_DIR -name "backup_*.tar.gz" -mtime +$RETENTION_DAYS -delete
```

### Restore Procedure

```bash
# Stop writes
systemctl stop mammoth

# Restore data
cd /var/lib/mammoth
mv data data.old.$(date +%s)
mammoth restore --input=/backups/mammoth/backup_20250330_120000.tar.gz

# Start service
systemctl start mammoth

# Verify
mammoth stats
```

### Point-in-Time Recovery

```bash
# Restore to specific transaction
mammoth restore \
  --input=/backups/mammoth/backup_20250330_120000.tar.gz \
  --target-time="2025-03-30T14:30:00Z"
```

---

## Security Hardening

### Network Security

```bash
# Firewall rules (iptables)
iptables -A INPUT -p tcp --dport 27017 -s 10.0.0.0/8 -j ACCEPT
iptables -A INPUT -p tcp --dport 27017 -j DROP

# Use with reverse proxy (nginx)
```

### Access Control

```javascript
// Create admin user
db.createUser({
  user: "admin",
  pwd: "SecurePassword123",
  roles: ["root"]
})

// Create read-only user
db.createUser({
  user: "readonly",
  pwd: "ReadOnly123",
  roles: [{ role: "read", db: "app" }]
})
```

### Encryption

```bash
# Enable encryption at rest
export MAMMOTH_ENCRYPTION_KEY=$(openssl rand -base64 32)

# Verify encryption
mammoth validate --check-encryption
```

---

## Troubleshooting

### Common Issues

#### Connection Refused

```bash
# Check service status
systemctl status mammoth

# Check logs
journalctl -u mammoth -f

# Verify port binding
netstat -tlnp | grep 27017
```

#### High Memory Usage

```bash
# Check memory stats
curl http://localhost:9100/metrics | grep mammoth_mem

# Adjust memtable size in config
memtable-size = 33554432  # 32MB
```

#### Slow Queries

```bash
# Enable slow query logging
mammoth serve --slow-query-threshold=50ms

# Check audit log
tail -f /var/log/mammoth/audit.log | grep slow
```

### Diagnostic Commands

```bash
# Server statistics
mammoth stats

# Database validation
mammoth validate

# Compact storage
mammoth compact

# Check current operations
mammoth current-op
```

### Support

- GitHub Issues: https://github.com/mammothengine/mammoth/issues
- Documentation: https://mammoth.engine/docs
- Community: https://discord.gg/mammoth

---

## License

Apache 2.0 - See LICENSE file
