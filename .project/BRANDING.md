# Mammoth Engine — BRANDING

> Visual identity, messaging, and brand guidelines for Mammoth Engine.

---

## 1. Brand Identity

### 1.1 Name

**Mammoth Engine**

- Full name: Mammoth Engine
- Short name: Mammoth
- CLI command: `mammoth`
- Go module: `github.com/mammothengine/mammoth`
- NPM scope: N/A (pure Go project)
- Docker image: `mammothengine/mammoth`

### 1.2 Tagline

**Primary:** *"The Untamed Document Engine"*

**Alternatives for context:**
- *"Documents at Mammoth Scale"* — for scalability messaging
- *"Store Everything. Fear Nothing."* — for reliability messaging
- *"One Binary. Zero Limits."* — for simplicity messaging
- *"Born to Replace. Built to Last."* — for competitive positioning
- *"The Last Document Engine You'll Ever Need"* — for landing page hero

### 1.3 Elevator Pitch

**One sentence:**
Mammoth Engine is a MongoDB-compatible document database engine delivered as a single Go binary with zero dependencies.

**One paragraph:**
Mammoth Engine is a modern document database engine that speaks MongoDB's wire protocol, letting you drop in a single binary and replace the entire MongoDB ecosystem — no mongos, no config servers, no operational headaches. Built from scratch in pure Go with zero external dependencies, it features an LSM-Tree storage engine, ACID transactions with MVCC, Raft-based replication, automatic sharding, and an embedded mode for SQLite-like usage. All your existing MongoDB drivers, tools, and ODMs work out of the box.

### 1.4 Brand Personality

| Trait | Description |
|-------|-------------|
| **Powerful** | Mammoth-scale strength. Handles anything thrown at it. |
| **Primal** | Raw, fundamental, from-scratch. No dependencies, no compromises. |
| **Untamed** | Not domesticated by corporate processes. Wild, free, open source. |
| **Enduring** | Like the mammoth's legacy — built to outlast trends and hype cycles. |
| **Minimal** | One binary, one config, one command. Brutally simple. |

---

## 2. Visual Identity

### 2.1 Logo Concept

**Primary symbol:** A mammoth silhouette merged with engine/mechanical elements.

**Direction options:**

1. **Geometric Mammoth** — Minimal, low-poly mammoth silhouette. Angular lines suggest precision engineering. Tusk curves into a gear or piston shape.

2. **Mammoth Head Profile** — Side view of mammoth head, tusk prominent. Clean vector style. Mechanical texture or circuit-board pattern subtly integrated into the body.

3. **Tusk + Document** — Abstract: mammoth tusk curving around a document/page icon. Merges "document store" with "mammoth" identity.

4. **Monoline Mammoth** — Single continuous line drawing of a mammoth. Modern, clean, scalable from favicon to billboard.

**Recommendation:** Option 1 (Geometric Mammoth) — balances recognizability with modern tech aesthetic. Works at all sizes.

### 2.2 Color Palette

**Primary Colors:**

| Name | Hex | Usage |
|------|-----|-------|
| **Mammoth Dark** | `#1A1A2E` | Primary background, headers, hero sections |
| **Mammoth Tusk** | `#E8D5B7` | Primary accent, CTAs, highlights, tusk in logo |
| **Mammoth Ice** | `#4ECDC4` | Secondary accent, links, success states, code highlights |

**Supporting Colors:**

| Name | Hex | Usage |
|------|-----|-------|
| **Permafrost** | `#F0F0F5` | Light backgrounds, cards |
| **Glacier** | `#2C3E6B` | Secondary backgrounds, code blocks |
| **Bone** | `#FAFAF8` | Page background (light mode) |
| **Ember** | `#FF6B6B` | Error states, destructive actions, warnings |
| **Moss** | `#95D5B2` | Success states, healthy indicators |
| **Amber** | `#FFB347` | Warning states, in-progress indicators |

**Dark Mode (default for developer tools):**
- Background: `#0D0D1A`
- Surface: `#1A1A2E`
- Text: `#E8E8F0`
- Muted text: `#8888AA`

### 2.3 Typography

**Headings:** Inter (or JetBrains Mono for technical context)
**Body:** Inter
**Code:** JetBrains Mono

Fallback stack: `-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif`

### 2.4 Iconography

- **Style:** Outlined, 2px stroke, rounded corners
- **Grid:** 24x24px base
- **Source:** Lucide icons (consistent with modern dev tools)
- **Custom icons:** Mammoth symbol for brand identity, engine gears for system features

---

## 3. Messaging Framework

### 3.1 Key Messages

**For developers:**
> "Drop in one binary. Your MongoDB drivers already work. Everything else is better."

**For DevOps/SRE:**
> "Replace mongod + mongos + config servers with a single process. 50MB RAM baseline, not 1GB+."

**For CTOs/decision makers:**
> "Apache 2.0 licensed. No SSPL ambiguity. No vendor lock-in. Run it anywhere."

**For the open-source community:**
> "Built from scratch in pure Go. Zero external dependencies. Every line is original. #NOFORKANYMORE"

### 3.2 Competitive Positioning

| vs MongoDB | Mammoth Engine Advantage |
|------------|------------------------|
| Multi-process deployment | Single binary |
| ~1GB+ RAM baseline | < 50MB RAM baseline |
| SSPL license | Apache 2.0 |
| No embedded mode | SQLite-like embedded mode |
| Complex replica set setup | One command Raft cluster |
| Separate mongos for sharding | Built-in router in every node |
| Requires extensive tuning | Secure by default, sane defaults |

| vs FerretDB | Mammoth Engine Advantage |
|-------------|------------------------|
| Proxy over Postgres/SQLite | Native document engine |
| Limited performance (translation overhead) | Native speed, no translation |
| Subset of MongoDB features | Full MongoDB compatibility target |
| No replication of its own | Built-in Raft replication |
| No sharding | Built-in sharding |

### 3.3 Feature Highlights (for README/landing page)

```
🦣 MongoDB Compatible — Wire protocol compatible. Existing drivers just work.
📦 Single Binary — One file. No dependencies. No Docker required.
⚡ Lightning Fast — LSM-Tree engine, zero-copy reads, lock-free memtable.
🔒 Secure by Default — TLS, auth, encryption at rest — all built in.
🔄 Raft Replication — True consensus. Automatic failover in seconds.
📊 Built-in Admin UI — Query playground, monitoring, user management.
🧩 Embedded Mode — Use as a Go library, like SQLite for documents.
📜 Apache 2.0 — No SSPL, no ambiguity, no vendor lock-in.
🏗️ Zero Dependencies — Pure Go standard library. No CGo, no vendored C code.
🐧 Cross-Platform — Linux, macOS, Windows. amd64 + arm64. < 30MB binary.
```

---

## 4. Web Presence

### 4.1 Domain

- **Primary:** mammothengine.com
- **GitHub:** github.com/mammothengine/mammoth
- **Docker Hub:** hub.docker.com/r/mammothengine/mammoth
- **X (Twitter):** @mammothengine (if available)

### 4.2 Landing Page Structure

```
1. Hero Section
   - Logo + "Mammoth Engine"
   - Tagline: "The Untamed Document Engine"
   - One-liner: "MongoDB-compatible. Single binary. Zero dependencies."
   - CTA: [Get Started] [GitHub]
   - Terminal animation: `curl -sL mammothengine.com/install | sh && mammoth server`

2. Feature Grid (2x5 icons)
   - MongoDB Compatible
   - Single Binary
   - Zero Dependencies
   - ACID Transactions
   - Raft Replication
   - Embedded Mode
   - Built-in Admin UI
   - Encryption at Rest
   - Cross-Platform
   - Apache 2.0 License

3. Comparison Table
   - Mammoth vs MongoDB vs FerretDB
   - RAM usage, binary size, deployment complexity, license

4. Quick Start
   - 3 steps: Download → Start → Connect
   - Code sample with mongosh

5. Architecture Diagram
   - Visual showing single binary containing all components

6. Testimonials / GitHub Stars counter

7. Footer
   - Links: Docs, GitHub, Discord, Twitter
   - ECOSTACK TECHNOLOGY OÜ
```

### 4.3 README.md Structure

```markdown
# 🦣 Mammoth Engine

> The Untamed Document Engine

MongoDB-compatible document database. Single binary. Zero dependencies. Pure Go.

## Quick Start

### Install
curl -sL mammothengine.com/install | sh

### Start
mammoth server --data-dir ./data

### Connect
mongosh mongodb://localhost:27017

## Features
[Feature grid]

## Benchmarks
[Performance comparison table]

## Documentation
[Links to docs]

## Contributing
[Contribution guidelines]

## License
Apache 2.0
```

---

## 5. Community

### 5.1 Channels

| Channel | Platform | Purpose |
|---------|----------|---------|
| GitHub Issues | github.com/mammothengine/mammoth/issues | Bug reports, feature requests |
| GitHub Discussions | github.com/mammothengine/mammoth/discussions | Q&A, ideas, showcase |
| Discord | discord.gg/mammothengine | Real-time community chat |
| X (Twitter) | @mammothengine | Announcements, dev updates |

### 5.2 Content Strategy

- **Release announcements** on GitHub, X, and blog
- **Technical deep-dives** on how the storage engine works (blog posts)
- **Comparison posts** (Mammoth vs MongoDB on YCSB benchmarks)
- **Turkish developer content** on X (via Ersin's personal account + x-persona-ersin skill)
- **Conference talks** on building a database from scratch in Go

### 5.3 Open Source Strategy

- **License:** Apache 2.0 (permissive, no SSPL concerns)
- **Governance:** Benevolent dictator (Ersin), open to contributors
- **Contributing guide:** Clear PR guidelines, code style, test requirements
- **Code of Conduct:** Standard Contributor Covenant
- **Roadmap:** Public in TASKS.md, community input via GitHub Discussions

---

## 6. Nano Banana 2 Logo Prompt

For generating logo concepts with Nano Banana 2:

### Prompt 1 — Geometric Mammoth Logo

```
Minimal geometric mammoth head logo for a database software called "Mammoth Engine". Low-poly angular style with clean vector lines. The mammoth's tusk subtly curves into a gear/cog shape to represent "engine". Color palette: dark navy (#1A1A2E) body with warm ivory/cream (#E8D5B7) tusk accent. Flat design, no gradients, no text. Suitable for app icon and favicon. White background. Professional tech company logo aesthetic.
```

### Prompt 2 — Monoline Mammoth Logo

```
Single continuous line drawing of a woolly mammoth for tech brand "Mammoth Engine". Modern monoline style, one stroke weight, elegant curves. The line forms both the mammoth silhouette and suggests mechanical precision. Teal accent color (#4ECDC4) on dark background (#1A1A2E). Minimal, scalable from 16px to billboard. No text in image. Clean vector logo style.
```

### Prompt 3 — Mammoth + Document Icon

```
Abstract logo combining a mammoth tusk shape with a document/file icon for "Mammoth Engine" database. The curved tusk wraps around or integrates with a stylized document page. Clean geometric style. Colors: ivory tusk (#E8D5B7) on dark navy (#1A1A2E). Flat design, suitable for GitHub profile picture and social media avatar. Professional, modern, tech brand aesthetic.
```

---

*Branding is a living document. Visual assets will be refined as the project matures.*

**ECOSTACK TECHNOLOGY OÜ — #NOFORKANYMORE**
