# NATS Inventory Sync — Day 1-2 Solo Recon Prototype

**Assignment 1** — The Meridian Pivot (PLP Evaluation Track, Week 2)
**Unfamiliar tool:** Message queue → chosen implementation: **NATS**

---

## What This Proves

- A **publisher** simulating a warehouse system emitting stock-update events every 4s.
- A **subscriber** in a queue group (`inventory-workers`) that builds a live in-memory stock cache.
- An **HTTP query endpoint** (`/api/query/:sku`) simulating the "is this in stock?" lookup.
- A live **web dashboard** showing the cache and recent messages (auto-refreshes every 2s).

**Key NATS concept demonstrated:** queue groups. Two subscribers in the same group →
NATS load-balances messages between them (one message goes to exactly one worker),
rather than broadcasting to all.

---

## Requirements

- **Node.js 18+** (uses native ES modules + the `nats` npm client)
- **No Docker needed** — the `nats-server` binary is downloaded automatically by `postinstall`
- **curl + tar** — used by `download-nats.js` to fetch the binary (standard on Linux/macOS)

---

## Quick Start

```bash
cd prototype-1-nats
npm install        # Downloads nats-server binary + installs Node deps
npm start          # Starts broker + publisher + subscriber + dashboard
# Open: http://localhost:3000
```

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Live dashboard (auto-refreshes every 2s) |
| `GET` | `/api/state` | Full state dump — cache + recent messages + connection status |
| `GET` | `/api/query/:sku` | Query a specific SKU from the in-memory stock cache |
| `GET` | `/api/publish-now` | Manually trigger an immediate stock update (demo button) |

### Example: Query a SKU
```bash
curl http://localhost:3000/api/query/SKU-1001
# → { "sku": "SKU-1001", "quantity": 73, "warehouse": "NBO-01", "updatedAt": "..." }
```

---

## Architecture

```
                     NATS subject: inventory.updates
                     ┌────────────────────────────┐
  Warehouse mock ──→ │ Publisher (every 4s)       │ → NATS broker
                     └────────────────────────────┘   (embedded nats-server binary)
                                                           │
                     ┌────────────────────────────┐        │ queue group: inventory-workers
                     │ Subscriber                 │ ←──────┘
                     │  - receives one message    │
                     │  - updates in-memory cache │
                     └────────────────────────────┘
                                  │
                     ┌────────────────────────────┐
                     │ Express HTTP server        │
                     │  GET /api/state            │
                     │  GET /api/query/:sku        │
                     │  GET / (dashboard)          │
                     └────────────────────────────┘
```

---

## Files

| File | Purpose |
|---|---|
| `server.js` | All-in-one: spawns NATS broker, connects publisher + subscriber, serves dashboard |
| `download-nats.js` | `postinstall` script — downloads the nats-server binary for the current platform |
| `package.json` | Dependencies (`express`, `nats`), scripts |
| `BLOCKER_JOURNAL.md` | Learning journal — resources consulted, errors hit, how blockers were resolved |

---

## Key Concepts Demonstrated

| Concept | Where |
|---|---|
| **Subject-based routing** | `SUBJECT = 'inventory.updates'` — messages are addressed to a string, not a queue name |
| **Queue groups** | `{ queue: 'inventory-workers' }` — load-balanced delivery, not fan-out broadcast |
| **StringCodec** | Encodes JSON → `Uint8Array` for NATS; decodes back on receive |
| **Async iterator consumption** | `for await (const msg of sub)` — non-blocking message processing |
| **Graceful shutdown** | `nc.drain()` — flushes pending messages before closing the connection |
| **Embedded broker** | `spawn('./nats-server', ['-js'])` — no Docker required |

---

## Deploying to Get a Public URL (Render)

NATS needs a persistent process — not a serverless function — so use **Render Web Service**:

1. Push this folder to a GitHub repo (ensure `nats-server` binary and `render.yaml` are included).
2. Go to https://render.com → **New +** → **Web Service** → connect the repo.
3. Render detects `render.yaml` automatically. Otherwise set:
   - **Build Command:** `npm install`
   - **Start Command:** `npm start`
   - **Plan:** Free
4. Deploy. You'll get a URL like `https://nats-inventory-prototype.onrender.com`.

> **Note:** Free-tier services spin down after inactivity. Expect a 30-60s cold start.
