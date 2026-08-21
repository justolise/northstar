/**
 * server.js — NATS Inventory Sync, All-in-One
 *
 * Assignment 1: mini-prototype demonstrating NATS message queue as an
 * unfamiliar tool. This single process:
 *   1. Spawns the vendored nats-server binary as a child process (the broker)
 *   2. Connects a subscriber in the "inventory-workers" queue group, building
 *      an in-memory stock cache from received messages
 *   3. Connects a publisher that emits a mock warehouse stock update every 4s
 *   4. Exposes a REST API (/api/state, /api/publish-now, /api/query/:sku)
 *   5. Serves a live dashboard at http://localhost:PORT
 *
 * Key NATS concepts demonstrated:
 *   - Subject-based routing (inventory.updates)
 *   - Queue groups (load-balanced consumption — NOT fan-out broadcast)
 *   - StringCodec for (de)serialization
 *   - Graceful drain on shutdown
 */

import { connect, StringCodec } from 'nats';
import { spawn }                from 'child_process';
import express                  from 'express';
import { existsSync }           from 'fs';

// ─── constants ────────────────────────────────────────────────────────────────
const NATS_PORT   = 4222;
const PORT        = process.env.PORT ?? 3000;
const SUBJECT     = 'inventory.updates';
const QUEUE_GROUP = 'inventory-workers';

const MOCK_SKUS = [
  'SKU-1001', 'SKU-1002', 'SKU-1003',
  'SKU-1004', 'SKU-1005', 'SKU-2001',
];
const WAREHOUSES = ['NBO-01', 'NBO-02', 'MSA-01'];

const sc = StringCodec();

// ─── shared in-memory state ───────────────────────────────────────────────────
const state = {
  connected:       false,
  stockCache:      /** @type {Record<string, {quantity:number,warehouse:string,updatedAt:string}>} */ ({}),
  recentMessages:  /** @type {Array<{sku:string,quantity:number,warehouse:string,timestamp:string}>} */ ([]),
  messageCount:    0,
};

/** Keep the recent-messages list capped at 30 entries. */
function pushRecent(msg) {
  state.recentMessages.unshift(msg);
  if (state.recentMessages.length > 30) state.recentMessages.pop();
  state.messageCount++;
}

// ─── 1. NATS server (broker) ──────────────────────────────────────────────────
function startNatsServer() {
  return new Promise((resolve, reject) => {
    if (!existsSync('./nats-server')) {
      reject(new Error(
        'nats-server binary not found. Run `npm install` to download it automatically, ' +
        'or download manually from https://nats.io/download/'
      ));
      return;
    }

    const server = spawn('./nats-server', ['-js', '-p', String(NATS_PORT)], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    const onData = (data) => {
      const line = data.toString();
      if (line.includes('Server is ready') || line.includes('Listening for')) {
        resolve(undefined);
      }
    };
    server.stdout.on('data', onData);
    server.stderr.on('data', onData);
    server.on('error', reject);

    // Fallback: give the server 2s to start even if we miss the log line
    setTimeout(() => resolve(undefined), 2000);

    // Propagate early exits
    server.on('exit', (code) => {
      if (code !== 0 && code !== null) {
        console.error(`[nats-server] exited with code ${code}`);
      }
    });
  });
}

// ─── 2. subscriber: queue-group consumer ─────────────────────────────────────
async function startSubscriber() {
  const nc = await connect({ servers: `127.0.0.1:${NATS_PORT}` });
  state.connected = true;

  const sub = nc.subscribe(SUBJECT, { queue: QUEUE_GROUP });

  // Async iterator — NATS delivers one message at a time to this worker.
  // Other workers in the same queue group get the other messages (load-balancing).
  (async () => {
    for await (const msg of sub) {
      try {
        const data = JSON.parse(sc.decode(msg.data));
        // Build/update the live stock cache
        state.stockCache[data.sku] = {
          quantity:  data.quantity,
          warehouse: data.warehouse,
          updatedAt: data.timestamp,
        };
        pushRecent(data);
      } catch (e) {
        console.error('[subscriber] parse error:', e.message);
      }
    }
  })();

  return nc;
}

// ─── 3. publisher: emits mock stock updates on a timer ───────────────────────
let _pubNc = null;

async function startPublisher() {
  _pubNc = await connect({ servers: `127.0.0.1:${NATS_PORT}` });

  setInterval(() => {
    publishUpdate();
  }, 4000);

  return _pubNc;
}

function publishUpdate() {
  if (!_pubNc) return;
  const sku = MOCK_SKUS[Math.floor(Math.random() * MOCK_SKUS.length)];
  const wh  = WAREHOUSES[Math.floor(Math.random() * WAREHOUSES.length)];
  const update = {
    sku,
    quantity:  Math.floor(Math.random() * 200),
    warehouse: wh,
    timestamp: new Date().toISOString(),
  };
  _pubNc.publish(SUBJECT, sc.encode(JSON.stringify(update)));
  // Note: we do NOT call pushRecent here — the subscriber will receive the
  // message and update state, which is the whole point: the cache is driven
  // by consumption, not by the publisher's memory of what it sent.
}

// ─── 4. Express HTTP API + dashboard ─────────────────────────────────────────
const app = express();

/** Full state dump — polled by the dashboard every 2s. */
app.get('/api/state', (_req, res) => {
  res.json(state);
});

/** Manually trigger a publish event (for the demo "Publish Now" button). */
app.get('/api/publish-now', (_req, res) => {
  publishUpdate();
  res.json({ ok: true, message: 'Stock update published to NATS subject.' });
});

/** Query a specific SKU from the in-memory cache (the product of /api query endpoint). */
app.get('/api/query/:sku', (req, res) => {
  const entry = state.stockCache[req.params.sku.toUpperCase()];
  if (!entry) {
    return res.status(404).json({ error: 'SKU not in cache — no update received yet.' });
  }
  res.json({ sku: req.params.sku.toUpperCase(), ...entry });
});

// ─── 5. Dashboard ─────────────────────────────────────────────────────────────
app.get('/', (_req, res) => {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(DASHBOARD_HTML);
});

// ─── boot ─────────────────────────────────────────────────────────────────────
async function main() {
  console.log('[boot] Starting NATS server binary...');
  await startNatsServer();
  console.log('[boot] NATS broker ready on port', NATS_PORT);

  await startSubscriber();
  console.log('[boot] Subscriber connected (queue group: inventory-workers)');

  await startPublisher();
  console.log('[boot] Publisher connected — emitting stock updates every 4s');

  app.listen(PORT, () => {
    console.log(`[boot] ✓ Dashboard: http://localhost:${PORT}`);
    console.log(`[boot]   API:       http://localhost:${PORT}/api/state`);
    console.log(`[boot]   Query SKU: http://localhost:${PORT}/api/query/SKU-1001`);
  });
}

main().catch((err) => {
  console.error('[boot] Fatal error:', err.message);
  process.exit(1);
});

// ─── Dashboard HTML (inlined for single-file deployment) ──────────────────────
const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>NATS Inventory Sync — Day 1-2 Prototype</title>
  <meta name="description" content="Assignment 1 mini-prototype: NATS pub/sub message queue demo for live inventory sync." />
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet" />
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root {
      --bg:        #09090f;
      --bg2:       #111118;
      --surface:   #16161f;
      --border:    #252535;
      --accent:    #2dce89;
      --accent2:   #00bcd4;
      --warn:      #f5a623;
      --text:      #e8e8f0;
      --muted:     #7878a0;
      --radius:    12px;
      --glow:      0 0 20px rgba(45,206,137,.15);
    }

    body {
      background: var(--bg);
      color: var(--text);
      font-family: 'Inter', system-ui, sans-serif;
      min-height: 100vh;
      padding: 24px 20px 48px;
    }

    /* ── header ── */
    header {
      max-width: 1100px;
      margin: 0 auto 32px;
      display: flex;
      align-items: center;
      gap: 16px;
      flex-wrap: wrap;
    }
    .logo {
      width: 42px; height: 42px;
      background: linear-gradient(135deg, var(--accent), var(--accent2));
      border-radius: 10px;
      display: flex; align-items: center; justify-content: center;
      font-size: 1.4rem;
      flex-shrink: 0;
      box-shadow: var(--glow);
    }
    h1 { font-size: 1.25rem; font-weight: 700; letter-spacing: -.02em; }
    h1 span { color: var(--muted); font-weight: 400; font-size: .85rem; display:block; margin-top:2px; }
    .status-pill {
      margin-left: auto;
      display: flex; align-items: center; gap: 8px;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 999px;
      padding: 6px 14px;
      font-size: .8rem;
      color: var(--muted);
    }
    .status-dot {
      width: 8px; height: 8px; border-radius: 50%;
      background: #555;
      transition: background .3s;
    }
    .status-dot.ok { background: var(--accent); animation: pulse 2s infinite; }
    @keyframes pulse { 0%,100% { opacity:1; box-shadow: 0 0 0 0 rgba(45,206,137,.4); }
      50% { opacity:.8; box-shadow: 0 0 0 4px rgba(45,206,137,0); } }

    /* ── main grid ── */
    .grid {
      max-width: 1100px;
      margin: 0 auto;
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 20px;
    }
    @media (max-width: 700px) { .grid { grid-template-columns: 1fr; } }

    /* ── cards ── */
    .card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 20px;
      transition: border-color .2s;
    }
    .card:hover { border-color: rgba(45,206,137,.3); }
    .card-header {
      display: flex; align-items: center; justify-content: space-between;
      margin-bottom: 16px;
    }
    .card-title { font-size: .9rem; font-weight: 600; color: var(--text); }
    .card-badge {
      font-size: .7rem; font-weight: 600; padding: 2px 8px;
      border-radius: 999px; background: rgba(45,206,137,.12);
      color: var(--accent); border: 1px solid rgba(45,206,137,.25);
    }

    /* ── stat bar ── */
    .stats {
      display: flex; gap: 12px; flex-wrap: wrap;
      max-width: 1100px; margin: 0 auto 20px;
    }
    .stat {
      background: var(--surface); border: 1px solid var(--border);
      border-radius: var(--radius); padding: 14px 20px;
      flex: 1; min-width: 140px;
    }
    .stat-value { font-size: 1.6rem; font-weight: 700; color: var(--accent); }
    .stat-label { font-size: .75rem; color: var(--muted); margin-top: 2px; }

    /* ── table ── */
    table { width: 100%; border-collapse: collapse; }
    th { font-size: .72rem; font-weight: 600; color: var(--muted); text-transform: uppercase;
         letter-spacing: .05em; padding: 6px 8px; border-bottom: 1px solid var(--border); text-align:left; }
    td { font-size: .82rem; padding: 8px 8px; border-bottom: 1px solid rgba(37,37,53,.6);
         font-family: 'JetBrains Mono', monospace; color: var(--text); }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: rgba(255,255,255,.02); }
    .qty { color: var(--accent); font-weight: 600; }
    .ts  { color: var(--muted); font-size: .75rem; }
    .wh  { color: var(--accent2); }
    .sku { color: #c792ea; }

    /* ── query bar ── */
    .query-bar { display:flex; gap:8px; margin-bottom:16px; }
    .query-bar input {
      flex:1; background: var(--bg2); border: 1px solid var(--border);
      border-radius: 8px; padding: 8px 12px; color: var(--text);
      font-family: 'JetBrains Mono', monospace; font-size: .85rem; outline:none;
    }
    .query-bar input:focus { border-color: var(--accent); }
    .query-bar button, .btn {
      background: linear-gradient(135deg, var(--accent), var(--accent2));
      border: none; border-radius: 8px; padding: 8px 16px;
      color: #000; font-weight: 700; font-size: .8rem; cursor:pointer;
      transition: opacity .2s, transform .1s;
      white-space: nowrap;
    }
    .btn:hover, .query-bar button:hover { opacity:.9; transform: translateY(-1px); }
    .btn:active { transform: scale(.97); }
    .btn-sm { padding: 6px 12px; font-size:.75rem; }

    /* ── result box ── */
    #queryResult {
      background: var(--bg2); border: 1px solid var(--border);
      border-radius: 8px; padding: 12px; font-family: 'JetBrains Mono',monospace;
      font-size: .82rem; color: var(--accent); white-space: pre-wrap;
      min-height: 56px; line-height: 1.6;
    }

    /* ── empty state ── */
    .empty { color: var(--muted); font-size: .82rem; text-align:center; padding:24px 0; }

    /* ── full-width card ── */
    .full { grid-column: 1 / -1; }

    /* ── concepts panel ── */
    .concepts { display: flex; flex-wrap:wrap; gap:10px; }
    .concept {
      background: var(--bg2); border: 1px solid var(--border);
      border-radius: 8px; padding: 10px 14px; flex: 1; min-width: 180px;
    }
    .concept-name { font-size: .78rem; font-weight:700; color: var(--accent); margin-bottom:4px; }
    .concept-desc { font-size: .75rem; color: var(--muted); line-height:1.5; }
  </style>
</head>
<body>
  <header>
    <div class="logo">📦</div>
    <div>
      <h1>NATS Inventory Sync
        <span>Assignment 1 — Day 1-2 Solo Mini-Prototype · Meridian Pivot Week 2</span>
      </h1>
    </div>
    <div class="status-pill" id="statusPill">
      <div class="status-dot" id="statusDot"></div>
      <span id="statusText">connecting…</span>
    </div>
  </header>

  <div class="stats">
    <div class="stat">
      <div class="stat-value" id="statSkus">0</div>
      <div class="stat-label">SKUs in cache</div>
    </div>
    <div class="stat">
      <div class="stat-value" id="statMsgs">0</div>
      <div class="stat-label">Messages consumed</div>
    </div>
    <div class="stat">
      <div class="stat-value" id="statSubject" style="font-size:1rem;margin-top:4px;font-family:'JetBrains Mono',monospace">inventory.updates</div>
      <div class="stat-label">NATS Subject</div>
    </div>
    <div class="stat">
      <div class="stat-value" id="statGroup" style="font-size:1rem;margin-top:4px;font-family:'JetBrains Mono',monospace">inventory-workers</div>
      <div class="stat-label">Queue Group</div>
    </div>
  </div>

  <div class="grid">

    <!-- Stock Cache -->
    <div class="card">
      <div class="card-header">
        <span class="card-title">📊 Live Stock Cache</span>
        <span class="card-badge">Built from consumed messages</span>
      </div>
      <div id="cacheWrap">
        <table>
          <thead><tr><th>SKU</th><th>Qty</th><th>Warehouse</th><th>Updated</th></tr></thead>
          <tbody id="cacheBody"></tbody>
        </table>
        <div class="empty" id="cacheEmpty" style="display:none">Waiting for first message…</div>
      </div>
    </div>

    <!-- Recent Messages -->
    <div class="card">
      <div class="card-header">
        <span class="card-title">📨 Recent Published Messages</span>
        <button class="btn btn-sm" id="pubNowBtn" onclick="publishNow()">Publish Now</button>
      </div>
      <table>
        <thead><tr><th>SKU</th><th>Qty</th><th>Warehouse</th><th>Time</th></tr></thead>
        <tbody id="recentBody"></tbody>
      </table>
      <div class="empty" id="recentEmpty">Waiting for publisher…</div>
    </div>

    <!-- Query endpoint -->
    <div class="card">
      <div class="card-header">
        <span class="card-title">🔍 Query Endpoint <code style="font-size:.7rem;color:var(--muted)">/api/query/:sku</code></span>
      </div>
      <div class="query-bar">
        <input type="text" id="skuInput" placeholder="SKU-1001" value="SKU-1001" />
        <button onclick="querySku()">Query</button>
      </div>
      <div id="queryResult">Enter a SKU and click Query…</div>
    </div>

    <!-- Key concepts -->
    <div class="card">
      <div class="card-header">
        <span class="card-title">🧠 Key NATS Concepts Demonstrated</span>
      </div>
      <div class="concepts">
        <div class="concept">
          <div class="concept-name">Subject-based routing</div>
          <div class="concept-desc">Messages are addressed to <code>inventory.updates</code> — not to a named queue. Any subscriber to that subject receives them.</div>
        </div>
        <div class="concept">
          <div class="concept-name">Queue Groups</div>
          <div class="concept-desc">Subscribing with <code>{ queue: "inventory-workers" }</code> turns fan-out into load-balanced delivery — each message goes to exactly one member.</div>
        </div>
        <div class="concept">
          <div class="concept-name">Fire-and-forget</div>
          <div class="concept-desc">Without JetStream, NATS has no persistence. A subscriber that is down when a message is published simply misses it.</div>
        </div>
        <div class="concept">
          <div class="concept-name">StringCodec</div>
          <div class="concept-desc">NATS messages are raw bytes. <code>StringCodec</code> encodes/decodes between UTF-8 strings and <code>Uint8Array</code>.</div>
        </div>
      </div>
    </div>

  </div>

  <script>
    async function refresh() {
      try {
        const data = await fetch('/api/state').then(r => r.json());

        // connection
        const dot  = document.getElementById('statusDot');
        const text = document.getElementById('statusText');
        if (data.connected) {
          dot.className = 'status-dot ok';
          text.textContent = 'connected to NATS';
        } else {
          dot.className = 'status-dot';
          text.textContent = 'connecting…';
        }

        // stats
        const skuCount = Object.keys(data.stockCache).length;
        document.getElementById('statSkus').textContent = skuCount;
        document.getElementById('statMsgs').textContent = data.messageCount ?? data.recentMessages.length;

        // cache table
        const cacheBody  = document.getElementById('cacheBody');
        const cacheEmpty = document.getElementById('cacheEmpty');
        const entries    = Object.entries(data.stockCache);
        if (entries.length === 0) {
          cacheBody.innerHTML = '';
          cacheEmpty.style.display = 'block';
        } else {
          cacheEmpty.style.display = 'none';
          cacheBody.innerHTML = entries.map(([sku, v]) =>
            \`<tr>
              <td class="sku">\${sku}</td>
              <td class="qty">\${v.quantity}</td>
              <td class="wh">\${v.warehouse}</td>
              <td class="ts">\${new Date(v.updatedAt).toLocaleTimeString()}</td>
            </tr>\`
          ).join('');
        }

        // recent messages
        const recentBody  = document.getElementById('recentBody');
        const recentEmpty = document.getElementById('recentEmpty');
        if (data.recentMessages.length === 0) {
          recentBody.innerHTML = '';
          recentEmpty.style.display = 'block';
        } else {
          recentEmpty.style.display = 'none';
          recentBody.innerHTML = data.recentMessages.slice(0,15).map(m =>
            \`<tr>
              <td class="sku">\${m.sku}</td>
              <td class="qty">\${m.quantity}</td>
              <td class="wh">\${m.warehouse}</td>
              <td class="ts">\${new Date(m.timestamp).toLocaleTimeString()}</td>
            </tr>\`
          ).join('');
        }
      } catch (e) {
        document.getElementById('statusText').textContent = 'error fetching state';
      }
    }

    async function publishNow() {
      const btn = document.getElementById('pubNowBtn');
      btn.textContent = '⚡ Sending…';
      btn.disabled = true;
      await fetch('/api/publish-now');
      setTimeout(() => { btn.textContent = 'Publish Now'; btn.disabled = false; }, 600);
      await refresh();
    }

    async function querySku() {
      const sku = document.getElementById('skuInput').value.trim().toUpperCase();
      const el  = document.getElementById('queryResult');
      if (!sku) { el.textContent = 'Enter a SKU first.'; return; }
      try {
        const res  = await fetch(\`/api/query/\${sku}\`);
        const data = await res.json();
        el.textContent = JSON.stringify(data, null, 2);
        el.style.color = res.ok ? 'var(--accent)' : 'var(--warn)';
      } catch (e) {
        el.textContent = 'Request failed: ' + e.message;
        el.style.color = 'var(--warn)';
      }
    }

    // Enter key on SKU input
    document.getElementById('skuInput').addEventListener('keydown', (e) => {
      if (e.key === 'Enter') querySku();
    });

    // Auto-refresh every 2s
    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>`;
