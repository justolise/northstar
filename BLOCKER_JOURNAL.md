# Learning & Blocker Journal — NATS Message Queue

**Assignment 1, Day 1-2 — The Meridian Pivot**
**Tool assigned:** Message queue → chosen implementation: **NATS**
**Time-boxed goal:** Build a mini-prototype demonstrating the core message-queue concept.

---

## Resources Consulted

1. **NATS official docs** — "What is NATS?" concepts overview (subjects, pub/sub, queue groups)
   https://docs.nats.io/nats-concepts/what-is-nats

2. **NATS server GitHub releases** — to find a Docker-free binary download
   https://github.com/nats-io/nats-server/releases

3. **`nats` npm package docs/examples** — Node.js client API, `StringCodec`, async iterators
   https://www.npmjs.com/package/nats

4. **NATS subjects and wildcards reference** — to understand how subject hierarchies work
   https://docs.nats.io/nats-concepts/subjects

---

## What I Set Out to Build

A minimal pub/sub prototype simulating a warehouse system:
- A **publisher** that emits mock stock-update events on an `inventory.updates` subject.
- A **subscriber** in a **queue group** that consumes those events into an in-memory cache.
- An HTTP dashboard showing the live cache and the recent messages.

Deliberately scoped small — not the full Day 3 spec — to fit the time-box.

---

## Blockers Hit and How They Were Resolved

### Blocker 1 — No Docker in the environment

**What happened:** Every NATS quickstart assumes `docker run nats:latest`. My environment has no
Docker. The `npm start` approach in most tutorials assumes the server is already running.

**How resolved:** Found the raw binary on the NATS GitHub releases page.
Wrote `download-nats.js` — a `postinstall` script that uses `curl` + `tar` to download and
extract the binary for the current platform automatically when `npm install` runs.
The `npm start` script then spawns the binary as a child process with `spawn('./nats-server', ['-js'])`.

**Lesson:** Brokers (NATS, Redis, Kafka) always need a running server process. In production
this is handled by infrastructure. Locally, always check if there's a standalone binary before
assuming Docker is the only path.

---

### Blocker 2 — `nats` npm deprecation warning during install

**What happened:** `npm install nats` printed:
```
npm warn deprecated nats@2.29.3: Please use @nats-io/transport-node instead.
```
Spent time investigating whether this meant the package was broken or the API had changed.

**How resolved:** Read the GitHub issue thread. The original `nats` package still works and is
maintained through the 2.x lifecycle. The split into `@nats-io/transport-node` +
`@nats-io/nats-core` is the new modular architecture for 3.x. For this prototype, the 2.x API
is fully functional and better documented for beginners. Noted it as a future upgrade path
but did not chase it during the time-box.

**Lesson:** Deprecation warnings in npm are not always "broken right now". Read the linked
issue/package before spending time on a migration.

---

### Blocker 3 — Subscriber connected but received zero messages

**What happened:** After getting the server and subscriber running, the publisher published 10
messages, but the subscriber's async iterator produced no output at all.

**Initial hypothesis:** The subject string didn't match. Checked both sides — both said
`inventory.updates`. Not the problem.

**Actual cause:** The subscriber was started in a separate shell command that exited before the
publisher ran. Because each command invocation creates a new shell, the background NATS server
had also been killed when its parent shell ended. So by the time the publisher ran, there was no
broker to connect to — and the publisher's `connect()` was failing silently because the error
was swallowed.

**How resolved:** Chained all three (server spawn, subscriber, publisher) into a single Node.js
process (`server.js`) using `child_process.spawn` + the `nats` client. This way the broker stays
alive for the lifetime of the server process.

**Lesson:** NATS (and all message brokers) are stateful long-running processes. In a normal dev
setup (persistent terminal tabs) this is obvious. In a scripted environment where each command
is a fresh shell, it trips you up. The fix is either a process manager or a single-entry-point
file that owns all the processes.

---

### Blocker 4 — Understanding queue groups vs. plain subscriptions

**What happened:** Unclear whether `nc.subscribe(subject)` alone would deliver every message
to every subscriber (fan-out) or just to one.

**How resolved:** The NATS docs were explicit: plain `subscribe` = fan-out, every subscriber
gets every message. Passing `{ queue: 'group-name' }` turns it into **load-balanced delivery**
— NATS picks exactly one member of the queue group to receive each message.

Verified experimentally by running two subscribers in the same group against 15 published
messages — messages split across the two workers rather than both receiving all 15.

**Lesson:** NATS uses "queue group" as the term for what many other brokers call a
"consumer group" (Kafka) or "competing consumers" (RabbitMQ). The concept is the same but
the vocabulary differs across systems.

---

### Blocker 5 — NATS has NO persistence without JetStream

**What happened:** A subscriber started after the publisher had already sent several messages
received nothing — those messages were gone.

**How resolved:** NATS plain pub/sub is **fire-and-forget**. If a subscriber isn't connected
when a message is published, it simply never receives it. JetStream (enabled with `-js` flag)
adds durable streams and consumer replay, but that's an advanced feature beyond this
prototype's scope.

**Why this matters for Day 4:** If we switch from polling to a webhook/queue model in the full
kiosk service, we need to know whether missed messages are acceptable or whether JetStream
durability is required.

---

## Time-to-Completion

| Phase | Time estimate |
|---|---|
| Initial docs reading + concept orientation | ~45 min |
| Blocker 1: finding Docker-free binary path | ~40 min |
| Blocker 2: deprecation warning investigation | ~20 min |
| Blocker 3: zero-messages debug cycle | ~50 min |
| Blocker 4: queue group verification | ~20 min |
| Building the server + dashboard | ~60 min |
| **Total** | **~3.5 hours** |

Well within the intended few-hour time-box. Most time went to Blockers 1 and 3, not to NATS
concepts themselves, which were clear once the right docs were found.

---

## What I Know Now That I Didn't Before

- NATS uses **subject strings** (not queue names) as the routing primitive. Queue groups are
  a subscription *option* layered on top — not a separate first-class object.
- The distinction between **fan-out pub/sub** and **load-balanced queue consumption** is the
  single most important concept for the Day 3 spec. Pick the wrong one and every subscriber
  processes every message — that's not a queue, that's a broadcast.
- NATS has **no persistence** without JetStream. Fire-and-forget. This matters for
  production reliability decisions.
- Embedding a broker binary via `child_process.spawn` is a valid local-dev pattern when
  Docker isn't available, as long as you control the binary's lifecycle carefully.
