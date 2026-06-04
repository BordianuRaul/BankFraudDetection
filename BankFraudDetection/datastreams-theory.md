# Data Streams — Theory & Notes

> A learning companion for the *Fluxuri de Date* project. It explains the **concepts**
> behind the code so you can answer the professor's line-by-line questions, not just
> *where* something is implemented but *why* it exists.
>
> This document grows **one topic at a time**, in the order you need it while building.
> We start with the most fundamental idea: the **event**.

## Reading order / status

| # | Topic | Status |
|---|-------|--------|
| 1 | **Events** — the atom of a data stream | ✅ written (below) |
| 2 | Topics & partitions — the structure of a stream | ⬜ next |
| 3 | Keys, partitioning & ordering | ⬜ |
| 4 | Producers, consumers, consumer groups, offsets | ⬜ |
| 5 | Stream vs table (`KStream` / `KTable` / `GlobalKTable`) | ⬜ |
| 6 | Windowing (tumbling / sliding / session) — the *velocity* rule | ⬜ |
| 7 | State stores & stateful processing — *impossible travel*, *anomaly* | ⬜ |
| 8 | Joins (stream–stream, stream–table, GlobalKTable) | ⬜ |
| 9 | Time, watermarks, out-of-order & late events | ⬜ |
| 10 | Delivery semantics (at-most / at-least / exactly-once) | ⬜ |
| 11 | Serialization & schema evolution (JSON → Avro) | ⬜ |

---

# 1. Events — the atom of a data stream

## 1.1 What is an event?

An **event** is a record of **something that happened**, at a **point in time**. It is a
**fact**. Facts about the past do not change — so an event, once written, is **immutable**.

Read these out loud; each is one event:

- *"Account `ACC-1001` paid `1450 EUR` to merchant `MERCH-55` at `18:22:01`."*
- *"Account `ACC-1001` was created with risk profile `LOW`."*
- *"Merchant `MERCH-13` was added to the blacklist."*

Notice they are all phrased in the **past tense**. That is the tell-tale sign of an event:
it describes a thing that *already occurred*. A common naming convention is past-tense names —
`TransactionOccurred`, `AccountCreated`, `MerchantBlacklisted`. (Our project uses shorter
names — `Transaction`, `Account` — but mentally they are *"a transaction happened"*,
*"an account exists as of now"*.)

A **data stream** is then simply an **ordered, append-only sequence of events**. New events
are added to the end; old ones are never edited in place. Think of an infinite logbook.

## 1.2 The mental shift from databases

This is the single idea that everything else depends on. Coming from relational databases,
you are used to storing **current state**:

```
UPDATE accounts SET balance = 1200 WHERE account_id = 'ACC-1001';
```

After that statement runs, the old balance is **gone**. The row holds *what is true now*.

Streaming flips this. You don't overwrite — you **append a new fact**:

```
event: { accountId: ACC-1001, type: DEPOSIT,    amount:  500, ts: 10:00 }
event: { accountId: ACC-1001, type: WITHDRAWAL,  amount: -300, ts: 10:05 }
```

The current balance is *derived* by replaying the facts. Nothing is lost: you keep the
entire history. This is why streaming systems can **replay** the past, **audit** what
happened, and **reprocess** old data with a brand-new rule. (DESIGN.md §2 lists exactly
this: *"Replay history to test a new rule"*.)

> **Database = the latest state. Stream = every fact that led to it.**

## 1.3 Event vs. relational row (your intuition, sharpened)

You said: *"the data from the streams is structured in events, similar to what a DB
relational schema would be."* You're right — and here is the precise version. An event
**has a schema** (named, typed fields) exactly like a table row. What differs is the
**meaning** and the **lifecycle**:

| Aspect | Relational row (table) | Event (stream) |
|--------|------------------------|----------------|
| Represents | The **current state** of a thing | A **fact that happened**, at a time |
| Mutability | Mutable — `UPDATE` in place | **Immutable** — never changed once written |
| To change it… | Overwrite the row | **Append a new event** |
| History | Lost (only the latest is kept) | **Preserved** — the log *is* the history |
| Time | Usually implicit ("now") | **First-class** — every event has a timestamp |
| Identity | Primary key | **Key** (what it's about) + **offset** (where in the log) |
| Read pattern | Query latest by key | **Replay** in order from an offset |
| Schema | Columns + types | Fields + types ← **same idea** |

So: *same notion of a typed record, opposite philosophy about change and time.* A row is a
**noun frozen at "now"**; an event is a **verb pinned to a timestamp**.

## 1.4 Anatomy of an event (in Kafka)

In Kafka, the thing on the wire is called a **record** — that is the concrete form of our
"event." It is **not** just the JSON payload; it has several parts:

| Part | What it is | In one of our `Transaction` events |
|------|-----------|-------------------------------------|
| **Topic** | the named stream it belongs to | `transactions` |
| **Partition** | the lane *within* the topic | `hash(accountId) % 3` → e.g. partition `2` |
| **Offset** | its position in that partition (assigned by Kafka) | e.g. `42` |
| **Key** | *what the event is about*; drives partition + ordering | `accountId` = `"ACC-1001"` |
| **Value** | the **payload** — the fields you designed | the `Transaction` JSON (amount, currency, merchant, …) |
| **Timestamp** | when it happened (event time) | `2026-06-04T18:22:01Z` |
| **Headers** | optional metadata (key/value pairs) | e.g. `schema-version=1`, a trace id |

Two of these — **offset** and **partition** — are *assigned by Kafka*, not part of your data
model. The **key** and **value** are what *you* design. In DESIGN.md §5 the JSON blocks you
see are the **value**; the **key** is called out separately in the topic table (§4) — for
`transactions` the key is `accountId`. Keep those two ideas separate; the professor will.

## 1.5 Event time vs. processing time

Every event carries the time it **happened** (its timestamp — in our model the `timestamp`
field, and also the Kafka record timestamp). That is **event time**. It is different from
**processing time** — the moment *your application actually reads and handles* the event.

```
event time      18:22:01   (when the card was swiped)
processing time 18:22:04   (when our consumer got around to it)
```

They drift apart because of network delay, retries, broker buffering, a consumer that fell
behind, or events arriving **out of order**. This distinction is not academic: our
**velocity** rule ("> 5 transactions in 60s") must count by **event time**, otherwise a
burst that arrives late or out of order would be miscounted. We'll return to this under
*Windowing* and *Watermarks*, but plant the flag now: **time is a property of the event,
not of when you looked at it.**

## 1.6 Three flavors of events (and they're all in our project)

Not every event plays the same role. Three categories cover almost everything, and our five
topics map cleanly onto them:

1. **Fact / activity events** — "X happened." High volume, kept for a while, then aged out.
   → `Transaction` (a payment occurred).

2. **Entity / state events** — "the latest truth about entity X." Keyed by the entity id;
   a new event for the same key **supersedes** the old one. Because only the *latest per
   key* matters, these are stored **compacted** and behave like a **table**.
   → `Account`, `BlacklistEntry`.

3. **Derived events** — produced by *processing* other events.
   → `EnrichedTransaction` (a `Transaction` joined with its `Account`),
     `FraudAlert` (the output of the scoring rules). A `FraudAlert` is both *derived* and a
     *fact* ("an alert was raised") — those categories aren't mutually exclusive.

## 1.7 The log: ordered, append-only, replayable

Within one partition, events are a strict sequence ordered by offset:

```
partition 2:  [off 40][off 41][off 42][off 43][off 44] →  (new events appended here)
```

Three properties fall out of "append-only + immutable", and each buys you something:

- **Replay** — a consumer can rewind to any offset and read the history again. (Test a new
  fraud rule against *yesterday's* traffic without a time machine.)
- **Many independent readers** — because events aren't consumed-and-destroyed, the
  enrichment processor, the fraud engine, and Kafka UI can all read the same `transactions`
  events at their own pace. (This is *why* pub/sub works.)
- **Audit** — the log itself is the record of what happened; nothing was silently overwritten.

> A queue *deletes* a message once it's read. A log *keeps* it. Kafka is a log. That single
> difference is why replay, multiple consumers, and reprocessing are even possible.

## 1.8 Stream–table duality (the big one)

This is where your "events are like a relational schema" intuition pays off completely.
A **stream** and a **table** are two views of the **same** information:

- **Stream → Table:** take the stream of `Account` events and, key by key, keep only the
  **latest** value. What you get is the *accounts table* — current state per `accountId`.
  That is **exactly** what a Kafka **compacted topic** + a **`GlobalKTable`** do in our
  design (DESIGN.md §3, §4). Compaction throws away superseded events and keeps the latest
  per key; the `GlobalKTable` is the in-memory table built from it.

- **Table → Stream:** every change to a table (insert / update / delete) can be emitted as
  an event. That sequence of changes is the table's **changelog** — a stream.

A special case worth memorizing: a **tombstone** is an event with a key but a `null` value.
In a compacted topic it means *"delete this key."* That's how a merchant gets *removed* from
the `blacklist`: you append `{ key: "MERCH-13", value: null }`.

> **A table is a snapshot of a stream at a moment in time. A stream is a table's changelog.**
> Same facts, two shapes. Choosing "stream" vs "table" for a topic is choosing which view you
> need — activity (`transactions`) wants the stream; reference data (`accounts`) wants the table.

## 1.9 Events are a contract (schema & serialization)

An event crosses a boundary: one component **produces** it, others **consume** it. The
**schema** — the set of fields and their types — is the **contract** between them. If the
producer renames `amount` to `value`, every consumer breaks. So the schema is a real
interface, not just "some JSON."

Our project starts with **JSON** (easy to read in Kafka UI, zero setup). The listed
extension is **Avro + Schema Registry**, which makes the contract *explicit* and supports
**schema evolution** — adding a field without breaking old consumers (DESIGN.md §5, §12).
For now, just hold the idea: **the event's schema is a versioned contract**, and we'll see
how to evolve it safely later.

## 1.10 How events map to THIS project

Your one-glance reference. When the professor points at a topic and asks "what is this,
conceptually?", this table is the answer:

| Topic | Event (value) | Flavor (§1.6) | Key | Cleanup | Behaves like |
|-------|---------------|---------------|-----|---------|--------------|
| `transactions` | `Transaction` | fact | `accountId` | delete (retention) | **stream** |
| `transactions.enriched` | `EnrichedTransaction` | derived fact | `accountId` | delete | **stream** |
| `fraud.alerts` | `FraudAlert` | derived fact | `accountId` | delete | **stream** |
| `accounts` | `Account` | entity / state | `accountId` | **compact** | **table** (`GlobalKTable`) |
| `blacklist` | `BlacklistEntry` | entity / state | `entityId` | **compact** | **table** (`GlobalKTable`) |

The whole pipeline shares the **same key (`accountId`)** wherever it can, so one account's
events — transaction → enriched → alert — all land in the **same partition lane**, which
keeps them **ordered** and cheap to **join** (DESIGN.md §4, "design notes").

## 1.11 Likely exam questions (with short answers)

- **Q: Why are events immutable?**
  A: So the log can be replayed, audited, and read by many consumers independently, and so
  old data can be reprocessed by a new rule. You model change by **appending a new event**,
  not by editing an old one.

- **Q: Conceptually, how does your `accounts` topic differ from `transactions`?**
  A: `accounts` holds **entity-state** events, **compacted** (latest per key), so it behaves
  like a **table** / `GlobalKTable`. `transactions` holds **fact** events, kept by
  **retention**, so it behaves like a **stream**.

- **Q: What decides which partition a transaction goes to?**
  A: The **key** (`accountId`), hashed modulo the partition count. Same account → same
  partition → ordered processing for that account.

- **Q: Event time vs. processing time in the velocity rule?**
  A: Velocity counts transactions inside an **event-time** window using each event's own
  timestamp — not the time we happened to process it — so late or out-of-order events are
  still counted in the right window.

- **Q: What is a tombstone?**
  A: An event with a key and a `null` value; in a compacted topic it **deletes** that key
  (e.g. removing a merchant from the `blacklist`).

- **Q: Your `Transaction` is just JSON — what role does its schema play?**
  A: It's the **contract** between the producer and every consumer. Changing it can break
  consumers; safe evolution is what Avro + Schema Registry would give us (an extension).

---

## Glossary (events)

- **Event / record** — an immutable fact about something that happened, at a timestamp.
- **Key** — what the event is *about*; determines partition and per-key ordering.
- **Value / payload** — the typed fields (the part you design in the data model).
- **Offset** — an event's position within a partition (assigned by Kafka).
- **Event time** — when the event happened. **Processing time** — when you handled it.
- **Compaction** — keep only the latest event per key (turns a stream into a table).
- **Tombstone** — a `null`-value event; deletes a key in a compacted topic.
- **Stream–table duality** — a stream aggregated-by-latest-key *is* a table; a table's
  changelog *is* a stream.
- **Schema** — the fields + types of an event; the producer↔consumer contract.

---

*Next topic to write: **§2 Topics & partitions** — how individual events are organized into
the named, parallelized streams you'll actually create in Milestone 0/1.*
