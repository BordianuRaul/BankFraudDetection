# Code Walkthrough — read the project in the order the data flows

> Goal: understand everything built so far (Phase 0 → Milestone 1 → Phase 2) by
> following **one transaction** from "invented" to "fraud alert on a REST API".
> Read the files **in the numbered order below** — each stop sets up the next.
>
> Companions: [`DESIGN.md`](DESIGN.md) (architecture), [`datastreams-theory.md`](datastreams-theory.md)
> (concepts), [`tasking.md`](tasking.md) (what/when).
>
> How to use each stop: open the file, read it, then read the **3 lines under it** —
> *Read for* (the one line that matters), *Concept* (what it teaches), *Connects to*
> (how it links to the neighbours). The **Prof** line is a likely line-by-line question.

---

## The 10-second mental model

```
                                   ┌───────────────────────────── the app (Spring Boot) ─────────────────────────────┐
                                   │                                                                                  │
 (5) Transaction  ──produce(7)──▶  │  topic: transactions  ──▶  (8) TransactionLogConsumer  (just logs)              │
        event                      │            │                                                                    │
                                   │            └──▶  (10) StatelessFraudEngine  ──runs (9) HighAmountRule──┐         │
                                   │                                                                        ▼         │
                                   │                                                       (11) FraudAlert event     │
                                   │                                                                        │ produce │
                                   │                                              topic: fraud.alerts  ◀────┘         │
                                   │                                                        │                         │
                                   │                       (14) AlertListener  ◀────────────┘                         │
                                   │                                 │ save                                           │
                                   │                       (12/13) AlertEntity / AlertRepository  ──▶  H2 database    │
                                   │                                 ▲                                      │         │
                                   │                       (15) AlertController  ◀───────────────── GET /api/alerts   │
                                   └──────────────────────────────────────────────────────────────────────────────────┘
   Broker + viewer live OUTSIDE the app, in Docker (4): topics `transactions`, `fraud.alerts`; Kafka UI at :8081
```

Everything keyed by **`accountId`** — follow that one thread and the rest follows.

---

## Part A — Bootstrap: how the app runs at all

You don't need to *understand* these deeply, just know what each switches on.

**1. `pom.xml`**
- *Read for:* the `<parent>` (Spring Boot) + the 5 `<dependencies>` (web, spring-kafka, kafka-streams, data-jpa, h2).
- *Concept:* the starters pull in everything; you write no version numbers.
- *Connects to:* every other file — they only compile because of these.
- *Prof:* "What does `spring-boot-starter-parent` give you?" → managed dependency versions + plugin config.

**2. `src/main/java/org/example/fraud/FraudDetectionApplication.java`**
- *Read for:* `@SpringBootApplication` (starts everything, scans `org.example.fraud.*`) and `@EnableScheduling` (turns on the producer's timer).
- *Concept:* one entry point; component-scan finds the rest by package.
- *Connects to:* enables (7) the `@Scheduled` generator.
- *Prof:* "Why is this class in package `org.example.fraud`?" → so scanning covers all sub-packages (config, model, generator…).

**3. `src/main/resources/application.yml`**
- *Read for:* `spring.kafka.producer/consumer` serializers (String key, **JSON** value) and `spring.json.trusted.packages`; the H2 datasource block.
- *Concept:* this is *why* you never write serialization code — config wires object ↔ JSON ↔ bytes.
- *Connects to:* (5) the `Transaction` becomes JSON because of this; (7) producer and (8) consumer both read this.
- *Prof:* "How does a `Transaction` object get onto Kafka as bytes?" → the JSON serializer named here.

**4. `docker-compose.yml`**
- *Read for:* two services (`kafka`, `kafka-ui`) and the 3-listener block.
- *Concept:* the broker lives outside the app; the app connects to `localhost:9092`.
- *Connects to:* (3) `bootstrap-servers: localhost:9092` points here.
- *Prof:* "How does kafka-ui reach the broker?" → over the `DOCKER` listener `kafka:29092` (service name), not localhost.

---

## Part B — One transaction's journey

**5. `model/Transaction.java`**  ← the EVENT, the atom of the whole system
- *Read for:* it's a `record` (immutable); `accountId` is the **key**; `amount` is `BigDecimal`; `timestamp` is event time.
- *Concept:* an event = an immutable, timestamped fact (theory §1.1, §1.4).
- *Connects to:* produced in (7), consumed in (8) and (10).
- *Prof:* "Which field is the Kafka key and why?" → `accountId`, for per-account ordering (theory §1.7).

**6. `config/KafkaTopicConfig.java`**  ← where events live
- *Read for:* two `NewTopic` beans — `transactions` and `fraud.alerts`, each 3 partitions / 1 replica. The `TRANSACTIONS` / `FRAUD_ALERTS` String constants are reused everywhere (one source of truth).
- *Concept:* a topic is a named, partitioned stream; `KafkaAdmin` creates these beans at startup (broker auto-create is off).
- *Connects to:* (7)(8)(10)(14) all reference these two constants.
- *Prof:* "Why 3 partitions?" → 3 parallel lanes; lets a consumer group split work (theory §1.7).

**7. `generator/TransactionGenerator.java`**  ← PRODUCE
- *Read for:* `@Scheduled` `emit()` → `kafka.send(TRANSACTIONS, tx.accountId(), tx)`. **Arg 2 is the key.** Also the `bigTicket` line that occasionally makes a >10 000 amount.
- *Concept:* a producer; the key decides the partition (theory §1.4).
- *Connects to:* fills topic (6); its big-ticket amounts are what (9) later catches.
- *Prof:* "What decides the partition of this transaction?" → `hash(accountId) % 3`.

**8. `consumer/TransactionLogConsumer.java`**  ← the simplest CONSUME
- *Read for:* `@KafkaListener(topics = TRANSACTIONS, groupId = "tx-logger")` → just logs.
- *Concept:* a consumer in its own group; reads every transaction independently.
- *Connects to:* reads the same topic (6) that (10) also reads — two groups, no interference (theory §1.7).
- *Prof:* "If this and the fraud engine both read `transactions`, do they steal messages from each other?" → no, different consumer groups each get all messages.

---

## Part C — Turning a transaction into an alert (Phase 2)

**9. `detection/rules/HighAmountRule.java`**  ← the decision
- *Read for:* `check(tx)` returns a reason if `amount > threshold`; threshold via `@Value("${fraud.thresholds.high-amount}")`.
- *Concept:* **stateless** — the decision uses only this one transaction, no memory, no lookup.
- *Connects to:* called by (10); threshold comes from (3) application.yml.
- *Prof:* "Why is this rule stateless?" → it needs nothing but the current event; contrast with velocity/impossible-travel (Phase 4) which need history.

**10. `detection/StatelessFraudEngine.java`**  ← detect, then PRODUCE an alert
- *Read for:* `@KafkaListener` on `transactions` → runs the rule(s) → if any fired, builds a `FraudAlert` and `kafka.send(FRAUD_ALERTS, accountId, alert)`. Note `score = 0.5 → MEDIUM` is a placeholder.
- *Concept:* a consumer **and** a producer in one — the heart of stream processing (read one stream, write another).
- *Connects to:* consumes (6 transactions), uses (9), emits (11) onto (6 fraud.alerts).
- *Prof:* "This both reads and writes Kafka — which topic each way?" → reads `transactions`, writes `fraud.alerts`.

**11. `model/FraudAlert.java`**  ← the OUTPUT event
- *Read for:* a `record`; keyed (when sent) by `accountId`; carries `triggeredRules`, `severity`, `explanation`.
- *Concept:* a derived event — computed from an input event (theory §1.6).
- *Connects to:* created in (10), consumed in (14).
- *Prof:* "Why share the same key (`accountId`) as the transaction?" → keeps a tx and its alerts in the same partition lane.

---

## Part D — Storing & serving alerts (Phase 2)

**12. `alert/AlertEntity.java`**  ← the DB shape
- *Read for:* `@Entity`, `@Id alertId`, a `protected` no-arg constructor, getters; `triggeredRules` stored as a comma-joined String.
- *Concept:* JPA needs a mutable, no-arg-constructable class — so this is **separate** from the immutable `FraudAlert` record. Same data, two jobs.
- *Connects to:* built from (11) inside (14); read by (15).
- *Prof:* "Why two classes for an alert (record + entity)?" → messaging wants immutable; JPA wants mutable + no-arg ctor.

**13. `alert/AlertRepository.java`**
- *Read for:* `interface ... extends JpaRepository<AlertEntity, String>` — that's the whole file.
- *Concept:* Spring Data generates the CRUD implementation; `String` = the `@Id` type.
- *Connects to:* used by (14) to save, (15) to read.
- *Prof:* "Where's the implementation of `save`/`findAll`?" → generated by Spring Data at runtime.

**14. `alert/AlertListener.java`**  ← CONSUME alert → save
- *Read for:* `@KafkaListener(topics = FRAUD_ALERTS, groupId = "alert-store")` → maps `FraudAlert` to `AlertEntity` → `repository.save(...)`.
- *Concept:* an independent storage stage; its own group means persistence is decoupled from detection.
- *Connects to:* consumes (11) from (6 fraud.alerts), writes via (13) into H2.
- *Prof:* "What converts the comma-joined `triggeredRules` String?" → `String.join(",", alert.triggeredRules())` here.

**15. `alert/AlertController.java`**  ← SERVE
- *Read for:* `@GetMapping` on `/api/alerts` → `repository.findAll(Sort ... DESC, "detectedAt")`.
- *Concept:* the read side — a plain REST endpoint over the stored alerts.
- *Connects to:* reads what (14) wrote; this is what you curl / open in the browser.
- *Prof:* "Walk the path of one alert from creation to this endpoint." → (10)→fraud.alerts→(14)→H2→(15).

---

## How the dots connect — 4 threads that run through everything

1. **The key thread (`accountId`).** Set in (5), used in (7) `send(topic, accountId, …)`, preserved in (10)/(11). One account's whole life — tx → alert — stays in one partition lane, ordered.
2. **The serialization thread.** (3) names JSON serializers once; (7) turns `Transaction`→JSON, (8)/(10) JSON→`Transaction`, (10) `FraudAlert`→JSON, (14) JSON→`FraudAlert`. You never wrote a line of it.
3. **The consumer-group thread.** Three independent readers: `tx-logger` (8), `fraud-engine` (10), `alert-store` (14). Each gets its own copy of the stream it cares about — that's pub/sub.
4. **The stream-vs-table thread.** *Not here yet.* Everything so far is **streams** (append-only facts). **Tables** (reference data: accounts, blacklist) arrive in Phase 3 — that's where joins begin (theory §1.8).

---

## Suggested way to study this (30–40 min)

1. Read Parts A→D in order, one file at a time, using the 3-line notes.
2. Start the stack + app, watch the logs: you'll literally see `produced ->` (7), `consumed <-` (8), `ALERT [...]` (10) interleave.
3. Open **Kafka UI** (:8081) → see the two topics from (6), click a message → Key = `accountId`, Value = the JSON from (5)/(11).
4. Open **`/api/alerts`** (15) and the **H2 console** → see what (14) stored.
5. For each thing you see, name the file that produced it. When you can do that for all 5 stops above, you've connected the dots.

---

## Professor drill — quick cross-file Q&A

- *"Trace one transaction end to end."* → (7) produce → `transactions` → (10) detect → `fraud.alerts` → (14) save → (15) serve.
- *"Where is the key set, and what does it control?"* → (7), controls partition + per-account ordering.
- *"Which components are consumers? producers? both?"* → consumers: (8),(14); producer: (7); both: (10).
- *"Stateless vs stateful — which rules do you have?"* → only stateless so far (9); stateful (velocity, travel, anomaly) is Phase 4.
- *"How is reference data different from transactions?"* → it isn't here yet; Phase 3 adds compacted `accounts`/`blacklist` as tables (theory §1.8).
