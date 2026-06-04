# Tasking — Bank Fraud Detection

> The ordered to-do list for building the project. Detail lives in
> [`DESIGN.md`](DESIGN.md); the concepts live in
> [`datastreams-theory.md`](datastreams-theory.md). This file is *what to do
> next* and *who does it*.

---

## How we work (rules of engagement)

1. **Claude writes the application code (Milestone 1 onward); you review it
   thoroughly.** Phase 0 was hand-written/guided by you; from M1 you're short on
   time, so Claude implements and you review. Code stays small, commented, and
   explainable.
2. **Agree on the approach before big/structural changes**, and Claude gives a
   short walkthrough after each batch so the review is quick.
3. **You must be able to explain every line.** The professor asks line-by-line, so
   each coding task carries a **"Be ready to explain"** note — if you can't yet,
   that's the signal to slow down and ask, not to move on.
4. **One concept at a time.** Each task maps to a small, demoable step and (where
   relevant) to a section of `datastreams-theory.md`.

## Legend

- Status: `[ ]` todo · `[~] ` in progress · `[x]` done
- Owner: **You** = writes the code · **Claude** = docs / explanation / review

---

## Milestone 0 — Bootstrap (make it a Spring Boot app)

*Goal: the project starts as a Spring Boot app and can talk to Kafka.*
*(DESIGN.md §8–9, Phase 0.)*

- [x] **0.1** Convert `pom.xml` to Spring Boot — parent + starters (web,
  spring-kafka, kafka-streams, data-jpa, h2, test). — *Claude (delegated boilerplate)*
  - *Be ready to explain:* what the `spring-boot-starter-parent` gives you, and
    why each dependency is here.
- [x] **0.2** Replace `Main.java` with `org.example.fraud.FraudDetectionApplication`
  (`@SpringBootApplication`). — *Claude*
  - *Be ready to explain:* what `@SpringBootApplication` actually switches on.
- [x] **0.3** `docker-compose.yml` (Kafka KRaft + Kafka UI) — drafted by you,
  completed by Claude. Brought up with `docker compose up -d`.
  - *Be ready to explain:* what a broker is; the 3-listener setup; what Kafka UI shows.
- [x] **0.4** `application.yml` (bootstrap-servers, JSON serdes, H2) — *Claude*.
  - *Be ready to explain:* producer vs consumer config, `auto-offset-reset`.
- [ ] **0.5** App boots green and connects to the broker. — **You** (verify)

> ✅ **Theory gate:** before 1.x, read `datastreams-theory.md` **§1 Events**.

---

## Milestone 1 — Produce & consume (first real stream)

*Goal: transactions flow onto a topic and something reads them.*
*(DESIGN.md Phase 1.)*

- [x] **1.1** `Transaction` model (the event value) — *Claude; review it*.
  - *Be ready to explain:* which field is the **key** and why (theory §1.4).
- [x] **1.2** `KafkaTopicConfig` — declares the `transactions` topic (3 partitions). — *Claude*
  - *Be ready to explain:* what a partition is; why 3 (theory §1.7).
- [x] **1.3** `TransactionGenerator` — `@Scheduled` producer keyed by `accountId`. — *Claude*
  - *Be ready to explain:* how the key chooses the partition.
- [x] **1.4** `TransactionLogConsumer` — `@KafkaListener` that logs each tx. — *Claude*
  - *Be ready to explain:* consumer group, offset, what "consuming" means.
- [ ] **1.5** Watch the events land in Kafka UI; confirm per-account ordering. — **You** (verify)
- [ ] **Theory:** write `datastreams-theory.md` **§2 Topics & partitions** + **§3 Keys/ordering**. — **Claude**

---

## Milestone 2 — Stateless rules → first alerts

*Goal: simple per-message rules raise alerts, persisted and queryable.*
*(DESIGN.md Phase 2, rules #1/#4/#6.)*

- [x] **2.1** `FraudAlert` model + `fraud.alerts` topic. — *Claude*
- [x] **2.2** `HighAmountRule` (stateless) + `StatelessFraudEngine` (`@KafkaListener`
  on `transactions`) → emits `FraudAlert`. — *Claude*
  - *Be ready to explain:* why this rule needs **no** state (decision uses one tx only).
- [~] **2.3** `HomeCountryRule` — **moved to Milestone 3**: needs the account's home
  country (reference data), which doesn't exist until enrichment (Phase 3).
- [x] **2.4** `AlertListener` → persists to H2 (`AlertEntity` + `AlertRepository`). — *Claude*
- [x] **2.5** `AlertController` → `GET /api/alerts`. — *Claude*
- [x] **2.6** Verified: big-ticket tx → `HIGH_AMOUNT` MEDIUM alert at `/api/alerts`.

---

## Milestone 3 — Reference data + enrichment (stream meets table)

*Goal: join the transaction stream against account/blacklist state.*
*(DESIGN.md Phase 3.)*

- [ ] **3.1** `Account` + `BlacklistEntry` models; compacted `accounts` / `blacklist` topics. — **You**
  - *Be ready to explain:* compaction & tombstones (theory §1.8).
- [ ] **3.2** `ReferenceDataLoader` — seed accounts + blacklist on startup. — **You**
- [ ] **3.3** `EnrichmentStream` — KStream join with the `accounts` GlobalKTable → `transactions.enriched`. — **You**
  - *Be ready to explain:* KStream vs KTable vs GlobalKTable (theory §1.8 + §5).
- [ ] **3.4** `BlacklistRule` (GlobalKTable lookup). — **You**
- [ ] **Theory:** write **§5 Stream vs Table** + **§8 Joins**. — **Claude**

---

## Milestone 4 — Stateful streaming rules (the core of the course)

*Goal: windowed + stateful detection in a Kafka Streams topology.*
*(DESIGN.md Phase 4, rules #2/#3/#5.)*

- [ ] **4.1** `FraudScoringTopology` skeleton (`@EnableKafkaStreams`, `StreamsBuilder`). — **You**
- [ ] **4.2** **Velocity** — windowed count per account (`> 5 tx / 60s`). — **You**
  - *Be ready to explain:* tumbling vs sliding windows; **event time** (theory §1.5).
- [ ] **4.3** **Impossible travel** — state store of last tx (geo+time) per account. — **You**
  - *Be ready to explain:* what a state store is; why this rule needs one.
- [ ] **4.4** **Amount anomaly** — running mean/stddev per account in a store. — **You**
- [ ] **Theory:** write **§6 Windowing** + **§7 State stores** + **§9 Time/late events**. — **Claude**

---

## Milestone 5 — Scoring & polish

*Goal: combine rules into a score/severity; tunable; demoable.*
*(DESIGN.md Phase 5.)*

- [ ] **5.1** `FraudScorer` — weighted aggregate of partial scores → severity. — **You**
- [ ] **5.2** Move weights/thresholds to `application.yml`. — **You**
- [ ] **5.3** Wire the scripted demo scenarios (DESIGN.md §11). — **You**
- [ ] **5.4** (Optional) dashboard / `@KafkaListener` scaling demo. — **You**

---

## Backlog / extensions (only if time allows)

See DESIGN.md §12. Candidates: Avro + Schema Registry (→ theory §11),
dead-letter topic, PostgreSQL + Grafana, exactly-once (→ theory §10),
consumer-group rebalance demo, ML scoring.

---

## Docs & theory track (Claude-owned)

- [x] `datastreams-theory.md` **§1 Events** — written.
- [ ] §2–§11 — written progressively at the milestone gates above.

---

## Right now

- [x] Phase 0, Milestone 1, Milestone 2 — all built and **verified end-to-end**.
- [x] Phase 2 proof: big-ticket tx → `HIGH_AMOUNT` alert → persisted → visible at `/api/alerts`.
- [ ] App is still running (background). Stop when done, or keep exploring Kafka UI / the API.
- [ ] **Next:** Milestone 3 — reference data (`accounts`, `blacklist`) + enrichment,
  then `HomeCountryRule` + `BlacklistRule`. (Theory §2–§5 folded in as we go.)