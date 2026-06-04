# Bank Fraud Detection — Design Document

> Real-time bank fraud detection on a stream of transactions, built with **Spring Boot** + **Apache Kafka**.
> Course: *Fluxuri de Date* (Data Streams).

---

## 1. Goals

- Ingest a continuous **stream of bank transactions** and detect fraud **in real time**.
- Demonstrate core **stream-processing** concepts: topics, partitioning, consumer groups, windowing, stateful aggregation, stream–table joins.
- Keep it a **Spring Boot** application end to end (the team's strength), introducing Kafka Streams progressively for the stateful parts.
- Be **demoable**: a simulator floods the system with transactions, fraud alerts pop out the other end, viewable via Kafka UI and a small REST API.

### Non-goals (for the course project)
- Production-grade ML models (we use rule-based + simple statistical scoring; ML is a listed extension).
- Real banking integrations, real PII, real money.
- High-availability multi-broker cluster tuning.

---

## 2. Why Kafka here

| Requirement | How Kafka delivers |
|---|---|
| High-throughput, never-ending input | Append-only partitioned log |
| React in real time | Consumers read as messages arrive |
| Decouple producers from detectors | Pub/sub via topics + consumer groups |
| Ordered per-account processing | Partition by `accountId` (order guaranteed within a partition) |
| Add new detectors without touching producers | New consumer group subscribes to the same topic |
| Replay history to test a new rule | Retention keeps messages after they're read |
| Scale detection horizontally | More consumers in a group = partitions split across them |

---

## 3. High-level architecture

Single Spring Boot application, internally modular. Each box below is a package/module; the arrows are Kafka topics. This can later be split into independent microservices with **zero change to the topic contracts**.

```
                         ┌──────────────────────────────────────────────────────────────┐
                         │                  Spring Boot application                       │
                         │                                                                │
  ┌───────────────┐      │  ┌─────────────┐        ┌──────────────────────────────────┐  │
  │  Transaction  │      │  │ Enrichment  │        │          Fraud Engine            │  │
  │   Generator   │──────┼─▶│  Processor  │───────▶│  (rules + Kafka Streams state)   │──┼──┐
  │  (simulator)  │  T1  │  │ join w/ ref │   T2   │                                  │  │  │ T3
  └───────────────┘      │  └─────────────┘        └──────────────────────────────────┘  │  │
                         │         ▲                                                      │  │
                         │         │ GlobalKTable (accounts, blacklist)                   │  │
                         │  ┌──────┴───────┐                              ┌────────────┐  │  │
                         │  │ Reference    │                              │   Alert    │◀─┼──┘
                         │  │ data loader  │                              │  Service   │  │
                         │  └──────────────┘                              └─────┬──────┘  │
                         │                                                      │         │
                         └──────────────────────────────────────────────────────┼─────────┘
                                                                                │
                                  ┌─────────────────────────────────────────────┼───────────────┐
                                  ▼                       ▼                       ▼               ▼
                            Alert store (DB)        REST API            (optional) dashboard   Kafka UI
                                                  /api/alerts            (web/console)      (topic inspector)

  Topics:  T1 = transactions   T2 = transactions.enriched   T3 = fraud.alerts
```

### Component responsibilities

| Component | Responsibility | Kafka role |
|---|---|---|
| **Transaction Generator** | Simulate realistic transactions (normal + injected fraud patterns) | Producer → `transactions` |
| **Enrichment Processor** | Join raw tx with account/customer reference data; flag obviously invalid input | Consumer `transactions` → Producer `transactions.enriched` (Kafka Streams + GlobalKTable join) |
| **Fraud Engine** | Run all detectors, compute a fraud score, decide alert/no-alert | Consumer `transactions.enriched` → Producer `fraud.alerts` (mix of `@KafkaListener` + Kafka Streams) |
| **Alert Service** | Persist alerts, expose REST API, (optional) push notifications | Consumer `fraud.alerts` → DB + HTTP |
| **Reference data loader** | Load accounts + blacklist into compacted topics / GlobalKTables | Producer to `accounts`, `blacklist` |

---

## 4. Kafka topic design

| Topic | Key | Value | Partitions | Cleanup | Notes |
|---|---|---|---|---|---|
| `transactions` | `accountId` | `Transaction` (JSON) | 3 | delete (retention) | Raw ingress. Keyed by account → per-account ordering & co-location |
| `transactions.enriched` | `accountId` | `EnrichedTransaction` | 3 | delete | Tx + account/customer context |
| `fraud.alerts` | `accountId` | `FraudAlert` | 3 | delete | One per detected alert |
| `accounts` | `accountId` | `Account` | 3 | **compact** | Reference data, materialized as GlobalKTable |
| `blacklist` | `entityId` | `BlacklistEntry` | 1 | **compact** | Merchant/country denylist |

**Design notes**
- **Partition count = 3** is plenty for a demo and lets you *show* a consumer group rebalancing across 3 consumers.
- **Same key everywhere (`accountId`)** keeps an account's whole pipeline (tx → enriched → alert) in the same partition lane → ordered and join-friendly.
- **Compacted topics** for reference data: Kafka keeps only the latest value per key, perfect for a "current state" lookup table.

---

## 5. Data model

```jsonc
// Transaction (topic: transactions)
{
  "transactionId": "uuid",
  "accountId": "ACC-1001",
  "cardId": "CARD-77",
  "amount": 1450.00,
  "currency": "EUR",
  "timestamp": "2026-06-04T18:22:01Z",
  "channel": "ONLINE",            // POS | ATM | ONLINE | TRANSFER
  "merchantId": "MERCH-55",
  "merchantCategory": "ELECTRONICS",
  "country": "RO",
  "city": "Iasi",
  "lat": 47.1585,
  "lon": 27.6014
}

// Account (topic: accounts, compacted) — reference data
{
  "accountId": "ACC-1001",
  "customerId": "CUST-9",
  "homeCountry": "RO",
  "riskProfile": "LOW",           // LOW | MEDIUM | HIGH
  "avgMonthlySpend": 2200.00,
  "createdAt": "2024-01-10T00:00:00Z"
}

// BlacklistEntry (topic: blacklist, compacted)
{
  "entityId": "MERCH-13",         // merchantId or country code
  "type": "MERCHANT",             // MERCHANT | COUNTRY
  "reason": "known fraud ring",
  "addedAt": "2026-05-01T00:00:00Z"
}

// EnrichedTransaction (topic: transactions.enriched)
{
  "transaction": { /* Transaction */ },
  "account": { /* Account, or null if unknown */ },
  "homeCountryMismatch": true
}

// FraudAlert (topic: fraud.alerts)
{
  "alertId": "uuid",
  "transactionId": "uuid",
  "accountId": "ACC-1001",
  "score": 0.82,                  // 0..1 aggregated
  "severity": "HIGH",             // LOW | MEDIUM | HIGH
  "triggeredRules": ["VELOCITY", "IMPOSSIBLE_TRAVEL"],
  "explanation": "5 tx in 38s; RO→US in 4 min",
  "detectedAt": "2026-06-04T18:22:02Z"
}
```

**Serialization:** start with **JSON** (Jackson `JsonSerializer`/`JsonDeserializer`) — zero setup, easy to read in Kafka UI. Listed extension: switch to **Avro + Schema Registry** for schema evolution (the "production" choice).

---

## 6. Fraud detection rules

Each detector returns a partial score in `[0,1]` + a reason. The engine aggregates (weighted sum, capped at 1.0); above a threshold → `FraudAlert`. Mixing stateless and stateful rules is intentional — it shows the full range of stream processing.

| # | Rule | Type | Mechanism | Example trigger |
|---|---|---|---|---|
| 1 | **High amount** | Stateless | Per-message threshold (`@KafkaListener`) | `amount > 10 000` |
| 2 | **Velocity** | **Stateful — windowed** | Kafka Streams tumbling/sliding window count per `accountId` | `> 5 tx in 60s` |
| 3 | **Impossible travel** | **Stateful** | Kafka Streams state store: keep last tx (geo+time) per account; compute required speed | `RO→US within 4 min` |
| 4 | **Blacklist** | Stateless lookup | GlobalKTable join on merchant/country | merchant/country on denylist |
| 5 | **Amount anomaly** | **Stateful — aggregation** | Running mean/stddev per account in state store | `amount > mean + 3·stddev` |
| 6 | **Home-country mismatch** | Stateless | Field from enrichment | tx country ≠ `homeCountry` (low weight on its own) |

### Scoring sketch
```
score = min(1.0,  0.5·highAmount
                + 0.7·velocity
                + 0.9·impossibleTravel
                + 1.0·blacklist
                + 0.6·amountAnomaly
                + 0.2·homeCountryMismatch )

severity = score >= 0.8 ? HIGH : score >= 0.5 ? MEDIUM : LOW
alert if score >= 0.5
```
Weights/thresholds live in `application.yml` so they're tunable during the demo.

---

## 7. Project structure (packages)

Single Maven/Gradle module, packages by feature:

```
src/main/java/org/example/fraud/
├── FraudDetectionApplication.java        // @SpringBootApplication
├── config/
│   ├── KafkaTopicConfig.java             // NewTopic beans (auto-create topics)
│   ├── KafkaStreamsConfig.java           // @EnableKafkaStreams, StreamsBuilder
│   └── SerdeConfig.java                  // JSON Serdes
├── model/
│   ├── Transaction.java
│   ├── EnrichedTransaction.java
│   ├── Account.java
│   ├── BlacklistEntry.java
│   └── FraudAlert.java
├── generator/
│   └── TransactionGenerator.java         // @Scheduled producer; normal + fraud scenarios
├── enrichment/
│   └── EnrichmentStream.java             // KStream join w/ accounts GlobalKTable
├── detection/
│   ├── FraudScoringTopology.java         // Kafka Streams topology (velocity, travel, anomaly)
│   ├── rules/
│   │   ├── HighAmountRule.java
│   │   ├── BlacklistRule.java
│   │   └── HomeCountryRule.java
│   └── FraudScorer.java                  // aggregate partial scores → FraudAlert
├── alert/
│   ├── AlertListener.java                // @KafkaListener on fraud.alerts → persist
│   ├── AlertRepository.java              // Spring Data
│   └── AlertController.java              // REST: GET /api/alerts
└── refdata/
    └── ReferenceDataLoader.java          // seed accounts + blacklist on startup
```

---

## 8. Tech stack & dependencies

| Concern | Choice |
|---|---|
| Language / build | Java 21, **Maven** (already in repo) |
| Framework | Spring Boot 3.3.x |
| Kafka (clients + listeners) | `spring-kafka` |
| Stream processing | **Kafka Streams** (`org.apache.kafka:kafka-streams`, wired via spring-kafka) |
| Persistence | Spring Data JPA + **H2** (in-memory, zero setup) → swap to PostgreSQL as extension |
| API | Spring Web (`spring-boot-starter-web`) |
| JSON | Jackson (`spring-kafka` JSON Serde) |
| Local infra | **Docker Compose**: Kafka (KRaft, no Zookeeper) + Kafka UI |
| Testing | `spring-kafka-test` (embedded broker), `kafka-streams-test-utils` (`TopologyTestDriver`) |

### `pom.xml` dependencies to add
```xml
<parent>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-parent</artifactId>
  <version>3.3.5</version>
</parent>

<dependencies>
  <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-web</artifactId></dependency>
  <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka</artifactId></dependency>
  <dependency><groupId>org.apache.kafka</groupId><artifactId>kafka-streams</artifactId></dependency>
  <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-data-jpa</artifactId></dependency>
  <dependency><groupId>com.h2database</groupId><artifactId>h2</artifactId><scope>runtime</scope></dependency>

  <!-- tests -->
  <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-test</artifactId><scope>test</scope></dependency>
  <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka-test</artifactId><scope>test</scope></dependency>
</dependencies>
```
> Note: the current `pom.xml` is a plain Maven project. Converting it to a Spring Boot project (adding the parent + starters above) is **Phase 0**.

---

## 9. Local dev setup

### `docker-compose.yml` (Kafka in KRaft mode + Kafka UI)
```yaml
services:
  kafka:
    image: apache/kafka:3.8.0          # KRaft, no Zookeeper needed
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8081:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
    depends_on:
      - kafka
```
- `docker compose up -d` → Kafka on `localhost:9092`, Kafka UI on `http://localhost:8081`.
- Spring app connects with `spring.kafka.bootstrap-servers: localhost:9092`.

### `application.yml` skeleton
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    consumer:
      group-id: fraud-engine
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "org.example.fraud.model"
    streams:
      application-id: fraud-streams
      properties:
        default.key.serde: org.apache.kafka.common.serialization.Serdes$StringSerde

fraud:
  thresholds:
    high-amount: 10000
    velocity-count: 5
    velocity-window-seconds: 60
    alert-score: 0.5
```

---

## 10. Implementation roadmap (phases)

Build it incrementally — each phase is independently demoable.

- **Phase 0 — Bootstrap.** Convert the Maven project to Spring Boot (parent + starters). Add `docker-compose.yml`. App starts, connects to Kafka.
- **Phase 1 — Produce & consume.** `TransactionGenerator` (`@Scheduled`) → `transactions`. A trivial `@KafkaListener` logs them. *Learn: producer, consumer, topics, partitions, keys.* Watch it in Kafka UI.
- **Phase 2 — Stateless rules.** Implement High-amount + Blacklist + Home-country → emit `fraud.alerts`. `AlertListener` persists to H2; `AlertController` exposes `GET /api/alerts`.
- **Phase 3 — Reference data + enrichment.** Compacted `accounts`/`blacklist` topics → GlobalKTable; `EnrichmentStream` joins and writes `transactions.enriched`. *Learn: KTable, GlobalKTable, stream–table joins.*
- **Phase 4 — Stateful streaming rules.** Kafka Streams topology: **velocity** (windowed count), **impossible travel** (state store), **amount anomaly** (running stats). *Learn: windowing, state stores, aggregations — the core data-streams content.*
- **Phase 5 — Scoring & polish.** `FraudScorer` aggregates partial scores → severity. Tunable thresholds. Demo scenarios + (optional) dashboard.

---

## 11. Demo scenarios

Scripted patterns the generator can inject on demand (great for a live presentation):

| Scenario | What the generator does | Expected alert |
|---|---|---|
| Normal traffic | Random small tx within home country | No alerts |
| Big-ticket purchase | One tx `amount = 25 000` | HIGH_AMOUNT |
| Card testing / velocity | 8 tx for one account in 30s | VELOCITY |
| Stolen card abroad | RO tx then US tx 3 min later | IMPOSSIBLE_TRAVEL |
| Blacklisted merchant | tx to `MERCH-13` | BLACKLIST |
| Combo | velocity + abroad together | HIGH severity, multiple rules |

---

## 12. Possible extensions (if time allows / for a higher grade)

- **Avro + Schema Registry** instead of JSON (schema evolution).
- **Split into microservices** (ingestion / detection / alerting) — topics are already the contract.
- **PostgreSQL** instead of H2; **Grafana** dashboard over alert metrics.
- **Dead-letter topic** for malformed messages + error handling.
- **ML scoring**: export features to a model (e.g. isolation forest) instead of hand-tuned weights.
- **Exactly-once semantics** (Kafka transactions) for the alert pipeline.
- **Consumer-group scaling demo**: run 3 instances, show partition rebalancing.

---

## 13. Design decisions

| # | Decision | Choice | Status |
|---|---|---|---|
| 1 | **Deployment shape** | **Modular monolith** — one Spring Boot app, packages by feature; topics remain the contract so it can be split into microservices later | ✅ Decided |
| 2 | **Processing engine** | **Hybrid** — `@KafkaListener` for stateless rules (high-amount, blacklist, home-country); **Kafka Streams** for stateful rules (velocity windows, impossible-travel state store, amount-anomaly aggregation) | ✅ Decided |
| 3 | **Serialization** | **JSON** (Jackson Serdes) to start; Avro + Schema Registry listed as an extension | Default (revisit) |
| 4 | **Storage** | **H2** in-memory; PostgreSQL listed as an extension | Default (revisit) |

Decisions 1–2 are locked. Decisions 3–4 keep their defaults (fastest path to a demoable project) and can be revisited if time allows.

---

## 14. Status

Design complete and approved (modular monolith + hybrid processing). **No application code written yet** — next step when ready is **Phase 0 (Bootstrap)**: convert the Maven skeleton to Spring Boot and add `docker-compose.yml` + `application.yml`.
```
