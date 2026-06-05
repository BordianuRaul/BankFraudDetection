package org.example.fraud.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Reference data about an account — NOT an event, but a "current state" fact.
 *
 * This is the VALUE of a record on the `accounts` topic (KEY = {@link #accountId()}).
 * That topic is COMPACTED: Kafka keeps only the latest value per key, so the topic behaves
 * like a lookup table ("the current account") rather than an append-only history. In Phase 3
 * we materialize it as a GlobalKTable and join transactions against it (theory §1.8).
 */
public record Account(
        String accountId,          // the account this describes -> the Kafka KEY (and the join key)
        String customerId,         // the customer who owns it
        String homeCountry,        // where the account lives -> HomeCountryRule compares tx.country to this
        String riskProfile,        // LOW | MEDIUM | HIGH
        BigDecimal avgMonthlySpend,// money -> BigDecimal; used later by the amount-anomaly rule (Phase 4)
        Instant createdAt          // when the account was opened
) {
}