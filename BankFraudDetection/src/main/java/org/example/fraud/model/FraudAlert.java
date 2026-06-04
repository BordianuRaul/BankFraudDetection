package org.example.fraud.model;

import java.time.Instant;
import java.util.List;

/**
 * A fraud alert — the output of the detection rules for one suspicious transaction.
 *
 * VALUE of a record on the `fraud.alerts` topic; the KEY is accountId (same key as the
 * transaction it came from, so an account's tx and its alerts share a partition lane).
 */
public record FraudAlert(
        String alertId,
        String transactionId,
        String accountId,
        double score,                 // 0..1 aggregated suspicion (full scoring arrives in Phase 5)
        String severity,              // LOW | MEDIUM | HIGH
        List<String> triggeredRules,  // e.g. ["HIGH_AMOUNT"]
        String explanation,           // human-readable reason(s)
        Instant detectedAt
) {
}
