package org.example.fraud.model;

import java.time.Instant;
import java.util.List;

public record FraudAlert(
        String alertId,
        String transactionId,
        String accountId,
        double score,
        String severity,
        List<String> triggeredRules,
        String explanation,
        Instant detectedAt
) {
}
