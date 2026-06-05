package org.example.fraud.model;

import java.math.BigDecimal;
import java.time.Instant;

public record Transaction(
        String transactionId,
        String accountId,
        String cardId,
        BigDecimal amount,
        String currency,
        Instant timestamp,
        String channel,
        String merchantId,
        String merchantCategory,
        String country,
        String city,
        double lat,
        double lon
) {
}