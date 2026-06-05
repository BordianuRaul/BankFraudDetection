package org.example.fraud.model;

import java.math.BigDecimal;
import java.time.Instant;

public record Account(
        String accountId,
        String customerId,
        String homeCountry,
        String riskProfile,
        BigDecimal avgMonthlySpend,
        Instant createdAt
) {
}
