package org.example.fraud.model;

public record EnrichedTransaction(
        Transaction transaction,
        Account account,
        boolean homeCountryMismatch,
        boolean blacklisted,
        String blacklistReason
) {
}
