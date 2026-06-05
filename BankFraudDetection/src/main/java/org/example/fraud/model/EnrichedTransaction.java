package org.example.fraud.model;

/**
 * A transaction joined with reference data — the OUTPUT of the enrichment step.
 *
 * VALUE of a record on the `transactions.enriched` topic (KEY = the same accountId as the
 * transaction, so the whole pipeline tx -> enriched -> alert stays in one partition lane).
 *
 * It is a DERIVED event: the EnrichmentStream computes it by joining the `transactions`
 * stream against the `accounts` AND `blacklist` GlobalKTables (theory §1.6 derived events,
 * §1.8 stream-table join). The boolean flags are PRECOMPUTED there so the downstream rules
 * stay trivial field-checks.
 */
public record EnrichedTransaction(
        Transaction transaction,     // the original event, unchanged
        Account account,             // the matched account, or null if the join found no account
        boolean homeCountryMismatch, // tx.country != account.homeCountry (HomeCountryRule reads this)
        boolean blacklisted,         // tx.merchantId OR tx.country is on the blacklist (BlacklistRule reads this)
        String blacklistReason       // why it's blacklisted, or null if not blacklisted
) {
}