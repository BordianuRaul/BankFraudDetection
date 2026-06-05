package org.example.fraud.detection.rules;

import org.example.fraud.model.EnrichedTransaction;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Stateless rule: flags a transaction whose merchant or country is on the blacklist.
 *
 * Like HomeCountryRule, the heavy lifting (the GlobalKTable lookups) happened in the enrichment
 * topology, which set {@code blacklisted} + {@code blacklistReason}. This rule just reports them.
 * Blacklist is a strong signal — it gets the highest weight in the Phase-5 scorer.
 */
@Component
public class BlacklistRule {

    public static final String NAME = "BLACKLIST";

    /** @return a reason string if the rule fires, otherwise empty. */
    public Optional<String> check(EnrichedTransaction enriched) {
        if (enriched.blacklisted()) {
            return Optional.of("blacklisted: " + enriched.blacklistReason());
        }
        return Optional.empty();
    }
}