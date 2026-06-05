package org.example.fraud.detection.rules;

import org.example.fraud.model.EnrichedTransaction;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Stateless rule: flags a transaction whose country differs from the account's home country.
 *
 * The actual comparison was already done by the enrichment join (which had the account on hand),
 * so this rule just READS the precomputed {@code homeCountryMismatch} flag. The "lookup" state
 * lives in the GlobalKTable, not here — from this rule's point of view it's still stateless.
 * On its own this is a weak signal (people travel), hence a low weight in the Phase-5 scorer.
 */
@Component
public class HomeCountryRule {

    public static final String NAME = "HOME_COUNTRY";

    /** @return a reason string if the rule fires, otherwise empty. */
    public Optional<String> check(EnrichedTransaction enriched) {
        if (enriched.homeCountryMismatch()) {
            String home = enriched.account() != null ? enriched.account().homeCountry() : "?";
            return Optional.of("tx country %s != home %s".formatted(enriched.transaction().country(), home));
        }
        return Optional.empty();
    }
}