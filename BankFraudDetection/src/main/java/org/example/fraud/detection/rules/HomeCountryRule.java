package org.example.fraud.detection.rules;

import org.example.fraud.model.EnrichedTransaction;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class HomeCountryRule {

    public static final String NAME = "HOME_COUNTRY";

    public Optional<String> check(EnrichedTransaction enriched) {
        if (enriched.homeCountryMismatch()) {
            String home = enriched.account() != null ? enriched.account().homeCountry() : "?";
            return Optional.of("transaction country %s != home %s".formatted(enriched.transaction().country(), home));
        }
        return Optional.empty();
    }
}