package org.example.fraud.detection.rules;

import org.example.fraud.model.EnrichedTransaction;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class BlacklistRule {

    public static final String NAME = "BLACKLIST";

    public Optional<String> check(EnrichedTransaction enriched) {
        if (enriched.blacklisted()) {
            return Optional.of("blacklisted: " + enriched.blacklistReason());
        }
        return Optional.empty();
    }
}