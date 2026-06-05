package org.example.fraud.model;

import java.time.Instant;

public record BlacklistEntry(
        String entityId,
        String type,
        String reason,
        Instant addedAt
) {
}
