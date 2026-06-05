package org.example.fraud.model;

import java.time.Instant;

/**
 * A denylist entry — a merchant or country we consider fraudulent.
 *
 * VALUE of a record on the `blacklist` topic (KEY = {@link #entityId()}). Like `accounts`,
 * this topic is COMPACTED: only the latest value per key is kept, so it reads as a current
 * "is this entity blacklisted?" lookup. Removing an entry = publishing a tombstone (null value).
 */
public record BlacklistEntry(
        String entityId,   // merchantId (e.g. "MERCH-13") OR country code (e.g. "US") -> the Kafka KEY
        String type,       // MERCHANT | COUNTRY -> tells us which field of a transaction to match against
        String reason,     // human-readable why it's blacklisted
        Instant addedAt    // when it was added to the denylist
) {
}