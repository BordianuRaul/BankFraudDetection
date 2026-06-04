package org.example.fraud.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A bank transaction — an immutable fact that a payment happened.
 *
 * This is the VALUE of a record on the `transactions` topic; the KEY is {@link #accountId()}.
 * It is a {@code record} on purpose: events never change once created (see
 * datastreams-theory.md §1.1 "immutability" and §1.4 "anatomy of an event").
 */
public record Transaction(
        String transactionId,     // unique id of this transaction (uuid)
        String accountId,         // the account it belongs to  -> used as the Kafka KEY
        String cardId,            // which card was used
        BigDecimal amount,        // money -> BigDecimal, never double, to avoid rounding errors
        String currency,          // e.g. EUR
        Instant timestamp,        // EVENT time: when it happened (not when we process it)
        String channel,           // POS | ATM | ONLINE | TRANSFER
        String merchantId,
        String merchantCategory,
        String country,           // country where the tx happened (ISO code)
        String city,
        double lat,
        double lon
) {
}