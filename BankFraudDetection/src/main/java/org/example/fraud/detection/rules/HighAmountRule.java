package org.example.fraud.detection.rules;

import org.example.fraud.model.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Stateless rule: flags a transaction whose amount exceeds a threshold.
 *
 * "Stateless" = the decision uses ONLY this single transaction — no memory of past
 * events, no lookups. That's why it can run in a plain @KafkaListener with nothing else.
 * The threshold is read from application.yml (fraud.thresholds.high-amount).
 */
@Component
public class HighAmountRule {

    public static final String NAME = "HIGH_AMOUNT";

    private final double threshold;

    public HighAmountRule(@Value("${fraud.thresholds.high-amount}") double threshold) {
        this.threshold = threshold;
    }

    /** @return a reason string if the rule fires, otherwise empty. */
    public Optional<String> check(Transaction tx) {
        if (tx.amount().doubleValue() > threshold) {
            return Optional.of("amount %.2f > %.0f".formatted(tx.amount().doubleValue(), threshold));
        }
        return Optional.empty();
    }
}
