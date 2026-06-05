package org.example.fraud.detection.rules;

import org.example.fraud.model.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class HighAmountRule {

    public static final String NAME = "HIGH_AMOUNT";

    private final double threshold;

    public HighAmountRule(@Value("${fraud.thresholds.high-amount}") double threshold) {
        this.threshold = threshold;
    }

    public Optional<String> check(Transaction transaction) {
        if (transaction.amount().doubleValue() > threshold) {
            return Optional.of("amount %.2f > %.0f".formatted(transaction.amount().doubleValue(), threshold));
        }
        return Optional.empty();
    }
}
