package org.example.fraud.detection;

import org.example.fraud.detection.rules.BlacklistRule;
import org.example.fraud.detection.rules.HighAmountRule;
import org.example.fraud.detection.rules.HomeCountryRule;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;

@Component
public class FraudScorer {

    private final Map<String, Double> weights;
    private final double alertScore;

    public FraudScorer(
            @Value("${fraud.weights.high-amount}") double highAmount,
            @Value("${fraud.weights.velocity}") double velocity,
            @Value("${fraud.weights.impossible-travel}") double impossibleTravel,
            @Value("${fraud.weights.blacklist}") double blacklist,
            @Value("${fraud.weights.amount-anomaly}") double amountAnomaly,
            @Value("${fraud.weights.home-country}") double homeCountry,
            @Value("${fraud.thresholds.alert-score}") double alertScore) {
        this.weights = Map.of(
                HighAmountRule.NAME, highAmount,
                "VELOCITY", velocity,
                "IMPOSSIBLE_TRAVEL", impossibleTravel,
                BlacklistRule.NAME, blacklist,
                "AMOUNT_ANOMALY", amountAnomaly,
                HomeCountryRule.NAME, homeCountry);
        this.alertScore = alertScore;
    }

    public double aggregate(Collection<String> firedRules) {
        double sum = firedRules.stream().mapToDouble(r -> weights.getOrDefault(r, 0.0)).sum();
        return Math.min(1.0, sum);
    }

    public String severity(double score) {
        return score >= 0.8 ? "HIGH" : score >= 0.5 ? "MEDIUM" : "LOW";
    }

    public boolean alerts(double score) {
        return score >= alertScore;
    }
}
