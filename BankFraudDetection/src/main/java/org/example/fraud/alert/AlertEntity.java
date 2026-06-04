package org.example.fraud.alert;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import java.time.Instant;

/**
 * How a fraud alert is STORED in H2 (a JPA entity).
 *
 * Kept separate from the Kafka model ({@code FraudAlert} record) on purpose: JPA needs
 * a mutable class with a no-arg constructor, while the Kafka event is an immutable record.
 * Same data, two representations for two different jobs (messaging vs. persistence).
 */
@Entity
public class AlertEntity {

    @Id
    private String alertId;
    private String transactionId;
    private String accountId;
    private double score;
    private String severity;
    @Column(length = 200)
    private String triggeredRules;   // comma-joined, e.g. "HIGH_AMOUNT"
    @Column(length = 500)
    private String explanation;
    private Instant detectedAt;

    protected AlertEntity() {
        // required by JPA
    }

    public AlertEntity(String alertId, String transactionId, String accountId, double score,
                       String severity, String triggeredRules, String explanation, Instant detectedAt) {
        this.alertId = alertId;
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.score = score;
        this.severity = severity;
        this.triggeredRules = triggeredRules;
        this.explanation = explanation;
        this.detectedAt = detectedAt;
    }

    public String getAlertId() { return alertId; }
    public String getTransactionId() { return transactionId; }
    public String getAccountId() { return accountId; }
    public double getScore() { return score; }
    public String getSeverity() { return severity; }
    public String getTriggeredRules() { return triggeredRules; }
    public String getExplanation() { return explanation; }
    public Instant getDetectedAt() { return detectedAt; }
}
