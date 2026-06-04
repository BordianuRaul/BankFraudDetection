package org.example.fraud.detection;

import org.example.fraud.config.KafkaTopicConfig;
import org.example.fraud.detection.rules.HighAmountRule;
import org.example.fraud.model.FraudAlert;
import org.example.fraud.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Phase 2 detection: consumes raw transactions, runs the STATELESS rules, and
 * publishes a {@link FraudAlert} to `fraud.alerts` whenever any rule fires.
 *
 * Built to grow: more stateless rules just add to the `triggered`/`reasons` lists.
 * (In Phase 3 the input switches from `transactions` to `transactions.enriched`,
 * once account reference data exists to enrich with.)
 */
@Component
public class StatelessFraudEngine {

    private static final Logger log = LoggerFactory.getLogger(StatelessFraudEngine.class);

    private final HighAmountRule highAmountRule;
    private final KafkaTemplate<String, FraudAlert> kafka;

    public StatelessFraudEngine(HighAmountRule highAmountRule, KafkaTemplate<String, FraudAlert> kafka) {
        this.highAmountRule = highAmountRule;
        this.kafka = kafka;
    }

    @KafkaListener(topics = KafkaTopicConfig.TRANSACTIONS, groupId = "fraud-engine")
    public void onTransaction(Transaction tx) {
        List<String> triggered = new ArrayList<>();
        List<String> reasons = new ArrayList<>();

        highAmountRule.check(tx).ifPresent(reason -> {
            triggered.add(HighAmountRule.NAME);
            reasons.add(reason);
        });

        if (triggered.isEmpty()) {
            return;   // nothing suspicious -> no alert
        }

        double score = 0.5;   // high-amount weight; real aggregation/scoring is Phase 5
        String severity = score >= 0.8 ? "HIGH" : score >= 0.5 ? "MEDIUM" : "LOW";

        FraudAlert alert = new FraudAlert(
                UUID.randomUUID().toString(),
                tx.transactionId(),
                tx.accountId(),
                score,
                severity,
                triggered,
                String.join("; ", reasons),
                Instant.now()
        );

        // key by accountId, same as the transaction -> alert lands in that account's lane
        kafka.send(KafkaTopicConfig.FRAUD_ALERTS, alert.accountId(), alert);
        log.warn("ALERT [{}] {} {} -> {}", alert.severity(), alert.accountId(), triggered, alert.explanation());
    }
}
