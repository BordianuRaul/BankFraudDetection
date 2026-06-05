package org.example.fraud.detection;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.fraud.detection.rules.BlacklistRule;
import org.example.fraud.detection.rules.HighAmountRule;
import org.example.fraud.detection.rules.HomeCountryRule;
import org.example.fraud.model.EnrichedTransaction;
import org.example.fraud.model.FraudAlert;
import org.example.fraud.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

class RiskEvaluationProcessor implements Processor<String, EnrichedTransaction, String, FraudAlert> {

    private static final Logger log = LoggerFactory.getLogger(RiskEvaluationProcessor.class);
    private static final String VELOCITY = "VELOCITY";
    private static final String AMOUNT_ANOMALY = "AMOUNT_ANOMALY";

    private final HighAmountRule highAmountRule;
    private final HomeCountryRule homeCountryRule;
    private final BlacklistRule blacklistRule;
    private final FraudScorer scorer;
    private final String velocityStoreName;
    private final String amountStoreName;
    private final int velocityCount;
    private final long velocityWindowSeconds;
    private final int anomalyMinSamples;
    private final double anomalyStddevs;

    private ProcessorContext<String, FraudAlert> context;
    private KeyValueStore<String, RecentActivity> velocityStore;
    private KeyValueStore<String, AccountStats> amountStore;

    RiskEvaluationProcessor(HighAmountRule highAmountRule, HomeCountryRule homeCountryRule,
                            BlacklistRule blacklistRule, FraudScorer scorer,
                            String velocityStoreName, String amountStoreName,
                            int velocityCount, long velocityWindowSeconds,
                            int anomalyMinSamples, double anomalyStddevs) {
        this.highAmountRule = highAmountRule;
        this.homeCountryRule = homeCountryRule;
        this.blacklistRule = blacklistRule;
        this.scorer = scorer;
        this.velocityStoreName = velocityStoreName;
        this.amountStoreName = amountStoreName;
        this.velocityCount = velocityCount;
        this.velocityWindowSeconds = velocityWindowSeconds;
        this.anomalyMinSamples = anomalyMinSamples;
        this.anomalyStddevs = anomalyStddevs;
    }

    @Override
    public void init(ProcessorContext<String, FraudAlert> context) {
        this.context = context;
        this.velocityStore = context.getStateStore(velocityStoreName);
        this.amountStore = context.getStateStore(amountStoreName);
    }

    @Override
    public void process(Record<String, EnrichedTransaction> record) {
        String accountId = record.key();
        EnrichedTransaction enriched = record.value();
        if (accountId == null || enriched == null) return;
        Transaction transaction = enriched.transaction();

        List<String> triggered = new ArrayList<>();
        List<String> reasons = new ArrayList<>();

        highAmountRule.check(transaction).ifPresent(r -> { triggered.add(HighAmountRule.NAME); reasons.add(r); });
        homeCountryRule.check(enriched).ifPresent(r -> { triggered.add(HomeCountryRule.NAME); reasons.add(r); });
        blacklistRule.check(enriched).ifPresent(r -> { triggered.add(BlacklistRule.NAME); reasons.add(r); });
        checkVelocity(accountId, record.timestamp()).ifPresent(r -> { triggered.add(VELOCITY); reasons.add(r); });
        checkAmountAnomaly(accountId, transaction).ifPresent(r -> { triggered.add(AMOUNT_ANOMALY); reasons.add(r); });

        if (triggered.isEmpty()) return;

        double score = scorer.aggregate(triggered);
        if (!scorer.alerts(score)) return;

        String severity = scorer.severity(score);
        String explanation = String.join("; ", reasons);
        FraudAlert alert = new FraudAlert(UUID.randomUUID().toString(), transaction.transactionId(), accountId,
                score, severity, triggered, explanation, Instant.now());
        context.forward(new Record<>(accountId, alert, record.timestamp()));
        log.warn("ALERT [{}] {} {} -> {}", severity, accountId, triggered, explanation);
    }
    private Optional<String> checkVelocity(String accountId, long nowMs) {
        RecentActivity activity = velocityStore.get(accountId);
        if (activity == null) activity = RecentActivity.empty();
        activity = activity.record(nowMs, velocityWindowSeconds * 1000);
        velocityStore.put(accountId, activity);
        if (activity.count() > velocityCount) {
            return Optional.of("%d transactions within %ds".formatted(activity.count(), velocityWindowSeconds));
        }
        return Optional.empty();
    }

    private Optional<String> checkAmountAnomaly(String accountId, Transaction transaction) {
        double amount = transaction.amount().doubleValue();
        AccountStats stats = amountStore.get(accountId);
        if (stats == null) stats = AccountStats.empty();
        Optional<String> reason = Optional.empty();
        if (stats.count() >= anomalyMinSamples && stats.stddev() > 0) {
            double threshold = stats.mean() + anomalyStddevs * stats.stddev();
            if (amount > threshold) {
                reason = Optional.of("amount %.2f > mean %.2f + %.0f*stddev %.2f".formatted(
                        amount, stats.mean(), anomalyStddevs, stats.stddev()));
            }
        }
        amountStore.put(accountId, stats.add(amount));
        return reason;
    }
}
