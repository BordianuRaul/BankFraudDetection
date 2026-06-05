package org.example.fraud.detection;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.Stores;
import org.example.fraud.config.KafkaTopicConfig;
import org.example.fraud.detection.rules.BlacklistRule;
import org.example.fraud.detection.rules.HighAmountRule;
import org.example.fraud.detection.rules.HomeCountryRule;
import org.example.fraud.model.EnrichedTransaction;
import org.example.fraud.model.FraudAlert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

/**
 * Unified detection topology (Kafka Streams): reads `transactions.enriched`, runs every rule per
 * transaction in {@link RiskEvaluationProcessor}, and emits one combined {@link FraudAlert}.
 * Velocity and amount-anomaly keep per-account state in the two stores registered here.
 */
@Component
public class FraudScoringTopology {

    private static final String VELOCITY_STORE = "velocity-activity";
    private static final String AMOUNT_STATS_STORE = "amount-stats";

    private final HighAmountRule highAmountRule;
    private final HomeCountryRule homeCountryRule;
    private final BlacklistRule blacklistRule;
    private final FraudScorer scorer;
    private final int velocityCount;
    private final long velocityWindowSeconds;
    private final int anomalyMinSamples;
    private final double anomalyStddevs;

    public FraudScoringTopology(HighAmountRule highAmountRule, HomeCountryRule homeCountryRule,
                                BlacklistRule blacklistRule, FraudScorer scorer,
                                @Value("${fraud.thresholds.velocity-count}") int velocityCount,
                                @Value("${fraud.thresholds.velocity-window-seconds}") long velocityWindowSeconds,
                                @Value("${fraud.thresholds.anomaly-min-samples}") int anomalyMinSamples,
                                @Value("${fraud.thresholds.anomaly-stddevs}") double anomalyStddevs) {
        this.highAmountRule = highAmountRule;
        this.homeCountryRule = homeCountryRule;
        this.blacklistRule = blacklistRule;
        this.scorer = scorer;
        this.velocityCount = velocityCount;
        this.velocityWindowSeconds = velocityWindowSeconds;
        this.anomalyMinSamples = anomalyMinSamples;
        this.anomalyStddevs = anomalyStddevs;
    }

    /** spring-kafka hands us the shared StreamsBuilder (same streams app as EnrichmentStream). */
    @Autowired
    public void buildDetection(StreamsBuilder builder) {
        Serde<String> keySerde = Serdes.String();
        Serde<EnrichedTransaction> enrichedSerde = new JsonSerde<>(EnrichedTransaction.class).ignoreTypeHeaders();
        Serde<FraudAlert> alertSerde = new JsonSerde<>(FraudAlert.class);
        Serde<RecentActivity> activitySerde = new JsonSerde<>(RecentActivity.class).ignoreTypeHeaders();
        Serde<AccountStats> statsSerde = new JsonSerde<>(AccountStats.class).ignoreTypeHeaders();

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(VELOCITY_STORE), keySerde, activitySerde));
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(AMOUNT_STATS_STORE), keySerde, statsSerde));

        ProcessorSupplier<String, EnrichedTransaction, String, FraudAlert> detection =
                () -> new RiskEvaluationProcessor(highAmountRule, homeCountryRule, blacklistRule, scorer,
                        VELOCITY_STORE, AMOUNT_STATS_STORE, velocityCount, velocityWindowSeconds,
                        anomalyMinSamples, anomalyStddevs);

        builder.stream(KafkaTopicConfig.TRANSACTIONS_ENRICHED, Consumed.with(keySerde, enrichedSerde))
                .process(detection, VELOCITY_STORE, AMOUNT_STATS_STORE)
                .to(KafkaTopicConfig.FRAUD_ALERTS, Produced.with(keySerde, alertSerde));
    }
}
