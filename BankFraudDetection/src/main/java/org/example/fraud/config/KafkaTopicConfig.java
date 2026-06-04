package org.example.fraud.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the Kafka topics this app owns.
 *
 * Spring's auto-configured {@code KafkaAdmin} sees every {@link NewTopic} bean and
 * creates the topic on the broker at startup. We do this explicitly because the broker
 * has auto-create disabled (KAFKA_AUTO_CREATE_TOPICS_ENABLE=false in docker-compose.yml),
 * so a topic only exists if we declare it here.
 */
@Configuration
public class KafkaTopicConfig {

    /** Raw ingress: every transaction lands here, keyed by accountId. */
    public static final String TRANSACTIONS = "transactions";

    @Bean
    public NewTopic transactionsTopic() {
        return TopicBuilder.name(TRANSACTIONS)
                .partitions(3)   // 3 lanes -> lets us show a consumer group splitting work later
                .replicas(1)     // single-node dev broker, so only 1 copy is possible
                .build();
    }

    /** One record per detected fraud alert, keyed by accountId. */
    public static final String FRAUD_ALERTS = "fraud.alerts";

    @Bean
    public NewTopic fraudAlertsTopic() {
        return TopicBuilder.name(FRAUD_ALERTS)
                .partitions(3)
                .replicas(1)
                .build();
    }
}