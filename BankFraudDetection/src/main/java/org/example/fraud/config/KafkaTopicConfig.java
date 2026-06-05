package org.example.fraud.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String TRANSACTIONS          = "transactions";
    public static final String TRANSACTIONS_ENRICHED = "transactions.enriched";
    public static final String FRAUD_ALERTS          = "fraud.alerts";
    public static final String ACCOUNTS              = "accounts";
    public static final String BLACKLIST             = "blacklist";

    @Bean
    public NewTopic transactionsTopic() {
        return TopicBuilder.name(TRANSACTIONS)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic transactionsEnrichedTopic() {
        return TopicBuilder.name(TRANSACTIONS_ENRICHED)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic fraudAlertsTopic() {
        return TopicBuilder.name(FRAUD_ALERTS)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic accountsTopic() {
        return TopicBuilder.name(ACCOUNTS)
                .partitions(3)
                .replicas(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic blacklistTopic() {
        return TopicBuilder.name(BLACKLIST)
                .partitions(1)
                .replicas(1)
                .compact()
                .build();
    }
}
