package org.example.fraud.consumer;

import org.example.fraud.config.KafkaTopicConfig;
import org.example.fraud.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionLogConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransactionLogConsumer.class);

    @KafkaListener(topics = KafkaTopicConfig.TRANSACTIONS, groupId = "transaction-logger")
    public void onTransaction(Transaction transaction) {
        log.info("consumed <- {} amount={} {} via {} in {}",
                transaction.accountId(), transaction.amount(), transaction.currency(), transaction.channel(), transaction.country());
    }
}