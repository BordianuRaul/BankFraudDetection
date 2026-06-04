package org.example.fraud.consumer;

import org.example.fraud.config.KafkaTopicConfig;
import org.example.fraud.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Phase 1 learning consumer: simply logs every transaction so you can watch the
 * full producer -> topic -> consumer flow end to end.
 *
 * It is temporary scaffolding — the real detectors (Milestone 2+) replace it.
 * Its own groupId ("tx-logger") keeps it independent of the fraud-engine consumers,
 * so both can read the same `transactions` events without stealing from each other.
 */
@Component
public class TransactionLogConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransactionLogConsumer.class);

    @KafkaListener(topics = KafkaTopicConfig.TRANSACTIONS, groupId = "tx-logger")
    public void onTransaction(Transaction tx) {
        log.info("consumed <- {} amount={} {} via {} in {}",
                tx.accountId(), tx.amount(), tx.currency(), tx.channel(), tx.country());
    }
}