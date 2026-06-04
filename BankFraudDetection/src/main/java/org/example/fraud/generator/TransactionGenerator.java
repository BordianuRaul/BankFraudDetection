package org.example.fraud.generator;

import org.example.fraud.config.KafkaTopicConfig;
import org.example.fraud.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Simulates a live stream of transactions.
 *
 * Every tick it builds one random {@link Transaction} and PRODUCES it to the
 * `transactions` topic, keyed by accountId. Keying by accountId means all of one
 * account's events go to the SAME partition, so they stay ordered (theory §1.7).
 */
@Component
public class TransactionGenerator {

    private static final Logger log = LoggerFactory.getLogger(TransactionGenerator.class);

    // Small fixed pools so the demo data is readable and a few accounts repeat.
    private static final List<String> ACCOUNTS   = List.of("ACC-1001", "ACC-1002", "ACC-1003", "ACC-1004", "ACC-1005");
    private static final List<String> CHANNELS   = List.of("POS", "ATM", "ONLINE", "TRANSFER");
    private static final List<String> COUNTRIES  = List.of("RO", "DE", "FR", "US", "GB");
    private static final List<String> CATEGORIES = List.of("GROCERIES", "ELECTRONICS", "TRAVEL", "FUEL", "RESTAURANT");

    private final KafkaTemplate<String, Transaction> kafka;

    public TransactionGenerator(KafkaTemplate<String, Transaction> kafka) {
        this.kafka = kafka;
    }

    /** Produces one transaction per interval (default 1s; override with fraud.generator.interval-ms). */
    @Scheduled(fixedRateString = "${fraud.generator.interval-ms:1000}")
    public void emit() {
        Transaction tx = randomTransaction();
        // send(topic, KEY, VALUE) -> the key (accountId) decides the partition.
        kafka.send(KafkaTopicConfig.TRANSACTIONS, tx.accountId(), tx);
        log.info("produced -> {} {} {} {}", tx.accountId(), tx.amount(), tx.currency(), tx.channel());
    }

    private Transaction randomTransaction() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        // ~1 in 12 transactions is a suspicious "big-ticket" amount (above the high-amount
        // threshold) so the HighAmountRule has something to fire on. Scripted fraud scenarios
        // (velocity, impossible travel) come in later phases.
        boolean bigTicket = r.nextInt(12) == 0;
        BigDecimal amount = bigTicket
                ? BigDecimal.valueOf(r.nextDouble(15_000.0, 30_000.0)).setScale(2, RoundingMode.HALF_UP)
                : BigDecimal.valueOf(r.nextDouble(1.0, 500.0)).setScale(2, RoundingMode.HALF_UP);
        return new Transaction(
                UUID.randomUUID().toString(),
                pick(ACCOUNTS),
                "CARD-" + r.nextInt(1, 100),
                amount,
                "EUR",
                Instant.now(),
                pick(CHANNELS),
                "MERCH-" + r.nextInt(1, 60),
                pick(CATEGORIES),
                pick(COUNTRIES),
                "city-" + r.nextInt(1, 20),
                r.nextDouble(-90.0, 90.0),
                r.nextDouble(-180.0, 180.0)
        );
    }

    private static String pick(List<String> options) {
        return options.get(ThreadLocalRandom.current().nextInt(options.size()));
    }
}