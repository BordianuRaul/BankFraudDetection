package org.example.fraud.generator;

import org.example.fraud.config.KafkaTopicConfig;
import org.example.fraud.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
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
 *
 * ~1 tick in `burstEveryNTicks` it instead fires a BURST of `burstSize` transactions for a
 * single account, all within one tick -> that account crosses the velocity window's count
 * threshold, giving the velocity rule an "abnormal" pattern to detect (DESIGN §11 scenario).
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
    private final int burstEveryNTicks;   // ~1-in-N chance per tick to fire a burst instead of one tx
    private final int burstSize;          // how many transactions a burst sends for one account

    public TransactionGenerator(KafkaTemplate<String, Transaction> kafka,
                                @Value("${fraud.generator.burst.every-n-ticks:20}") int burstEveryNTicks,
                                @Value("${fraud.generator.burst.size:10}") int burstSize) {
        this.kafka = kafka;
        this.burstEveryNTicks = burstEveryNTicks;
        this.burstSize = burstSize;
    }

    /** Each tick: usually one transaction, but ~1 in N ticks a whole burst (the velocity scenario). */
    @Scheduled(fixedRateString = "${fraud.generator.interval-ms:1000}")
    public void emit() {
        if (ThreadLocalRandom.current().nextInt(burstEveryNTicks) == 0) {
            emitBurst();
        } else {
            send(randomTransaction(pick(ACCOUNTS)));
        }
    }

    /** Fire `burstSize` transactions for ONE account within this tick -> trips the velocity window. */
    private void emitBurst() {
        String account = pick(ACCOUNTS);
        for (int i = 0; i < burstSize; i++) {
            send(randomTransaction(account));
        }
        log.warn("BURST -> {} x{} (velocity scenario)", account, burstSize);
    }

    private void send(Transaction tx) {
        // send(topic, KEY, VALUE) -> the key (accountId) decides the partition.
        kafka.send(KafkaTopicConfig.TRANSACTIONS, tx.accountId(), tx);
        log.info("produced -> {} {} {} {}", tx.accountId(), tx.amount(), tx.currency(), tx.channel());
    }

    /** Builds one random transaction for the given account (account fixed so a burst hits one lane). */
    private Transaction randomTransaction(String accountId) {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        // ~1 in 12 transactions is a suspicious "big-ticket" amount (above the high-amount
        // threshold) so the HighAmountRule has something to fire on.
        boolean bigTicket = r.nextInt(12) == 0;
        BigDecimal amount = bigTicket
                ? BigDecimal.valueOf(r.nextDouble(15_000.0, 30_000.0)).setScale(2, RoundingMode.HALF_UP)
                : BigDecimal.valueOf(r.nextDouble(1.0, 500.0)).setScale(2, RoundingMode.HALF_UP);
        return new Transaction(
                UUID.randomUUID().toString(),
                accountId,
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
