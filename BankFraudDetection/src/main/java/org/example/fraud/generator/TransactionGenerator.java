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


@Component
public class TransactionGenerator {

    private static final Logger log = LoggerFactory.getLogger(TransactionGenerator.class);

    private static final List<String> ACCOUNTS   = List.of("ACC-1001", "ACC-1002", "ACC-1003", "ACC-1004", "ACC-1005");
    private static final List<String> CHANNELS   = List.of("POS", "ATM", "ONLINE", "TRANSFER");
    private static final List<String> COUNTRIES  = List.of("RO", "DE", "FR", "US", "GB");
    private static final List<String> CATEGORIES = List.of("GROCERIES", "ELECTRONICS", "TRAVEL", "FUEL", "RESTAURANT");

    private final KafkaTemplate<String, Transaction> kafka;
    private final int burstEveryNTicks;
    private final int burstSize;

    public TransactionGenerator(KafkaTemplate<String, Transaction> kafka,
                                @Value("${fraud.generator.burst.every-n-ticks:20}") int burstEveryNTicks,
                                @Value("${fraud.generator.burst.size:10}") int burstSize) {
        this.kafka = kafka;
        this.burstEveryNTicks = burstEveryNTicks;
        this.burstSize = burstSize;
    }

    @Scheduled(fixedRateString = "${fraud.generator.interval-ms:1000}")
    public void emit() {
        if (ThreadLocalRandom.current().nextInt(burstEveryNTicks) == 0) {
            emitBurst();
        } else {
            send(randomTransaction(pick(ACCOUNTS)));
        }
    }

    private void emitBurst() {
        String account = pick(ACCOUNTS);
        for (int i = 0; i < burstSize; i++) {
            send(randomTransaction(account));
        }
        log.warn("BURST -> {} x{} (velocity scenario)", account, burstSize);
    }

    private void send(Transaction transaction) {
        kafka.send(KafkaTopicConfig.TRANSACTIONS, transaction.accountId(), transaction);
        log.info("produced -> {} {} {} {}", transaction.accountId(), transaction.amount(), transaction.currency(), transaction.channel());
    }

    private Transaction randomTransaction(String accountId) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        boolean bigTicket = random.nextInt(12) == 0;
        BigDecimal amount = bigTicket
                ? BigDecimal.valueOf(random.nextDouble(15_000.0, 30_000.0)).setScale(2, RoundingMode.HALF_UP)
                : BigDecimal.valueOf(random.nextDouble(1.0, 500.0)).setScale(2, RoundingMode.HALF_UP);

        return createTransaction(accountId, random, amount);
    }

    private Transaction createTransaction(String accountId, ThreadLocalRandom random, BigDecimal amount) {
        return new Transaction(
                UUID.randomUUID().toString(),
                accountId,
                "CARD-" + random.nextInt(1, 100),
                amount,
                "EUR",
                Instant.now(),
                pick(CHANNELS),
                "MERCH-" + random.nextInt(1, 60),
                pick(CATEGORIES),
                pick(COUNTRIES),
                "city-" + random.nextInt(1, 20),
                random.nextDouble(-90.0, 90.0),
                random.nextDouble(-180.0, 180.0)
        );
    }

    private String pick(List<String> options) {
        return options.get(ThreadLocalRandom.current().nextInt(options.size()));
    }
}
