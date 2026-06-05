package org.example.fraud.refdata;

import org.example.fraud.config.KafkaTopicConfig;
import org.example.fraud.model.Account;
import org.example.fraud.model.BlacklistEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.example.fraud.config.KafkaTopicConfig.*;

@Component
public class ReferenceDataLoader implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ReferenceDataLoader.class);

    private final KafkaTemplate<String, Object> kafka;

    public ReferenceDataLoader(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    @Override
    public void run(ApplicationArguments args) {
        seedAccounts();
        seedBlacklist();
    }

    /** The 5 accounts the generator uses (ACC-1001..1005), each with a home country & risk profile. */
    private void seedAccounts() {
        List<Account> accounts = List.of(
                new Account("ACC-1001", "CUST-1", "RO", "LOW",    new BigDecimal("2200.00"), Instant.parse("2024-01-10T00:00:00Z")),
                new Account("ACC-1002", "CUST-2", "DE", "MEDIUM", new BigDecimal("3500.00"), Instant.parse("2023-06-01T00:00:00Z")),
                new Account("ACC-1003", "CUST-3", "FR", "LOW",    new BigDecimal("1800.00"), Instant.parse("2024-03-15T00:00:00Z")),
                new Account("ACC-1004", "CUST-4", "US", "HIGH",   new BigDecimal("5000.00"), Instant.parse("2022-11-20T00:00:00Z")),
                new Account("ACC-1005", "CUST-5", "GB", "MEDIUM", new BigDecimal("2700.00"), Instant.parse("2024-05-05T00:00:00Z"))
        );

        accounts.forEach(a -> kafka.send(ACCOUNTS, a.accountId(), a));
        log.info("seeded {} accounts into '{}'", accounts.size(), ACCOUNTS);
    }

    /** A few denylist entries: two merchants (in the generator's MERCH-1..59 range) and one country. */
    private void seedBlacklist() {
        List<BlacklistEntry> entries = List.of(
                new BlacklistEntry("MERCH-13", "MERCHANT", "known fraud ring",   Instant.parse("2026-05-01T00:00:00Z")),
                new BlacklistEntry("MERCH-7",  "MERCHANT", "repeated chargebacks", Instant.parse("2026-05-10T00:00:00Z")),
                new BlacklistEntry("US",       "COUNTRY",  "sanctioned corridor",  Instant.parse("2026-05-15T00:00:00Z"))
        );

        entries.forEach(e -> kafka.send(BLACKLIST, e.entityId(), e));
        log.info("seeded {} blacklist entries into '{}'", entries.size(), BLACKLIST);
    }
}