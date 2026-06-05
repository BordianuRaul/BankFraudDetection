package org.example.fraud.enrichment;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.example.fraud.config.KafkaTopicConfig;
import org.example.fraud.model.Account;
import org.example.fraud.model.BlacklistEntry;
import org.example.fraud.model.EnrichedTransaction;
import org.example.fraud.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

/**
 * The enrichment topology: join the `transactions` STREAM against the `accounts` and `blacklist` TABLES.
 * This is the Phase-3 headline — "stream meets table". For every transaction that flows by we look up
 * its account (by accountId) and check the blacklist (by merchantId, then country) in GlobalKTables,
 * attach the results, and produce an {@link EnrichedTransaction} on `transactions.enriched`.
 * One transaction in -> one enriched out (a decoration, not an aggregation — theory §1.8).
 */
@Component
public class EnrichmentStream {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentStream.class);

    /**
     * spring-kafka calls this @Autowired method once at startup, handing us the StreamsBuilder
     * created by @EnableKafkaStreams. We describe the topology here; the framework then starts it.
     *
     * Serdes = how each topic's bytes turn into objects and back. Key is always a String.
     */
    @Autowired
    public void buildPipeline(StreamsBuilder builder) {
        Serde<String> keySerde = Serdes.String();
        Serde<Transaction> transactionSerde = new JsonSerde<>(Transaction.class).ignoreTypeHeaders();
        Serde<Account> accountSerde = new JsonSerde<>(Account.class).ignoreTypeHeaders();
        Serde<BlacklistEntry> blacklistSerde = new JsonSerde<>(BlacklistEntry.class).ignoreTypeHeaders();
        Serde<EnrichedTransaction> enrichedSerde = new JsonSerde<>(EnrichedTransaction.class);

        GlobalKTable<String, Account> accounts = builder.globalTable(
                KafkaTopicConfig.ACCOUNTS,
                Consumed.with(keySerde, accountSerde));


        GlobalKTable<String, BlacklistEntry> blacklist = builder.globalTable(
                KafkaTopicConfig.BLACKLIST,
                Consumed.with(keySerde, blacklistSerde));


        KStream<String, Transaction> transactions = builder.stream(
                KafkaTopicConfig.TRANSACTIONS,
                Consumed.with(keySerde, transactionSerde));

        transactions
                .leftJoin(accounts,
                        (key, transaction) -> transaction.accountId(),
                        EnrichmentStream::enrichWithAccount)
                .leftJoin(blacklist,
                        (key, enriched) -> enriched.transaction().merchantId(),
                        EnrichmentStream::applyBlacklist)
                .leftJoin(blacklist,
                        (key, enriched) -> enriched.transaction().country(),
                        EnrichmentStream::applyBlacklist)
                .to(KafkaTopicConfig.TRANSACTIONS_ENRICHED,
                        Produced.with(keySerde, enrichedSerde));

        log.info("enrichment topology wired: {} join {} (accounts) + {} (blacklist x2) -> {}",
                KafkaTopicConfig.TRANSACTIONS, KafkaTopicConfig.ACCOUNTS,
                KafkaTopicConfig.BLACKLIST, KafkaTopicConfig.TRANSACTIONS_ENRICHED);
    }

    private static EnrichedTransaction enrichWithAccount(Transaction transaction, Account account) {
        boolean homeCountryMismatch =
                account != null && !account.homeCountry().equalsIgnoreCase(transaction.country());
        return new EnrichedTransaction(transaction, account, homeCountryMismatch, false, null);
    }

    private static EnrichedTransaction applyBlacklist(EnrichedTransaction enriched, BlacklistEntry entry) {
        if (entry == null) {
            return enriched;
        }
        String hit = entry.type().toLowerCase() + " " + entry.entityId() + " (" + entry.reason() + ")";
        String reason = enriched.blacklistReason() == null ? hit : enriched.blacklistReason() + "; " + hit;
        return new EnrichedTransaction(
                enriched.transaction(), enriched.account(), enriched.homeCountryMismatch(), true, reason);
    }
}