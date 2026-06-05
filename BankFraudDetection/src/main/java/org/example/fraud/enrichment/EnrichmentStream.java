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
     */
    @Autowired
    public void buildPipeline(StreamsBuilder builder) {
        // Serdes = how each topic's bytes turn into objects and back. Key is always a String.
        // ignoreTypeHeaders() => always deserialize to THIS class, regardless of any __TypeId__
        // header on the record (robust, and no trusted-packages config needed for the streams side).
        Serde<String> keySerde = Serdes.String();
        Serde<Transaction> txSerde = new JsonSerde<>(Transaction.class).ignoreTypeHeaders();
        Serde<Account> accountSerde = new JsonSerde<>(Account.class).ignoreTypeHeaders();
        Serde<BlacklistEntry> blacklistSerde = new JsonSerde<>(BlacklistEntry.class).ignoreTypeHeaders();
        Serde<EnrichedTransaction> enrichedSerde = new JsonSerde<>(EnrichedTransaction.class);

        // TABLE side #1: the compacted `accounts` topic as a GlobalKTable. "Global" = every app
        // instance holds the WHOLE table (all partitions), so the join needs no co-partitioning and
        // we can look up by any key. Perfect for small reference data.
        GlobalKTable<String, Account> accounts = builder.globalTable(
                KafkaTopicConfig.ACCOUNTS,
                Consumed.with(keySerde, accountSerde));

        // TABLE side #2: the compacted `blacklist` topic, keyed by entityId (a merchantId OR a country).
        GlobalKTable<String, BlacklistEntry> blacklist = builder.globalTable(
                KafkaTopicConfig.BLACKLIST,
                Consumed.with(keySerde, blacklistSerde));

        // STREAM side: the live transactions.
        KStream<String, Transaction> transactions = builder.stream(
                KafkaTopicConfig.TRANSACTIONS,
                Consumed.with(keySerde, txSerde));

        // The pipeline = three stream-table joins, each leftJoin so a "no match" never drops the
        // transaction (it just passes through with the flag unset):
        //   1) join accounts  by accountId            -> attach the account + homeCountryMismatch
        //   2) join blacklist by merchantId           -> set blacklisted if the merchant is listed
        //   3) join blacklist by country              -> set blacklisted if the country  is listed
        // We look the blacklist table up TWICE because a transaction can be blacklisted by either field.
        transactions
                .leftJoin(accounts,
                        (key, tx) -> tx.accountId(),
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

    /** First join result: attach the account and precompute the home-country mismatch (blacklist still unknown). */
    private static EnrichedTransaction enrichWithAccount(Transaction tx, Account account) {
        boolean homeCountryMismatch =
                account != null && !account.homeCountry().equalsIgnoreCase(tx.country());
        return new EnrichedTransaction(tx, account, homeCountryMismatch, false, null);
    }

    /**
     * Blacklist join result: if this lookup matched an entry, mark the transaction blacklisted and
     * record why. Called twice (merchant, then country); a null entry means "no match on this field",
     * so we return the record unchanged. If both fields hit, the reasons are concatenated.
     */
    private static EnrichedTransaction applyBlacklist(EnrichedTransaction enriched, BlacklistEntry entry) {
        if (entry == null) {
            return enriched;   // no blacklist match on this field -> leave the record as-is
        }
        String hit = entry.type().toLowerCase() + " " + entry.entityId() + " (" + entry.reason() + ")";
        String reason = enriched.blacklistReason() == null ? hit : enriched.blacklistReason() + "; " + hit;
        return new EnrichedTransaction(
                enriched.transaction(), enriched.account(), enriched.homeCountryMismatch(), true, reason);
    }
}