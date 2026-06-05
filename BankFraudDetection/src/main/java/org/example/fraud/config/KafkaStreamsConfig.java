package org.example.fraud.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * Turns on Kafka Streams for this application.
 *
 * {@code @EnableKafkaStreams} makes spring-kafka build a {@code StreamsBuilder} (and start the
 * stream threads) from the {@code spring.kafka.streams.*} settings in application.yml
 * (application-id "fraud-streams", bootstrap servers). Any bean/method that wants to define a
 * topology can then receive that {@code StreamsBuilder} by injection — see {@code EnrichmentStream}.
 *
 * This is a SECOND Kafka API alongside the {@code @KafkaListener} consumers we already use:
 *  - {@code @KafkaListener} = consume records one by one (good for simple, stateless reactions).
 *  - Kafka Streams        = a declarative topology (joins, windows, state) — needed for the join here
 *                           and for the stateful rules in Phase 4.
 */
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {
}