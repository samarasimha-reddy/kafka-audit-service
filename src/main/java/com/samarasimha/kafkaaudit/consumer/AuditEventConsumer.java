package com.samarasimha.kafkaaudit.consumer;

import com.samarasimha.kafkaaudit.model.AuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Listens to the audit-events Kafka topic.
 *
 * For now this just logs each event. In Session 2 we'll wire in the
 * Bloom-filter-based duplicate detection and sequence-gap detection.
 */
@Service
public class AuditEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuditEventConsumer.class);

    @KafkaListener(
            topics = "${kafka.topics.audit-events:audit-events}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(
            @Payload AuditEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        try {
            log.info("Consumed event {} from partition {} offset {} (source={}, seq={})",
                    event.getEventId(), partition, offset,
                    event.getSource(), event.getSequenceNumber());

            // TODO Session 2: Bloom filter dup check + sequence gap detection

            ack.acknowledge();  // commit offset only on successful processing
        } catch (Exception e) {
            log.error("Failed to process event {}: {}", event.getEventId(), e.getMessage(), e);
            // Don't ack — Kafka will redeliver on next poll
        }
    }
}
