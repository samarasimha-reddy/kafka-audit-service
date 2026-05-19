package com.samarasimha.kafkaaudit.producer;

import com.samarasimha.kafkaaudit.model.AuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Thin wrapper around KafkaTemplate that publishes AuditEvents to the
 * configured topic, partitioning by event source for ordering guarantees.
 */
@Service
public class AuditEventProducer {

    private static final Logger log = LoggerFactory.getLogger(AuditEventProducer.class);

    private final KafkaTemplate<String, AuditEvent> kafkaTemplate;
    private final String topic;

    public AuditEventProducer(
            KafkaTemplate<String, AuditEvent> kafkaTemplate,
            @Value("${kafka.topics.audit-events:audit-events}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public CompletableFuture<Void> publish(AuditEvent event) {
        // Use 'source' as the partition key so events from the same source
        // always land on the same partition (preserves per-source ordering).
        return kafkaTemplate.send(topic, event.getSource(), event)
                .thenAccept(result -> {
                    var meta = result.getRecordMetadata();
                    log.info("Published event {} to {}-{} @ offset {}",
                            event.getEventId(), meta.topic(), meta.partition(), meta.offset());
                })
                .exceptionally(ex -> {
                    log.error("Failed to publish event {}: {}", event.getEventId(), ex.getMessage(), ex);
                    return null;
                });
    }
}
