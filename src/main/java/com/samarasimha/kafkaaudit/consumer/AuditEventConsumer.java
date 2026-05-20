package com.samarasimha.kafkaaudit.consumer;

import com.samarasimha.kafkaaudit.audit.DuplicateDetector;
import com.samarasimha.kafkaaudit.audit.SequenceGapDetector;
import com.samarasimha.kafkaaudit.model.AuditEvent;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * Consumes AuditEvents and runs them through duplicate and gap detection.
 * Emits Prometheus metrics for every observation:
 *
 *  - audit.events.consumed        (counter, tagged by source + partition)
 *  - audit.duplicates.detected    (counter, tagged by source)
 *  - audit.missing.events         (counter, tagged by source)
 *  - audit.processing.duration    (timer, p50/p95/p99 latency histogram)
 */
@Service
public class AuditEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuditEventConsumer.class);

    private final DuplicateDetector duplicateDetector;
    private final SequenceGapDetector gapDetector;
    private final MeterRegistry meterRegistry;
    private final Timer processingTimer;

    public AuditEventConsumer(DuplicateDetector duplicateDetector,
                              SequenceGapDetector gapDetector,
                              MeterRegistry meterRegistry) {
        this.duplicateDetector = duplicateDetector;
        this.gapDetector = gapDetector;
        this.meterRegistry = meterRegistry;
        this.processingTimer = Timer.builder("audit.processing.duration")
                .description("Time to fully audit a single Kafka event")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);
    }

    @KafkaListener(
            topics = "${kafka.topics.audit-events:audit-events}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(
            @Payload AuditEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        long startNanos = System.nanoTime();
        try {
            // 1. duplicate detection
            boolean dup = duplicateDetector.checkAndRecord(event.getEventId());

            // 2. sequence-gap detection
            long missing = gapDetector.checkSequence(event.getSource(), event.getSequenceNumber());

            // 3. metrics
            Counter.builder("audit.events.consumed")
                    .description("Total events consumed and audited")
                    .tag("source", event.getSource())
                    .tag("partition", String.valueOf(partition))
                    .register(meterRegistry)
                    .increment();

            if (dup) {
                Counter.builder("audit.duplicates.detected")
                        .description("Total duplicate events detected")
                        .tag("source", event.getSource())
                        .register(meterRegistry)
                        .increment();
            }

            if (missing > 0) {
                Counter.builder("audit.missing.events")
                        .description("Total missing events inferred from sequence gaps")
                        .tag("source", event.getSource())
                        .register(meterRegistry)
                        .increment(missing);
            }

            log.info("Audited event {} (partition={} offset={} source={} seq={} dup={} missing={})",
                    event.getEventId(), partition, offset,
                    event.getSource(), event.getSequenceNumber(), dup, missing);

            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to audit event {}: {}", event.getEventId(), e.getMessage(), e);
            // Don't ack -> Kafka redelivers
        } finally {
            processingTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
        }
    }
}
