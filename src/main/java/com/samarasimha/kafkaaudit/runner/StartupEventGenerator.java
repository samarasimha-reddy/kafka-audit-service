package com.samarasimha.kafkaaudit.runner;

import com.samarasimha.kafkaaudit.model.AuditEvent;
import com.samarasimha.kafkaaudit.producer.AuditEventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * Sends a mix of clean, duplicated, and gap-laden events on startup,
 * to demonstrate every detection path of the audit service.
 *
 * Sequence plan for source="catalog-service":
 *  - seq 1, 2, 3, 4    (clean, in order)
 *  - seq 8             (jumps from 4 -> 8, missing 5/6/7)
 *  - one duplicate eventId resent (Bloom filter should flag it)
 */
@Component
public class StartupEventGenerator implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(StartupEventGenerator.class);

    private final AuditEventProducer producer;

    public StartupEventGenerator(AuditEventProducer producer) {
        this.producer = producer;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info(">>> Generating mixed startup audit events (clean + dup + gap)...");

        // 4 clean in-order events
        AuditEvent event1 = build("catalog-service", 1, "SKU update #1");
        AuditEvent event2 = build("catalog-service", 2, "SKU update #2");
        AuditEvent event3 = build("catalog-service", 3, "SKU update #3");
        AuditEvent event4 = build("catalog-service", 4, "SKU update #4");

        producer.publish(event1);
        producer.publish(event2);
        producer.publish(event3);
        producer.publish(event4);

        // Sequence gap: jump from seq=4 to seq=8 (missing 5, 6, 7)
        AuditEvent event8 = build("catalog-service", 8, "SKU update #8 (after gap)");
        producer.publish(event8);

        // Intentional duplicate: send event2 again with same eventId
        producer.publish(event2);

        // Second source to prove per-source tracking
        producer.publish(build("payment-service", 1, "Payment #1"));
        producer.publish(build("payment-service", 2, "Payment #2"));

        log.info(">>> Done generating events. Expected results:");
        log.info(">>>   - 8 events consumed");
        log.info(">>>   - 1 duplicate detected (event2)");
        log.info(">>>   - 3 missing events inferred for catalog-service (seq 5/6/7)");
    }

    private AuditEvent build(String source, long seq, String payload) {
        return new AuditEvent(
                UUID.randomUUID().toString(),
                source,
                seq,
                payload,
                Instant.now()
        );
    }
}
