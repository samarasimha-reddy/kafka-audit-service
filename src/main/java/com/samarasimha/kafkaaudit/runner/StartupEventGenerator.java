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
 * Sends a handful of synthetic audit events on application startup,
 * to prove the producer/consumer wiring works end-to-end.
 *
 * Remove or disable for production; this is for the local demo only.
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
        log.info(">>> Generating 5 startup audit events...");

        for (int i = 1; i <= 5; i++) {
            AuditEvent event = new AuditEvent(
                    UUID.randomUUID().toString(),
                    "catalog-service",
                    i,
                    "SKU update #" + i,
                    Instant.now()
            );
            producer.publish(event);
        }

        log.info(">>> Done generating startup events.");
    }
}
