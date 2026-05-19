package com.samarasimha.kafkaaudit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Kafka Audit Service - entry point.
 *
 * Production-style Kafka message audit service that detects message loss
 * and duplicates across topics in real time using Bloom filters and
 * Count-Min Sketch, exposing live metrics via Prometheus.
 */
@SpringBootApplication
public class KafkaAuditApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaAuditApplication.class, args);
    }
}
