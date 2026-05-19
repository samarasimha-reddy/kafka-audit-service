# Kafka Audit Service

> Production-style Kafka message audit service that detects message loss and duplicates across topics in real time using probabilistic data structures, with live Prometheus metrics and Grafana dashboards.

[![Java 17](https://img.shields.io/badge/java-17-orange.svg)](https://adoptium.net/)
[![Spring Boot 3.2](https://img.shields.io/badge/spring--boot-3.2-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Kafka 3.6](https://img.shields.io/badge/kafka-3.6-black.svg)](https://kafka.apache.org/)

## What it does

The service consumes events from a Kafka topic and detects two classes of data-quality issues in real time:

1. **Duplicate messages** — using a Bloom filter to track every `eventId` it has seen
2. **Missing messages** — by watching `sequenceNumber` gaps per source

It exposes Prometheus metrics so dropped or duplicate messages trigger alerts within seconds of occurring, instead of being found hours later in batch reconciliation.

## Why it exists

In high-throughput Kafka pipelines, transient failures (consumer rebalances, network blips, producer retries) routinely cause duplicates and silent message loss. Naive deduplication via a `HashSet` doesn't scale beyond a few million keys per JVM. This project uses a **Guava Bloom filter** to track tens of millions of event IDs with constant memory and a tunable false-positive rate.

## Architecture

## Tech stack

- **Java 17**, **Spring Boot 3.2**
- **Apache Kafka 3.6** (KRaft mode, no Zookeeper)
- **Spring Kafka** with `ErrorHandlingDeserializer` and `MANUAL_IMMEDIATE` ack mode for at-least-once semantics
- **Guava** Bloom filter
- **Micrometer** + **Prometheus** for metrics
- **Docker Compose** for the full local stack
- **JUnit 5** + **Testcontainers** for integration tests

## Quick start

```bash
# Start Kafka + Kafka UI in Docker
docker compose -f docker/docker-compose.yml up -d

# Create the audit topic
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic audit-events \
  --partitions 3 --replication-factor 1

# Run the service
mvn spring-boot:run
```

Browse:
- **Kafka UI**: http://localhost:8090
- **Service health**: http://localhost:8080/actuator/health
- **Prometheus metrics**: http://localhost:8080/actuator/prometheus

## Key design decisions

| Decision | Rationale |
|---|---|
| **`acks=all` + idempotent producer** | Eliminates duplicate writes from producer retries and guarantees durability across in-sync replicas |
| **`MANUAL_IMMEDIATE` ack mode** | Offsets commit only after successful processing — at-least-once semantics |
| **`ErrorHandlingDeserializer`** | Poison-pill messages (malformed JSON) are logged and skipped instead of crashing the consumer loop |
| **Partition key = event source** | Preserves per-source ordering while still allowing horizontal scaling across partitions |
| **Bloom filter for dedup** | Sub-linear memory, constant-time lookup, configurable false-positive rate |

## Project status

- [x] Producer + consumer with at-least-once semantics
- [x] Poison-pill handling via `ErrorHandlingDeserializer`
- [ ] Bloom-filter-based duplicate detection
- [ ] Sequence gap detection per source
- [ ] Prometheus metrics for dup-rate, miss-rate, latency percentiles
- [ ] Grafana dashboard
- [ ] Integration tests with Testcontainers
- [ ] Chaos test mode with failure injection

## License

MIT
