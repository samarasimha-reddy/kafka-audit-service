package com.samarasimha.kafkaaudit.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable audit event flowing through Kafka.
 *
 * Each event has a unique eventId (UUID) and a sequenceNumber per source.
 * The audit service uses eventId to detect duplicates (via Bloom filter)
 * and sequenceNumber to detect gaps/missing events.
 */
public class AuditEvent {

    private final String eventId;
    private final String source;
    private final long sequenceNumber;
    private final String payload;
    private final Instant timestamp;

    @JsonCreator
    public AuditEvent(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("source") String source,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("payload") String payload,
            @JsonProperty("timestamp") Instant timestamp) {
        this.eventId = Objects.requireNonNull(eventId, "eventId");
        this.source = Objects.requireNonNull(source, "source");
        this.sequenceNumber = sequenceNumber;
        this.payload = payload;
        this.timestamp = Objects.requireNonNull(timestamp, "timestamp");
    }

    public String getEventId() { return eventId; }
    public String getSource() { return source; }
    public long getSequenceNumber() { return sequenceNumber; }
    public String getPayload() { return payload; }
    public Instant getTimestamp() { return timestamp; }

    @Override
    public String toString() {
        return "AuditEvent{" +
                "eventId='" + eventId + '\'' +
                ", source='" + source + '\'' +
                ", seq=" + sequenceNumber +
                ", ts=" + timestamp +
                '}';
    }
}
