package com.samarasimha.kafkaaudit.web;

import com.samarasimha.kafkaaudit.model.AuditEvent;
import com.samarasimha.kafkaaudit.producer.AuditEventProducer;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Manual event-injection endpoints for demos and screenshots.
 * Useful for generating live traffic against the Grafana dashboard.
 *
 *   curl -X POST 'http://localhost:8080/inject/burst?count=100'
 *   curl -X POST 'http://localhost:8080/inject/with-dups?count=50&dupRate=0.1'
 */
@RestController
@RequestMapping("/inject")
public class EventInjectionController {

    private final AuditEventProducer producer;

    public EventInjectionController(AuditEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/burst")
    public Map<String, Object> burst(@RequestParam(defaultValue = "100") int count,
                                     @RequestParam(defaultValue = "demo-source") String source) {
        long seq = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            producer.publish(new AuditEvent(
                    UUID.randomUUID().toString(),
                    source,
                    seq + i,
                    "burst-" + i,
                    Instant.now()
            ));
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("published", count);
        resp.put("source", source);
        return resp;
    }

    @PostMapping("/with-dups")
    public Map<String, Object> withDups(@RequestParam(defaultValue = "50") int count,
                                        @RequestParam(defaultValue = "0.1") double dupRate) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        long seq = System.currentTimeMillis();
        AuditEvent[] sent = new AuditEvent[count];
        int dups = 0;

        for (int i = 0; i < count; i++) {
            AuditEvent ev = new AuditEvent(
                    UUID.randomUUID().toString(),
                    "demo-source",
                    seq + i,
                    "test-" + i,
                    Instant.now()
            );
            sent[i] = ev;
            producer.publish(ev);

            // Possibly resend a previously-sent event to create a duplicate
            if (i > 0 && rng.nextDouble() < dupRate) {
                AuditEvent dup = sent[rng.nextInt(i)];
                producer.publish(dup);
                dups++;
            }
        }

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("publishedNew", count);
        resp.put("publishedDuplicates", dups);
        return resp;
    }
}
