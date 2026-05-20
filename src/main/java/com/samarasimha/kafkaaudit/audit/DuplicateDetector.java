package com.samarasimha.kafkaaudit.audit;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Detects duplicate events using a Bloom filter over eventId.
 *
 * Memory footprint for 10M expected events at 1% FPR is ~12 MB
 * (vs ~500 MB for an equivalent HashSet<String>).
 *
 * Thread-safe: Guava's BloomFilter.put / mightContain are atomic at the
 * bit-array level and safe for concurrent use across consumer threads.
 *
 * Note: false positives are possible (configurable rate). To distinguish
 * a real duplicate from a Bloom false positive in production you'd back
 * this with a small LRU exact set or a Redis SET; for this audit service
 * the Bloom answer is good enough since alerts trigger only on *rate*
 * exceeding a threshold.
 */
@Service
public class DuplicateDetector {

    private static final Logger log = LoggerFactory.getLogger(DuplicateDetector.class);

    @Value("${audit.bloom-filter.expected-insertions:10000000}")
    private int expectedInsertions;

    @Value("${audit.bloom-filter.false-positive-rate:0.01}")
    private double falsePositiveRate;

    private BloomFilter<String> seenEventIds;
    private final AtomicLong totalChecks = new AtomicLong();
    private final AtomicLong duplicatesDetected = new AtomicLong();

    @PostConstruct
    public void init() {
        this.seenEventIds = BloomFilter.create(
                Funnels.stringFunnel(StandardCharsets.UTF_8),
                expectedInsertions,
                falsePositiveRate);
        log.info("Initialized Bloom filter: expectedInsertions={}, fpr={}",
                expectedInsertions, falsePositiveRate);
    }

    /**
     * Records an eventId and reports whether it was likely seen before.
     *
     * @return true if the event is suspected to be a duplicate
     */
    public boolean checkAndRecord(String eventId) {
        totalChecks.incrementAndGet();
        boolean possibleDuplicate = seenEventIds.mightContain(eventId);
        if (possibleDuplicate) {
            duplicatesDetected.incrementAndGet();
            log.warn("Possible duplicate detected: eventId={}", eventId);
            return true;
        }
        seenEventIds.put(eventId);
        return false;
    }

    public long getTotalChecks() {
        return totalChecks.get();
    }

    public long getDuplicatesDetected() {
        return duplicatesDetected.get();
    }

    /** Approximate fill percentage; useful for alerting before FPR degrades. */
    public double getExpectedFpp() {
        return seenEventIds.expectedFpp();
    }
}
