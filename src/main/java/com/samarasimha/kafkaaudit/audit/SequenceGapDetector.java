package com.samarasimha.kafkaaudit.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Detects missing events by tracking the last sequence number observed
 * per source, and reporting any gap when the next sequence arrives.
 *
 * Out-of-order arrivals are handled: if a smaller seqNum arrives after
 * a larger one (e.g. due to partition rebalance + replay), we treat it
 * as a duplicate or late delivery, not a gap.
 *
 * Per-source state lives in a ConcurrentHashMap; we use the merge() and
 * compute() primitives so increments are atomic without external locking.
 */
@Service
public class SequenceGapDetector {

    private static final Logger log = LoggerFactory.getLogger(SequenceGapDetector.class);

    /** source -> highest seqNum observed so far */
    private final ConcurrentMap<String, Long> lastSeenBySource = new ConcurrentHashMap<>();

    private final AtomicLong totalMissing = new AtomicLong();

    /**
     * Returns the gap size (number of missing events) detected by this arrival.
     * Returns 0 if there is no gap (in-order arrival or late/duplicate).
     */
    public long checkSequence(String source, long sequenceNumber) {
        long[] gap = new long[1];

        lastSeenBySource.compute(source, (key, lastSeen) -> {
            if (lastSeen == null) {
                // first event from this source - no gap by definition
                return sequenceNumber;
            }
            if (sequenceNumber <= lastSeen) {
                // late or duplicate delivery - leave state unchanged
                return lastSeen;
            }
            long missing = sequenceNumber - lastSeen - 1;
            if (missing > 0) {
                gap[0] = missing;
                log.warn("Sequence gap detected: source={} expected={} got={} missing={}",
                        source, lastSeen + 1, sequenceNumber, missing);
            }
            return sequenceNumber;
        });

        if (gap[0] > 0) {
            totalMissing.addAndGet(gap[0]);
        }
        return gap[0];
    }

    public long getTotalMissing() {
        return totalMissing.get();
    }

    public int getTrackedSources() {
        return lastSeenBySource.size();
    }
}
