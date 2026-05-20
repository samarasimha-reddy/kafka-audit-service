package com.samarasimha.kafkaaudit.web;

import com.samarasimha.kafkaaudit.audit.DuplicateDetector;
import com.samarasimha.kafkaaudit.audit.SequenceGapDetector;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Read-only HTTP endpoints for inspecting the live audit state.
 *
 * Examples:
 *   curl http://localhost:8080/audit/stats
 *   curl http://localhost:8080/audit/stats/bloom
 */
@RestController
@RequestMapping("/audit")
public class AuditStatsController {

    private final DuplicateDetector duplicateDetector;
    private final SequenceGapDetector gapDetector;

    public AuditStatsController(DuplicateDetector duplicateDetector,
                                SequenceGapDetector gapDetector) {
        this.duplicateDetector = duplicateDetector;
        this.gapDetector = gapDetector;
    }

    @GetMapping("/stats")
    public Map<String, Object> overallStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("totalEventsChecked", duplicateDetector.getTotalChecks());
        stats.put("duplicatesDetected", duplicateDetector.getDuplicatesDetected());
        stats.put("missingEvents", gapDetector.getTotalMissing());
        stats.put("trackedSources", gapDetector.getTrackedSources());
        return stats;
    }

    @GetMapping("/stats/bloom")
    public Map<String, Object> bloomFilterStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("totalChecks", duplicateDetector.getTotalChecks());
        stats.put("duplicatesDetected", duplicateDetector.getDuplicatesDetected());
        stats.put("currentExpectedFpp", duplicateDetector.getExpectedFpp());
        return stats;
    }
}
