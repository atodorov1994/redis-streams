package com.consumer.metrics;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

import static com.consumer.config.Constants.METRICS_RATE_MS;

/**
 * Tracks and reports throughput metrics based on the Redis output stream size.
 * Logs periodic updates and calculates an average throughput at shutdown.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class MetricsService {

    private final RedisTemplate<String, String> redisTemplate;

    // Stores deltas of processed message counts between polling intervals
    private final List<Long> processed = new ArrayList<>();

    @Value("${redis.output.stream.key}")
    private String processedStreamKey;

    private long lastCount = 0;

    /**
     * Scheduled task to report throughput by polling the size of the output stream.
     * Runs every METRICS_RATE_MS milliseconds.
     */
    @Scheduled(fixedRate = METRICS_RATE_MS)
    public void reportThroughput() {
        Long currentCount = redisTemplate.opsForStream().size(processedStreamKey);

        if (currentCount == null) {
            log.warn("Could not read stream length for '{}'", processedStreamKey);
            return;
        }

        long delta = currentCount - lastCount;
        log.info("Processed {} new messages in last 3 seconds (total: {})", delta, currentCount);
        lastCount = currentCount;

        if (delta != 0) {
            processed.add(delta);
        }
    }

    /**
     * Calculates and logs the average throughput across the lifetime of the service.
     * Called automatically before shutdown.
     */
    @PreDestroy
    public void calculateAvrg() {
        var avrg = processed.stream().reduce(0L, Long::sum) / processed.size();
        log.info("Average: {}", avrg);
    }
}
