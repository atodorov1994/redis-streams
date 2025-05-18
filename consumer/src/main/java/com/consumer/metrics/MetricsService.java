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

@Service
@RequiredArgsConstructor
@Slf4j
public class MetricsService {

    private final RedisTemplate<String, String> redisTemplate;

    private final List<Long> processed = new ArrayList<>();

    @Value("${redis.output.stream.key}")
    private String processedStreamKey;

    private long lastCount = 0;

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

    @PreDestroy
    public void calculateAvrg() {
        var avrg = processed.stream().reduce(0L, Long::sum) / processed.size();
        log.info("Average: {}", avrg);
    }
}
