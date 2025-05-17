package com.consumer.metrics;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class MetricsService {

    private final RedisTemplate<String, String> redisTemplate;

    @Value("${redis.output.stream.key}")
    private String processedStreamKey;

    private long lastCount = 0;

    @Scheduled(fixedRate = 3000)
    public void reportThroughput() {
        Long currentCount = redisTemplate.opsForStream().size(processedStreamKey);

        if (currentCount == null) {
            log.warn("Could not read stream length for '{}'", processedStreamKey);
            return;
        }

        long delta = currentCount - lastCount;
        log.info("Processed {} new messages in last 3 seconds (total: {})", delta, currentCount);
        lastCount = currentCount;
    }
}
