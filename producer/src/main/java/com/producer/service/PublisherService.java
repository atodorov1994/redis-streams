package com.producer.service;

import com.producer.publisher.RedisMessagePublisher;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Service
@RequiredArgsConstructor
@Slf4j
public class PublisherService {

    private final RedisMessagePublisher publisher;

    @Value("${redis.batch-size}")
    private int batchSize;

    @Value("${redis.throttle-min-ms}")
    private int throttleMinMs;

    @Value("${redis.throttle-max-ms}")
    private int throttleMaxMs;

    @Value("${redis.publish-duration-sec}")
    private int publishDurationSec;

    @PostConstruct
    public void startPublishingMessages() throws InterruptedException {

        Duration targetDuration = Duration.ofSeconds(publishDurationSec);

        Instant start = Instant.now();
        int totalMessages = 0;

        while (Duration.between(start, Instant.now()).compareTo(targetDuration) < 0) {

            List<String> messages = new ArrayList<>();

            for (int i = 0; i < batchSize; i++) {
                messages.add(String.format("{\"message_id\":\"%s\"}", UUID.randomUUID()));
            }

            publisher.publishBatch(messages);

            totalMessages += batchSize;

            long sleepMillis = ThreadLocalRandom.current().nextLong(throttleMinMs, throttleMaxMs);
            Thread.sleep(sleepMillis);
        }

        log.info("Total messages published: {}", totalMessages);
    }
}
