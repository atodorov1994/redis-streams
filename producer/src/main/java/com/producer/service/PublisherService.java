package com.producer.service;

import com.producer.publisher.RedisMessagePublisher;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Service
@RequiredArgsConstructor
public class PublisherService {

    private final RedisMessagePublisher publisher;

    private final int BATCH_SIZE = 1000;
    private final Duration TARGET_DURATION = Duration.ofMinutes(1);

    @PostConstruct
    public void startPublishingMessages() throws InterruptedException {

        Instant start = Instant.now();
        int totalMessages = 0;

        while (Duration.between(start, Instant.now()).compareTo(TARGET_DURATION) < 0) {

            List<String> messages = new ArrayList<>();

            for (int i = 0; i < BATCH_SIZE; i++) {
                messages.add(String.format("{\"message_id\":\"%s\"}", UUID.randomUUID()));
            }

            publisher.publishBatch(messages);

            totalMessages += BATCH_SIZE;

            // Sleep randomly between 100ms and 500ms
            long sleepMillis = ThreadLocalRandom.current().nextLong(100, 501);
            Thread.sleep(sleepMillis);
        }

        System.out.println("Total messages published: " + totalMessages);
    }
}
