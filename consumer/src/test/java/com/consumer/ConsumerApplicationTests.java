package com.consumer;

import com.consumer.util.RedisContainerFactory;
import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.*;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("test")
class ConsumerApplicationTests {

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private RedisConnectionFactory connectionFactory;

    @Value("${redis.stream.key}")
    private String streamKey;

    @Value("${redis.output.stream.key}")
    private String processedStreamKey;

    @Value("${redis.consumer-group.size}")
    private int consumerGroupSize;

    private Set<String> allConsumers = new HashSet<>();
    private final Map<String, List<String>> messageConsumerMap = new HashMap<>();

    private static final RedisContainer redisContainer = RedisContainerFactory.create();
    private static final String CONSUMER_NAME = "someConsumer";

    static {
        redisContainer.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redisContainer::getHost);
        registry.add("spring.data.redis.port", () -> redisContainer.getMappedPort(6379));
    }

    @Test
    void testProcessedMessages_noDuplicates_alConsumersProcessing() {
        var msgCount = 15;
        ensureStreamAndGroup();
        addMessagesToStream(msgCount);
        getListener().start();

        await()
                .atMost(Duration.ofSeconds(10))
                .until(() -> redisTemplate.opsForStream().size(processedStreamKey) >= msgCount);

        // Assert all consumers processed messages
        assertEquals(consumerGroupSize, allConsumers.size());
        // Assert no duplicates
        assertTrue(messageConsumerMap.values().stream().noneMatch(consumers -> consumers.size() > 1));
    }

    private void addMessagesToStream(int count) {
        for (int i = 0; i <= count; i++) {
            Map<String, String> message = new HashMap<>();
            message.put("message_id", String.format("{\"message_id\":\"%s\"}", UUID.randomUUID()));

            redisTemplate.opsForStream().add(
                    StreamRecords.string(message)
                            .withStreamKey(streamKey)
                            .withId(RecordId.autoGenerate())
            );
        }
    }

    private StreamMessageListenerContainer<String, MapRecord<String, String, String>> getListener() {

        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
                StreamMessageListenerContainer.StreamMessageListenerContainerOptions.builder()
                        .pollTimeout(Duration.ofMillis(100))
                        .build();

        StreamMessageListenerContainer<String, MapRecord<String, String, String>> listenerContainer =
                StreamMessageListenerContainer.create(connectionFactory, options);

        listenerContainer.receiveAutoAck(
                Consumer.from(CONSUMER_NAME, UUID.randomUUID().toString()),
                StreamOffset.create(processedStreamKey, ReadOffset.lastConsumed()),
                record -> {
                    try {
                        Map<String, String> message = record.getValue();
                        var msgId = message.get("message_id");
                        var consumerId = message.get("consumerId");
                        if (Objects.nonNull(msgId) && Objects.nonNull(consumerId)) {
                            allConsumers.add(consumerId);
                            messageConsumerMap.computeIfAbsent(msgId, s -> new ArrayList<>()).add(consumerId);
                        }
                    } catch (Exception e) {
                        System.out.println(e);
                    }
                }
        );

        return listenerContainer;
    }

    public void ensureStreamAndGroup() {
        try {
            redisTemplate.opsForStream().add(processedStreamKey, Map.of("init", "true"));
            redisTemplate.opsForStream().createGroup(processedStreamKey, ReadOffset.from("0"), CONSUMER_NAME);
        } catch (RedisSystemException e) {
            if (!e.getMessage().contains("BUSYGROUP")) {
                System.out.println("Consumer group exists.");
            }
            System.out.println("Error ensuring stream creation" + e);
        }
    }
}
