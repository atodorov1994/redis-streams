package com.consumer.config;

import com.consumer.listener.StreamConsumer;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.consumer.config.Constants.SUBSCRIPTION_BEAN_NAME;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class AppConfig {

    private final RedisConnectionFactory connectionFactory;
    private final RedisTemplate<String, String> redisTemplate;

    @Value("${redis.stream.key}")
    private String messageTopicName;

    @Value("${redis.consumer-group.id}")
    private String consumerGroupName;

    @Value("${redis.poll-timeout.ms}")
    private int pollTimeoutMs;

    public record ConsumerSubscription(String id, Subscription subscription) {}

    /**
     * Ensures the Redis stream and its consumer group exist.
     * If the stream does not exist, it will be created with a dummy message.
     * If the consumer group already exists, it skips creation.
     */
    @PostConstruct
    public void ensureStreamAndGroup() {
        try {
            redisTemplate.opsForStream().add(messageTopicName, Map.of("init", "true"));
            redisTemplate.opsForStream().createGroup(messageTopicName, ReadOffset.from("0"), consumerGroupName);
        } catch (RedisSystemException e) {
            if (!e.getMessage().contains("BUSYGROUP")) {
                log.info("Consumer group exists.");
            }
            log.error("Error ensuring stream creation", e);
        }
    }

    @Bean
    ExecutorService executorService() {
        var numberOfThreads = Runtime.getRuntime().availableProcessors() * 2;
        return Executors.newFixedThreadPool(numberOfThreads);
    }

    /**
     * Creates a prototype-scoped bean for a Redis Stream consumer subscription,
     * that is a specific implementation of {@link StreamListener <String,  ObjectRecord <String,String>>}
     * I am using the implementation instead the interface, because i need the consumer id
     * This sets up a StreamMessageListenerContainer with a dedicated consumer instance
     * and returns a handle to manage the subscription.
     *
     * @param streamListener the listener instance for handling messages
     * @return a ConsumerSubscription representing the active stream consumer
     */
    @Bean(SUBSCRIPTION_BEAN_NAME)
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public ConsumerSubscription streamMessageSubscription(StreamConsumer streamListener) {

        var options = StreamMessageListenerContainer
                .StreamMessageListenerContainerOptions.builder()
                .pollTimeout(Duration.ofMillis(pollTimeoutMs))
                .targetType(String.class)
                .executor(executorService())
                .build();

        var listenerContainer = StreamMessageListenerContainer
                .create(connectionFactory, options);

        var subscription = listenerContainer.receiveAutoAck(
                Consumer.from(consumerGroupName, streamListener.getConsumerId()),
                StreamOffset.create(messageTopicName, ReadOffset.lastConsumed()),
                streamListener
        );

        listenerContainer.start();
        return new ConsumerSubscription(streamListener.getConsumerId(), subscription);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
