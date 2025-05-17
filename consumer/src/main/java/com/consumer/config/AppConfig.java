package com.consumer.config;

import com.consumer.listener.StreamConsumer;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
@RequiredArgsConstructor
public class AppConfig {

    @Value("${redis.stream.key}")
    private String messageTopicName;

    @Value("${redis.consumer-group.id}")
    private String consumerGroupName;

    private final RedisConnectionFactory connectionFactory;
    private final RedisTemplate<String, String> redisTemplate;

    public record ConsumerSubscription(String id, Subscription subscription) {}

    @PostConstruct
    public void ensureStreamAndGroup() {
        try {
            redisTemplate.opsForStream().add(messageTopicName, Map.of("init", "true"));
            redisTemplate.opsForStream().createGroup(messageTopicName, ReadOffset.from("0"), consumerGroupName);
        } catch (RedisSystemException e) {
            if (!e.getMessage().contains("BUSYGROUP")) {
                System.out.println("Consumer group exists.");
            }
        }
    }

    @Bean
    ExecutorService executorService() {
        var numberOfThreads = Runtime.getRuntime().availableProcessors() * 2;
        return Executors.newFixedThreadPool(numberOfThreads);
    }

    @Bean
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public ConsumerSubscription streamMessageSubscription(StreamConsumer streamListener) {

        var options = StreamMessageListenerContainer
                .StreamMessageListenerContainerOptions.builder()
                .pollTimeout(Duration.ofMillis(100))
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
