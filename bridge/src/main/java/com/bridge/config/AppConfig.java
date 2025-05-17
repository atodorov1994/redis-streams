package com.bridge.config;

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
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

@Configuration
@RequiredArgsConstructor
public class AppConfig {

    @Value("${redis.topic}")
    private String messageTopicName;

    @Value("${redis.consumer-group.id}")
    private String consumerGroupName;

    private final RedisConnectionFactory connectionFactory;
    private final RedisTemplate<String, String> redisTemplate;

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
    RedisMessageListenerContainer redisMessageListenerContainer() {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setTaskExecutor(executorService());
        return container;
    }

    @Bean
    ChannelTopic messageTopic() {
        return new ChannelTopic(messageTopicName);
    }

    @Bean
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    BlockingQueue<String> buffer() {
        return new LinkedBlockingQueue<>(10000);
    }

    @Bean("bufferContainer")
    List<BlockingQueue<String>> bufferContainer() {
        return new ArrayList<>();
    }

    @Bean
    ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
