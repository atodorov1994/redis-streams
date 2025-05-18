package com.bridge.config;

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

import static com.bridge.config.Constants.BUFFER_CONTAINER_BEAN_NAME;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class AppConfig {

    private final RedisConnectionFactory connectionFactory;
    private final RedisTemplate<String, String> redisTemplate;

    @Value("${redis.topic}")
    private String messageTopicName;

    @Value("${redis.consumer-group.id}")
    private String consumerGroupName;

    @Value("${redis.batch-buffer-size}")
    private int bufferSize;

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
        return new LinkedBlockingQueue<>(bufferSize);
    }

    @Bean(BUFFER_CONTAINER_BEAN_NAME)
    List<BlockingQueue<String>> bufferContainer() {
        return new ArrayList<>();
    }

    @Bean
    ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
