package com.bridge.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;

@Service("messageListener")
@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@RequiredArgsConstructor
@Slf4j
public class MessageSubscriber implements MessageListener {

    @Qualifier("bufferContainer")
    private final List<BlockingQueue<String>> bufferContainer;
    private final BlockingQueue<String> buffer;
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    @PostConstruct
    public void init() {
        bufferContainer.add(buffer);
    }

    @Value("spring.application.name")
    private String appName;

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            String body = new String(message.getBody(), StandardCharsets.UTF_8);
            JsonNode node = objectMapper.readTree(body);
            String messageId = node.get("message_id").asText();

            String lockKey = "bridge:lock:" + messageId;
            Boolean acquired = redisTemplate.opsForValue().setIfAbsent(
                    lockKey,
                    appName,
                    java.time.Duration.ofSeconds(15)
            );

            if (Boolean.TRUE.equals(acquired)) {
                boolean offered = buffer.offer(body);
                if (!offered) {
                    log.warn("Buffer full! Dropping message with ID: {}", messageId);
                }
            } else {
                log.debug("Lock exists for message_id {}, skipping", messageId);
            }
        } catch (Exception e) {
            log.warn("Failed to process incoming message: {}", e.getMessage());
        }
    }
}
