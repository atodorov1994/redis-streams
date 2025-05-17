package com.consumer.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;

@Service("streamMessageConsumer")
@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@RequiredArgsConstructor
@Slf4j
public class StreamConsumer implements StreamListener<String, ObjectRecord<String,String>> {

    @Value("${redis.output.stream.key}")
    private String outputStreamKey;

    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public void onMessage(ObjectRecord<String, String> message) {
        Map<String, String> valueMap;
        try {
            valueMap = objectMapper.readValue(message.getValue(), Map.class);
        } catch (Exception e) {
            log.warn("Error processing message", e);
            return;
        }

        valueMap.put("randomId", UUID.randomUUID().toString());

        redisTemplate.opsForStream().add(StreamRecords.mapBacked(valueMap).withStreamKey(outputStreamKey));
    }
}
