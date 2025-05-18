package com.consumer.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
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

import static com.consumer.config.Constants.CONSUMER_BEAN_NAME;

@Service(CONSUMER_BEAN_NAME)
@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@RequiredArgsConstructor
@Slf4j
public class StreamConsumer implements StreamListener<String, ObjectRecord<String,String>> {

    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    @Value("${redis.output.stream.key}")
    private String outputStreamKey;

    @Getter
    private final String consumerId = UUID.randomUUID().toString();

    @Override
    public void onMessage(ObjectRecord<String, String> message) {
        Map<String, String> valueMap;
        try {
            valueMap = objectMapper.readValue(message.getValue(), Map.class);
        } catch (Exception e) {
            log.warn("Error processing message", e);
            return;
        }

        valueMap.put("consumerId", consumerId);

        redisTemplate.opsForStream().add(StreamRecords.mapBacked(valueMap).withStreamKey(outputStreamKey));
    }
}
