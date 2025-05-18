package com.producer.publisher;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class RedisMessagePublisher implements MessagePublisher {

    private final RedisTemplate<String, String> redisTemplate;
    private final ChannelTopic topic;

    /**
     * Publishes a batch of messages to a Redis Pub/Sub channel using pipelining for efficiency.
     * @param messages the list of messages to publish (must not be null)
     */
    @Override
    public void publishBatch(List<String> messages) {
        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            messages.forEach(message ->
                    connection.publish(topic.getTopic().getBytes(), message.getBytes()));
            return null;
        });
    }
}
