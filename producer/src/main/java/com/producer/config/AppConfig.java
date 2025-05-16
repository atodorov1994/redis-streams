package com.producer.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.listener.ChannelTopic;

@Configuration
public class AppConfig {

    @Value("${redis.topic}")
    private String messageTopicName;

    @Bean
    ChannelTopic messageTopic() {
        return new ChannelTopic(messageTopicName);
    }
}
