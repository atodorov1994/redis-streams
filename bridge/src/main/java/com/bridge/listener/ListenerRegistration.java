package com.bridge.listener;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

import static com.bridge.config.Constants.LISTENER_BEAN_NAME;

@Component
@RequiredArgsConstructor
public class ListenerRegistration {

    @Value("${redis.bridge-group.size}")
    private int bridgeGroupSize;

    private final ApplicationContext context;
    private final RedisMessageListenerContainer messageListenerContainer;
    private final ChannelTopic topic;

    List<MessageSubscriber> activeSubscriptions = new ArrayList<>();

    @PostConstruct
    public void registerListeners() {

        for (int i = 0; i < bridgeGroupSize; i++) {
            var listener = context.getBean(LISTENER_BEAN_NAME, MessageSubscriber.class);
            activeSubscriptions.add(listener);
            messageListenerContainer.addMessageListener(listener, topic);
        }

        messageListenerContainer.start();
    }
}
