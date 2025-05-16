package com.consumer.listener;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@RequiredArgsConstructor
public class ConsumerRegistration {

    @Value("${redis.consumer-group.size}")
    private int consumerGroupSize;

    private final ApplicationContext context;

    List<Subscription> activeSubscriptions = new ArrayList<>();

    @PostConstruct
    public void registerConsumers() {

        for (int i = 0; i < consumerGroupSize; i++) {
            var subscription = context.getBean("streamMessageSubscription", Subscription.class);
            activeSubscriptions.add(subscription);
        }
    }
}
