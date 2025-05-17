package com.consumer.listener;

import com.consumer.config.AppConfig;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Component
@RequiredArgsConstructor
public class SubscriptionManager implements SmartLifecycle {

    private boolean isRunning = false;

    @Value("${redis.consumer-group.size}")
    private int consumerGroupSize;

    @Value("${redis.active-subscription-key}")
    private String activeSubscriptionKey;

    private final ApplicationContext context;
    private final RedisTemplate<String, String> redisTemplate;

    Map<String, Subscription> activeSubscriptions = new HashMap<>();

    @PostConstruct
    public void registerConsumers() {

        for (int i = 0; i < consumerGroupSize; i++) {
            registerSubscription();
        }
    }

    @Scheduled(fixedDelay = 10_000)
    public void checkSubscriptionActivity() {
        Set<String> inactiveSubs = new HashSet<>();
        activeSubscriptions.forEach((id, subscription) -> {
            if (!subscription.isActive()) {
                inactiveSubs.add(id);
            }
        });
        for (String inactiveSub : inactiveSubs) {
            activeSubscriptions.remove(inactiveSub);
            redisTemplate.opsForList().remove(activeSubscriptionKey, 1, inactiveSub);
            registerSubscription();
        }
    }

    private void registerSubscription() {
        var subscription = context.getBean("streamMessageSubscription", AppConfig.ConsumerSubscription.class);
        activeSubscriptions.put(subscription.id(), subscription.subscription());
        redisTemplate.opsForList().rightPush(activeSubscriptionKey, subscription.id());
    }

    @Override
    public void start() {
        isRunning = true;
    }

    @Override
    public void stop() {
        for (String subscriptionId : activeSubscriptions.keySet()) {
            redisTemplate.opsForList().remove(activeSubscriptionKey, 1, subscriptionId);
        }
        activeSubscriptions.clear();

        isRunning = false;
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }
}
