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

import static com.consumer.config.Constants.SUBSCRIPTION_BEAN_NAME;
import static com.consumer.config.Constants.SUBSCRIPTION_CHECK_RATE_MS;

/**
 * Manages Redis Stream consumer subscriptions.
 * - Automatically registers a fixed number of stream consumers at startup.
 * - Monitors and restarts inactive subscriptions on a scheduled basis.
 * - Implements SmartLifecycle for clean startup and shutdown
 *   and to execute a cleanUp logic on the activeSubscriptions RedisList
 */
@Component
@RequiredArgsConstructor
public class SubscriptionManager implements SmartLifecycle {

    private final ApplicationContext context;
    private final RedisTemplate<String, String> redisTemplate;

    @Value("${redis.consumer-group.size}")
    private int consumerGroupSize;

    @Value("${redis.active-subscription-key}")
    private String activeSubscriptionKey;

    Map<String, Subscription> activeSubscriptions = new HashMap<>();

    private boolean isRunning = false;

    @PostConstruct
    public void registerConsumers() {

        for (int i = 0; i < consumerGroupSize; i++) {
            registerSubscription();
        }
    }

    /**
     * Periodically checks for inactive subscriptions and replaces them.
     * Runs every 10 seconds.
     */
    @Scheduled(fixedRate = SUBSCRIPTION_CHECK_RATE_MS)
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

    /**
     * Registers a new consumer subscription and stores it both in memory and Redis.
     */
    private void registerSubscription() {
        var subscription = context.getBean(SUBSCRIPTION_BEAN_NAME, AppConfig.ConsumerSubscription.class);
        activeSubscriptions.put(subscription.id(), subscription.subscription());
        redisTemplate.opsForList().rightPush(activeSubscriptionKey, subscription.id());
    }


    // SmartLifecycle lifecycle management methods

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
