package com.consumer.util;

import com.redis.testcontainers.RedisContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class RedisContainerFactory {

    public static final String NETWORK_ALIAS = "redis";
    public static final int REDIS_PORT = 6379;

    public static RedisContainer create() {

        try (RedisContainer redisContainer = new RedisContainer("redis:8.0.1-alpine")
                     .withExposedPorts(REDIS_PORT)
                     .withNetworkAliases(NETWORK_ALIAS)
                     .waitingFor(Wait.forListeningPort())) {

            return redisContainer;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
