package com.consumer.config;

public interface Constants {
    String CONSUMER_BEAN_NAME = "streamMessageConsumer";
    String SUBSCRIPTION_BEAN_NAME = "streamMessageSubscription";
    int METRICS_RATE_MS = 3000;
    int SUBSCRIPTION_CHECK_RATE_MS = 10_000;
}
