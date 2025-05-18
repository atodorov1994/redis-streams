package com.bridge.config;

public interface Constants {

    int BATCH_DRAIN_RATE_MS = 100;
    int LOCK_TTL_MS = 15_000;
    String LISTENER_BEAN_NAME = "messageListener";
    String BUFFER_CONTAINER_BEAN_NAME = "bufferContainer";
}
