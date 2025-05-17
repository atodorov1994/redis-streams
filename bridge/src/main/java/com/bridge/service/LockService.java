package com.bridge.service;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
@RequiredArgsConstructor
public class LockService {

    private final ConcurrentMap<String, Long> seenMessages = new ConcurrentHashMap<>();

    private static final long TTL = 15_000;

    public boolean tryLock(String messageId) {
        long now = System.currentTimeMillis();
        return seenMessages.putIfAbsent(messageId, now) == null;
    }

    @Scheduled(fixedDelay = TTL)
    public void cleanup() {
        long now = System.currentTimeMillis();
        seenMessages.entrySet().removeIf(entry -> now - entry.getValue() > TTL);
    }
}
