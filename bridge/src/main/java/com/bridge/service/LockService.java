package com.bridge.service;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.bridge.config.Constants.LOCK_TTL_MS;

@Component
@RequiredArgsConstructor
public class LockService {

    private final ConcurrentMap<String, Long> seenMessages = new ConcurrentHashMap<>();

    public boolean tryLock(String messageId) {
        long now = System.currentTimeMillis();
        return seenMessages.putIfAbsent(messageId, now) == null;
    }

    @Scheduled(fixedDelay = LOCK_TTL_MS)
    public void cleanup() {
        long now = System.currentTimeMillis();
        seenMessages.entrySet().removeIf(entry -> now - entry.getValue() > LOCK_TTL_MS);
    }
}
