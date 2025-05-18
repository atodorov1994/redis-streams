package com.bridge.service;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.bridge.config.Constants.LOCK_TTL_MS;


/**
 * A simple in-memory lock mechanism to deduplicate messages using their message ID.
 * Each message ID is stored with a timestamp to track when it was first seen.
 * Old entries are periodically cleaned up to prevent memory leaks.
 * If we want to make the application scale horizontally
 * the only thing we need to do is implement a distributed lock here
 * But beware - the efficiency drops with redis distributed synchronisation
 */
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
