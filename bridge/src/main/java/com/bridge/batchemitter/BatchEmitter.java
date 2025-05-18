package com.bridge.batchemitter;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.bridge.config.Constants.BATCH_DRAIN_RATE_MS;

@Component
@RequiredArgsConstructor
@Slf4j
public class BatchEmitter {

    @Qualifier("bufferContainer")
    private final List<BlockingQueue<String>> bufferContainer;
    private final RedisTemplate<String, String> redisTemplate;
    private final ChannelTopic topic;

    @Value("${redis.bridge-group.size}")
    private int bridgeGroupSize;

    @Value("${redis.batch-drain-size}")
    private int batchDrainSize;

    private ExecutorService executorService;

    @PostConstruct
    public void init() {
        executorService = Executors.newFixedThreadPool(bridgeGroupSize);
    }

    @Scheduled(fixedDelay = BATCH_DRAIN_RATE_MS)
    public void drainBuffers() {
        bufferContainer.forEach(que -> executorService.submit(() -> flushBufferToStream(que)));
    }

    private void flushBufferToStream(BlockingQueue<String> buffer) {
        List<String> batch = new ArrayList<>();
        buffer.drainTo(batch, batchDrainSize);

        if (batch.isEmpty()) return;

        try {
            redisTemplate.executePipelined((RedisCallback<?>) (redisConnection) -> {
                for (String msg : batch) {
                    try {
                        Map<byte[], byte[]> serializedFields = Map.of(
                                "body".getBytes(StandardCharsets.UTF_8), msg.getBytes());

                        var record = StreamRecords
                                .newRecord()
                                .in(topic.getTopic().getBytes(StandardCharsets.UTF_8))
                                .ofMap(serializedFields)
                                .withId(RecordId.autoGenerate());

                        redisConnection.streamCommands().xAdd(record);
                    } catch (Exception e) {
                        log.warn("Skipping message due to error during pipelined write: {}", e.getMessage());
                    }
                }
                return null;
            });
        } catch (Exception e) {
            log.error("Failed to flush buffer to Redis stream", e);
        }
    }
}
