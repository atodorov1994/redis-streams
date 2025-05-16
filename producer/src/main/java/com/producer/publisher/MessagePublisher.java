package com.producer.publisher;

import java.util.List;

public interface MessagePublisher {

    void publishBatch(List<String> messages);
}
