package com.consumer.listener;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Service;

@Service("streamMessageConsumer")
@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class StreamConsumer implements StreamListener<String, ObjectRecord<String,String>> {

    @Override
    public void onMessage(ObjectRecord<String, String> message) {
        System.out.println();
        System.out.printf("%s-%s-%s", this, Thread.currentThread(), message);
    }
}
