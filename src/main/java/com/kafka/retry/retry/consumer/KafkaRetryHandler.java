package com.kafka.retry.retry.consumer;

import com.google.common.primitives.Ints;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaRetryHandler {

    @Autowired
    KafkaTemplate <String,String> template;

    @Autowired
    RetryStateMachine retryStateMachine;


    public void handle(ConsumerRecord<String, String> record) throws ExecutionException, InterruptedException {
        Integer retryCount = Optional.ofNullable(record.headers().lastHeader("retryCount"))
                .map(Header::value)
                .map(bytes -> ByteBuffer.wrap(bytes).getInt())
                .orElse(0);
        Integer retryInterval = Optional.ofNullable(record.headers().lastHeader("retryInterval"))
                .map(Header::value)
                .map(bytes -> ByteBuffer.wrap(bytes).getInt())
                .orElse(1);

        RetryState retryState = new RetryState(retryCount, retryInterval);
        RetryState nextState = retryStateMachine.getNextState(retryState);

        String topic = "retry"  + nextState.getRetryInterval();
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, record.key(), record.value());
        producerRecord.headers().add("retryCount", Ints.toByteArray(nextState.getRetryCount()));
        producerRecord.headers().add("retryInterval", Ints.toByteArray(nextState.getRetryInterval()));
        producerRecord.headers().add("originalTopic", record.topic().getBytes());

        SendResult<String, String> stringStringSendResult = template.send(producerRecord).get();
        System.out.println("retrey produced with " + stringStringSendResult.getRecordMetadata().timestamp());
    }
}
