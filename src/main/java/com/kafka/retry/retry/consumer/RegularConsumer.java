package com.kafka.retry.retry.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
public class RegularConsumer {


    @Autowired
    KafkaRetryHandler retryHandler;


    @KafkaListener(topics = "main",groupId = "main")
    public void listener(ConsumerRecord<String,String> record, Acknowledgment acknowledgment) throws ExecutionException, InterruptedException {
        if(isError(record)){
            retryHandler.handle(record);
        }
        System.out.println("consumed " + record.value());
        Thread.sleep(100);
        acknowledgment.acknowledge();
    }

    private void sendToRetry() {

    }

    private boolean isError(ConsumerRecord<String, String> record) {

        if(record.value().equals("5"))
            return true;

        return false;
    }
}
