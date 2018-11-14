package com.kafka.retry.retry.retry;

import lombok.Synchronized;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaListeners;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class RetryProcess {

    private final String topic;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    private String id;
    private KafkaTemplate<String,String> kafkaTemplate;

    private long timestamp = System.currentTimeMillis();

    private boolean paused = false;


    public RetryProcess(String topic,KafkaTemplate kafkaTemplate) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }


    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }


    public String getTopic() {
        return this.topic;
    }


    @KafkaListener(beanRef = "__x", topics = "#{__x.topic}",
            groupId = "#{__x.topic}.group"
            ,clientIdPrefix = "#{__x.topic}",autoStartup = "false",containerFactory = "myFactory")
    public void listen(List<ConsumerRecord<String,String>> records, Consumer<String,String> consumer,Acknowledgment acknowledgment) throws ExecutionException, InterruptedException {
       for (ConsumerRecord<String,String> record : records) {
            if (record.timestamp() > timestamp) {
                System.out.println("record with " + record.offset() + "shouldn't be commited");
                if(consumer.paused().size() >0)
                    break;
                System.out.println("paused");
                setPaused(true);
                consumer.pause(consumer.assignment());
                consumer.seek(new TopicPartition(topic,record.partition()),record.offset());
                break;
            }
            System.out.println("retry consumer with value:" + record.value() + "from topic: " + topic + "from offset  " + record.offset());
            record.headers();
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("main", record.value());
            record.headers().forEach(header -> producerRecord.headers().add(header));
            ListenableFuture<SendResult<String, String>> send = kafkaTemplate.send(producerRecord);
            acknowledgment.acknowledge();
            // System.out.println("produced to " + stringStringSendResult.getRecordMetadata().offset());
        }
    }





    @EventListener(condition = "event.getConsumer().paused().size() > 0 "  )
    public void eventHandler(ListenerContainerIdleEvent event ) {

        if(!isPaused() && event.getConsumer().paused().stream().findFirst().get().topic().equals(topic)) {
            synchronized (event.getConsumer()) {
                Set<TopicPartition> paused = event.getConsumer().paused();
                System.out.println("resumed" );
                event.getConsumer().resume(paused);
            }
        }
    }

    public boolean isPaused() {
        return paused;
    }

    public void setPaused(boolean paused) {
        this.paused = paused;
    }
}
