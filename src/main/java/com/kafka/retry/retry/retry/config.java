package com.kafka.retry.retry.retry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.Lifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.stream.Stream;

@EnableSampleServices
@Configuration
@EnableScheduling
public class config {

    @Autowired
    ApplicationContext context;

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry registry;






    @Scheduled(initialDelay = 10000,fixedDelay = 10000)
    public void start(){
        RetryProcess oneMintueRetry = context.getBean("1MinutesIntervalRetry", RetryProcess.class);
        oneMintueRetry.setTimestamp(System.currentTimeMillis());
        oneMintueRetry.setPaused(false);
        System.out.println("resuming");
        registry.getListenerContainers().stream()
                .filter(messageListenerContainer -> messageListenerContainer
                        .getContainerProperties()
                        .getGroupId()
                        .equals("retry1.group"))
                .filter(messageListenerContainer -> !messageListenerContainer.isRunning())
                .forEach(messageListenerContainer -> {
                    oneMintueRetry.setId(messageListenerContainer.getContainerProperties().getClientId());
                    messageListenerContainer.start();
                });


    }


    @Scheduled(initialDelay = 20000,fixedDelay = 20000)
    public void start1() {
        RetryProcess oneMintueRetry = context.getBean("5MinutesIntervalRetry", RetryProcess.class);
        oneMintueRetry.setTimestamp(System.currentTimeMillis());
        oneMintueRetry.setPaused(false);
     registry.getListenerContainers().stream()
                .filter(messageListenerContainer -> messageListenerContainer
                        .getContainerProperties()
                        .getGroupId()
                        .equals("retry5.group"))
                .filter(messageListenerContainer -> !messageListenerContainer.isRunning())
             .forEach(messageListenerContainer -> {
                 oneMintueRetry.setId(messageListenerContainer.getContainerProperties().getClientId());
                 messageListenerContainer.start();
             });

    }
}
