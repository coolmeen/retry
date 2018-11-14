package com.kafka.retry.retry.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.retry.support.RetryTemplate;

import java.util.Map;

@Configuration
public class configuration {


    @Autowired
    KafkaProperties properties;


    @Bean(name = "myFactory")
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String ,String>> batchFactory(ConsumerFactory<String,String > consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String ,String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);  // <<<<<<<<<<<<<<<<<<<<<<<<<
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setIdleEventInterval(1000l);
        return factory;
    }




}
