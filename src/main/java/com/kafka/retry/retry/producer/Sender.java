package com.kafka.retry.retry.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class Sender implements CommandLineRunner {


    @Autowired
    KafkaTemplate<String,String> template;

    @Override
    public void run(String... args) throws Exception {

        for(int i =0;i<10;i++){
            template.send("main",String.valueOf(i));
        }
    }
}
