package com.pms.transactional.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.pms.transactional.Transaction;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.transactions.publishing-topic}")
    private static String TOPIC;

    public void publishMessage(String key, Transaction transaction) {
        System.out.println("Hi from publisher");
        kafkaTemplate.send(TOPIC,key,transaction)
                .whenComplete((res,ex)->{
                    if (ex == null){
                        System.out.println("Kafka Offset: " + res.getRecordMetadata());
                    }
                });
    }

}
