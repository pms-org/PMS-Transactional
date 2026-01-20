package com.pms.transactional.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.pms.transactional.Trade;

@Service
public class KafkaTradeMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.trades.consumer.listening-topic}")
    private  String TOPIC;
    public void publishTradeMessage(String key, Trade trade){
        System.out.println("Hi from publisher");

        kafkaTemplate.send(TOPIC, key, trade)
                .whenComplete((res, ex) -> {
                    if (ex == null) {
                        System.out.println("Kafka Offset: " + res.getRecordMetadata());
                    } else {
                        System.out.println("Failed to publish message: " + ex.getMessage());
                    }
                });
    }

}
