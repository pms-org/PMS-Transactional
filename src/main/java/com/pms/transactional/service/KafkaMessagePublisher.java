package com.pms.transactional.service;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void publishMessage(String message) {
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("transactions-topic", message);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                System.out.println("Failed to send message: " + ex.getMessage());
            } else {
                System.out.println("Message sent successfully to topic: " +
                        result.getRecordMetadata().topic());
            }
        });

        // kafkaTemplate.send("transactions-topic", message);

    }

}
