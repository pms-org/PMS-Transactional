package com.pms.transactional.service;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.pms.transactional.entities.TransactionsEntity;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void publishMessage(TransactionsEntity transactions) {
        try {
            String portfolioKey = transactions.getTrade().getPortfolioId().toString();
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("transactions-topic",
                    portfolioKey,
                    transactions);
            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    System.out.println("Failed to send message: " + ex.getMessage());
                } else {
                    System.out.println("Message sent successfully to topic: " +
                            result.getRecordMetadata().topic());
                }
            });

            // kafkaTemplate.send("transactions-topic",message.getPortfolioId().toString(),
            // message);
        } catch (Exception e) {
            System.out.println("Exception in publishing message: " + e.getMessage());
        }
    }

}
