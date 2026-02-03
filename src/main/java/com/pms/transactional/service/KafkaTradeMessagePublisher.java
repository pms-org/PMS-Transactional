package com.pms.transactional.service;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.pms.transactional.Trade;
import com.pms.transactional.Transaction;

@Service
public class KafkaTradeMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.trades.consumer.listening-topic}")
    private  String TRADES_TOPIC;

    @Value("${app.transactions.publishing-topic}")
    private String TRANSACTIONS_TOPIC;

    public void publishTradeMessage(String key, Trade trade){
        System.out.println("Hi from trade publisher");

        kafkaTemplate.send(TRADES_TOPIC, key, trade)
                .whenComplete((res, ex) -> {
                    if (ex == null) {
                        System.out.println("Trades Kafka Offset: " + res.getRecordMetadata());
                    } else {
                        System.out.println("Failed to publish message: " + ex.getMessage());
                    }
                });
        }

    public void publishTransactionMessage(String key, Transaction transaction) {
        try{ 
            var future = kafkaTemplate.send(TRANSACTIONS_TOPIC, key, transaction);
            var result = future.get(5, TimeUnit.SECONDS); 
            System.out.println("Transactions Kafka Offset: " + result.getRecordMetadata());
        } 
        catch(Exception e){
            System.out.println("Failed to publish transaction message: " + e.getMessage());
        } 
    }


}
