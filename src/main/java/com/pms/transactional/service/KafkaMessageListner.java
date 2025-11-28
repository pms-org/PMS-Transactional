package com.pms.transactional.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListner {

    Logger logger = LoggerFactory.getLogger(KafkaMessageListner.class);

    @KafkaListener(topics = "transactions-topic", groupId = "transactions")
    public void consume1(String message) {
        logger.info("Consumer1 message: " + message);
    }

    @KafkaListener(topics = "transactions-topic", groupId = "transactions")
    public void consume2(String message) {
        logger.info("Consumer2 message: " + message);
    }

    @KafkaListener(topics = "transactions-topic", groupId = "transactions")
    public void consume3(String message) {
        logger.info("Consumer 3 message: " + message);
    }

    @KafkaListener(topics = "transactions-topic", groupId = "transactions")
    public void consume4(String message) {
        logger.info("Consumer 4 message: " + message);
    }
}
