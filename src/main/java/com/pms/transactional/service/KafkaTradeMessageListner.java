package com.pms.transactional.service;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pms.transactional.Trade;

@Service
public class KafkaTradeMessageListner {

    Logger logger = LoggerFactory.getLogger(KafkaTradeMessageListner.class);

    @Autowired
    private BlockingQueue<Trade> buffer;

    @Autowired
    private BatchProcessor batchProcessor;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Value("${app.trades.consumer.dlt-topic}")
    private String dltTopic;

    @KafkaListener(id="${app.trades.consumer.consumer-id}",topics = "${app.trades.consumer.listening-topic}", groupId = "${app.trades.consumer.group-id}", containerFactory = "tradekafkaListenerContainerFactory")
    public void listen(List<Trade> trades) {
        for(Trade trade:trades){
            boolean addedToBuffer = buffer.offer(trade);
            if (!addedToBuffer) {
                logger.error("Buffer full!");
                batchProcessor.handleConsumerThread(false);
                break;
            }
        }
        
        batchProcessor.checkAndFlush();
    }

}
