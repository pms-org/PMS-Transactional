package com.pms.transactional.service;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.pms.transactional.Trade;
import com.pms.transactional.wrapper.PollBatch;

@Service
public class KafkaTradeMessageListner {

    Logger logger = LoggerFactory.getLogger(KafkaTradeMessageListner.class);

    @Autowired
    private BlockingQueue<PollBatch> buffer;

    @Autowired
    private BatchProcessor batchProcessor;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @KafkaListener(id="${app.trades.consumer.consumer-id}",topics = "${app.trades.consumer.listening-topic}", groupId = "${app.trades.consumer.group-id}", containerFactory = "tradekafkaListenerContainerFactory")
    public void listen(List<Trade> trades,Acknowledgment ack) {   
        buffer.offer(new PollBatch(trades,ack)); 
        if(buffer.size() >= 40){
            batchProcessor.handleConsumerThread(false);
        }
        batchProcessor.checkAndFlush();
    }
}
    