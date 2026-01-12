package com.pms.transactional.service;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.pms.transactional.TradeProto;

@Service
public class KafkaTradeMessageListner {

    Logger logger = LoggerFactory.getLogger(KafkaTradeMessageListner.class);

    @Autowired
    private BlockingQueue<TradeProto> buffer;

    @Autowired
    private BatchProcessor batchProcessor;


    @KafkaListener(id="tradesConsumer",topics = "${app.trades.consumer.listening-topic}", groupId = "${app.trades.consumer.group-id}", containerFactory = "tradekafkaListenerContainerFactory")
    public void listen(List<TradeProto> trades) {
        trades.forEach(buffer::offer);
        batchProcessor.checkAndFlush();
    }
    
    @DltHandler
    public void ListenDLT(TradeProto trade){
        logger.error("DLT reached for trade: {}", trade);
    }

}
