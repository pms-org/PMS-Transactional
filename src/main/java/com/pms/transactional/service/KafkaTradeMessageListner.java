package com.pms.transactional.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.pms.transactional.TradeProto;
import com.pms.transactional.dto.TradeRecord;

@Service
public class KafkaTradeMessageListner {

    Logger logger = LoggerFactory.getLogger(KafkaMessageListner.class);

    @Autowired
    private BlockingQueue<List<TradeProto>> buffer;

    @Autowired
    private BatchProcessor batchProcessor;

    @KafkaListener(topics = "valid-trades-topic", groupId = "trades", containerFactory = "tradekafkaListenerContainerFactory")
    public void listen(List<TradeProto> protoList) {
        buffer.offer(new ArrayList<TradeProto>(protoList));
        batchProcessor.checkAndFlush();
    }
    
    @DltHandler
    public void ListenDLT(TradeProto trade) {
        logger.error("DLT reached for trade: {}", trade);
    }


    // @KafkaListener(topics = "validatedtrades-topic", groupId = "trades", containerFactory = "tradekafkaListenerContainerFactory")
    // public void consume2(TradeProto trade) {
    //         logger.info("Consumer message (parsed): {}", trade);
    //         if("BUY".equalsIgnoreCase(trade.getSide())){
    //             System.out.println("It is a buy trade");
    //             transactionService.handleBuy(trade);
    //         }else if("SELL".equalsIgnoreCase(trade.getSide())){
    //             System.out.println("It is a sell trade");
    //             transactionService.handleSell(trade);
    //         }
    // }
}
