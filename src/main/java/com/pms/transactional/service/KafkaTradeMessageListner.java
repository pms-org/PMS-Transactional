package com.pms.transactional.service;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import com.pms.transactional.TradeProto;
import com.pms.transactional.config.BufferConfig;
import com.pms.transactional.dao.TradesDao;
import com.pms.transactional.mapper.TradeMapper;

@Service
public class KafkaTradeMessageListner {

    Logger logger = LoggerFactory.getLogger(KafkaMessageListner.class);

    @Autowired
    private BlockingQueue<TradeProto> buffer;

    @Autowired
    private BatchProcessor batchProcessor;

    @RetryableTopic(attempts = "4", backoff = @Backoff(delay = 3000, multiplier = 2, maxDelay = 10000))
    @KafkaListener(topics = "valid-trades-topic", groupId = "trades", containerFactory = "tradekafkaListenerContainerFactory")
    public void listen(TradeProto proto) {
        buffer.offer(proto);
        batchProcessor.checkAndFlush();
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

    @DltHandler
    public void ListenDLT(TradeProto trade) {
        logger.error("DLT reached for trade: {}", trade);
    }

}
