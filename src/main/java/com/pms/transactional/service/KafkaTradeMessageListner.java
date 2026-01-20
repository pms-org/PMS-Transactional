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
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @Value("${app.trades.consumer.dlt-topic}")
    private String dltTopic;

    @KafkaListener(id="${app.trades.consumer.consumer-id}",topics = "${app.trades.consumer.listening-topic}", groupId = "${app.trades.consumer.group-id}", containerFactory = "tradekafkaListenerContainerFactory")
    public void listen(List<ConsumerRecord<String, byte[]>> trades) {
        for(ConsumerRecord<String, byte[]> trade:trades){
            try{
                Trade tradeProto = Trade.parseFrom(trade.value());
                boolean addedToBuffer = buffer.offer(tradeProto);

                if (!addedToBuffer) {
                    logger.error("Buffer full! Dropped trade at offset {}", trade.offset());
                    batchProcessor.handleConsumerThread(false);
                    break;
                }
            }
            catch(InvalidProtocolBufferException e){
                logger.error("Corrupt Protobuf at offset {}. Routing to DLT.", trade.offset());
                kafkaTemplate.send(dltTopic,trade.key(),trade.value());
            }
        }
        
        batchProcessor.checkAndFlush();
    }

}
