package com.pms.transactional.service;

import java.util.List;

import org.apache.kafka.shaded.io.opentelemetry.proto.trace.v1.Span.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.pms.rttm.client.clients.RttmClient;
import com.pms.rttm.client.dto.TradeEventPayload;
import com.pms.rttm.client.enums.EventStage;
import com.pms.rttm.client.enums.EventType;
import com.pms.transactional.Trade;

@Service
public class KafkaTradeMessageListner {

    Logger logger = LoggerFactory.getLogger(KafkaTradeMessageListner.class);

    @Autowired
    private BatchProcessor batchProcessor;

    @Autowired
    private RttmClient rttmClient;

    @Value("${app.trades.consumer.group-id}")
    private String consumerGroupId;

    @Value("${app.transactions.publishing-topic}")
    private String publishingTopic;

    @KafkaListener(id = "${app.trades.consumer.consumer-id}", topics = "${app.trades.consumer.listening-topic}", groupId = "${app.trades.consumer.group-id}", containerFactory = "tradekafkaListenerContainerFactory")
    public void listen(
            List<Trade> trades,
            @Header(value = KafkaHeaders.OFFSET, required = false) List<Long> offsets,
            @Header(value = KafkaHeaders.PARTITION, required = false) List<Integer> partitions,
            @Header(value = KafkaHeaders.RECEIVED_TOPIC, required = false) List<String> recievedTopics, 
            Acknowledgment ack) {
        for (int i = 0; i < trades.size(); i++) {
            Trade trade = trades.get(i);

            Long offset = (offsets != null && offsets.size() > i)
                    ? offsets.get(i)
                    : 0;

            Integer partition = (partitions != null && partitions.size() > i) ?partitions.get(i):0;

            String recievedTopic = (recievedTopics != null && recievedTopics.size() > i) ? recievedTopics.get(i): "";

            System.out.println("Processing tradeId: " + trade.getTradeId() +
                    " at offset: " + offset + " partition: " + partition);

            TradeEventPayload tradePayload = TradeEventPayload.builder()
                    .tradeId(trade.getTradeId())
                    .serviceName("pms-transactional")
                    .eventType(EventType.TRADE_ENRICHED)
                    .eventStage(EventStage.ENRICHED)
                    .eventStatus("ENRICHED")
                    .sourceQueue(recievedTopic)
                    .targetQueue(publishingTopic)
                    .topicName(recievedTopic)
                    .consumerGroup(consumerGroupId)
                    .partitionId(partition)
                    .offsetValue(offset)

                    .build();

            try {
                rttmClient.sendTradeEvent(tradePayload);
                logger.info("RTTM trade event publish succeeded for tradeId={} after consuming", trade.getTradeId());
            } catch (Exception e) {
                logger.error("RTTM publish failed for tradeId={}", trade.getTradeId(), e);
            }
        }
        batchProcessor.checkAndFlush(trades,offsets,partitions,recievedTopics, ack);
    }
}