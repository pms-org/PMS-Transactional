package com.pms.transactional.service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pms.transactional.Transaction;
import com.pms.transactional.entities.OutboxEventEntity;

@Component
public class OutboxEventProcessor {

    @Value("${app.transactions.publishing-topic}")
    private String PUBLISH_TOPIC;

    private KafkaTemplate<String, Object> kafkaTemplate;

    public OutboxEventProcessor(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public ProcessingResult process(List<OutboxEventEntity> events) {

        List<UUID> successfulIds = new ArrayList<>();

        for (OutboxEventEntity event : events) {
            try {
                Transaction proto = Transaction.parseFrom(event.getPayload());

                kafkaTemplate.send(
                        PUBLISH_TOPIC,
                        event.getPortfolioId().toString(),
                        proto);

                successfulIds.add(event.getTransactionOutboxId());

            } catch (InvalidProtocolBufferException e) {
                return ProcessingResult.poisonPill(successfulIds, event);
            } catch (Exception e) {
                return ProcessingResult.systemFailure(successfulIds);
            }
        }

        return ProcessingResult.success(successfulIds);
    }
}
