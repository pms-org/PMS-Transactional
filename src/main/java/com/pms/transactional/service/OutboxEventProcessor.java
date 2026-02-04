package com.pms.transactional.service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.errors.RecordTooLargeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import org.apache.kafka.common.errors.SerializationException;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pms.transactional.Transaction;
import com.pms.transactional.entities.OutboxEventEntity;
import com.pms.transactional.exceptions.PoisonPillException;
import com.pms.transactional.exceptions.SystemFailureException;

@Component
public class OutboxEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(OutboxEventProcessor.class);

    @Value("${app.transactions.publishing-topic}")
    private String PUBLISH_TOPIC;

    @Value("${app.outbox.kafka-send-timeout-ms:5000}")
    private long kafkaSendTimeoutMs;

    private KafkaTemplate<String, Object> kafkaTemplate;

    public OutboxEventProcessor(@Qualifier("kafkaOutboxTemplate") KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public ProcessingResult process(List<OutboxEventEntity> events) {

        List<UUID> successfulIds = new ArrayList<>();

        for (OutboxEventEntity event : events) {
            try {
                send(event);
                successfulIds.add(event.getTransactionOutboxId());

            } catch (PoisonPillException ppe) {
                log.error("Poison pill detected for event {} : {}",
                        ppe.getEventId(), ppe.getMessage());
                return ProcessingResult.withPoisonPill(successfulIds, ppe);

            } catch (SystemFailureException sfe) {
                log.error("System failure detected: {}", sfe.getMessage());
                return ProcessingResult.systemFailure(successfulIds);
            }
        }

        return ProcessingResult.success(successfulIds);
    }

    private void send(OutboxEventEntity event)
            throws PoisonPillException, SystemFailureException {

        try {
            Transaction proto = Transaction.parseFrom(event.getPayload());

            kafkaTemplate.send(
                    PUBLISH_TOPIC,
                    event.getPortfolioId().toString(),
                    proto).get(kafkaSendTimeoutMs, TimeUnit.MILLISECONDS);
            log.info("Transaction Proto sent to analytics,{} : ", proto.toString());
            log.debug("Sent outbox event {}", event.getTransactionOutboxId());

        } catch (InvalidProtocolBufferException e) {
            throw new PoisonPillException(
                    event.getTransactionOutboxId(),
                    "Invalid protobuf payload", e);

        } catch (ExecutionException e) {
            classifyAndThrow(event.getTransactionOutboxId(), e.getCause());

        } catch (TimeoutException e) {
            throw new SystemFailureException(
                    "Kafka send timeout after " + kafkaSendTimeoutMs + "ms", e);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SystemFailureException("Kafka send interrupted", e);
        }
    }

    private void classifyAndThrow(UUID eventId, Throwable cause)
            throws PoisonPillException, SystemFailureException {

        Throwable root = cause;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }

        String msg = root.getClass().getSimpleName() + ": " + root.getMessage();

        if (root instanceof SerializationException) {
            throw new PoisonPillException(eventId, "Kafka serialization failed: " + msg, cause);
        }

        if (root instanceof RecordTooLargeException) {
            throw new PoisonPillException(eventId, "Kafka record too large: " + msg, cause);
        }

        if (root instanceof IllegalArgumentException || root instanceof NullPointerException) {
            throw new PoisonPillException(eventId, "Invalid event data: " + msg, cause);
        }

        throw new SystemFailureException("Kafka system failure: " + msg, cause);
    }

}
