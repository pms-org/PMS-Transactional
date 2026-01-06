package com.pms.transactional.service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pms.transactional.TransactionProto;
import com.pms.transactional.entities.OutboxEventEntity;

@Component
public class OutboxEventProcessor {

    private  KafkaTemplate<String, Object> kafkaTemplate;

    public OutboxEventProcessor(KafkaTemplate<String, Object> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    public ProcessingResult process(List<OutboxEventEntity> events){

        List<UUID> successfulIds = new ArrayList<>();
<<<<<<< Updated upstream
            
        for (OutboxEventEntity event : events) {
            try {
=======

        for (OutboxEventEntity event : events){
            try{
>>>>>>> Stashed changes
                TransactionProto proto =
                        TransactionProto.parseFrom(event.getPayload());

                kafkaTemplate.send(
                        "transactions-topic",
                        event.getAggregateId().toString(),
                        proto
                );

                successfulIds.add(event.getTransactionOutboxId());

<<<<<<< Updated upstream
            } catch (InvalidProtocolBufferException e) {
                return ProcessingResult.poisonPill(successfulIds, event);
            } catch (Exception e) {
=======
            }
            // POISON PILL (bad data)
            catch(InvalidProtocolBufferException | SerializationException e){
                return ProcessingResult.poisonPill(successfulIds, event);
            }
            // SYSTEM FAILURE (Kafka down / timeout)
            catch(TimeoutException | InterruptedException e){
                Thread.currentThread().interrupt();
                return ProcessingResult.systemFailure(successfulIds);
            }
            catch(Exception e){
>>>>>>> Stashed changes
                return ProcessingResult.systemFailure(successfulIds);
            }
        }

        return ProcessingResult.success(successfulIds);
    }
}


