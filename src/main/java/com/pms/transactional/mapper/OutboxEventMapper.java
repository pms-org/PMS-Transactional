package com.pms.transactional.mapper;

import java.time.LocalDateTime;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pms.transactional.entities.OutboxEventEntity;
import com.pms.transactional.entities.TransactionsEntity;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class OutboxEventMapper{

    private final ObjectMapper objectMapper;

    public OutboxEventEntity toEntity(TransactionsEntity transaction) throws JsonProcessingException{
        OutboxEventEntity entity = new OutboxEventEntity();
        entity.setAggregateId(transaction.getTransactionId());
        entity.setCreatedAt(LocalDateTime.now());
        entity.setPayload(objectMapper.writeValueAsString(transaction));
        entity.setStatus("PENDING");
        return entity;
    }
    
}