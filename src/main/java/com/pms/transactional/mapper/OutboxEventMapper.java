package com.pms.transactional.mapper;

import java.time.LocalDateTime;

import org.springframework.stereotype.Component;

import com.pms.transactional.Transaction;
import com.pms.transactional.entities.OutboxEventEntity;
import com.pms.transactional.entities.TransactionsEntity;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class OutboxEventMapper{
    
    public OutboxEventEntity toEntity(
            TransactionsEntity txn,
            Transaction proto) {

        OutboxEventEntity entity = new OutboxEventEntity();
        entity.setAggregateId(txn.getTransactionId());
        entity.setPayload(proto.toByteArray());
        entity.setStatus("PENDING");
        entity.setCreatedAt(LocalDateTime.now());
        return entity;
    }
    
}