package com.pms.transactional.mapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.pms.transactional.TransactionProto;
import com.pms.transactional.entities.TradesEntity;
import com.pms.transactional.entities.TransactionsEntity;

@Component
public class TransactionMapper{

    @Autowired
    TradeMapper tradeMapper;

    public TransactionsEntity toEntity(TransactionProto transaction){
        TransactionsEntity entity = new TransactionsEntity();
        entity.setBuyPrice(new java.math.BigDecimal(transaction.getSellPrice()));
        entity.setSellPrice(new java.math.BigDecimal(transaction.getSellPrice()));
        entity.setRemainingQuantity(transaction.getRemainingQuantity());
        entity.setSellQuantity(transaction.getSellQuantity());

        TradesEntity trade = tradeMapper.toEntity(transaction.getTrade());
        entity.setTrade(trade);
        return entity;
    }
    
}