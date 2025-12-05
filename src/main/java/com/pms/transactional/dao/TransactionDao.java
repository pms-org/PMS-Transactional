package com.pms.transactional.dao;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

import com.pms.transactional.entities.TransactionsEntity;

public interface TransactionDao extends JpaRepository<TransactionsEntity, UUID>{
    
}