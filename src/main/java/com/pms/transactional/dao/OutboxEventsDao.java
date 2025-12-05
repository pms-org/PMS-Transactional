package com.pms.transactional.dao;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.pms.transactional.entities.OutboxEventEntity;

@Repository
public interface OutboxEventsDao extends JpaRepository<OutboxEventEntity, UUID>{
    List<OutboxEventEntity> findByStatusOrderByCreatedAt(String status);
}