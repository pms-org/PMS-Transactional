package com.pms.transactional.dao;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.pms.transactional.entities.OutboxEvents;

@Repository
public interface OutboxEventsDao extends JpaRepository<OutboxEvents, UUID>{

}