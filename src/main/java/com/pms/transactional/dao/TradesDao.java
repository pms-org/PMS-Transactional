package com.pms.transactional.dao;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.pms.transactional.entities.TradesEntity;


@Repository
public interface TradesDao extends JpaRepository<TradesEntity, UUID> {
    @Modifying
    @Query(value = """
        INSERT INTO trades (trade_id, portfolio_id, symbol, side, price_per_stock, quantity, timestamp)
        VALUES (:#{#t.tradeId}, :#{#t.portfolioId}, :#{#t.symbol}, :#{#t.side.name()},:#{#t.pricePerStock}, :#{#t.quantity}, :#{#t.timestamp})
        ON CONFLICT(trade_id) DO NOTHING""", nativeQuery = true)
    void upsert(@Param("t") TradesEntity trade);
}
