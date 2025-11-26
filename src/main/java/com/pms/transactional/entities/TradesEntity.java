package com.pms.transactional.entities;

import java.time.Instant;
import java.util.UUID;

import com.pms.transactional.enums.TradeSide;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class TradesEntity{
    private UUID tradeId;
    private UUID portfolioId;
    private String cusipId;
    private TradeSide side;
    private float unitPrice;
    private long quantity;
    private Instant timestamp;
}