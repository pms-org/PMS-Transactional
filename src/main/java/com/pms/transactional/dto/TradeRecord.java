package com.pms.transactional.dto;

import com.pms.transactional.Trade;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class TradeRecord {
    private Trade trade;
}