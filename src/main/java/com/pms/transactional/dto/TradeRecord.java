package com.pms.transactional.dto;

import com.pms.transactional.TradeProto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class TradeRecord {
    private TradeProto trade;
}