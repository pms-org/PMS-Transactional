package com.pms.transactional.wrapper;

import org.springframework.kafka.support.Acknowledgment;

import com.pms.transactional.Trade;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class TradeRecord {
    Trade tradeProto;
    Acknowledgment ack;
}
