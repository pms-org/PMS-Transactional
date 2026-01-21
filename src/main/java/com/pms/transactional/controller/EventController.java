package com.pms.transactional.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.pms.transactional.Trade;
import com.pms.transactional.dto.TradeDTO;
import com.pms.transactional.service.KafkaTradeMessagePublisher;

@RequestMapping
@RestController

public class EventController {

    @Autowired
    private KafkaTradeMessagePublisher tradePublisher;

    @PostMapping("/trades/publish")
    public ResponseEntity<?> publishTradeMessage(@RequestBody TradeDTO trade) {
        System.out.println("Hello");
        try {
            Trade tradeProto = convertDTOToProto(trade);
            tradePublisher.publishTradeMessage(
                    trade.getPortfolioId().toString(),
                    tradeProto);

            return ResponseEntity.ok("Trade Message published successfully");

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to publish trade message: " + e.getMessage());
        }
    }

    private Trade convertDTOToProto(TradeDTO trade) {
        return Trade.newBuilder()
                .setTradeId(trade.getTradeId().toString())
                .setPortfolioId(trade.getPortfolioId().toString())
                .setSymbol(trade.getSymbol())
                .setSide(trade.getSide().name())
                .setPricePerStock(trade.getPricePerStock().doubleValue())
                .setQuantity(trade.getQuantity())
                .setTimestamp(
                        com.google.protobuf.Timestamp.newBuilder()
                                .setSeconds(trade.getTimestamp().toEpochSecond(java.time.ZoneOffset.UTC))
                                .setNanos(trade.getTimestamp().getNano())
                                .build())
                .build();
    }

}
