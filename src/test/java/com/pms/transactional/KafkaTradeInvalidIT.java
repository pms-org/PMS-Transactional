package com.pms.transactional;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import com.google.protobuf.Timestamp;
import com.pms.transactional.dao.TradesDao;
import com.pms.transactional.TradeProto;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(
        topics = "valid-trades-topic",
        partitions = 1
)
class KafkaTradeInvalidIT extends BaseIntegrationTest {

    @Autowired
    KafkaTemplate<String, TradeProto> kafkaTemplate;

    @Autowired
    TradesDao tradesDao;

    @Test
    void invalidTradeShouldNotBeSaved() {

        TradeProto trade = TradeProto.newBuilder()
                .setPortfolioId("P-101")
                .setTradeId("T-INVALID")
                .setSymbol("AAPL")
                .setSide("BUY")
                .setPricePerStock(150)
                .setQuantity(0) // INVALID
                .setTimestamp(
                        Timestamp.newBuilder()
                                .setSeconds(Instant.now().getEpochSecond())
                                .build()
                )
                .build();

        kafkaTemplate.send("valid-trades-topic", trade);

        Awaitility.await().untilAsserted(() ->
                assertThat(tradesDao.findAll()).isEmpty()
        );
    }
}
