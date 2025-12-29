package com.pms.transactional;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import com.google.protobuf.Timestamp;
import com.pms.transactional.dao.OutboxEventsDao;
import com.pms.transactional.dao.TradesDao;
import com.pms.transactional.entities.OutboxEventEntity;
import com.pms.transactional.entities.TradesEntity;
import com.pms.transactional.TradeProto;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(topics = "valid-trades-topic", partitions = 1)
class KafkaTradeConsumerIT extends BaseIntegrationTest {

	@Autowired
	KafkaTemplate<String, TradeProto> kafkaTemplate;

	@Autowired
	TradesDao tradesDao;

	@Autowired
	OutboxEventsDao outboxEventsDao;

	@Test
	void shouldConsumeKafkaMessageAndSaveTradeAndOutbox() {

		TradeProto trade = TradeProto.newBuilder()
				.setPortfolioId("P-100")
				.setTradeId(UUID.randomUUID().toString())
				.setSymbol("AAPL")
				.setSide("BUY")
				.setPricePerStock(150.25)
				.setQuantity(10)
				.setTimestamp(
						Timestamp.newBuilder()
								.setSeconds(Instant.now().getEpochSecond())
								.build())
				.build();

		kafkaTemplate.send("valid-trades-topic", trade);

		Awaitility.await().untilAsserted(() -> {

			TradesEntity savedTrade = tradesDao.findAll().get(0);

			assertThat(savedTrade.getSymbol()).isEqualTo("AAPL");
			assertThat(savedTrade.getQuantity()).isEqualTo(10);

			OutboxEventEntity outbox = outboxEventsDao.findAll().get(0);

			// assertThat(outbox.getAggregateType()).isEqualTo("TRADE");
			assertThat(outbox.getStatus()).isEqualTo("PENDING");
		});
	}

	@Test
	void outboxEventShouldMoveToPublished() {

		Awaitility.await().untilAsserted(() -> {
			OutboxEventEntity event = outboxEventsDao.findAll().get(0);
			assertThat(event.getStatus()).isEqualTo("PUBLISHED");
		});
	}

}
