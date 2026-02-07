package com.pms.transactional.service;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Service;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.pms.rttm.client.clients.RttmClient;
import com.pms.rttm.client.dto.QueueMetricPayload;
import org.apache.kafka.common.TopicPartition;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;

@Service
public class QueueMetricsPublisher implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(QueueMetricsPublisher.class);

    private final Executor executor;
    private final ConsumerFactory<String, String> consumerFactory;
    private final RttmClient rttmClient;

    private volatile boolean running = false;

    @Value("${app.transactions.publishing-topic}")
    private String publishingTopic;

    @Value("${app.transactions.consumer.group-id}")
    private String consumerGroup;

    @Value("${spring.application.name}")
    private String serviceName;

    public QueueMetricsPublisher(
            @Qualifier("queueMetricsScheduler") Executor executor,
            @Qualifier("metricsConsumerFactory") ConsumerFactory<String, String> consumerFactory,
            RttmClient rttmClient) {

        this.executor = executor;
        this.consumerFactory = consumerFactory;
        this.rttmClient = rttmClient;
    }

    @Override
    public void start() {
        running = true;
        executor.execute(this::loop);
        log.info("QueueMetricsPublisher started");
    }

    private void loop() {
        while (running) {
            try {
                sendQueueMetrics();
                Thread.sleep(30_000); // 30s
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running = false;
            } catch (Exception ex) {
                log.error("Error while sending queue metrics. Backing off...", ex);

                try {
                    Thread.sleep(30_000); // 30s backoff on error
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    running = false;
                }
            }
        }
    }

    @Override
    public void stop() {
        running = false;
        log.info("QueueMetricsPublisher stopping");
    }

    /**
     * Same behavior as validation QueueMetricsService
     * Sends metrics ONLY for publishing topic
     */
    private void sendQueueMetrics() {

        try (KafkaConsumer<String, String> consumer = (KafkaConsumer<String, String>) consumerFactory.createConsumer(
                consumerGroup, "", "-metrics")) {

            sendMetricsForAllPartitions(consumer, publishingTopic);

            log.debug("Queue metrics sent successfully for topic {}", publishingTopic);

        } catch (Exception ex) {
            log.warn("Failed to send queue metrics: {}", ex.getMessage(), ex);
        }
    }

    private void sendMetricsForAllPartitions(
            KafkaConsumer<String, String> consumer,
            String topicName) {

        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topicName);

        if (partitionInfos == null || partitionInfos.isEmpty()) {
            log.warn("[METRICS] No partitions found for topic {}", topicName);
            return;
        }

        List<TopicPartition> topicPartitions = partitionInfos.stream()
                .map(p -> new TopicPartition(topicName, p.partition()))
                .toList();

        // 🔥 THIS IS THE KEY FIX
        consumer.assign(topicPartitions);

        log.info("[METRICS] Assigned partitions: {}", topicPartitions);

        for (TopicPartition tp : topicPartitions) {
            sendMetricForTopic(consumer, topicName, tp.partition());
        }
    }

    private void sendMetricForTopic(
            KafkaConsumer<String, String> consumer,
            String topicName,
            int partitionId) {

        log.info(
                "[METRICS-CHECK] topic={} partition={} group={}",
                topicName,
                partitionId,
                consumerGroup);

        try {
            TopicPartition tp = new TopicPartition(topicName, partitionId);

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singleton(tp));
            long producedOffset = endOffsets.getOrDefault(tp, 0L);

            OffsetAndMetadata committed = consumer.committed(Collections.singleton(tp), Duration.ofSeconds(5))
                    .get(tp);
            long consumedOffset = committed != null ? committed.offset() : 0L;

            QueueMetricPayload metric = QueueMetricPayload.builder()
                    .serviceName(serviceName)
                    .topicName(topicName)
                    .partitionId(partitionId)
                    .producedOffset(producedOffset)
                    .consumedOffset(consumedOffset)
                    .consumerGroup(consumerGroup)
                    .build();

            rttmClient.sendQueueMetric(metric);

            log.info(
                    "Sent queue metric | topic={} partition={} produced={} consumed={} lag={}",
                    topicName,
                    partitionId,
                    producedOffset,
                    consumedOffset,
                    producedOffset - consumedOffset);

        } catch (Exception ex) {
            log.warn(
                    "Failed to send queue metric for topic {} partition {}",
                    topicName,
                    partitionId,
                    ex);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        // start AFTER Kafka listeners
        return Integer.MAX_VALUE - 200;
    }
}
