package com.pms.transactional.service;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import com.pms.rttm.client.clients.RttmClient;
import com.pms.rttm.client.dto.QueueMetricPayload;

@Service
public class QueueMetricsPublisher implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(QueueMetricsPublisher.class);

    private final Executor executor;
    private final ConsumerFactory<String, String> consumerFactory;
    private final RttmClient rttmClient;

    private KafkaConsumer<String, String> metricsConsumer;
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

    // ===================== LIFECYCLE =====================

    @Override
    public void start() {
        running = true;

        metricsConsumer = (KafkaConsumer<String, String>) consumerFactory.createConsumer(consumerGroup, "", "-metrics");

        executor.execute(this::loop);

        log.info("QueueMetricsPublisher started");
    }

    @Override
    public void stop() {
        running = false;

        if (metricsConsumer != null) {
            metricsConsumer.close();
        }

        log.info("QueueMetricsPublisher stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        // Start AFTER Kafka listeners
        return Integer.MAX_VALUE - 200;
    }

    // ===================== LOOP =====================

    private void loop() {
        while (running) {
            try {
                sendQueueMetrics();
                Thread.sleep(30_000); // every 30s
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running = false;
            } catch (Exception ex) {
                log.error("Queue metrics failure, retrying in 30s", ex);
            }
        }
    }

    // ===================== METRICS =====================

    private void sendQueueMetrics() {
        try {
            sendMetricsForAllPartitions(metricsConsumer, publishingTopic);
        } catch (Exception ex) {
            log.warn("Failed to send queue metrics", ex);
        }
    }

    private void sendMetricsForAllPartitions(
            KafkaConsumer<String, String> consumer,
            String topicName) {

        List<PartitionInfo> partitions = consumer.partitionsFor(topicName);

        if (partitions == null || partitions.isEmpty()) {
            log.warn("[METRICS] No partitions found for topic {}", topicName);
            return;
        }

        List<TopicPartition> topicPartitions = partitions.stream()
                .map(p -> new TopicPartition(topicName, p.partition()))
                .toList();

        // MANUAL assignment (no group rebalance)
        consumer.assign(topicPartitions);

        log.info("[METRICS] Assigned partitions: {}", topicPartitions);

        for (TopicPartition tp : topicPartitions) {
            sendMetricForPartition(consumer, topicName, tp);
        }
    }

    private void sendMetricForPartition(
            KafkaConsumer<String, String> consumer,
            String topicName,
            TopicPartition tp) {

        try {
            // Produced offset (log end offset)
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singleton(tp));

            long producedOffset = endOffsets.getOrDefault(tp, 0L);

            // Committed offset of REAL consumer group
            OffsetAndMetadata committed = consumer.committed(Collections.singleton(tp), Duration.ofSeconds(5))
                    .get(tp);

            long consumedOffset = committed != null ? committed.offset() : 0L;

            long lag = producedOffset - consumedOffset;

            QueueMetricPayload payload = QueueMetricPayload.builder()
                    .serviceName(serviceName)
                    .topicName(topicName)
                    .partitionId(tp.partition())
                    .producedOffset(producedOffset)
                    .consumedOffset(consumedOffset)
                    .consumerGroup(consumerGroup)
                    .build();

            rttmClient.sendQueueMetric(payload);

            log.info(
                    "[QUEUE-METRIC] topic={} partition={} produced={} consumed={} lag={}",
                    topicName,
                    tp.partition(),
                    producedOffset,
                    consumedOffset,
                    lag);

        } catch (Exception ex) {
            log.warn(
                    "Failed metric for topic={} partition={}",
                    topicName,
                    tp.partition(),
                    ex);
        }
    }
}
