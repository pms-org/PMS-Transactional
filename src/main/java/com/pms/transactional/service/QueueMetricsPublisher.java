package com.pms.transactional.service;

import java.util.Collection;
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

import com.pms.rttm.client.clients.RttmClient;
import com.pms.rttm.client.dto.QueueMetricPayload;
import org.apache.kafka.common.TopicPartition;

@Service
public class QueueMetricsPublisher implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(QueueMetricsPublisher.class);

    private final Executor executor;
    private final KafkaListenerEndpointRegistry registry;
    private final ConsumerFactory<?, ?> consumerFactory;
    private final RttmClient rttmClient;

    private volatile boolean running = false;

    public QueueMetricsPublisher(
            @Qualifier("queueMetricsScheduler") Executor executor,
            KafkaListenerEndpointRegistry registry,
            ConsumerFactory<?, ?> consumerFactory,
            RttmClient rttmClient) {

        this.executor = executor;
        this.registry = registry;
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
                publishQueueMetrics();
                Thread.sleep(30_000); // ⏱️ 30 seconds
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running = false;
            } catch (Exception e) {
                log.error("Queue metrics publish failed", e);
            }
        }
    }

    @Override
    public void stop() {
        running = false;
        log.info("QueueMetricsPublisher stopping");
    }

    private void publishQueueMetrics() {
        log.debug("Publishing queue metrics snapshot");

        registry.getListenerContainers().forEach(container -> {
            try {
                Collection<TopicPartition> assignedPartitions = container.getAssignedPartitions();

                if (assignedPartitions != null && !assignedPartitions.isEmpty()) {

                    Map<String, Object> consumerProps = consumerFactory.getConfigurationProperties();

                    String consumerGroup = (String) consumerProps.get(
                            ConsumerConfig.GROUP_ID_CONFIG);

                    try (Consumer<?, ?> consumer = consumerFactory.createConsumer(
                            consumerGroup, "", "-metrics")) {

                        assignedPartitions.forEach(tp -> {
                            try {
                                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(List.of(tp));

                                Long producedOffset = endOffsets.get(tp);

                                OffsetAndMetadata committed = consumer.committed(Set.of(tp)).get(tp);

                                Long consumedOffset = committed != null
                                        ? committed.offset()
                                        : 0L;

                                rttmClient.sendQueueMetric(
                                        QueueMetricPayload.builder()
                                                .serviceName("pms-transactional")
                                                .topicName(tp.topic())
                                                .partitionId(tp.partition())
                                                .producedOffset(
                                                        producedOffset != null
                                                                ? producedOffset
                                                                : 0L)
                                                .consumedOffset(
                                                        consumedOffset != null
                                                                ? consumedOffset
                                                                : 0L)
                                                .consumerGroup(consumerGroup)
                                                .build());

                                log.debug(
                                        "Published metric for topic={}, partition={}, lag={}",
                                        tp.topic(),
                                        tp.partition(),
                                        (producedOffset != null
                                                ? producedOffset
                                                : 0L)
                                                - (consumedOffset != null
                                                        ? consumedOffset
                                                        : 0L));

                            } catch (Exception e) {
                                log.error(
                                        "Failed to publish metric for {}",
                                        tp, e);
                            }
                        });
                    }
                }
            } catch (Exception e) {
                log.error(
                        "Error publishing metrics for container {}",
                        container.getListenerId(), e);
            }
        });

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
