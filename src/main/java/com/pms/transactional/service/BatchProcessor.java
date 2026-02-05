package com.pms.transactional.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;

import com.pms.rttm.client.clients.RttmClient;
import com.pms.rttm.client.dto.TradeEventPayload;
import com.pms.rttm.client.enums.EventStage;
import com.pms.rttm.client.enums.EventType;
import com.pms.transactional.Trade;
import com.pms.transactional.wrapper.PollBatch;

@Service
public class BatchProcessor implements SmartLifecycle {
    Logger logger = LoggerFactory.getLogger(BatchProcessor.class);

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private LinkedBlockingDeque<PollBatch> buffer;

    @Autowired
    private RttmClient rttmClient;

    @Autowired
    @Qualifier("batchExecutor")
    private ThreadPoolTaskExecutor batchProcessorExecutor;

    @Autowired
    @Qualifier("batchFlushScheduler")
    private ThreadPoolTaskScheduler batchFlushScheduler;

    @Autowired
    @Qualifier("dbRecoveryScheduler")
    private ThreadPoolTaskScheduler dbRecoveryScheduler;

    @Autowired
    private TransactionService transactionService;

    @Autowired
    @Qualifier("rttmExecutor")
    private ThreadPoolTaskExecutor rttmExecutor;

    @Value("${app.batch.size}")
    private int BATCH_SIZE;

    @Value("${app.transactions.publishing-topic}")
    private String publishingTopic;

    @Value("${app.trades.consumer.group-id}")
    private String consumerGroupId;

    @Value("${app.buffer.size}")
    private int totalBufferCapacity;

    @Value("${app.flush-interval-ms}")
    private long FLUSH_INTERVAL_MS;

    @Value("${app.trades.consumer.consumer-id}")
    private String CONSUMER_ID;

    private final AtomicInteger totalTradeCount = new AtomicInteger(0);

    private boolean isRecovering = false;
    private ScheduledFuture<?> recoveryTask;
    private boolean isRunning = false;

    public void checkAndFlush(List<Trade> trades,List<Long> offsets, List<Integer> partitions,String recievedTopic ,Acknowledgment ack) {
        int incomingTradeCount = trades.size();

        if (buffer.size() >= 0.8 * totalBufferCapacity) {
            logger.warn("Buffer reached 80 percent. Pausing and clearing 50 percent");
            handleConsumerThread(false);
            while (buffer.size() > (totalBufferCapacity / 2)) {
                flushBatch();
            }
            resumeConsumer();
        }

        if(buffer.offer(new PollBatch(trades,offsets,recievedTopic,partitions,ack))){
            totalTradeCount.addAndGet(incomingTradeCount);
        }

        if (totalTradeCount.get() >= BATCH_SIZE) {
            batchProcessorExecutor.execute(this::flushBatch);
        }
    }

    public synchronized void flushBatch() {
        if (buffer.isEmpty()) {
            return;
        }

        List<PollBatch> pollsInTheBatch = new ArrayList<>();
        List<Trade> batchTrades = new ArrayList<>(BATCH_SIZE);
        int currentRecordCount = 0;
        while (currentRecordCount < BATCH_SIZE) {
            PollBatch nextPoll = buffer.peek();
            if (nextPoll == null)
                break;
            if (currentRecordCount + nextPoll.getTradeProtos().size() > 5000 && !pollsInTheBatch.isEmpty())
                break;

            PollBatch poll = buffer.poll();
            pollsInTheBatch.add(poll);
            batchTrades.addAll(poll.getTradeProtos());
            currentRecordCount += poll.getTradeProtos().size();
        }
        if (batchTrades.isEmpty())
            return;
        try {
            Map<String, List<Trade>> grouped = batchTrades.stream().collect(Collectors.groupingBy(Trade::getSide));
            transactionService.processUnifiedBatch(grouped.getOrDefault("BUY", List.of()),
                    grouped.getOrDefault("SELL", List.of()), pollsInTheBatch);
            pollsInTheBatch.forEach(poll -> poll.getAck().acknowledge());
            totalTradeCount.addAndGet(-batchTrades.size());
            final List<PollBatch> pollsToReport = new ArrayList<>(pollsInTheBatch);
            rttmExecutor.execute(()->{     
                pollsToReport.forEach(poll->{
                    for(int i = 0; i < poll.getTradeProtos().size(); i++){
                        Trade trade = poll.getTradeProtos().get(i);
                        Long offset = (poll.getOffsets() != null && poll.getOffsets().size() > i) ? poll.getOffsets().get(i) : 0;
                        Integer partition = (poll.getPartitions() != null && poll.getPartitions().size() > i) ? poll.getPartitions().get(i) : 0; 
                        System.out.println("Processing tradeId: " + trade.getTradeId() +" at offset: " + offset + " partition: " + partition);

                        TradeEventPayload tradePayload = TradeEventPayload.builder()
                                .tradeId(trade.getTradeId())
                                .serviceName("pms-transactional")
                                .eventType(EventType.TRADE_COMMITTED)
                                .eventStage(EventStage.COMMITTED)
                                .eventStatus("COMMITTED")
                                .sourceQueue(poll.getListening_topic())
                                .targetQueue(publishingTopic)
                                .topicName(poll.getListening_topic())
                                .consumerGroup(consumerGroupId)
                                .partitionId(partition)
                                .offsetValue(offset)
                                .build();
                        try{
                            rttmClient.sendTradeEvent(tradePayload);
                            logger.info("RTTM trade event publish succeeded for tradeId={} after persisting in DB", trade.getTradeId());
                        } catch(Exception e){
                            logger.error("RTTM trade event publish failed for tradeId={} after persisting in DB",trade.getTradeId(), e);
                        }
                    }
                });
            }); 
        } 
        catch(DataAccessResourceFailureException e){
            logger.error("DB Connection failure. Pausing consumer.");
            for (int i = pollsInTheBatch.size() - 1; i >= 0; i--) {
                buffer.offerFirst(pollsInTheBatch.get(i));
            }
            handleConsumerThread(true);
        } catch (Exception e) {
            String rootMsg = e.getMessage();
            logger.error("Exception occured", rootMsg);
            throw e;
        }
    }

    @Override
    public void start() {
        logger.info("BatchProcessor starting: Initializing time-based flush");
        batchFlushScheduler.scheduleWithFixedDelay(this::flushBatch, Duration.ofMillis(FLUSH_INTERVAL_MS));
        this.isRunning = true;
    }

    @Override
    public void stop(Runnable callback) {
        logger.info("BatchProcessor stopping: Performing final flush");
        batchFlushScheduler.shutdown();
        if (!buffer.isEmpty()) {
            flushBatch();
        }
        this.isRunning = false;
        callback.run();
    }

    public void handleConsumerThread(boolean startDaemon) {
        synchronized (this) {
            if (isRecovering)
                return;
            isRecovering = true;
        }

        MessageListenerContainer container = kafkaListenerEndpointRegistry.getListenerContainer(CONSUMER_ID);
        if (container != null && !container.isContainerPaused()) {
            container.pause();
            logger.warn("Kafka Consumer paused.");
        }
        if (startDaemon) {
            logger.warn(" Starting background probe daemon...");
            startDaemon();
        }

    }

    private void resumeConsumer() {
        MessageListenerContainer container = kafkaListenerEndpointRegistry.getListenerContainer(CONSUMER_ID);
        if (container != null && container.isContainerPaused()) {
            container.resume();
            logger.info("Buffer cleared to 50% ({} batches). Resuming consumer.", buffer.size());
            synchronized (this) {
                isRecovering = false;
            }
        }
    }

    private void startDaemon() {
        recoveryTask = dbRecoveryScheduler.scheduleWithFixedDelay(() -> {
            try {
                jdbcTemplate.execute("SELECT 1");
                logger.info("Database is up! Resuming consumer and stopping daemon.");

                MessageListenerContainer container = kafkaListenerEndpointRegistry
                        .getListenerContainer(CONSUMER_ID);
                if (container != null)
                    container.resume();

                synchronized (this) {
                    isRecovering = false;
                    if (recoveryTask != null) {
                        recoveryTask.cancel(false);
                        recoveryTask = null;
                    }
                }
            } catch (Exception e) {
                logger.warn("Daemon: Database still down. Retrying in 10s...");
            }
        }, Duration.ofMillis(10000));
    }

    @Override
    public void stop() {
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}