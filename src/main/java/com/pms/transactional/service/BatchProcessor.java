package com.pms.transactional.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledFuture;
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

import com.pms.transactional.Trade;
import com.pms.transactional.wrapper.TradeRecord;


@Service
public class BatchProcessor implements SmartLifecycle{
    Logger logger = LoggerFactory.getLogger(BatchProcessor.class);

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private LinkedBlockingDeque<TradeRecord> buffer;

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

    @Value("${app.batch.size}")
    private int BATCH_SIZE;

    @Value("${app.buffer.size}")
    private int totalBufferCapacity;

    @Value("${app.flush-interval-ms}")
    private long FLUSH_INTERVAL_MS;

    @Value("${app.trades.consumer.consumer-id}")
    private String CONSUMER_ID;

    private boolean isRecovering = false;
    private ScheduledFuture<?> recoveryTask; 
    private boolean isRunning = false;

    public void checkAndFlush(){
        if(buffer.size() >= BATCH_SIZE){
            batchProcessorExecutor.execute(this::flushBatch);
        }
    }

    public synchronized void flushBatch(){
        if(buffer.isEmpty()) return;

        List<TradeRecord> batch = new ArrayList<>(BATCH_SIZE);
        buffer.drainTo(batch, BATCH_SIZE);

        List<Trade> batchTrades = batch.stream()
                                        .map(TradeRecord::getTradeProto).toList();

        try{
            Map<String, List<Trade>> grouped = batchTrades.stream().collect(Collectors.groupingBy(Trade::getSide));
            transactionService.processUnifiedBatch(grouped.getOrDefault("BUY", List.of()), grouped.getOrDefault("SELL", List.of()));
            batch.stream().map(TradeRecord::getAck).distinct().forEach(Acknowledgment::acknowledge);
        }
        catch(DataAccessResourceFailureException e) {
            logger.error("DB Connection failure. Pausing consumer.");
            for(int i=batch.size()-1; i>=0;i--){
                buffer.offerFirst(batch.get(i));
            }
            handleConsumerThread(true);
            throw e;
        }
        catch(Exception e){
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
        if(!buffer.isEmpty()){
            flushBatch();
        }
        this.isRunning = false;
        callback.run();
    }

    

    public void handleConsumerThread(boolean startDaemon){
        synchronized(this){
            if(isRecovering){
                return;
            }
            isRecovering=true;
        }
        MessageListenerContainer container = kafkaListenerEndpointRegistry.getListenerContainer(CONSUMER_ID);
        if(container != null){
            container.pause();
            logger.warn("Kafka Consumer paused.");
        }
        if(startDaemon){
            logger.warn(" Starting background probe daemon...");
            startDaemon();
        }
        else{
            batchProcessorExecutor.execute(()->{
                logger.info("Buffer is full.Flushing the buffer.");
                while(buffer.remainingCapacity() < 0.5*totalBufferCapacity){
                    flushBatch();
                }
                logger.info("50 percent of the buffer is freed. Resuming consumer..");
                if(container != null){
                    container.resume();
                    logger.info("Consumer resumed..");
                }
                synchronized(this){
                    if(!isRecovering){
                        return;
                    }
                    isRecovering=false;
                }
            }); 
        }
        
    }
        
    private void startDaemon() {
        recoveryTask = dbRecoveryScheduler.scheduleWithFixedDelay(() -> {
            try{
                jdbcTemplate.execute("SELECT 1");
                logger.info("Database is up! Resuming consumer and stopping daemon.");

                MessageListenerContainer container = kafkaListenerEndpointRegistry
                        .getListenerContainer(CONSUMER_ID);
                if(container != null) container.resume();

                synchronized(this){
                    isRecovering = false;
                    if (recoveryTask != null) {
                        recoveryTask.cancel(false);
                        recoveryTask = null;
                    }
                }
            } 
            catch(Exception e) {
                logger.warn("Daemon: Database still down. Retrying in 10s...");
            }
        }, Duration.ofMillis(10000));
    }

    @Override
    public void stop(){}

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public int getPhase(){
        return Integer.MAX_VALUE;
    }
}