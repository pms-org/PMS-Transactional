package com.pms.transactional.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Service;

import com.pms.transactional.TradeProto;

@Service
public class BatchProcessor implements SmartLifecycle{
    Logger logger = LoggerFactory.getLogger(BatchProcessor.class);

    @Autowired
    private BlockingQueue<TradeProto> buffer;
   
    @Autowired
    private TransactionService transactionService;

    private static final int BATCH_SIZE = 10;
<<<<<<< Updated upstream
=======
    private static final long FLUSH_INTERVAL_MS = 10000;
>>>>>>> Stashed changes

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private boolean isRunning = false;

    public void checkAndFlush(){
        if(buffer.size() >= BATCH_SIZE){
            flushBatch();
        }
    }

    public synchronized void flushBatch(){
        if(buffer.isEmpty()) return;

        while(!buffer.isEmpty()){
            List<TradeProto> batch = new ArrayList<>(BATCH_SIZE);
            buffer.drainTo(batch, BATCH_SIZE);

            if(batch.isEmpty()) return;

            List<TradeProto> buyBatch = batch.stream().filter(record->(record.getSide()).equals("BUY")).collect(Collectors.toList());

            List<TradeProto> sellBatch = batch.stream()
                                            .filter(record->(record.getSide()).equals("SELL"))
                                            .collect(Collectors.toList());

<<<<<<< Updated upstream
        processBuyBatch(buyBatch);
        processSellBatch(sellBatch);

=======
            transactionService.processUnifiedBatch(buyBatch, sellBatch);
        }
        
>>>>>>> Stashed changes
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public void start() {
        logger.info("BatchProcessor starting: Initializing time-based flush heartbeat");
        scheduler.scheduleWithFixedDelay(this::flushBatch, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        this.isRunning = true;
    }

    @Override
    public void stop(Runnable callback) {
        logger.info("BatchProcessor stopping: Performing final flush");
        scheduler.shutdown();

        if(!buffer.isEmpty()){
            flushBatch();
        }
        
        this.isRunning = false;
        callback.run();
    }

    @Override
    public void stop(){};

    @Override
    public int getPhase(){
        return Integer.MAX_VALUE;
    }

}