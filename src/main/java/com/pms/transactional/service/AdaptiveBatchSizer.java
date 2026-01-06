package com.pms.transactional.service;

import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AdaptiveBatchSizer{

    @Value("${app.outbox.target-latency-ms:200}")
    private long targetLatencyMs;

    @Value("${app.outbox.min-batch:10}")
    private int minBatchSize;

    @Value("${app.outbox.max-batch:500}")
    private int maxBatchSize;

    private final AtomicInteger currentBatchSize = new AtomicInteger(10);

    public void adjust(long timeTakenMs, int recordsProcessed){
        int current = currentBatchSize.get();
        int next = current;

        // Drain phase
        if (recordsProcessed < current){
            next = minBatchSize;
        }
        // Growth / shrink phase
        else if(timeTakenMs < targetLatencyMs){
            next = Math.min((int)(current * 1.2), maxBatchSize);
        } 
        else{
            next = Math.max((int)(current * 0.7), minBatchSize);
        }

        currentBatchSize.set(next);
    }

    public int getCurrentSize(){
        return currentBatchSize.get();
    }

    public void reset(){
        currentBatchSize.set(minBatchSize);
    }
}

