package com.pms.transactional.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.pms.transactional.dao.InvalidTradesDao;
import com.pms.transactional.dao.OutboxEventsDao;
import com.pms.transactional.entities.InvalidTradesEntity;
import com.pms.transactional.entities.OutboxEventEntity;

@Service
public class OutboxDispatcher implements SmartLifecycle {

    @Autowired
    private InvalidTradesDao invalidTradesDao;

    private OutboxEventsDao outboxDao;
    private OutboxEventProcessor processor;
    private AdaptiveBatchSizer batchSizer;

    private volatile boolean running = false;

    public OutboxDispatcher(
            OutboxEventsDao outboxDao,
            OutboxEventProcessor processor,
            AdaptiveBatchSizer batchSizer) {
        this.outboxDao = outboxDao;
        this.processor = processor;
        this.batchSizer = batchSizer;
    }

    @Override
    public void start() {
        running = true;
        Thread t = new Thread(this::loop, "outbox-dispatcher");
        t.setDaemon(true);
        t.start();
    }

    private void loop() {
        while (running) {
            try {
                ProcessingResult result = dispatchOnce();
                if (result.systemFailure()) {
                    batchSizer.reset();
                    Thread.sleep(3000); // wait ONLY on system failure
                } else if (result.successfulIds().isEmpty()
                        && result.poisonPill() == null) {
                    Thread.sleep(200); //  wait (empty outbox)
                }
            } catch (Exception e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    @Transactional
    protected ProcessingResult dispatchOnce() {

        int limit = batchSizer.getCurrentSize();

        List<OutboxEventEntity> batch = outboxDao.findPendingWithPortfolioXactLock(limit);

        if (batch.isEmpty()) {
            batchSizer.reset();
            return ProcessingResult.success(List.of());
        }

        // START timing
        long start = System.currentTimeMillis();

        ProcessingResult result = processor.process(batch);

        // END timing
        long duration = System.currentTimeMillis() - start;

        if (!result.successfulIds().isEmpty()) {
            outboxDao.markAsSent(result.successfulIds());
        }

        if (!result.systemFailure() && result.poisonPill() == null) {
            batchSizer.adjust(duration, batch.size());
        }

        if (result.poisonPill() != null) {

            OutboxEventEntity poison = result.poisonPill();

            InvalidTradesEntity invalid = new InvalidTradesEntity();
            invalid.setAggregateId(poison.getAggregateId());
            invalid.setPayload(poison.getPayload());
            invalid.setErrorMessage("Poison pill – processing failed");

            invalidTradesDao.save(invalid);
            outboxDao.markAsFailed(poison.getTransactionOutboxId());
        }

        return result;
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}

