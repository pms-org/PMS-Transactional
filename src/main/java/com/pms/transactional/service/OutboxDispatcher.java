package com.pms.transactional.service;

import java.util.List;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.beans.factory.annotation.Value;

import com.pms.transactional.dao.InvalidTradesDao;
import com.pms.transactional.dao.OutboxEventsDao;
import com.pms.transactional.entities.InvalidTradesEntity;
import com.pms.transactional.entities.OutboxEventEntity;
import com.pms.transactional.exceptions.PoisonPillException;

import lombok.SneakyThrows;

@Service
public class OutboxDispatcher implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxDispatcher.class);

    private final OutboxEventsDao outboxdao;
    private final InvalidTradesDao invalidtrdesdao;
    private final OutboxEventProcessor processor;
    private final AdaptiveBatchSizer batchSizer;
    private final Executor taskExecutor;
    private final TransactionTemplate transactionTemplate;

    @Value("${app.outbox.system-failure-backoff-ms:1000}")
    private long systemFailureBackoffMs;

    @Value("${app.outbox.max-backoff-ms:30000}")
    private long maxBackoffMs;

    private volatile boolean running = false;
    private volatile long currentBackoff = 0;

    public OutboxDispatcher(
            OutboxEventsDao outboxdao,
            InvalidTradesDao invalidtrdesdao,
            OutboxEventProcessor processor,
            AdaptiveBatchSizer batchSizer,
            @Qualifier("outboxExecutor") Executor taskExecutor,
            TransactionTemplate transactionTemplate) {

        this.outboxdao = outboxdao;
        this.invalidtrdesdao = invalidtrdesdao;
        this.processor = processor;
        this.batchSizer = batchSizer;
        this.taskExecutor = taskExecutor;
        this.transactionTemplate = transactionTemplate;
    }

    @SneakyThrows
    @Override
    public void start() {
        if (running)
            return;
        log.info("------------Starting Portfolio-Ordered Outbox Dispatcher...---------------");
        running = true;

        // Submit the long-running loop to the Spring-managed thread pool
        taskExecutor.execute(this::dispatchLoop);
    }

    @Override
    public void stop() {
        log.info("-------------------Stopping Outbox Dispatcher...-----------------------");
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return SmartLifecycle.super.isAutoStartup();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1000;
    }

    private void dispatchLoop() {
        while (running) {
            try {
                // Apply backoff if previous iteration had system failure
                if (currentBackoff > 0) {
                    log.warn("System failure backoff active: sleeping {}ms", currentBackoff);
                    sleep(currentBackoff);
                }

                long startTime = System.currentTimeMillis();

                // STEP 1: Fetch batch with advisory lock-based portfolio isolation
                int limit = batchSizer.getCurrentSize();
                List<OutboxEventEntity> batch = transactionTemplate
                        .execute(status -> outboxdao.findPendingWithPortfolioXactLock(limit));

                if (batch == null || batch.isEmpty()) {
                    // No work to do (or all portfolios locked by other pods)
                    batchSizer.reset();
                    currentBackoff = 0; // Reset backoff on idle
                    sleep(50);
                    continue;
                }

                // STEP 2: Group by portfolio (maintains insertion order)
                // Note: All events in batch are from portfolios THIS pod locked
                var eventsByPortfolio = new java.util.LinkedHashMap<java.util.UUID, java.util.ArrayList<OutboxEventEntity>>();
                for (OutboxEventEntity event : batch) {
                    eventsByPortfolio.computeIfAbsent(event.getPortfolioId(), k -> new java.util.ArrayList<>())
                            .add(event);
                }

                // STEP 3: Process each portfolio's batch (maintains strict ordering)
                for (var entry : eventsByPortfolio.entrySet()) {
                    java.util.UUID portfolioId = entry.getKey();
                    List<OutboxEventEntity> portfolioBatch = entry.getValue();

                    // Process this portfolio's events (prefix-safe, failure-classified)
                    ProcessingResult result = processor.process(portfolioBatch);

                    // STEP 4: Handle results within transaction
                    transactionTemplate.execute(status -> {
                        // 4a. Mark successful prefix as SENT (SINGLE DB UPDATE per portfolio)
                        if (!result.hasSystemFailure() && !result.getSuccessfulIds().isEmpty()) {
                            outboxdao.markAsSent(result.getSuccessfulIds());
                            log.info("Portfolio {}: Marked {} events as SENT", portfolioId,
                                    result.getSuccessfulIds().size());
                            
                        }

                        // 4b. Handle poison pill (if any)
                        if (result.hasPoisonPill()) {
                            PoisonPillException ppe = result.getPoisonPill();
                            OutboxEventEntity poisonEvent = findEventById(portfolioBatch, ppe.getEventId());
                            if (poisonEvent != null) {
                                moveToDlq(poisonEvent, ppe.getMessage());
                                log.warn("Portfolio {}: Routed poison pill {} to DLQ", portfolioId, ppe.getEventId());
                            }
                        }

                        return null;
                    });

                    // STEP 5: Backoff strategy for system failures
                    if (result.hasSystemFailure()) {
                        // Exponential backoff
                        currentBackoff = currentBackoff == 0 ? systemFailureBackoffMs
                                : Math.min(currentBackoff * 2, maxBackoffMs);
                        log.error("Portfolio {}: System failure detected. Backoff={}ms. Will retry on next iteration.",
                                portfolioId, currentBackoff);
                        break; // Stop processing other portfolios, apply backoff
                    } else {
                        // Success or poison pill (not a system issue)
                        currentBackoff = 0; // Reset backoff
                    }
                }

                // Feedback for adaptive sizing (only if no system failure)
                if (currentBackoff == 0) {
                    long duration = System.currentTimeMillis() - startTime;
                    batchSizer.adjust(duration, batch.size());
                }

            } catch (Exception e) {
                log.error("Unexpected error in dispatch loop", e);
                // Defensive: backoff and continue
                currentBackoff = systemFailureBackoffMs;
                sleep(currentBackoff);
            }
        }
    }

    private void moveToDlq(OutboxEventEntity event, String errorMsg) {

        InvalidTradesEntity invalid = new InvalidTradesEntity();
        invalid.setAggregateId(event.getAggregateId());
        invalid.setPayload(event.getPayload());
        invalid.setErrorMessage("Poison pill: " + errorMsg);

        invalidtrdesdao.save(invalid);

        // IMPORTANT: Do NOT delete → mark as FAILED to preserve audit trail
        outboxdao.markAsFailed(event.getTransactionOutboxId());
    }

    private OutboxEventEntity findEventById(
            List<OutboxEventEntity> batch,
            java.util.UUID eventId) {

        return batch.stream()
                .filter(e -> e.getTransactionOutboxId().equals(eventId))
                .findFirst()
                .orElse(null);
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }

}
