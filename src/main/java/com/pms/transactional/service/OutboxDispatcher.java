// package com.pms.transactional.service;

// import java.util.List;

// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.kafka.core.KafkaTemplate;
// import org.springframework.scheduling.annotation.EnableScheduling;
// import org.springframework.scheduling.annotation.Scheduled;
// import org.springframework.stereotype.Service;

// import com.pms.transactional.TransactionProto;
// import com.pms.transactional.dao.OutboxEventsDao;
// import com.pms.transactional.entities.OutboxEventEntity;

// @Service
// @EnableScheduling
// public class OutboxPoller {

//     @Autowired
//     OutboxEventsDao outboxDao;

//     @Autowired
//     private KafkaTemplate<String, Object> kafkaTemplate;

//     @Scheduled(fixedRate = 10000)
//     public void pollAndPublish() {
//         List<OutboxEventEntity> pendingList = outboxDao.findByStatusOrderByCreatedAt("PENDING");

//         for (OutboxEventEntity event : pendingList) {
//             try{
//                 TransactionProto proto = TransactionProto.parseFrom(event.getPayload());
//                 kafkaTemplate.send("transactions-topic", proto).get();
//                 event.setStatus("SENT");
//                 outboxDao.save(event);

//             } catch(Exception e) {
//                 e.printStackTrace();
//                 event.setStatus("FAILED");
//                 outboxDao.save(event);
//             }
//         }
        

//     }

// }

package com.pms.transactional.service;

import java.util.List;

import org.springframework.context.SmartLifecycle;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import com.pms.transactional.dao.OutboxEventsDao;
import com.pms.transactional.entities.OutboxEventEntity;

@Service
public class OutboxDispatcher implements SmartLifecycle {

    private final OutboxEventsDao outboxDao;
    private final OutboxEventProcessor processor;
    private final AdaptiveBatchSizer batchSizer;

    private volatile boolean running = false;
    private long backoffMs = 0;

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
        new Thread(this::dispatchLoop, "outbox-dispatcher").start();
    }

    private void dispatchLoop() {
        while (running) {
            try {
                if (backoffMs > 0) {
                    Thread.sleep(backoffMs);
                }

                long start = System.currentTimeMillis();
                int limit = batchSizer.getCurrentSize();

                List<OutboxEventEntity> batch =
                        outboxDao.findByStatusOrderByCreatedAt(
                                "PENDING",
                                PageRequest.of(0, limit)
                        );

                if (batch.isEmpty()) {
                    batchSizer.reset();
                    backoffMs = 0;
                    Thread.sleep(50);
                    continue;
                }

                ProcessingResult result = processor.process(batch);

                // PREFIX-SAFE UPDATE
                if (!result.successfulIds().isEmpty()) {
                    outboxDao.markAsSent(result.successfulIds());
                }

                // POISON PILL
                if (result.poisonPill() != null) {
                    // TODO: DLQ
                    outboxDao.delete(result.poisonPill());
                }

                // SYSTEM FAILURE
                if (result.systemFailure()) {
                    backoffMs = backoffMs == 0 ? 1000 : Math.min(backoffMs * 2, 30000);
                    continue;
                }

                long duration = System.currentTimeMillis() - start;
                batchSizer.adjust(duration, batch.size());
                backoffMs = 0;

            } catch (Exception e) {
                backoffMs = 1000;
            }
        }
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
