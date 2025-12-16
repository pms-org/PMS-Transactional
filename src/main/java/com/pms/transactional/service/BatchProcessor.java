package com.pms.transactional.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pms.transactional.TradeProto;
import com.pms.transactional.dao.OutboxEventsDao;
import com.pms.transactional.dao.TradesDao;
import com.pms.transactional.dao.TransactionDao;
import com.pms.transactional.entities.OutboxEventEntity;
import com.pms.transactional.entities.TradesEntity;
import com.pms.transactional.entities.TransactionsEntity;

import jakarta.transaction.Transactional;

@Service
public class BatchProcessor{
    Logger logger = LoggerFactory.getLogger(BatchProcessor.class);
    @Autowired
    private BlockingQueue<TradeProto>  buffer;
    @Autowired
    private TradesDao tradesDao;
    @Autowired
    private TransactionDao transactionDao;
    @Autowired
    private OutboxEventsDao outboxDao;
    @Autowired
    private TransactionService transactionService;
    private static final int BATCH_SIZE = 1000;
    private static final long FLUSH_INTERVAL_MS = 10000;
    private long lastFlushTime = System.currentTimeMillis();

    public void checkAndFlush() {
        long now = System.currentTimeMillis();
        boolean sizeExceeded = buffer.size() >= BATCH_SIZE;
        boolean timeExceeded = (now - lastFlushTime) >= FLUSH_INTERVAL_MS;

        if(sizeExceeded || timeExceeded){
            flushBatch();
            lastFlushTime = now;
        }
    }

    @Transactional
    public void flushBatch() {
        List<TradeProto> batch = new ArrayList<>(BATCH_SIZE);
        buffer.drainTo(batch, BATCH_SIZE);

        if (batch.isEmpty()){
            return;
        }

        List<TradesEntity> trades = new ArrayList<>();
        List<TransactionsEntity> txns = new ArrayList<>();
        List<OutboxEventEntity> outbox = new ArrayList<>();

        for (TradeProto proto : batch){
            if(proto.getSide().equals("BUY")){
                transactionService.processBuy(proto, trades, txns, outbox);
            } 
            else if(proto.getSide().equals("SELL")){
                transactionService.processSell(proto, trades, txns, outbox);
            }
        }

        tradesDao.saveAll(trades);
        transactionDao.saveAll(txns);
        outboxDao.saveAll(outbox);

        System.out.println("Batch Flushed: Trades=" + trades.size() + " Transactions=" + txns.size() + " Outbox=" + outbox.size());
    }


}