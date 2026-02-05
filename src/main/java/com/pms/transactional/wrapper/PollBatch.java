package com.pms.transactional.wrapper;

import java.util.List;

import org.springframework.kafka.support.Acknowledgment;

import com.pms.transactional.Trade;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class PollBatch {
    List<Trade> tradeProtos;
    List<Long> offsets;
    int partition;
    Acknowledgment ack;
}
