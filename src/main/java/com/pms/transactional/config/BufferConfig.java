package com.pms.transactional.config;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.pms.transactional.TradeProto;


@Configuration
public class BufferConfig{
    @Value("${app.buffer.size}")
    private long bufferSize;
    @Bean
    public BlockingQueue<TradeProto> protoBuffer() {
        return new LinkedBlockingQueue<>(bufferSize);
    }

}