package com.pms.transactional.config;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.pms.transactional.TradeProto;


@Configuration
public class BufferConfig{
    @Bean
    public BlockingQueue<TradeProto> eventBuffer() {
        return new LinkedBlockingQueue<>(50000);  
    }
}