package com.pms.transactional.config;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.pms.transactional.Trade;


@Configuration
public class BufferConfig{
    @Value("${app.buffer.size}")
    private int bufferSize;
    @Bean
    public LinkedBlockingDeque<Trade> protoBuffer() {
        return new LinkedBlockingDeque<Trade>(bufferSize);
    }

}