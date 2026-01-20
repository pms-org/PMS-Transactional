package com.pms.transactional.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class ThreadExecutorConfig {
    
    @Bean(name="batchExecutor")
    public ThreadPoolTaskExecutor batchProcessorExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);   
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(1); 
        executor.setThreadNamePrefix("batch-processing-exec-");
        executor.initialize();
        return executor;
    }

    @Bean(name="outboxExecutor")
    public ThreadPoolTaskExecutor outboxExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(1);
        executor.setThreadNamePrefix("outbox-executor-");
        executor.initialize();
        return executor;
    }
}
