package com.pms.transactional.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class ThreadExecutorConfig {

    @Bean(name = "batchExecutor")
    public ThreadPoolTaskExecutor batchProcessorExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(1);
        executor.setThreadNamePrefix("batch-processing-exec-");
        executor.initialize();
        return executor;
    }

    @Bean(name = "outboxExecutor")
    public ThreadPoolTaskExecutor outboxExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(1);
        executor.setThreadNamePrefix("outbox-executor-");
        executor.initialize();
        return executor;
    }

    @Bean(name="rttmPostConsumeExecutor")
    public ThreadPoolTaskExecutor rttmPostConsumeExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(25);
        executor.setThreadNamePrefix("rttm-post-consume-executor-");
        executor.setRejectedExecutionHandler((runnable, exec) -> {
            try{
                exec.getQueue().put(runnable);
            } catch(InterruptedException e){
                Thread.currentThread().interrupt();
            }
        });        
        executor.initialize();
        return executor;
    }

    @Bean(name="rttmPostSaveExecutor")
    public ThreadPoolTaskExecutor rttmPostSaveExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(25);
        executor.setThreadNamePrefix("rttm-post-save-executor-");
        executor.setRejectedExecutionHandler((runnable, exec) -> {
            try{
                exec.getQueue().put(runnable);
            } catch(InterruptedException e){
                Thread.currentThread().interrupt();
            }
        });        
        executor.initialize();
        return executor;
    }
}
