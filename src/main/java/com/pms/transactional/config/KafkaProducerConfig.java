package com.pms.transactional.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public NewTopic createTopic() {
        return TopicBuilder.name("transactions-topic")
                .partitions(5)
                .replicas(1)
                .build();
    }
}
