package com.pms.transactional.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import com.pms.transactional.Trade;
import com.pms.transactional.Transaction;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;


@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public Map<String, Object> transactionConsumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "transactions");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        props.put("schema.registry.url",schemaRegistryUrl);
        props.put("specific.protobuf.value.type", Transaction.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

    @Bean
    public ConsumerFactory<String, Transaction> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(transactionConsumerConfigs());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Transaction> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Transaction> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(5);
        factory.getContainerProperties().setPollTimeout(3000);
        return factory;
    }

    @Bean
    public Map<String, Object> tradeConsumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "trades");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,1000);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 600000);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("specific.protobuf.value.type", Trade.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    @Bean
    public ConsumerFactory<String, Trade> tradeConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(tradeConsumerConfigs());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Trade> tradekafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Trade> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(tradeConsumerFactory());
        factory.setConcurrency(5);
        factory.getContainerProperties().setPollTimeout(3000);
        factory.setBatchListener(true);
        factory.getContainerProperties()
           .setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
