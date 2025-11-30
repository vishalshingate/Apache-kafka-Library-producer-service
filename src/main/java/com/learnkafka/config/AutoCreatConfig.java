package com.learnkafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class AutoCreatConfig {

    @Value("${spring.kafka.topic}")
    private String topicName;
    @Bean
    public NewTopic libraryEvent() {
        return TopicBuilder
            .name(topicName )
            .partitions(3)
            .replicas(3)
            .build();
    }
}
