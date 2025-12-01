package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.dto.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class LibraryEventProducer {

    private Logger logger = LoggerFactory.getLogger(LibraryEventProducer.class);
    @Value("${spring.kafka.topic}")
    private String topicName;

    private final ObjectMapper objectMapper;

    private final KafkaTemplate<Integer, String> kafkaTemplate;


    public LibraryEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }


    public CompletableFuture<SendResult<Integer,String>> sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);
        var completableFuture = kafkaTemplate.send(topicName,key, value);

      return completableFuture
            .whenComplete((sendResult, throwable) -> {
                if (throwable != null) {
                    handleFailure(key, value, throwable);
                }
                else {
                    handleSuccess(key, value, sendResult);
                }
            });

    }

    private void handleFailure(Integer key, String value, Throwable throwable) {

        logger.error("Error sending the message and the exception : {} ", throwable.getMessage(), throwable);

    }
    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {

        logger.info("Message Sent Successfully for the key : {} value : {} , partition : {} ",
            key, value, sendResult.getRecordMetadata().partition());

    }
}
