package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.dto.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

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

    /**
     * This is an async call
     * @param libraryEvent
     * @return
     * @throws JsonProcessingException
     */
    public CompletableFuture<SendResult<Integer,String>> sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

       /**  *
        * 1.blocking call - get metadata about the kafka cluster (only first request/ after that will have metadata unless it not refreshed)
        * 2.send message async - return a completable future
        */
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

    /**
     * This is an Sync call
     * @param libraryEvent
     * @return
     * @throws JsonProcessingException
     */
    public SendResult<Integer,String> sendLibraryEventSync(LibraryEvent libraryEvent)
        throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        /**  *
         * 1.blocking call - get metadata about the kafka cluster (only first request/ after that will have metadata unless it not refreshed)
         * 2.block and wait until the message is sent to kafka topic - synchronous call
         */

        var sendResult = kafkaTemplate.send(topicName,key, value)
            //.get(); you can wait like this also
            .get(5, java.util.concurrent.TimeUnit.SECONDS);
        handleSuccess(key, value, sendResult);

        return sendResult;

    }

    /**
     * This is an async call
     * @param libraryEvent
     * @return
     * @throws JsonProcessingException
     */
    public CompletableFuture<SendResult<Integer,String>> sendLibraryEventAsyncApproach_2(LibraryEvent libraryEvent) throws JsonProcessingException {


        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);
       /* Create Producer Record */
        // we are using producer record here to show how to add headers if required in future
        var producerRecord = buildProducerRecord(key, value);

        /**  *
         * 1.blocking call - get metadata about the kafka cluster (only first request/ after that will have metadata unless it not refreshed)
         * 2.send message async - return a completable future
         */
        var completableFuture = kafkaTemplate.send(producerRecord);
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

    private ProducerRecord<Integer,String > buildProducerRecord(Integer key, String value) {
    return new ProducerRecord<Integer, String>(topicName, key, value);
    }
}
