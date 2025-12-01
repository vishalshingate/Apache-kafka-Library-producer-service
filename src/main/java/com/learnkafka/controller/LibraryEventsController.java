package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.dto.LibraryEvent;
import com.learnkafka.producer.LibraryEventProducer;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LibraryEventsController {

    private final LibraryEventProducer libraryEventProducer;

    private static final Logger logger = LoggerFactory.getLogger(LibraryEventsController.class);

    public LibraryEventsController(LibraryEventProducer libraryEventProducer) {
        this.libraryEventProducer = libraryEventProducer;
    }

    @PostMapping( "/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody  LibraryEvent libraryEvent) throws JsonProcessingException {


        log.info("libraryEvent: {} ", libraryEvent);
        libraryEventProducer.sendLibraryEvent(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
