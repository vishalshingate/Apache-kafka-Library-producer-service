package com.learnkafka.dto;

public record LibraryEvent(
    Integer libraryEventId,
    LibraryEventType libraryEventType,
    Book book
) {
}
// to create dto starting from the java 14 we can use record , it is there for use to create the data container kind of classes
