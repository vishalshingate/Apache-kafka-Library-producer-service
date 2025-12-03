package com.learnkafka.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record LibraryEvent(
    Integer libraryEventId,
    LibraryEventType libraryEventType,
    @NotNull
    @Valid
    Book book
) {
}
// to create dto starting from the java 14 we can use record , it is there for use to create the data container kind of classes
