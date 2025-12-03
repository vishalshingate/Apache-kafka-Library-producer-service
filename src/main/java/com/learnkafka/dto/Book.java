package com.learnkafka.dto;

import jakarta.validation.constraints.NotNull;

public record Book(
    @NotNull
    Integer bookId,
    @NotNull
    String bookName,
    @NotNull
    String bookAuthor
) {
}
