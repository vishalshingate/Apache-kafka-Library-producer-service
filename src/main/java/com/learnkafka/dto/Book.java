package com.learnkafka.dto;

public record Book(
    Integer bookId,
    String bookName,
    String bookAuthor
) {
}
