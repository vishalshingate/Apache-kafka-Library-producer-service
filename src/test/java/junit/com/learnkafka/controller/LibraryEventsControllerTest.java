package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.dto.LibraryEvent;
import com.learnkafka.producer.LibraryEventProducer;
import com.learnkafka.util.TestUtil;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

//Test slice
@WebMvcTest(LibraryEventsController.class)
class LibraryEventsControllerTest {

    @Autowired
    MockMvc mockMvc;
    @Autowired
    ObjectMapper objectMapper;

    @MockitoBean
    LibraryEventProducer libraryEventProducer;

    @Test
    void postLibraryEvent() throws Exception {


        var json = objectMapper.writeValueAsString(TestUtil.libraryEventRecord());

        when(libraryEventProducer.sendLibraryEventAsyncApproach_2(isA(LibraryEvent.class)))
            .thenReturn(null);

        mockMvc.
            perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON)
            ).andExpect(status().isCreated());
    }

    @Test
    void postLibraryEventInvalidValues() throws Exception {


        var json = objectMapper.writeValueAsString(TestUtil.libraryEventRecordWithInvalidBook());

        when(libraryEventProducer.sendLibraryEventAsyncApproach_2(isA(LibraryEvent.class)))
            .thenReturn(null);

        mockMvc.
            perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON)
            ).andExpect(status().is4xxClientError());
    }
}
