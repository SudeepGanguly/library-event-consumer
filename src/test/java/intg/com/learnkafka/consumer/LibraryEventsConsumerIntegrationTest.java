package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entities.Book;
import com.learnkafka.entities.LibraryEvent;
import com.learnkafka.entities.LibraryEventType;
import com.learnkafka.repository.LibraryEventsRepository;
import com.learnkafka.services.LibraryEventsService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics={"library-events"},partitions = 3)
@TestPropertySource( properties =
        {
                "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}", /*Overriding producer props*/
                "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"  /*Overriding consumer props*/
        }
)

public class LibraryEventsConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<Integer,String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    private LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    private LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp(){
        for(MessageListenerContainer messageListenerContainer :endpointRegistry.getListenerContainers()){
            ContainerTestUtils.waitForAssignment(messageListenerContainer,embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown(){
        libraryEventsRepository.deleteAll();
    }

    @Test
    public void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = " {\"libraryEventId\":null,\"eventType\":\"NEW\",\"book\":{\"bookId\":456,\"name\":\"Kafka Using Spring Boot\",\"author\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        //when
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(3, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy,times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy,times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventsRepository.findAll();

        libraryEventList.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId()!=null;
            assertEquals(456,libraryEvent.getBook().getBookId());
        });
    }

    @Test
    public void updateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        //Save a record int he database for update later.
        String json = " {\"libraryEventId\":null,\"eventType\":\"NEW\",\"book\":{\"bookId\":456,\"name\":\"Kafka Using Spring Boot\",\"author\":\"Dilip\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json,LibraryEvent.class);
        libraryEventsRepository.save(libraryEvent);

        //Published the Updated record.
        libraryEvent.setEventType(LibraryEventType.UPDATE);
        Book updatedBook = Book.builder()
                                .bookId(456)
                                .name("Kafka Using Spring Boot-2.0")
                                .author("Sudeep")
                                .build();
        libraryEvent.setBook(updatedBook);
        String updateJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(),updateJson).get();

        //When
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(3,TimeUnit.SECONDS);

        //Then
        verify(libraryEventsConsumerSpy,times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy,times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        LibraryEvent updatedlibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();

        assertEquals("Kafka Using Spring Boot-2.0",updatedlibraryEvent.getBook().getName());
        assertEquals("Sudeep",updatedlibraryEvent.getBook().getAuthor());
    }


    @Test
    public void updateLibraryEvent_NullLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        Integer libraryEventId = null;
        String json = " {\"libraryEventId\":"+libraryEventId+",\"eventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"name\":\"Kafka Using Spring Boot\",\"author\":\"Dilip\"}}";

        kafkaTemplate.sendDefault(libraryEventId,json).get();

        //When
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(3,TimeUnit.SECONDS);

        //Then
        verify(libraryEventsConsumerSpy,times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy,times(1)).processLibraryEvent(isA(ConsumerRecord.class));

    }


    @Test
    public void updateLibraryEvent_RetriableibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        Integer libraryEventId = 00;
        String json = " {\"libraryEventId\":"+libraryEventId+",\"eventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"name\":\"Kafka Using Spring Boot\",\"author\":\"Dilip\"}}";

        kafkaTemplate.sendDefault(libraryEventId,json).get();

        //When
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(3,TimeUnit.SECONDS);

        //Then
        verify(libraryEventsConsumerSpy,times(4)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy,times(4)).processLibraryEvent(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy,times(1)).handleRecovery(isA(ConsumerRecord.class));
    }
}
