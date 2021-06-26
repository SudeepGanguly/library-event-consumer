package com.learnkafka.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.consumer.LibraryEventsConsumer;
import com.learnkafka.entities.Book;
import com.learnkafka.entities.LibraryEvent;
import com.learnkafka.repository.LibraryEventsRepository;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.transaction.Transactional;
import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {

    private ObjectMapper objectMapper;
    private LibraryEventsRepository libraryEventsRepository;
    private KafkaTemplate<Integer,String> kafkaTemplate;

    LibraryEventsService(ObjectMapper objectMapper,
                         LibraryEventsRepository libraryEventsRepository,
                         KafkaTemplate kafkaTemplate){
        this.objectMapper=objectMapper;
        this.libraryEventsRepository = libraryEventsRepository;
        this.kafkaTemplate=kafkaTemplate;
    }

    @Transactional
    public void processLibraryEvent(ConsumerRecord<Integer,String> consumerRecord) throws JsonProcessingException {
        //Consume the library Event
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(),LibraryEvent.class);
        log.info("LibraryEvent: {}",libraryEvent);

        //If below condition satisfies then consider it as recoverable exception
        if(libraryEvent.getLibraryEventId()!=null && libraryEvent.getLibraryEventId()==000){
            throw new RecoverableDataAccessException("Templorary Network Issue");
        }

        //Persist the Library Event
        switch(libraryEvent.getEventType()){
            case NEW:
                        save(libraryEvent);
                        break;
            case UPDATE:
                        validate(libraryEvent);
                        save(libraryEvent);
                        break;
            default:
                log.info("Invalid Library Event Type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if(libraryEvent==null || libraryEvent.getLibraryEventId()==null){
            throw new IllegalArgumentException("LibraryEventId cannot be null for Update");
        }

        Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository
                                            .findById(libraryEvent.getLibraryEventId());

        if(!libraryEventOptional.isPresent()){
            throw new IllegalArgumentException("Not a valid LibraryEvent");
        }

        log.info("Validation is Successfull for the library Event : {} ",libraryEventOptional.get());
    }

    private void save(LibraryEvent libraryEvent) {
        //Mapping the Book to the LibraryEvent - Building the Mapping
        //libraryEvent.getBook().setLibraryEvent(libraryEvent);
        Book book = libraryEvent.getBook();
        book.setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("Successfully Persisted the Library Event: {} ",libraryEvent);
    }

   public void handleRecovery(ConsumerRecord<Integer,String> record){
        Integer key = record.key();
        String message = record.value();
       ListenableFuture<SendResult<Integer,String>> listenableFuture =
                                    kafkaTemplate.sendDefault(key,message);

       listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
           @Override
           public void onFailure(Throwable ex) {
               handleFailure(key,message,ex);
           }

           @Override
           public void onSuccess(SendResult<Integer, String> result) {
               handleSuccess(key,message,result);
           }
       });
   }

    public void handleFailure(Integer key , String value , Throwable ex){
        log.error("Error Sending the message and the exception is {}",ex.getMessage());
        try {
            throw ex;
        } catch (Throwable throwable) {
            log.error("Error onFailure : {} ",throwable.getMessage());
        }
    }

    public void handleSuccess(Integer key , String value , SendResult<Integer,String> result){
        log.info("Mesage sent Successfully for the key : {} and the value is {} " +
                "and the partition is {}",key,value,result.getRecordMetadata().partition());
    }
}
