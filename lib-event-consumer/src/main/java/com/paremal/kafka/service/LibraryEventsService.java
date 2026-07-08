package com.paremal.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paremal.kafka.entity.LibraryEvent;
import com.paremal.kafka.entity.LibraryEventType;
import com.paremal.kafka.jpa.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class LibraryEventsService {

    @Autowired
    ObjectMapper objectmapper;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {

        LibraryEvent libraryEvent = objectmapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("libraryEvent : {} ", libraryEvent);

        if (libraryEvent.getLibraryEventId() != null && (libraryEvent.getLibraryEventId() == 999)) {
            throw new RecoverableDataAccessException("Temporary Network Issue");
        }

        switch (libraryEvent.getLibraryEventType()) {

            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
                validate(libraryEvent);
                save(libraryEvent);
        }
    }

    private void validate(LibraryEvent libraryEvent) {

        if (libraryEvent.getLibraryEventId()==null){
            throw  new IllegalArgumentException("Library Event Id is missing");
        }

        Optional<LibraryEvent> optionalLibraryEvent=libraryEventsRepository
                .findById(libraryEvent.getLibraryEventId());
        if (!optionalLibraryEvent.isPresent()){

            throw new IllegalArgumentException(" not a valid library event");
        }
        log.info("Validation Successful for the library event : {} ", libraryEvent);
    }

    private void save(LibraryEvent libraryEvent) {

        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("save libraryEvent : {} ", libraryEvent);

    }

    public void handleRecovery(ConsumerRecord<Integer,String> consumerRecord) {

        Integer key = consumerRecord.key();
        String message = consumerRecord.value();

        var completableFuture = kafkaTemplate.sendDefault( key, message);
        completableFuture.whenComplete((sendResult, throwable) -> {
            if(throwable!=null){
                handleFailure(key, message, throwable);
            } else{
                handleSuccess(key, message, sendResult);
            }
        });
    }

    private void handleSuccess(Integer key, String message, SendResult<Integer, String> sendResult) {
       log.info(" Message Send successfully for the key : {} and the value is {} , partition is {}",
               key, message, sendResult.getRecordMetadata().partition());

    }

    private void handleFailure(Integer key, String message, Throwable throwable) {

        log.info("Error sending the message and the exception is {}", throwable.getMessage() );
        try{
            throw throwable;
        }catch(Throwable ex){
            log.error("Error in OnFailure: {}", ex.getMessage() );
        }
    }


}
