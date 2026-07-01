package com.paremal.kafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.paremal.kafka.domain.LibraryEvent;
import com.paremal.kafka.domain.LibraryEventType;
import com.paremal.kafka.producer.LibraryEventProducer;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@Slf4j
public class LibraryEventsController {

    @Autowired
    LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<?> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {

        if(LibraryEventType.NEW !=libraryEvent.libraryEventType()){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only NEW event type is supported");
        }

        libraryEventProducer.sendLibraryEventType2(libraryEvent);

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {


        ResponseEntity<String> BAD_REQUEST = validateLibraryEvent(libraryEvent);
        if(BAD_REQUEST !=null) return BAD_REQUEST;

        libraryEventProducer.sendLibraryEventType2(libraryEvent);
        log.info("after produce call");
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }

    private static ResponseEntity<String> validateLibraryEvent(@Valid LibraryEvent libraryEvent) {
        if(libraryEvent.libraryEventId()==null){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Library event Id is missing");
        }
        if(!LibraryEventType.UPDATE.equals(libraryEvent.libraryEventType())){
            log.info("inside the if block");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only update supported");
        }
        return null;
    }


}
