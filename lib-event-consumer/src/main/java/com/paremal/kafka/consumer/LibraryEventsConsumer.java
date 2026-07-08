package com.paremal.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.paremal.kafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
//@KafkaListener

public class LibraryEventsConsumer {

    @Autowired
    private LibraryEventsService libraryEventsService;



    @KafkaListener(
            topics="${topics.retry:library-events.RETRY}" ,
            autoStartup = "${libraryListener.startup:true}",
            groupId = "libray-events-listener-group")
    public void onMessage(ConsumerRecord<Integer,String> consumerRecord) throws JsonProcessingException {
            log.info("ConsumerRecord : {} ", consumerRecord);
            libraryEventsService.processLibraryEvent(consumerRecord);
    }
}
