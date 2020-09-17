package com.learnkafka.service;

import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LibraryEventsService {

    @Autowired
    ObjectMapper objectMapper;
    
    @Autowired
    LibraryEventRepository libraryEventRepository;

    public void processLibraryEvents(ConsumerRecord<Integer, String> consumerRecord) throws JsonMappingException, JsonProcessingException {
	LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
	log.info("LibraryEvent : {} ",libraryEvent);
	
	switch(libraryEvent.getLibraryEventType()) {
	    case NEW:
	       //save operation
	       save(libraryEvent);
	       break;
	    case UPDATE:
		//validate library event
		validate(libraryEvent);
		//save
		save(libraryEvent);
	        break;
	    default:
		log.info("Invalid Library Event Type");
	}
    }

    private void validate(LibraryEvent libraryEvent) {
	if(StringUtils.isEmpty(libraryEvent.getLibraryEventId())) {
	    throw new IllegalArgumentException("Library event id is missing");
	}
	
	Optional<LibraryEvent> libraryEventOptional= libraryEventRepository.findById(libraryEvent.getLibraryEventId());
	if(!libraryEventOptional.isPresent()) {
	    throw new IllegalArgumentException("Not a valid library event");
	}
	log.info("Validation is successful for library event : {}",libraryEventOptional.get());	
    }

    private void save(LibraryEvent libraryEvent) {
	libraryEvent.getBook().setLibraryEvent(libraryEvent);
	libraryEventRepository.save(libraryEvent);
	log.info("Successfully persisted the library event : {}",libraryEvent);
    }
}
