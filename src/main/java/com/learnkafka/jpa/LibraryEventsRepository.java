package com.learnkafka.jpa;

import org.springframework.data.repository.CrudRepository;

import com.learnkafka.entity.LibraryEvent;

public interface LibraryEventsRepository extends CrudRepository<LibraryEvent, Integer>{

}
