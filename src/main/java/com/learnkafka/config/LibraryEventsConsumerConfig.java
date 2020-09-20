package com.learnkafka.config;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
	    ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
	    ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
	ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
	configurer.configure(factory,kafkaConsumerFactory.getIfAvailable());
	factory.setConcurrency(3);
	//factory.getContainerProperties().setAckMode(AckMode.MANUAL);
	factory.setErrorHandler((throwException,data)->{
	    log.info("Exception in configCosumer thrown exception is {} and data is {}",throwException.getMessage(),data);
	});
	return factory;
    }
}
