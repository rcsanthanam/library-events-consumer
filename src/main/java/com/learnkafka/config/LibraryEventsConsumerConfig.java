package com.learnkafka.config;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

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
	    log.info("Exception in configConsumer thrown exception is {} and data is {}",throwException.getMessage(),data);
	});
	factory.setRetryTemplate(retryTemplate());
	return factory;
    }

    private RetryTemplate retryTemplate() {
	RetryTemplate retryTemplate = new RetryTemplate();
	retryTemplate.setRetryPolicy(retryPolicy());
	retryTemplate.setBackOffPolicy(backOffPolicy());
	return retryTemplate;
    }

    private BackOffPolicy backOffPolicy() {
	FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
	fixedBackOffPolicy.setBackOffPeriod(1000);
	return fixedBackOffPolicy;
    }

    private RetryPolicy retryPolicy() {
	/*SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
	simpleRetryPolicy.setMaxAttempts(3);*/
	Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
	retryableExceptions.put(IllegalArgumentException.class,false);
	retryableExceptions.put(RecoverableDataAccessException.class,true);
	SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3,retryableExceptions,true);
	return simpleRetryPolicy;
    }
}
