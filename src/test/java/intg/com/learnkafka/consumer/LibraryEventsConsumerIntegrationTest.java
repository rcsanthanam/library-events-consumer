package com.learnkafka.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.ArgumentMatchers.isA;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.LibraryEventsRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Spy;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.service.LibraryEventsService;

import javax.swing.text.html.Option;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events"},partitions = 3)
@TestPropertySource(properties = {
	"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
	"spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
class LibraryEventsConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;
    
    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;
    
    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;
    
    @Spy
    ObjectMapper objectMapper;
    
    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;
    
    @SpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @Autowired
	LibraryEventsRepository libraryEventsRepository;
    
    @BeforeEach
    void setUp() throws Exception {
		for(MessageListenerContainer messageListenerContainer:endpointRegistry.getAllListenerContainers()) {
			ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
		}
    }

    @AfterEach
	void tearDown() throws Exception{
    	libraryEventsRepository.deleteAll();
	}
    
    @Test
    void publishNewLibraryEvents() throws Exception {
	
	// given
	Book book = Book.builder()
		        .bookId(123)
		        .bookName("Kafka using Spring Boot")
		        .bookAuthor("Ravi")
		        .build();
	
	LibraryEvent libraryEvent = LibraryEvent.builder()
		                                 .libraryEventId(456)
		                                 .book(book)
		                                 .build();
	
	String json = objectMapper.writeValueAsString(libraryEvent);
	kafkaTemplate.sendDefault(json).get();
	
	//when
	CountDownLatch latch = new CountDownLatch(1);
	latch.await(3, TimeUnit.SECONDS);
	
	//then
	verify(libraryEventsConsumerSpy,times(1)).onMessage(isA(ConsumerRecord.class));
	verify(libraryEventsServiceSpy,times(1)).processLibraryEvents((isA(ConsumerRecord.class)));

	Iterable<LibraryEvent> libraryEventList = libraryEventsRepository.findAll();

	libraryEventList.forEach(event ->{
		assertEquals(456,event.getLibraryEventId());
	});
    }

    @Test
    void publishUpdateLibraryEvent() throws Exception{
    	//given
		Book book = Book.builder()
				.bookId(123)
				.bookName("Kafka using Spring Boot")
				.bookAuthor("Ravi")
				.build();

		LibraryEvent libraryEvent = LibraryEvent.builder()
				.libraryEventId(null)
				.book(book)
				.build();
		//Adding bidirectional hibernate relation
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventsRepository.save(libraryEvent);

		Book updatedBook = Book.builder()
				               .bookId(123)
				               .bookName("Kafka using Spring Book 2.x")
				               .bookAuthor("Ravi")
				               .build();
		LibraryEvent updatedLibraryEvent = LibraryEvent.builder()
				                                       .libraryEventId(libraryEvent.getLibraryEventId())
				                                       .libraryEventType(LibraryEventType.UPDATE)
				                                       .book(updatedBook)
				                                       .build();
		String updatedJson = objectMapper.writeValueAsString(updatedLibraryEvent);
		kafkaTemplate.sendDefault(updatedLibraryEvent.getLibraryEventId(),updatedJson).get();

		//when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		//then
		//verify(libraryEventsConsumerSpy,times(1)).onMessage(isA(ConsumerRecord.class));
		//verify(libraryEventsServiceSpy,times(1)).processLibraryEvents((isA(ConsumerRecord.class)));

		Optional<LibraryEvent> persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
		assertTrue(persistedLibraryEvent.isPresent());
		assertEquals("Kafka using Spring Book 2.x",persistedLibraryEvent.get().getBook().getBookName());
	}

	@Test
	void publishModifyLibraryEvent_Not_A_Valid_LibraryEventId() throws Exception {
		//given
		Integer libraryEventId = 123;
		String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
		System.out.println(json);
		kafkaTemplate.sendDefault(libraryEventId, json).get();
		//when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);


		verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(1)).processLibraryEvents(isA(ConsumerRecord.class));

		Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEventId);
		assertFalse(libraryEventOptional.isPresent());
	}

	@Test
	void publishModifyLibraryEvent_Null_LibraryEventId() throws Exception {
		//given
		Integer libraryEventId = null;
		String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
		kafkaTemplate.sendDefault(libraryEventId, json).get();
		//when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);


		verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(1)).processLibraryEvents(isA(ConsumerRecord.class));
	}

	@Test
	void publishModifyLibraryEvent_000_LibraryEventId() throws Exception {
		//given
		Integer libraryEventId = 000;
		String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
		kafkaTemplate.sendDefault(libraryEventId, json).get();
		//when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);


		verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(3)).processLibraryEvents(isA(ConsumerRecord.class));
	}
}
