package com.narendra.kafka;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.util.backoff.FixedBackOff;

import com.narendra.kafka.model.Foo2;

@SpringBootApplication
public class KafkaApplication {

	
	private final Logger LOGGER = LoggerFactory.getLogger(KafkaApplication.class);

	
	private final TaskExecutor exec = new SimpleAsyncTaskExecutor();
	
	private final static CountDownLatch LATCH = new CountDownLatch(1);

	public static void main(String[] args) throws InterruptedException {
		ConfigurableApplicationContext context = SpringApplication.run(KafkaApplication.class, args);
		LATCH.await();
		Thread.sleep(5_000);
		context.close();
	}
	
	@Bean
	public SeekToCurrentErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
		return new SeekToCurrentErrorHandler(
				new DeadLetterPublishingRecoverer(template), new FixedBackOff(1000L, 2));
	}

	
	@Bean
	public RecordMessageConverter converter() {
		return new StringJsonMessageConverter();
	}
	
	@Bean
	public NewTopic topic() {
		return new NewTopic("topic1", 1, (short) 1);
	}

	@Bean
	public NewTopic dlt() {
		return new NewTopic("topic1.DLT", 1, (short) 1);
	}

	@Bean
	public ApplicationRunner runner() {
		return args -> {
			System.out.println("Hit Enter to terminate...");
			System.in.read();
		};
	}
	

	@KafkaListener(id = "fooGroup", topics = "topic1")
	public void listen(Foo2 foo) {
		LOGGER.info("Received: " + foo);
		if (foo.getFoo().startsWith("fail")) {
			throw new RuntimeException("failed");
		}
		this.exec.execute(() -> System.out.println("Hit Enter to terminate..."));
	}

	@KafkaListener(id = "dltGroup", topics = "topic1.DLT")
	public void dltListen(String in) {
		LOGGER.info("Received from DLT: " + in);
		this.exec.execute(() -> System.out.println("Hit Enter to terminate..."));
	}
	
	
	
	@Bean
	public NewTopic topic2() {
		return TopicBuilder.name("topic2").partitions(1).replicas(1).build();
	}

	@Bean
	public NewTopic topic3() {
		return TopicBuilder.name("topic3").partitions(1).replicas(1).build();
	}


	@Bean
	public BatchMessagingMessageConverter batchConverter() {
		return new BatchMessagingMessageConverter(converter());
	}

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@KafkaListener(id = "fooGroup2", topics = "topic2")
	public void listen1(List<Foo2> foos) throws IOException {
		LOGGER.info("Received: " + foos);
		foos.forEach(f -> kafkaTemplate.send("topic3", f.getFoo().toUpperCase()));
		LOGGER.info("Messages sent, hit Enter to commit tx");
		System.in.read();
	}

	@KafkaListener(id = "fooGroup3", topics = "topic3")
	public void listen2(List<String> in) {
		LOGGER.info("Received: " + in);
		LATCH.countDown();
	}

}
