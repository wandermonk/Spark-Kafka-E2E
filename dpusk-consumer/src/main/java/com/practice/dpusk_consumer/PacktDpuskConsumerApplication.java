package com.practice.dpusk_consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class PacktDpuskConsumerApplication {
	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(PacktDpuskConsumerApplication.class, args);
		OpenPaymentsConsumer consumer = context.getBean(OpenPaymentsConsumer.class);
		consumer.consume();
	}
}
