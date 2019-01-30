package com.practice.dpusk_consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Service;

@Service
public class OpenPaymentsConsumer {

	private static KafkaConsumer<String, String> consumer;

	@SuppressWarnings("deprecation")
	public void consume() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-3:9092,kafka-2:9092,kafka-1:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "Open-Payments-Consumer");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "Open-Payments-Consumer-1");
		props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
				"org.apache.kafka.clients.consumer.RoundRobinAssignor");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList("jason"));
		while (true) {
			try {
				ConsumerRecords<String, String> records = consumer.poll(100);
				records.forEach(record -> {
					System.out.println("RECORD INFO START");
					System.out.println("Key :: " + record.key());
					System.out.println("Value :: " + record.value());
					System.out.println("Offset :: " + record.offset());
					System.out.println("Partition :: " + record.partition());
					System.out.println("RECORD INFO END");
				});

			} catch (Exception e) {
				e.printStackTrace();
			}
			
			consumer.commitSync();
		}		
	}

	public void close() {
		consumer.close();
	}
}
