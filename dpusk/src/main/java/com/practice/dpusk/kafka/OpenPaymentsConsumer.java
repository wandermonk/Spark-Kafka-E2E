package com.practice.dpusk.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class OpenPaymentsConsumer {
	private static Properties kafkaProps;

	static {
		kafkaProps = new Properties();
		kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		kafkaProps.put("bootstrap.servers", "localhost:9092");
		kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "OPConsumerGroup");
	}

	public void recieveRecord() throws IOException {
		try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProps)) {
			kafkaConsumer.subscribe(Arrays.asList("jason"));
			while (true) {
				ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
				records.forEach(record -> {
					System.out.println("Offset :: "+record.offset());
					System.out.println("Partition :: "+record.partition());
					System.out.println("Key :: "+record.key());
					System.out.println("Value :: "+record.value());
					System.out.println("Topic :: "+record.topic());
				});
			}
		}
	}
}
