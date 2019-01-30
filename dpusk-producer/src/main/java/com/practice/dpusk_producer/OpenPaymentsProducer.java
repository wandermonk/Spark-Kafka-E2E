package com.practice.dpusk_producer;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OpenPaymentsProducer {
	private static final Logger logger = LoggerFactory.getLogger(OpenPaymentsProducer.class);
	//Step 1 - Create the properties object with all the producer configs 
	//which are relevant
	private static Properties props;
	static {
		props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				"kafka-1.asia-east1-b.c.bubbly-card-222014.internal:9092,kafka-2.asia-east1-b.c.bubbly-card-222014.internal:9092,kafka-3.asia-east1-b.c.bubbly-card-222014.internal:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "OpenPaymentsProducer");
		props.put(ProducerConfig.RETRIES_CONFIG, "0");
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, "100");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
		props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
	}
	//Step 2 - Create an instance of the KafkaProducer
	private static Producer<String, String> producer = new KafkaProducer<>(props);
	//Step 3 - Using the producer.send() method we are going to send the 
	//ProducerRecord to the Kafka Topic.
	public void sendMessage(String msg) {
		ProducerRecord<String, String> precord = 
				new ProducerRecord<>("jason", UUID.randomUUID().toString(), msg);
		producer.send(precord);
	}

	public Map<MetricName, ? extends Metric> metrics() {
		return producer.metrics();
	}

	public void close() {
		producer.close();
	}
}
