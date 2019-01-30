package com.practice.dpusk.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

@Service
public class OpenPaymentsProducer {

	private static Properties props;
	static {
		props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-3:9092,kafka-2:9092,kafka-1:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "OpenPaymentsProducer");
		props.put(ProducerConfig.RETRIES_CONFIG, "0");
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
		props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
	}

	private static Producer<String, String> producer = new KafkaProducer<>(props);

	public void sendMessage(String msg) throws InterruptedException, ExecutionException {
		ProducerRecord<String, String> precord = new ProducerRecord<>("jason", UUID.randomUUID().toString(), msg);
		producer.initTransactions();
		producer.beginTransaction();
		TopicPartition topicPartition = null;
		OffsetAndMetadata offsetAndMetaData = null;
		Future<RecordMetadata> metadata = producer.send(precord, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception e) {
				if (e != null) {
					e.printStackTrace();
				} else {
					System.out.println(metadata.offset() + " :: " + metadata.partition() + " :: " + metadata.topic()
							+ " :: " + metadata.timestamp());
				}
			}
		});

		topicPartition = new TopicPartition(metadata.get().topic(), metadata.get().partition());
		offsetAndMetaData = new OffsetAndMetadata(metadata.get().offset());

		Map<TopicPartition, OffsetAndMetadata> offsetsMap = new HashMap<TopicPartition, OffsetAndMetadata>();
		offsetsMap.put(topicPartition, offsetAndMetaData);

		producer.sendOffsetsToTransaction(offsetsMap, "Open-Payments-Producer");
		producer.commitTransaction();
	}

	public Map<MetricName, ? extends Metric> metrics() {
		return producer.metrics();
	}

	public void close() {
		producer.close();
	}
}
