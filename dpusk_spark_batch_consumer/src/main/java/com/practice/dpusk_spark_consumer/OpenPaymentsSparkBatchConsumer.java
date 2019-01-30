package com.practice.dpusk_spark_consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.OffsetRange;

import org.apache.spark.streaming.kafka010.LocationStrategies;

public class OpenPaymentsSparkBatchConsumer {
	private static final Logger logger = Logger.getLogger(OpenPaymentsSparkBatchConsumer.class);

	private static SparkConf conf;
	private static JavaSparkContext sc;
	private static KafkaConsumer<String, String> consumer;

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-3:9092,kafka-2:9092,kafka-1:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "Open-Payments-spark-consumer");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "Open-Payments-consumer-1");
		props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
				"org.apache.kafka.clients.consumer.RoundRobinAssignor");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList("jason"));
		conf = new SparkConf().setAppName(PropertyFileReader.getInstance().getProperty("spark_application_name"));
		sc = new JavaSparkContext(conf);
		Set<String> topics = Collections.singleton(PropertyFileReader.getInstance().getProperty("kafka_topic"));
		AdminClient admin = KafkaAdminClient.create(props);
		List<TopicPartitionInfo> topicPartitions = admin.describeTopics(topics).all().get()
				.get(PropertyFileReader.getInstance().getProperty("kafka_topic")).partitions();
		OffsetRange[] offsetRanges = (OffsetRange[]) topicPartitions.stream().map(x -> {
			TopicPartition partition = new TopicPartition(PropertyFileReader.getInstance().getProperty("kafka_topic"),
					x.partition());
			List<Long> offsets = new ArrayList<Long>(consumer.beginningOffsets(Arrays.asList(partition)).values());
			Long startOffset = offsets.get(0);
			Long endOffset = offsets.get(0);
			OffsetRange offsetRange = new OffsetRange(PropertyFileReader.getInstance().getProperty("kafka_topic"),
					x.partition(), startOffset, endOffset);
			return offsetRange;
		}).toArray();
		JavaRDD<ConsumerRecord<String, String>> messagesRDD = KafkaUtils.createRDD(sc, props, offsetRanges,
				LocationStrategies.PreferConsistent());
		JavaRDD<String> valueRDD = messagesRDD.map(eachMessage -> {
			String value = eachMessage.value();
			return value;
		});
		valueRDD.foreach(x -> {
			logger.info(x);
		});
	}

}

class JavaSparkSessionSingleton {
	private static transient SparkSession instance = null;

	public static SparkSession getInstance(SparkConf sparkConf) {
		if (instance == null) {
			instance = SparkSession.builder().config(sparkConf)
					.config("spark.sql.warehouse.dir",
							PropertyFileReader.getInstance().getProperty("spark_sql_warehouse_directory"))
					.enableHiveSupport().getOrCreate();
		}
		return instance;
	}
}
