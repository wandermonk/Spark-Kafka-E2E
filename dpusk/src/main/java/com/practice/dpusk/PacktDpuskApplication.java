package com.practice.dpusk;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.practice.dpusk.kafka.OpenPaymentsProducer;

@SpringBootApplication
public class PacktDpuskApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(PacktDpuskApplication.class, args);
		OpenPaymentsProducer producer = context.getBean(OpenPaymentsProducer.class);
		for (int i = 0; i < 10; i++) {
			System.out.println("Sending message :: " + "Test Message :: " + i);
			try {
				producer.sendMessage("The new Test Message :: " + i);
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}

		producer.close();

		Map<MetricName, ? extends Metric> data = producer.metrics();
		for (Map.Entry metric : data.entrySet()) {
			System.out.println("Metric Key :: " + metric.getKey());
			System.out.println("Metric Value :: " + metric.getValue());
		}
	}
}
