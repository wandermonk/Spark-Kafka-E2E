package com.practice.dpusk_producer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.practice.dpusk_producer.OpenPaymentsProducer;

@SpringBootApplication
public class PacktDpuskProducerApplication {

	private static final Logger logger = LoggerFactory.getLogger(PacktDpuskProducerApplication.class);
	private static long counter = 0;
	private static String filePath = System.getProperty("loadFromFilePath");

	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(PacktDpuskProducerApplication.class, args);
		OpenPaymentsProducer producer = context.getBean(OpenPaymentsProducer.class);
		LineIterator fileContents = null;
		try {
			fileContents = FileUtils.lineIterator(new File(filePath), StandardCharsets.UTF_8.name());
			while (fileContents.hasNext()) {
				if (counter < 30) {
					String line = fileContents.nextLine();
					logger.info(" Sending Message :: " + line);
					producer.sendMessage(line);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fileContents.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		++counter;
		producer.close();
	}
}
