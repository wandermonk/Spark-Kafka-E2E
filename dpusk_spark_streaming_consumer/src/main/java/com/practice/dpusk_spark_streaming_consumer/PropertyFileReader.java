package com.practice.dpusk_spark_streaming_consumer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

public class PropertyFileReader {

	private static final Logger LOGGER = Logger.getLogger(PropertyFileReader.class);

	private final Properties props = new Properties();

	private PropertyFileReader() {
		String filePath = new File("").getAbsolutePath();

		FileInputStream file;

		try {
			file = new FileInputStream(filePath + "/resources/sparkconsumer.properties");
			System.out.println(filePath);
			props.load(file);
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		}
	}

	private static class PropHolder {
		private static final PropertyFileReader INSTANCE = new PropertyFileReader();
	}

	public static PropertyFileReader getInstance() {
		return PropHolder.INSTANCE;
	}

	public String getProperty(String key) {
		return props.getProperty(key);
	}

	public Set<String> getAllPropertyNames() {
		return props.stringPropertyNames();
	}

	public boolean containsKey(String key) {
		return props.containsKey(key);
	}
}