package com.practice.dpusk.serdes;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CustomSerializer<T> implements Serializer<T> {

	private ObjectMapper om = new ObjectMapper();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public byte[] serialize(String topic, T data) {
		byte[] retval = null;
		try {
			System.out.println("Serializing data of type :: " + data.getClass());
			retval = om.writeValueAsString(data).getBytes();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return retval;
	}

	@Override
	public void close() {
	}

}
