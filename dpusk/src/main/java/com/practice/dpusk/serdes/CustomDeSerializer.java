package com.practice.dpusk.serdes;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CustomDeSerializer<T> implements Deserializer<T> {

	private ObjectMapper om = new ObjectMapper();
	private Class<T> type;

	public CustomDeSerializer(Class<T> type) {
		this.type = type;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		if (type == null) {
			type = (Class<T>) configs.get("type");
		}
	}

	@Override
	public T deserialize(String topic, byte[] bytes) {
		T data = null;
		if (bytes == null || bytes.length == 0) {
			return null;
		}
		try {
			System.out.println(getClassType());
			data = om.readValue(bytes, type);
		} catch (Exception e) {
			throw new SerializationException(e);
		}
		return data;
	}

	@Override
	public void close() {
	}

	protected Class<T> getClassType() {
		return type;
	}

}
