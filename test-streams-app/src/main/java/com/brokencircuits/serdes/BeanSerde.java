package com.brokencircuits.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class BeanSerde<T> implements Serde<T> {

  // use jackson mapper for serialization and deserialization
  private final ObjectMapper mapper = new ObjectMapper();
  private final Serializer<T> serializer;
  private final Deserializer<T> deserializer;

  public BeanSerde(Class<T> forClass) {
    serializer = (topic, data) -> {
      try {
        return mapper.writeValueAsBytes(data);
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }
    };
    deserializer = (topic, data) -> {
      try {
        return mapper.readValue(data, forClass);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    };
  }

  @Override
  public Serializer<T> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<T> deserializer() {
    return deserializer;
  }
}
