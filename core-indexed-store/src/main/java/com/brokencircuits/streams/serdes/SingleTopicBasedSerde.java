package com.brokencircuits.streams.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class SingleTopicBasedSerde<T> implements Serde<T> {

  private final Serializer<T> serializer;
  private final Deserializer<T> deserializer;

  public SingleTopicBasedSerde(Serde<T> inner, String topic) {
    serializer = (ignored, data) -> inner.serializer().serialize(topic, data);
    deserializer = (ignored, data) -> inner.deserializer().deserialize(topic, data);
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
