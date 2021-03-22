package com.brokencircuits.transform;

import com.brokencircuits.domain.Key;
import com.brokencircuits.domain.Topic;
import com.brokencircuits.domain.Value;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class TestTransformerSupplier implements TransformerSupplier<Key, Value, KeyValue<Key, Value>> {

  private final Topic<Key, Value> inputTopic;
  private final JdbcTemplate jdbcTemplate;

  @Override
  public Transformer<Key, Value, KeyValue<Key, Value>> get() {
    return new TestTransformer(inputTopic, jdbcTemplate);
  }
}
