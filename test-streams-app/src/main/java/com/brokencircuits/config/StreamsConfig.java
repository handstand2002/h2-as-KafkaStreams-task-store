package com.brokencircuits.config;

import com.brokencircuits.domain.Key;
import com.brokencircuits.domain.Topic;
import com.brokencircuits.domain.Value;
import com.brokencircuits.serdes.BeanSerde;
import com.brokencircuits.transform.TestTransformer;
import com.brokencircuits.transform.TestTransformerSupplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Slf4j
@Configuration
public class StreamsConfig {

//  @Bean(initMethod = "start", destroyMethod = "close")
//  KafkaStreams createStreams(StreamPropertyConfig props, Topology topology) {
//
//    Properties newProps = new Properties();
//    props.getStreams().forEach(newProps::put);
//    return new KafkaStreams(topology, newProps);
//  }

  @Bean
  Serde<Key> keySerde() {
    return new BeanSerde<>(Key.class);
  }

  @Bean
  Serde<Value> valueSerde() {
    return new BeanSerde<>(Value.class);
  }

  @Bean
  Topic<Key, Value> inputTopic(Serde<Key> keySerde, Serde<Value> valueSerde) {
    return new Topic<>("input-topic", keySerde, valueSerde);
  }

  @Bean
  Topic<Key, Value> outputTopic(Serde<Key> keySerde, Serde<Value> valueSerde) {
    return new Topic<>("output-topic", keySerde, valueSerde);
  }

  @Bean
  Topology topology(JdbcTemplate jdbcTemplate, Topic<Key, Value> inputTopic,
      Topic<Key, Value> outputTopic, TestTransformerSupplier supplier) {
    StreamsBuilder builder = new StreamsBuilder();

    builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(TestTransformer.STORE_NAME),
        inputTopic.getKeySerde(), inputTopic.getValueSerde()));

    builder.stream(inputTopic.getName(), inputTopic.consumedWith())
        .peek((k, v) -> log.info("Consumed {} | {}", k, v))
        .transform(supplier, TestTransformer.STORE_NAME)
        .peek((k, v) -> log.info("Produced {} | {}", k, v))
        .to(outputTopic.getName(), outputTopic.producedWith());

    return builder.build();
  }
}
