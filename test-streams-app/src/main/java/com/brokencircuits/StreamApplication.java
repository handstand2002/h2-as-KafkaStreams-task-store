package com.brokencircuits;

import com.brokencircuits.config.StreamPropertyConfig;
import com.brokencircuits.domain.Key;
import com.brokencircuits.domain.Topic;
import com.brokencircuits.domain.Value;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class StreamApplication {

  public static void main(String[] args) {
    SpringApplication.run(StreamApplication.class, args);
  }

  @Bean
  CommandLineRunner runner(StreamPropertyConfig config,
      Topology topology,
      Topic<Key, Value> inputTopic,
      Topic<Key, Value> outputTopic) {
    return args -> {
      TopologyTestDriver driver = new TopologyTestDriver(topology, config.getStreamProps());
      TestInputTopic<Key, Value> input = driver
          .createInputTopic(inputTopic.getName(), inputTopic.getKeySerde().serializer(),
              inputTopic.getValueSerde().serializer());

      TestOutputTopic<Key, Value> output = driver.createOutputTopic(outputTopic.getName(),
          outputTopic.getKeySerde().deserializer(),
          outputTopic.getValueSerde().deserializer());

      input.pipeInput(new Key("key1", "key2", "key3"), new Value("my-value-1"));
      input.pipeInput(new Key("key2", "key3", "key4"), new Value("my-value-2"));
      input.pipeInput(new Key("key3", "key4", "key5"), new Value("my-value-3"));

      Thread.sleep(Long.MAX_VALUE);
    };
  }
}
