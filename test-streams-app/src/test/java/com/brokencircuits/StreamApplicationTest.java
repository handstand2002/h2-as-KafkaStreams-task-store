package com.brokencircuits;

import com.brokencircuits.config.StreamPropertyConfig;
import com.brokencircuits.domain.Key;
import com.brokencircuits.domain.Topic;
import com.brokencircuits.domain.Value;
import lombok.SneakyThrows;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@SpringBootTest
@RunWith(SpringJUnit4ClassRunner.class)
@EnableAutoConfiguration
public class StreamApplicationTest {

  @Autowired
  private StreamPropertyConfig config;
  @Autowired
  private Topology topology;
  @Autowired
  private Topic<Key, Value> inputTopic;
  @Autowired
  private Topic<Key, Value> outputTopic;

  @Test
  @SneakyThrows
  public void runTest() {

    TopologyTestDriver driver = new TopologyTestDriver(topology, config.getStreamProps());
    TestInputTopic<Key, Value> input = driver
        .createInputTopic(this.inputTopic.getName(), this.inputTopic.getKeySerde().serializer(),
            this.inputTopic.getValueSerde().serializer());

    TestOutputTopic<Key, Value> output = driver.createOutputTopic(this.outputTopic.getName(),
        this.outputTopic.getKeySerde().deserializer(),
        this.outputTopic.getValueSerde().deserializer());

    input.pipeInput(new Key("key1", "key2", "key3"), new Value("my-value-1"));
    input.pipeInput(new Key("key2", "key3", "key4"), new Value("my-value-2"));
    input.pipeInput(new Key("key3", "key4", "key5"), new Value("my-value-3"));
  }
}