package com.brokencircuits.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "kafka")
public class StreamPropertyConfig {

  private final Map<String, String> streams = new HashMap<>();

  public Properties getStreamProps() {
    Properties props = new Properties();
    streams.forEach(props::put);
    return props;
  }
}
