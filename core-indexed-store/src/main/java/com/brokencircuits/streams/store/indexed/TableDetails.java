package com.brokencircuits.streams.store.indexed;

import com.brokencircuits.streams.serdes.SingleTopicBasedSerde;
import java.util.List;
import lombok.Value;

@Value
public class TableDetails<K, V> {

  List<ColumnDetails<K>> keyColumns;
  List<ColumnDetails<V>> valueColumns;
  SingleTopicBasedSerde<K> keySerde;
  SingleTopicBasedSerde<V> valueSerde;
}
