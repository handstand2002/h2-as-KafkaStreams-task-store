package com.brokencircuits.transform;

import com.brokencircuits.domain.Key;
import com.brokencircuits.domain.Topic;
import com.brokencircuits.domain.Value;
import com.brokencircuits.streams.serdes.SingleTopicBasedSerde;

import com.brokencircuits.streams.store.indexed.IndexedStateStore;
import com.brokencircuits.streams.store.indexed.inmemory.ColumnDetails;
import com.brokencircuits.streams.store.indexed.inmemory.MemoryIndexedStateStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.jdbc.core.JdbcTemplate;

@Slf4j
@RequiredArgsConstructor
public class TestTransformer implements Transformer<Key, Value, KeyValue<Key, Value>> {

  public final static String STORE_NAME = "my-store";

  private final Topic<Key, Value> inputTopic;
  private final JdbcTemplate jdbcTemplate;

  private IndexedStateStore<Key, Value> store;
  private final ColumnDetails<Key> id1Column = new ColumnDetails<>(Key::getField1);
  private final ColumnDetails<Key> id2Column = new ColumnDetails<>(Key::getField2);

  private MemoryIndexedStateStore<Key, Value> store2;

  @Override
  public void init(ProcessorContext context) {

//    store = IndexedStateStore.<Key, Value>builder()
//        .context(context)
//        .underlying(context.getStateStore(STORE_NAME))
//        .keySerde(new SingleTopicBasedSerde<>(inputTopic.getKeySerde(), inputTopic.getName()))
//        .jdbcTemplate(jdbcTemplate)
//        .keyColumn(id1Column)
//        .init();

    store2 = MemoryIndexedStateStore.<Key, Value>builder()
        .underlying(context.getStateStore(STORE_NAME))
        .keySerde(new SingleTopicBasedSerde<>(inputTopic.getKeySerde(), inputTopic.getName()))
        .keyColumn(id1Column)
        .keyColumn(id2Column)
        .init();
  }

  @Override
  public KeyValue<Key, Value> transform(Key key, Value value) {

//    WhereClause<Key, Value> whereClause = WhereClause.childGroupOr(store, root ->
//        root.compareKey(id1Column, "=", "key1-3")
//            .compareKey(id1Column, "=", "key2-4"));
//
//    Iterator<KeyValue<Key, Value>> where = store.where(whereClause);
//    where.forEachRemaining(kv -> log.info("Result: {}", kv));

    log.info("Updating store: {} | {}", key, value);
//    for (int i = 0; i < 5; i++) {
//      String suffix = "-" + i;
//      store.put(new Key(key.getField1() + suffix, key.getField2(), key.getField3()),
//          new Value(value.getValue() + suffix));
//    }

    return KeyValue.pair(key, value);
  }

  @Override
  public void close() {
    store.close();
  }
}
