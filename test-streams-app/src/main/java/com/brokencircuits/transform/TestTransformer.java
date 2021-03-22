package com.brokencircuits.transform;

import com.brokencircuits.domain.Key;
import com.brokencircuits.domain.Topic;
import com.brokencircuits.domain.Value;
import com.brokencircuits.streams.serdes.SingleTopicBasedSerde;
import com.brokencircuits.streams.store.indexed.ColumnDetails;
import com.brokencircuits.streams.store.indexed.ColumnType;
import com.brokencircuits.streams.store.indexed.IndexedStateStore;
import com.brokencircuits.streams.store.indexed.WhereClause;
import java.util.LinkedList;
import java.util.List;
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
  private final ColumnDetails<Key> id1Column = new ColumnDetails<>("id", ColumnType.VARCHAR,
      Key::getField1);

  @Override
  public void init(ProcessorContext context) {
    store = IndexedStateStore.<Key, Value>builder()
        .context(context)
        .underlying(context.getStateStore(STORE_NAME))
        .keySerde(
            new SingleTopicBasedSerde<>(inputTopic.getKeySerde(), inputTopic.getName()))
        .jdbcTemplate(jdbcTemplate)
        .keyColumn(id1Column)
        .init();
  }

  @Override
  public KeyValue<Key, Value> transform(Key key, Value value) {
    log.info("Updating store: {} | {}", key, value);
    for (int i = 0; i < 5; i++) {
      String suffix = "-" + i;
      store.put(new Key(key.getField1() + suffix, key.getField2(), key.getField3()),
          new Value(value.getValue() + suffix));
    }

    List<KeyValue<Key, Value>> res = query(WhereClause.all());

    return KeyValue.pair(key, value);
  }

  private List<KeyValue<Key, Value>> query(WhereClause<Key, Value> where) {
    List<KeyValue<Key, Value>> coll = new LinkedList<>();
    store.where(where).forEachRemaining(coll::add);

    return coll;
  }

  @Override
  public void close() {
    store.close();
  }
}
