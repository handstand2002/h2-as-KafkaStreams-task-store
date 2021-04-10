package com.brokencircuits.streams.store.indexed.inmemory;

import com.brokencircuits.streams.serdes.SingleTopicBasedSerde;
import com.brokencircuits.streams.store.indexed.ComparatorGroupType;
import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class MemoryIndexedStateStore<K, V> implements Closeable {

  private final KeyValueStore<K, V> inner;
  private final Map<IndexKey, Set<StoreKeyRaw>> index = new HashMap<>();
  private final SingleTopicBasedSerde<K> keySerde;
  private final Collection<ColumnDetails<K>> keyColumns;
  private final Collection<ColumnDetails<V>> valueColumns;

  @Value
  private static class IndexKey {

    ColumnDetails<?> fieldName;
    Object value;
  }

  @Value
  @EqualsAndHashCode
  private static class StoreKeyRaw {

    byte[] inner;
  }

  @Builder(buildMethodName = "init")
  MemoryIndexedStateStore(
      @NonNull SingleTopicBasedSerde<K> keySerde,
      @NonNull KeyValueStore<K, V> underlying,
      @Singular Collection<ColumnDetails<K>> keyColumns,
      @Singular Collection<ColumnDetails<V>> valueColumns) {

    this.keySerde = keySerde;
    this.inner = underlying;
    this.keyColumns = keyColumns;
    this.valueColumns = valueColumns;

    AtomicLong counter = new AtomicLong(0);
    try (KeyValueIterator<K, V> iter = underlying.all()) {
      iter.forEachRemaining(kv -> {
        putIndex(kv.key, kv.value);
        counter.incrementAndGet();
      });
    }
    log.info("Completed inserting {} updates into memory index", counter.get());
  }

  private void putIndex(K key, V value) {
    StoreKeyRaw keyBytes = new StoreKeyRaw(serializeKey(key));
    boolean isDelete = value == null;

    for (ColumnDetails<K> keyColumn : keyColumns) {
      Object indexValue = keyColumn.getExtractValue().apply(key);
      putIndexValue(keyBytes, isDelete, keyColumn, indexValue);
    }
    for (ColumnDetails<V> valueColumn : valueColumns) {
      Object indexValue = valueColumn.getExtractValue().apply(value);
      putIndexValue(keyBytes, isDelete, valueColumn, indexValue);
    }
  }

  private void putIndexValue(StoreKeyRaw keyBytes, boolean isDelete, ColumnDetails<?> columnDetails,
      Object indexValue) {
    IndexKey indexKey = new IndexKey(columnDetails, indexValue);
    if (!isDelete) {
      index.computeIfAbsent(indexKey, k -> new HashSet<>()).add(keyBytes);
    } else {
      index.computeIfPresent(indexKey, (k, set) -> {
        set.remove(keyBytes);
        return set.isEmpty() ? null : set;
      });
    }
  }

  public void put(K key, V value) {
    inner.put(key, value);
    putIndex(key, value);
  }

  public void putAll(List<KeyValue<K, V>> entries) {
    entries.forEach(kv -> put(kv.key, kv.value));
  }

  public V get(K key) {
    return inner.get(key);
  }

  @Override
  public void close() {
    index.clear();
  }

  private Set<StoreKeyRaw> evaluateGroup(ColumnComparatorGroup<K, V> group) {
    HashSet<ColumnComparator<?>> comparators = new HashSet<>(group.getKeyComparators());
    comparators.addAll(group.getValueComparators());

    Set<StoreKeyRaw> currentResult = new HashSet<>();
    boolean firstEvaluation = true;
    for (ColumnComparator<?> keyComparator : comparators) {
      IndexKey indexKey = new IndexKey(keyComparator.getColumn(),
          keyComparator.getCompareValue());
      Set<StoreKeyRaw> keys = index.getOrDefault(indexKey, Collections.emptySet());
      updateCurrentKeySet(firstEvaluation, group.getType(), currentResult, keys);
      firstEvaluation = false;
    }

    log.info("Group {} result: {}", group.getType(), currentResult);
    for (ColumnComparatorGroup<K, V> child : group.getChildren()) {
      Set<StoreKeyRaw> childKeys = evaluateGroup(child);
      log.info("Child {} result: {}", child.getType(), childKeys);
      updateCurrentKeySet(firstEvaluation, group.getType(), currentResult, childKeys);

      firstEvaluation = false;
    }

    log.info("After handling children {} result {}", group.getType(), currentResult);

    return currentResult;
  }

  private void updateCurrentKeySet(boolean firstEvaluation, ComparatorGroupType groupType,
      Set<StoreKeyRaw> runningResult, Set<StoreKeyRaw> evaluationResult) {

    if (groupType == ComparatorGroupType.AND) {
      if (firstEvaluation) {
        // the first evaluation for AND, the set will always be empty, so need to add all keys
        runningResult.addAll(evaluationResult);
      } else {
        // subsequent evaluations will remove any keys that don't appear in the new evaluation
        runningResult.removeIf(k -> !evaluationResult.contains(k));
      }
    } else {
      // OR
      // always add all new keys
      runningResult.addAll(evaluationResult);
    }
  }

  public void all(Consumer<KeyValue<K,V>> handle) {
    try (KeyValueIterator<K, V> iterator = inner.all()) {
      iterator.forEachRemaining(handle);
    }
  }

  public Iterator<KeyValue<K, V>> where(WhereClause<K, V> clause) {

    Set<StoreKeyRaw> keys = evaluateGroup(clause.getRoot());

    return keys.stream()
        .map(k -> deserializeKey(k.getInner()))
        .map(k -> KeyValue.pair(k, get(k))).iterator();
  }

  private byte[] serializeKey(K key) {
    return keySerde.serializer().serialize(null, key);
  }

  private K deserializeKey(byte[] data) {
    return keySerde.deserializer().deserialize(null, data);
  }

}
