package com.brokencircuits.streams.store.indexed.inmemory;

import com.brokencircuits.streams.store.indexed.ComparatorGroupType;
import java.util.function.Consumer;
import lombok.Value;

@Value
public class WhereClause<K, V> {

  ColumnComparatorGroup<K, V> root;

  public static <K, V> WhereClause<K, V> childGroupAnd(MemoryIndexedStateStore<K, V> forStore,
      Consumer<ColumnComparatorGroup.Builder<K, V>> build) {
    return create(ComparatorGroupType.AND, build);
  }

  public static <K, V> WhereClause<K, V> childGroupOr(MemoryIndexedStateStore<K, V> forStore,
      Consumer<ColumnComparatorGroup.Builder<K, V>> build) {
    return create(ComparatorGroupType.OR, build);
  }

  private static <K, V> WhereClause<K, V> create(ComparatorGroupType type,
      Consumer<ColumnComparatorGroup.Builder<K, V>> builder) {
    ColumnComparatorGroup.Builder<K, V> b = ColumnComparatorGroup
        .newBuilder(type);
    builder.accept(b);

    return new WhereClause<>(b.build());
  }
}
