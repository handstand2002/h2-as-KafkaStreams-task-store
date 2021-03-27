package com.brokencircuits.streams.store.indexed;

import java.util.Collection;
import java.util.LinkedList;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.Getter;

public class ColumnComparatorGroup<K, V> {

  @Getter(AccessLevel.PACKAGE)
  private final Collection<ColumnComparator<K>> keyComparators;
  @Getter(AccessLevel.PACKAGE)
  private final Collection<ColumnComparator<V>> valueComparators;
  @Getter(AccessLevel.PACKAGE)
  private final Collection<ColumnComparatorGroup<K, V>> children;
  @Getter(AccessLevel.PACKAGE)
  private final ComparatorGroupType type;

  public static <K,V> Builder<K,V> newBuilder(ComparatorGroupType type) {
    return new Builder<>(type);
  }

  private ColumnComparatorGroup(ComparatorGroupType type,
      Collection<ColumnComparatorGroup<K, V>> children,
      Collection<ColumnComparator<K>> keyComparators,
      Collection<ColumnComparator<V>> valueComparators) {
    this.keyComparators = keyComparators;
    this.valueComparators = valueComparators;
    this.children = children;
    this.type = type;
  }

  public static class Builder<K, V> {

    private final ComparatorGroupType type;
    private final Collection<ColumnComparator<K>> keyComparators = new LinkedList<>();
    private final Collection<ColumnComparator<V>> valueComparators = new LinkedList<>();
    private final Collection<ColumnComparatorGroup<K, V>> children = new LinkedList<>();

    private Builder(ComparatorGroupType type) {
      this.type = type;
    }

    public Builder<K, V> compareKey(ColumnDetails<K> column, String sqlComparator, Object value) {
      keyComparators.add(new ColumnComparator<>(column, sqlComparator, value));
      return this;
    }

    public Builder<K, V> value(ColumnDetails<V> column, String sqlComparator, Object value) {
      valueComparators.add(new ColumnComparator<>(column, sqlComparator, value));
      return this;
    }

    public Builder<K, V> childGroupAnd(Consumer<Builder<K, V>> childBuilder) {
      Builder<K, V> b = new Builder<>(ComparatorGroupType.AND);
      childBuilder.accept(b);
      children.add(b.build());
      return this;
    }

    public Builder<K, V> childGroupOr(Consumer<Builder<K, V>> childBuilder) {
      Builder<K, V> b = new Builder<>(ComparatorGroupType.AND);
      childBuilder.accept(b);
      children.add(b.build());
      return this;
    }

    ColumnComparatorGroup<K, V> build() {
      return new ColumnComparatorGroup<>(type, children, keyComparators, valueComparators);
    }
  }
}
