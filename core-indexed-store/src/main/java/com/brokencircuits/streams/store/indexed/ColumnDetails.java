package com.brokencircuits.streams.store.indexed;

import java.util.function.Function;
import lombok.Value;

@Value
public class ColumnDetails<T> {

  String name;
  ColumnType type;
  Function<T, Object> extractSqlColumn;

  /**
   * Create comparator equivalent to "WHERE $column $operator $value"
   */
  public ColumnComparator<T> createComparator(String operator, Object value) {
    return new ColumnComparator<>(this, operator, value);
  }
}
