package com.brokencircuits.streams.store.indexed.inmemory;

import java.util.function.Function;
import lombok.Value;

@Value
public class ColumnDetails<T> {

  Function<T, Object> extractValue;

  public ColumnComparator<T> createComparator(Object value) {
    return new ColumnComparator<T>(this, value);
  }
}
