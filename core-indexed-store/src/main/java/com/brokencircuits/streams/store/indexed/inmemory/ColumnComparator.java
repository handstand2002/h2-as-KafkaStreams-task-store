package com.brokencircuits.streams.store.indexed.inmemory;

import lombok.Value;

@Value
public class ColumnComparator<T> {

  ColumnDetails<T> column;
  Object compareValue;
}
