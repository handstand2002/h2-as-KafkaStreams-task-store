package com.brokencircuits.streams.store.indexed;

import lombok.Value;

@Value
public class ColumnComparator<T> {

  ColumnDetails<T> column;
  String sqlComparator;
  Object compareValue;
}
