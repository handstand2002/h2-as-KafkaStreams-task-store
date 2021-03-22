package com.brokencircuits.streams.store.indexed;

import lombok.Value;

@Value
public class ColumnComparator<T> {

  // TODO: introduce "ComparatorGroup", which is of type AND or OR (default AND), and may have nodeType SINGLETON or COLLECTION.
  //  singleton will have a single ColumnComparator (separate field), collection will have a collection of ComparatorGroups, so nesting may occur
  //  each ComparatorGroup will have collection of key comparators and value comparators
  //  whereClause will have a single inner ComparatorGroup (root note) which may be null
  ColumnDetails<T> column;
  String sqlComparator;
  Object compareValue;
}
