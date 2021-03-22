package com.brokencircuits.streams.store.indexed;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Singular;

@Builder
public final class WhereClause<K, V> {

  @Singular
  private final Collection<ColumnComparator<K>> keyColumnComparators;
  @Singular
  private final Collection<ColumnComparator<V>> valueColumnComparators;

  public static <K, V> WhereClause<K, V> all() {
    return WhereClause.<K, V>builder().build();
  }

  SqlWhereStatement createSql() {
    if (keyColumnComparators.isEmpty() && valueColumnComparators.isEmpty()) {
      return new SqlWhereStatement("", Collections.emptyList());
    }

    Collection<ColumnComparator<?>> comparators = new LinkedList<>();
    comparators.addAll(addPrefixToColumnNames(keyColumnComparators, "key_"));
    comparators.addAll(addPrefixToColumnNames(valueColumnComparators, "value_"));

    String columnComparisonString = comparators.stream()
        .map(c -> String.format("%s %s ?", c.getColumn().getName(), c.getSqlComparator()))
        .collect(Collectors.joining(" AND "));

    List<Object> params = Stream.of(keyColumnComparators, valueColumnComparators)
        .flatMap(Collection::stream)
        .map(ColumnComparator::getCompareValue)
        .collect(Collectors.toList());

    String sqlWhere = String.format("WHERE %s", columnComparisonString);
    return new SqlWhereStatement(sqlWhere, params);
  }

  private <T> Collection<ColumnComparator<T>> addPrefixToColumnNames(
      Collection<ColumnComparator<T>> comparators, String prefix) {
    return comparators.stream().map(c -> {
      ColumnDetails<T> col = c.getColumn();
      ColumnDetails<T> newCol = new ColumnDetails<>(prefix + col.getName(), col.getType(),
          col.getExtractSqlColumn());
      return new ColumnComparator<>(newCol, c.getSqlComparator(), c.getCompareValue());
    }).collect(Collectors.toList());
  }

}
