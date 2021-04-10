package com.brokencircuits.streams.store.indexed;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.Value;

@Value
public class WhereClause<K, V> {

  ColumnComparatorGroup<K, V> root;

  public static <K, V> WhereClause<K, V> childGroupAnd(IndexedStateStore<K, V> forStore,
      Consumer<ColumnComparatorGroup.Builder<K, V>> build) {
    return create(ComparatorGroupType.AND, build);
  }

  public static <K, V> WhereClause<K, V> childGroupOr(IndexedStateStore<K, V> forStore,
      Consumer<ColumnComparatorGroup.Builder<K, V>> build) {
    return create(ComparatorGroupType.OR, build);
  }

  private static <K, V> WhereClause<K, V> create(ComparatorGroupType type,
      Consumer<ColumnComparatorGroup.Builder<K, V>> builder) {
    ColumnComparatorGroup.Builder<K, V> b = ColumnComparatorGroup.newBuilder(type);
    builder.accept(b);

    return new WhereClause<>(b.build());
  }

  public static <K, V> WhereClause<K, V> all() {
    return WhereClause.create(ComparatorGroupType.AND, b -> {
    });
  }

  private static SqlWhereStatement createSql(ComparatorGroupType type,
      Collection<ColumnComparator<?>> comparators) {
    String columnComparisonString = comparators.stream()
        .map(c -> String.format("%s %s ?", c.getColumn().getName(), c.getSqlComparator()))
        .collect(Collectors.joining(String.format(" %s ", type.name())));

    List<Object> params = comparators.stream()
        .map(ColumnComparator::getCompareValue)
        .collect(Collectors.toList());

    return new SqlWhereStatement(columnComparisonString, params);
  }

  public SqlWhereStatement createSql() {
    SqlWhereStatement sql = createSql(root, true);
    if (!sql.getSql().isEmpty()) {
      return new SqlWhereStatement("WHERE " + sql.getSql(), sql.getParameters());
    }
    return sql;
  }

  private SqlWhereStatement createSql(ColumnComparatorGroup<K, V> group, boolean isUpperEmpty) {
    if (group.getKeyComparators().isEmpty() && group.getValueComparators().isEmpty()
        && group.getChildren().isEmpty()) {
      return new SqlWhereStatement("", Collections.emptyList());
    }

    Collection<ColumnComparator<?>> comparators = new LinkedList<>();
    comparators.addAll(
        addPrefixToColumnNames(group.getKeyComparators(), IndexedStateStore.KEY_COLUMN_PREFIX));
    comparators.addAll(
        addPrefixToColumnNames(group.getValueComparators(), IndexedStateStore.VALUE_COLUMN_PREFIX));

    SqlWhereStatement sql = createSql(group.getType(), comparators);
    StringBuilder sqlBuilder = new StringBuilder("(");
    sqlBuilder.append(sql.getSql());
    List<Object> params = sql.getParameters();

    for (ColumnComparatorGroup<K, V> child : group.getChildren()) {
      SqlWhereStatement childSql = createSql(child, sqlBuilder.length() == 0);

      if (sqlBuilder.length() > 0 || !isUpperEmpty) {
        sqlBuilder.append(String.format(" %s ", group.getType().name()));
      }
      sqlBuilder.append(childSql.getSql());
      params.addAll(childSql.getParameters());
    }
    sqlBuilder.append(")");

    return new SqlWhereStatement(sqlBuilder.toString(), params);
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
