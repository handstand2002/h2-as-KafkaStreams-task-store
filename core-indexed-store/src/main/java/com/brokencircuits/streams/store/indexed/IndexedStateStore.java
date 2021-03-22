package com.brokencircuits.streams.store.indexed;

import com.brokencircuits.streams.serdes.SingleTopicBasedSerde;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

@Slf4j
public class IndexedStateStore<K, V> implements Closeable {

  private static final String KEY_COLUMN_PREFIX = "key_";
  private static final String VALUE_COLUMN_PREFIX = "value_";
  private static final String RAW_KEY_COLUMN = "rawkey";
  private final List<EnhancedColumnDetails<?>> columns = new LinkedList<>();
  private final SingleTopicBasedSerde<K> keySerde;
  private final String tableName;
  private final JdbcTemplate jdbcTemplate;
  private final ColumnDetails<K> rawKeyColumn;
  private final String columnNamesCsv;
  private final String preparedStmtArgPlaceholders;
  private final KeyValueStore<K, V> inner;

  @Builder(buildMethodName = "init")
  IndexedStateStore(
      @NonNull ProcessorContext context,
      @NonNull SingleTopicBasedSerde<K> keySerde,
      @NonNull JdbcTemplate jdbcTemplate,
      @NonNull KeyValueStore<K, V> underlying,
      @Singular Collection<ColumnDetails<K>> keyColumns,
      @Singular Collection<ColumnDetails<V>> valueColumns) {

    this.jdbcTemplate = jdbcTemplate;
    this.keySerde = keySerde;
    this.inner = underlying;
    int part = context.partition();
    String partString = part >= 0 ? String.valueOf(part) : "N" + Math.abs(part);
    String fixedUnderlyingName = underlying.name().replaceAll("[^A-Za-z0-9]", "_");
    tableName = String.format("%s_%s", fixedUnderlyingName, partString);

    rawKeyColumn = new ColumnDetails<>(RAW_KEY_COLUMN, ColumnType.VARBINARY, this::serializeKey);

    keyColumns.forEach(col -> columns.add(enhancedColumn(col, true)));
    valueColumns.forEach(col -> columns.add(enhancedColumn(col, false)));

    String createStmt = sqlCreateStmt();
    log.info("Creating table:\n{}", createStmt);
    jdbcTemplate.update(createStmt);

    for (EnhancedColumnDetails<?> col : columns) {
      String stmt = indexCreateStmt(col);
      log.info("Creating index with '{}'", stmt);
      jdbcTemplate.update(stmt);
    }

    columnNamesCsv = Stream
        .of(Stream.of(rawKeyColumn), columns.stream().map(EnhancedColumnDetails::getInner))
        .flatMap(s -> s)
        .map(ColumnDetails::getName)
        .collect(Collectors.joining(", "));
    preparedStmtArgPlaceholders = columnNamesCsv.replaceAll("[^ ,]+", "?");

    AtomicLong counter = new AtomicLong(0);
    List<KeyValue<K, V>> batch = new LinkedList<>();
    underlying.all().forEachRemaining(kv -> {
      counter.incrementAndGet();
      batch.add(kv);
      if (batch.size() >= 1000) {
        batchUpdate(batch);
        batch.clear();
      }
    });
    batchUpdate(batch);
    log.info("Completed inserting {} updates into h2 index", counter.get());
  }

  public void put(K key, V value) {
    inner.put(key, value);
    batchUpdate(Collections.singletonList(KeyValue.pair(key, value)));
  }

  public void putAll(List<KeyValue<K, V>> entries) {
    inner.putAll(entries);
    batchUpdate(entries);
  }

  public V get(K key) {
    return inner.get(key);
  }

  @Override
  public void close() {
    jdbcTemplate.update(String.format("DROP TABLE %s", tableName));
  }

  public Iterator<KeyValue<K, V>> where(WhereClause<K, V> clause) {
    SqlWhereStatement whereStmt = clause.createSql();
    String selectSql = String
        .format("SELECT %s FROM %s %s", rawKeyColumn.getName(), tableName, whereStmt.getSql());

    log.info("Querying database with Query: {}; params: {}", selectSql, whereStmt.getParameters());

    List<K> result = jdbcTemplate.query(selectSql, (resultSet, i) -> {
      InputStream stream = resultSet.getBinaryStream(rawKeyColumn.getName());
      byte[] rawKey = readIntoByteArray(stream);
      return deserializeKey(rawKey);
    }, whereStmt.getParameters().toArray());

    return result.stream().map(k -> KeyValue.pair(k, get(k))).iterator();
  }

  private byte[] readIntoByteArray(InputStream stream) {
    byte[] byteArray;
    try {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      int nRead;
      byte[] data = new byte[1024];
      while ((nRead = stream.read(data, 0, data.length)) != -1) {
        buffer.write(data, 0, nRead);
      }

      buffer.flush();
      byteArray = buffer.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return byteArray;
  }

  private void batchUpdate(List<KeyValue<K, V>> batch) {

    Map<K, V> inserts = new HashMap<>();
    Set<K> deletes = new HashSet<>();
    for (KeyValue<K, V> kv : batch) {
      if (kv.value == null) {
        inserts.remove(kv.key);
        deletes.add(kv.key);
      } else {
        inserts.put(kv.key, kv.value);
        deletes.remove(kv.key);
      }
    }

    if (!inserts.isEmpty()) {
      batchInsert(inserts);
    }
    if (!deletes.isEmpty()) {
      batchDelete(deletes);
    }
  }

  private void batchDelete(Set<K> deletes) {
    String stmt = String.format("DELETE FROM %s WHERE %s=?", tableName, rawKeyColumn.getName());
    Iterator<K> iter = deletes.iterator();
    this.jdbcTemplate.batchUpdate(stmt, new BatchPreparedStatementSetter() {

      public void setValues(PreparedStatement ps, int i) throws SQLException {

        K key = iter.next();
        ps.setObject(1, rawKeyColumn.getExtractSqlColumn().apply(key),
            rawKeyColumn.getType().getSqlType());
      }

      public int getBatchSize() {
        return deletes.size();
      }

    });
  }

  private void batchInsert(Map<K, V> batch) {
    String stmt = String.format("MERGE INTO %s (%s) VALUES (%s)", tableName, columnNamesCsv,
        preparedStmtArgPlaceholders);
    Iterator<Entry<K, V>> iter = batch.entrySet().iterator();
    this.jdbcTemplate.batchUpdate(stmt, new BatchPreparedStatementSetter() {

      public void setValues(PreparedStatement ps, int i) throws SQLException {

        Entry<K, V> kv = iter.next();
        ps.setObject(1, rawKeyColumn.getExtractSqlColumn().apply(kv.getKey()),
            rawKeyColumn.getType().getSqlType());
        for (int k = 0; k < columns.size(); k++) {
          EnhancedColumnDetails<Object> col = (EnhancedColumnDetails<Object>) columns.get(k);
          Object applyTo = col.isKeyColumn() ? kv.getKey() : kv.getValue();
          ps.setObject(k + 2, col.getExtractSqlColumn().apply(applyTo),
              col.getType().getSqlType());
        }
      }

      public int getBatchSize() {
        return batch.size();
      }

    });
  }

  private byte[] serializeKey(K key) {
    return keySerde.serializer().serialize(null, key);
  }

  private K deserializeKey(byte[] data) {
    return keySerde.deserializer().deserialize(null, data);
  }

  private String indexCreateStmt(EnhancedColumnDetails<?> col) {
    return String.format("CREATE HASH INDEX %s_%s ON %1$s(%2$s)", tableName, col.getName());
  }

  public String sqlCreateStmt() {

    List<String> statements = new LinkedList<>();

    statements.add(columnCreateString(rawKeyColumn));

    for (EnhancedColumnDetails<?> col : columns) {
      statements.add(columnCreateString(col.getInner()));
    }
    statements.add(createPrimaryKeyString(Collections.singletonList(rawKeyColumn)));

    String columnSql = String.join(",\n", statements);
    return String.format("CREATE TABLE %s (\n%s\n)", tableName, columnSql);
  }

  private String columnCreateString(ColumnDetails<?> col) {
    return String.format("\t%s %s", col.getName(), col.getType().name());
  }

  private String createPrimaryKeyString(List<ColumnDetails<K>> columns) {
    if (columns.isEmpty()) {
      return "";
    }
    String keyColNames = columns.stream().map(ColumnDetails::getName)
        .collect(Collectors.joining(", "));
    return String.format("\tPRIMARY KEY(%s)\n", keyColNames);
  }

  private <T> EnhancedColumnDetails<T> enhancedColumn(ColumnDetails<T> col, boolean isKeyColumn) {
    String prefix = isKeyColumn ? KEY_COLUMN_PREFIX : VALUE_COLUMN_PREFIX;
    return new EnhancedColumnDetails<>(
        new ColumnDetails<>(prefix + col.getName(), col.getType(),
            col.getExtractSqlColumn()), isKeyColumn);
  }

  @Value
  private static class EnhancedColumnDetails<T> {

    @Delegate
    ColumnDetails<T> inner;
    boolean isKeyColumn;
  }
}
