package com.brokencircuits.streams.store.indexed;

import java.sql.Types;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ColumnType {
  VARCHAR(Types.VARCHAR, "VARCHAR"),
  BLOB(Types.BLOB, "BLOB"),
  VARBINARY(Types.VARBINARY, "VARBINARY");

  private final int sqlType;
  private final String name;
}
