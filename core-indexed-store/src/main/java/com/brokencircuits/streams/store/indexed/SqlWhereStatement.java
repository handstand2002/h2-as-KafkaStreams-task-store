package com.brokencircuits.streams.store.indexed;

import java.util.List;
import lombok.Value;

@Value
public class SqlWhereStatement {

  String sql;
  List<Object> parameters;
}
