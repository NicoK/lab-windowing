package com.ververica.benchmark;

import com.ververica.benchmark.IntegerLongSource.Record;
import com.ververica.windowing.AggregateFunctionWithTypes;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

class SumAggregateFunctionIntLong implements AggregateFunctionWithTypes<Record, Record, Record> {

  @Override
  public Record createAccumulator() {
    return Record.of(-1, 0);
  }

  @Override
  public Record add(Record value, Record accumulator) {
    return Record.of(value.key, accumulator.value + value.value);
  }

  @Override
  public Record getResult(Record accumulator) {
    return accumulator;
  }

  @Override
  public Record merge(Record a, Record b) {
    return Record.of(a.key, a.value + b.value);
  }

  @Override
  public TypeInformation<Record> getAccumulatorType() {
    return Types.POJO(Record.class);
  }
}
