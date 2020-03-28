package com.ververica.windowing;

import static org.apache.flink.util.Preconditions.checkArgument;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

public abstract class CountAggregateFunction<IN>
    implements AggregateFunctionWithTypes<IN, Tuple2<String, Long>, Tuple2<String, Long>> {

  @Override
  public Tuple2<String, Long> createAccumulator() {
    return Tuple2.of("", 0L);
  }

  @Override
  public Tuple2<String, Long> add(IN value, Tuple2<String, Long> accumulator) {
    accumulator.f0 = getKey(value);
    accumulator.f1 = accumulator.f1 + 1;
    return accumulator;
  }

  protected abstract String getKey(IN value);

  @Override
  public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
    return accumulator;
  }

  @Override
  public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
    checkArgument(a.f0.equals(b.f0));
    a.f1 = a.f1 + b.f1;
    return a;
  }

  @Override
  public TypeInformation<Tuple2<String, Long>> getAccumulatorType() {
    return Types.TUPLE(Types.STRING, Types.LONG);
  }
}
