package com.ververica.windowing;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public interface AggregateFunctionWithTypes<IN, ACC, OUT> extends AggregateFunction<IN, ACC, OUT> {

  TypeInformation<ACC> getAccumulatorType();
}
