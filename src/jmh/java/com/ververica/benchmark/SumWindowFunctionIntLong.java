package com.ververica.benchmark;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

class SumWindowFunctionIntLong<W extends Window>
    implements WindowFunction<IntegerLongSource.Record, IntegerLongSource.Record, Integer, W> {
  @Override
  public void apply(
      Integer key,
      W window,
      Iterable<IntegerLongSource.Record> input,
      Collector<IntegerLongSource.Record> out)
      throws Exception {
    long sum = 0L;
    for (IntegerLongSource.Record element : input) {
      sum += element.value;
    }
    out.collect(IntegerLongSource.Record.of(key, sum));
  }
}
