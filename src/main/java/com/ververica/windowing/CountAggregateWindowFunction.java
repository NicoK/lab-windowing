package com.ververica.windowing;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Counts the window contents and enriches the counter with the window's end.
 *
 * @param <IN> Input type.
 * @param <KEY> Key type.
 */
public class CountAggregateWindowFunction<IN, KEY>
    implements ProcessWindowFunction<IN, Tuple2<Long, Tuple2<KEY, Long>>, KEY> {

  @Override
  public void process(
      KEY key,
      TimeWindow window,
      Iterable<IN> input,
      Collector<Tuple2<Long, Tuple2<KEY, Long>>> out) {
    long count = 0;
    for (IN ignored : input) {
      ++count;
    }
    out.collect(Tuple2.of(window.getEnd(), Tuple2.of(key, count)));
  }
}
