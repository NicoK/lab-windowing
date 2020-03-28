package com.ververica.windowing;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Enriches a window's output with the window's end.
 *
 * @param <IN> Input type.
 * @param <KEY> Key type.
 */
public class EnrichingWindowFunction<IN, KEY>
    implements ProcessWindowFunction<KEY, IN, Tuple2<Long, IN>> {

  @Override
  public void process(KEY key, TimeWindow window, IN input, Collector<Tuple2<Long, IN>> out) {
    out.collect(Tuple2.of(window.getEnd(), input));
  }
}
