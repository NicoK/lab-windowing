package com.ververica.windowing;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public interface ProcessWindowFunction<KEY, IN, OUT> extends Function {
  /**
   * Evaluates the window and outputs none or several elements.
   *
   * @param input The elements in the window being evaluated.
   * @param out A collector for emitting elements.
   * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
   */
  void process(KEY key, TimeWindow window, IN input, Collector<OUT> out) throws Exception;
}
