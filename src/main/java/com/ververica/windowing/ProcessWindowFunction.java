package com.ververica.windowing;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public interface ProcessWindowFunction<IN, OUT, KEY> extends Function {
  /**
   * Evaluates the window and outputs none or several elements.
   *
   * @param key The key for which this window is evaluated.
   * @param window The window that is being evaluated.
   * @param input The elements in the window being evaluated.
   * @param out A collector for emitting elements.
   * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
   */
  void process(KEY key, TimeWindow window, Iterable<IN> input, Collector<OUT> out) throws Exception;
}
