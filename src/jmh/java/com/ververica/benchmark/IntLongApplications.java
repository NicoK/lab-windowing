/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.benchmark;

import com.ververica.windowing.AggregatingWindowWithProcessFunction;
import com.ververica.windowing.AggregatingWindowWithProcessFunctionNewAPI;
import com.ververica.windowing.PassThroughWindowFunction;
import com.ververica.windowing.WindowWithProcessFunction;
import com.ververica.windowing.WindowWithProcessFunctionNewAPI;
import com.ververica.windowing.WindowWithProcessFunctionNewAPI2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class IntLongApplications {
  public static <W extends Window> void reduceWithWindowAggregate(
      DataStreamSource<IntegerLongSource.Record> source, WindowAssigner<Object, W> windowAssigner) {
    source
        .map(new MultiplyIntLongByTwo())
        .keyBy(record -> record.key)
        .window(windowAssigner)
        .reduce(new SumReduceIntLong())
        .addSink(new CollectSink<>());
  }

  public static void reduceWithProcessFunctionAggregate(
      DataStreamSource<IntegerLongSource.Record> source,
      WindowAssigner<Object, TimeWindow> windowAssigner,
      WindowWithProcessFunctionBenchmarks.StateAPI stateAPI) {
    final KeyedProcessFunction<Integer, IntegerLongSource.Record, IntegerLongSource.Record>
        windowWithProcessFunction;

    if (stateAPI == WindowWithProcessFunctionBenchmarks.StateAPI.EXISTING) {
      windowWithProcessFunction =
          new AggregatingWindowWithProcessFunction<>(
              windowAssigner,
              new SumAggregateFunctionIntLong(),
              new PassThroughWindowFunction<IntegerLongSource.Record, Integer>() {});
    } else {
      windowWithProcessFunction =
          new AggregatingWindowWithProcessFunctionNewAPI<>(
              windowAssigner,
              new SumAggregateFunctionIntLong(),
              new PassThroughWindowFunction<IntegerLongSource.Record, Integer>() {});
    }

    source
        .map(new MultiplyIntLongByTwo())
        .keyBy(record -> record.key)
        .process(windowWithProcessFunction)
        .addSink(new CollectSink<>());
  }

  public static <W extends Window> void reduceWithWindowApply(
      DataStreamSource<IntegerLongSource.Record> source, WindowAssigner<Object, W> windowAssigner) {
    source
        .map(new MultiplyIntLongByTwo())
        .keyBy(record -> record.key)
        .window(windowAssigner)
        .apply(new SumWindowFunctionIntLong<>())
        .addSink(new CollectSink<>());
  }

  public static void reduceWithProcessFunctionApply(
      DataStreamSource<IntegerLongSource.Record> source,
      WindowAssigner<Object, TimeWindow> windowAssigner,
      WindowWithProcessFunctionBenchmarks.StateAPI stateAPI) {
    final KeyedProcessFunction<Integer, IntegerLongSource.Record, IntegerLongSource.Record>
        windowWithProcessFunction;

    if (stateAPI == WindowWithProcessFunctionBenchmarks.StateAPI.EXISTING) {
      windowWithProcessFunction =
          new WindowWithProcessFunction<>(
              source.getType(), windowAssigner, new SumWindowWithProcessFunctionIntLong());
    } else if (stateAPI == WindowWithProcessFunctionBenchmarks.StateAPI.TEMPORAL_STATE) {
      windowWithProcessFunction =
          new WindowWithProcessFunctionNewAPI<>(
              source.getType(), windowAssigner, new SumWindowWithProcessFunctionIntLong());
    } else if (stateAPI == WindowWithProcessFunctionBenchmarks.StateAPI.TEMPORAL_STATE_NO_TIMER_CONTEXT) {
      windowWithProcessFunction =
              new WindowWithProcessFunctionNewAPI2<>(
                      source.getType(), windowAssigner, new SumWindowWithProcessFunctionIntLong());
    } else {
      throw new UnsupportedOperationException("Unknown state API: " + stateAPI);
    }

    source
        .map(new MultiplyIntLongByTwo())
        .keyBy(record -> record.key)
        .process(windowWithProcessFunction)
        .addSink(new CollectSink<>());
  }
}
