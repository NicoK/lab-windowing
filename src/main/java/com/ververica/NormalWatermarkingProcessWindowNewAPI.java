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

package com.ververica;

import com.ververica.timestamps.BoundedOutOfOrderdnessPunctuatedWatermark;
import com.ververica.windowing.CountAggregateWindowFunction;
import com.ververica.windowing.WindowWithProcessFunction;
import com.ververica.windowing.WindowWithProcessFunctionNewAPI;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NormalWatermarkingProcessWindowNewAPI {

  private static final Logger LOG = LoggerFactory.getLogger(NormalWatermarkingProcessWindowNewAPI.class);

  public static void main(String[] args) throws Exception {
    ParameterTool tool = ParameterTool.fromArgs(args);

    StreamExecutionEnvironment env = getEnvironment(tool);
    env.getConfig().disableGenericTypes();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setParallelism(1);

    SingleOutputStreamOperator<Tuple2<Long, String>> sourceStream =
        env.fromElements(
                Tuple2.of(1L, "device1"),
                Tuple2.of(105L, "device2"),
                Tuple2.of(2L, "device1"),
                Tuple2.of(104L, "device2"))
            .name("source")
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrderdnessPunctuatedWatermark<Tuple2<Long, String>>(
                    Time.milliseconds(10L)) {
                  @Override
                  public long extractTimestamp(Tuple2<Long, String> event) {
                    return event.f0;
                  }
                });

    final OutputTag<Tuple2<Long, String>> lateElements =
        new OutputTag<Tuple2<Long, String>>("Late Elements") {};

    SingleOutputStreamOperator<Tuple2<Long, Tuple2<String, Long>>> process =
        sourceStream
            .keyBy(x -> x.f1)
            .process(
                new WindowWithProcessFunctionNewAPI<>(
                        sourceStream.getType(),
                        TumblingEventTimeWindows.of(Time.milliseconds(10)),
                        new CountAggregateWindowFunction<Tuple2<Long, String>, String>() {})
                    .sideOutputLateData(lateElements));

    process.getSideOutput(lateElements).printToErr("late");
    process.print("in time");

    env.execute();
  }

  public static StreamExecutionEnvironment getEnvironment(ParameterTool tool) {
    final boolean localWithUi = tool.has("local");
    final int parallelism = tool.getInt("parallelism", -1);
    final StreamExecutionEnvironment env;
    if (localWithUi) {
      env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
      if (parallelism > 0) {
        env.setParallelism(parallelism);
      }
    } else {
      env = StreamExecutionEnvironment.getExecutionEnvironment();
    }
    return env;
  }
}
