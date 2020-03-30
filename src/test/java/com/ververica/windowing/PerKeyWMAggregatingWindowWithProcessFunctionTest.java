/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.windowing;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;

import com.ververica.timestamps.KeyedBoundedOutOfOrdernessWatermark;
import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link PerKeyWMAggregatingWindowWithProcessFunction}, copied and adapted from {@link
 * org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorTest}.
 */
public class PerKeyWMAggregatingWindowWithProcessFunctionTest extends TestLogger {

  private static final TypeInformation<Tuple2<String, Integer>> STRING_INT_TUPLE =
      Types.TUPLE(Types.STRING, Types.INT);

  // late arriving event OutputTag<StreamRecord<IN>>
  private static final OutputTag<Tuple2<String, Integer>> lateOutputTag =
      new OutputTag<Tuple2<String, Integer>>("late-output") {};

  @Test
  public void testSlidingEventTimeWindowsReduce() throws Exception {
    final int windowSize = 3;
    final int windowSlide = 1;

    PerKeyWMAggregatingWindowWithProcessFunction<
            String,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>>
        processFunction =
            new PerKeyWMAggregatingWindowWithProcessFunction<>(
                new KeyedBoundedOutOfOrdernessWatermark(Time.milliseconds(1000L)),
                SlidingEventTimeWindows.of(
                    Time.of(windowSize, TimeUnit.SECONDS), Time.of(windowSlide, TimeUnit.SECONDS)),
                new SumAggregator(),
                new PassThroughWindowFunction<Tuple2<String, Integer>, String>() {});

    OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
        testHarness = createTestHarness(processFunction);

    testHarness.setup();
    testHarness.open();

    ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

    // add elements out-of-order
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3999)); // wm2: 2999
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3000)); // wm2: 2999

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 20)); //   wm1: -980
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 0)); //    wm1: -980
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 999)); //  wm1:   -1

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998)); // wm2: 2999
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1999)); // wm2: 2999
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000)); // wm2: 2999
    compareOutput(testHarness, expectedOutput);

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 1999)); // wm1:  999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 999));
    compareOutput(testHarness, expectedOutput);
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1999)); // wm2: 2999
    compareOutput(testHarness, expectedOutput);

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 2999)); // wm1: 1999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 4), 1999));
    compareOutput(testHarness, expectedOutput);
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2999)); // wm2: 2999
    compareOutput(testHarness, expectedOutput);

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 3999)); // wm1: 2999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 5), 2999));
    compareOutput(testHarness, expectedOutput);
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3999)); // wm2: 2999
    compareOutput(testHarness, expectedOutput);

    assertEquals(0, testHarness.numEventTimeTimers());

    // do a snapshot, close and restore again
    OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);
    testHarness.close();

    expectedOutput.clear();
    testHarness = createTestHarness(processFunction);
    testHarness.setup();
    testHarness.initializeState(snapshot);
    testHarness.open();

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 4999)); // wm1: 3999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 3999));
    compareOutput(testHarness, expectedOutput);
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 4999)); // wm2: 3999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 8), 3999));
    compareOutput(testHarness, expectedOutput);

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 5999)); // wm1: 4999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 4999));
    compareOutput(testHarness, expectedOutput);
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 5999)); // wm2: 4999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 5), 4999));
    compareOutput(testHarness, expectedOutput);

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 6999)); // wm1: 5999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 5999));
    compareOutput(testHarness, expectedOutput);
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 6999)); // wm2: 5999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 5), 5999));
    compareOutput(testHarness, expectedOutput);

    // none of the initial elements anymore
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 7999)); // wm1: 6999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 6999));
    compareOutput(testHarness, expectedOutput);
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 7999)); // wm2: 6999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 6999));
    compareOutput(testHarness, expectedOutput);

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 10999)); // wm1: 9999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 7999));
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 2), 8999));
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 1), 9999));
    compareOutput(testHarness, expectedOutput);
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 10999)); // wm2: 9999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 7999));
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 8999));
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 1), 9999));
    compareOutput(testHarness, expectedOutput);

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 20999)); // wm1: 19999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 1), 10999));
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 1), 11999));
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 1), 12999));
    compareOutput(testHarness, expectedOutput);
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 20999)); // wm2: 19999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 1), 10999));
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 1), 11999));
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 1), 12999));
    compareOutput(testHarness, expectedOutput);

    assertEquals(0, testHarness.numEventTimeTimers());

    testHarness.close();
  }

  @Test
  public void testTumblingEventTimeWindowsReduce() throws Exception {
    final int windowSize = 3;

    PerKeyWMAggregatingWindowWithProcessFunction<
            String,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>>
        processFunction =
            new PerKeyWMAggregatingWindowWithProcessFunction<>(
                new KeyedBoundedOutOfOrdernessWatermark(Time.milliseconds(2L)),
                TumblingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
                new SumAggregator(),
                new PassThroughWindowFunction<Tuple2<String, Integer>, String>() {});

    OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
        testHarness = createTestHarness(processFunction);

    ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

    testHarness.open();

    // add elements out-of-order
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3999)); // wm2: 2999
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3000)); // wm2: 2999

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 20)); //   wm1: -980
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 0)); //    wm1: -980
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 999)); //  wm1:   -1

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998)); // wm2: 2999
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1999)); // wm2: 2999
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000)); // wm2: 2999
    compareOutput(testHarness, expectedOutput);

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 1999)); // wm1:  999
    compareOutput(testHarness, expectedOutput);
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1999)); // wm2: 2999
    compareOutput(testHarness, expectedOutput);

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 2999)); // wm1: 1999
    compareOutput(testHarness, expectedOutput);
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2999)); // wm2: 2999
    compareOutput(testHarness, expectedOutput);

    assertEquals(0, testHarness.numEventTimeTimers());

    // do a snapshot, close and restore again
    OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);
    compareOutput(testHarness, expectedOutput);
    testHarness.close();

    testHarness = createTestHarness(processFunction);
    expectedOutput.clear();
    testHarness.setup();
    testHarness.initializeState(snapshot);
    testHarness.open();

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 3999)); // wm1: 2999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 5), 2999));
    compareOutput(testHarness, expectedOutput);
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3999)); // wm2: 2999
    compareOutput(testHarness, expectedOutput);

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 4999)); // wm1: 3999
    compareOutput(testHarness, expectedOutput);
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 4999)); // wm2: 3999
    compareOutput(testHarness, expectedOutput);

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 5999)); // wm1: 4999
    compareOutput(testHarness, expectedOutput);
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 5999)); // wm2: 4999
    compareOutput(testHarness, expectedOutput);

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 6999)); // wm1: 5999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 5999));
    compareOutput(testHarness, expectedOutput);
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 6999)); // wm2: 5999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 5), 5999));
    compareOutput(testHarness, expectedOutput);

    // none of the initial elements anymore
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 10999)); // wm1: 9999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 1), 8999));
    compareOutput(testHarness, expectedOutput);
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 10999)); // wm2: 9999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 1), 8999));
    compareOutput(testHarness, expectedOutput);

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 20999)); // wm1: 19999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 1), 11999));
    compareOutput(testHarness, expectedOutput);
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 20999)); // wm2: 19999
    expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 1), 11999));
    compareOutput(testHarness, expectedOutput);

    assertEquals(0, testHarness.numEventTimeTimers());

    testHarness.close();
  }

  @Test
  public void testLateness() throws Exception {
    final int windowSize = 2;
    final long lateness = 500;

    PerKeyWMAggregatingWindowWithProcessFunction<
            String,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>>
        processFunction =
            new PerKeyWMAggregatingWindowWithProcessFunction<>(
                    new KeyedBoundedOutOfOrdernessWatermark(Time.milliseconds(1000L)),
                    TumblingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
                    new SumAggregator(),
                    new PassThroughWindowFunction<Tuple2<String, Integer>, String>() {})
                .allowedLateness(Time.milliseconds(lateness))
                .sideOutputLateData(lateOutputTag)
                .triggerMode(TriggerResult.FIRE_AND_PURGE);

    OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
        testHarness = createTestHarness(processFunction);

    testHarness.open();

    ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedQueue<Object> lateExpected = new ConcurrentLinkedQueue<>();

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 500)); //  wm1: -500
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 500)); //  wm2: -500

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2500)); // wm2: 1500
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1300)); // wm2: 1500

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3300)); // wm2: 2300
    expected.add(new StreamRecord<>(new Tuple2<>("key2", 2), 1999));

    // not in side output because window.maxTimestamp() + allowedLateness > currentWatermark
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1997)); // wm2: 2300
    // this is 1 and not 3 because the trigger fires and purges
    expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 7000)); // wm2: 6000
    expected.add(new StreamRecord<>(new Tuple2<>("key2", 2), 3999));

    // in side output because window.maxTimestamp() + allowedLateness < currentWatermark
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998)); // wm2: 6000
    lateExpected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 8000)); // wm2: 7000

    // not late because wm for key1 and key2 differ
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 1000)); // wm1:    0
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 3000)); // wm1: 2000
    expected.add(new StreamRecord<>(new Tuple2<>("key1", 2), 1999));

    compareOutput(testHarness, expected);

    compareSideOutput(testHarness, lateExpected);

    assertEquals(0, testHarness.numEventTimeTimers());

    testHarness.close();
  }

  @Test
  public void testCleanupTimeOverflow() throws Exception {
    final int windowSize = 1000;
    final long lateness = 2000;

    TumblingEventTimeWindows windowAssigner =
        TumblingEventTimeWindows.of(Time.milliseconds(windowSize));

    PerKeyWMAggregatingWindowWithProcessFunction<
            String,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>>
        processFunction =
            new PerKeyWMAggregatingWindowWithProcessFunction<>(
                    new KeyedBoundedOutOfOrdernessWatermark(Time.milliseconds(0L)),
                    windowAssigner,
                    new SumAggregator(),
                    new PassThroughWindowFunction<Tuple2<String, Integer>, String>() {})
                .allowedLateness(Time.milliseconds(lateness));

    OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
        testHarness = createTestHarness(processFunction);

    testHarness.open();

    ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

    long timestamp = Long.MAX_VALUE - 1750;
    Collection<TimeWindow> windows =
        windowAssigner.assignWindows(
            new Tuple2<>("key2", 1),
            timestamp,
            new WindowAssigner.WindowAssignerContext() {
              @Override
              public long getCurrentProcessingTime() {
                return testHarness.getProcessingTime();
              }
            });
    TimeWindow window = Iterables.getOnlyElement(windows);

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), timestamp));

    // the garbage collection timer would wrap-around
    Assert.assertTrue(window.maxTimestamp() + lateness < window.maxTimestamp());

    // and it would prematurely fire with watermark (Long.MAX_VALUE - 1500)
    Assert.assertTrue(window.maxTimestamp() + lateness < Long.MAX_VALUE - 1500);

    // if we don't correctly prevent wrap-around in the garbage collection
    // timers this watermark will clean our window state for the just-added
    // element/window
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), Long.MAX_VALUE - 1500));
    compareOutput(testHarness, expected);

    // this watermark is before the end timestamp of our only window
    Assert.assertTrue(Long.MAX_VALUE - 1500 < window.maxTimestamp());
    Assert.assertTrue(window.maxTimestamp() < Long.MAX_VALUE);

    // push in a watermark that will trigger computation of our window
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), window.maxTimestamp()));
    // the last element is dropped because window assigner doesn't calculate correct start+end
    // (the watermark is still correct though)
    expected.add(new StreamRecord<>(new Tuple2<>("key2", 2), window.maxTimestamp()));

    compareOutput(testHarness, expected);

    assertEquals(0, testHarness.numEventTimeTimers());

    testHarness.close();
  }

  @Test
  public void testSideOutputDueToLatenessTumbling() throws Exception {
    final int windowSize = 2;

    PerKeyWMAggregatingWindowWithProcessFunction<
            String,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>>
        processFunction =
            new PerKeyWMAggregatingWindowWithProcessFunction<>(
                    new KeyedBoundedOutOfOrdernessWatermark(Time.milliseconds(0L)),
                    TumblingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
                    new SumAggregator(),
                    new PassThroughWindowFunction<Tuple2<String, Integer>, String>() {})
                .sideOutputLateData(lateOutputTag);

    OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
        testHarness = createTestHarness(processFunction);

    testHarness.open();

    ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedQueue<Object> sideExpected = new ConcurrentLinkedQueue<>();

    // normal element
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000)); // wm2: 1000
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1985)); // wm2: 1985

    // this will not be dropped because window.maxTimestamp() + allowedLateness > currentWatermark
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1980)); // wm2: 1985

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 3999)); // wm1: 3999
    expected.add(new StreamRecord<>(new Tuple2<>("key1", 1), 3999));
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1999)); // wm2: 1999
    expected.add(new StreamRecord<>(new Tuple2<>("key2", 4), 1999));

    // sideoutput as late, will reuse previous timestamp since only input tuple is sideoutputed
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 3998)); // wm1: 3999
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998)); // wm2: 1999
    sideExpected.add(new StreamRecord<>(new Tuple2<>("key1", 1), 3998));
    sideExpected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2001)); // wm2: 2001
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2999)); // wm2: 2999

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3999)); // wm2: 3999
    expected.add(new StreamRecord<>(new Tuple2<>("key2", 3), 3999));

    compareOutput(testHarness, expected);
    compareSideOutput(testHarness, sideExpected);

    assertEquals(0, testHarness.numEventTimeTimers());

    testHarness.close();
  }

  @Test
  public void testSideOutputDueToLatenessSliding() throws Exception {
    final int windowSize = 3;
    final int windowSlide = 1;

    PerKeyWMAggregatingWindowWithProcessFunction<
            String,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>>
        processFunction =
            new PerKeyWMAggregatingWindowWithProcessFunction<>(
                    new KeyedBoundedOutOfOrdernessWatermark(Time.milliseconds(0L)),
                    SlidingEventTimeWindows.of(
                        Time.of(windowSize, TimeUnit.SECONDS),
                        Time.of(windowSlide, TimeUnit.SECONDS)),
                    new SumAggregator(),
                    new PassThroughWindowFunction<Tuple2<String, Integer>, String>() {})
                .sideOutputLateData(lateOutputTag);

    OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
        testHarness = createTestHarness(processFunction);

    testHarness.open();

    ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedQueue<Object> sideExpected = new ConcurrentLinkedQueue<>();

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
    expected.add(new StreamRecord<>(new Tuple2<>("key2", 2), 1999));

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2000));
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3000));
    expected.add(new StreamRecord<>(new Tuple2<>("key2", 3), 2999));

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 3001));

    // lateness is set to 0 and window size = 3 sec and slide 1, the following 2 elements (2400)
    // are assigned to windows ending at 2999, 3999, 4999.
    // The 2999 is dropped because it is already late (WM = 2999) but the rest are kept.

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2400));
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2400));
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 3001));
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3900));

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 6000));
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 6000));

    expected.add(new StreamRecord<>(new Tuple2<>("key2", 7), 3999));
    expected.add(new StreamRecord<>(new Tuple2<>("key1", 2), 3999));

    expected.add(new StreamRecord<>(new Tuple2<>("key2", 5), 4999));
    expected.add(new StreamRecord<>(new Tuple2<>("key1", 2), 4999));

    expected.add(new StreamRecord<>(new Tuple2<>("key2", 2), 5999));
    expected.add(new StreamRecord<>(new Tuple2<>("key1", 2), 5999));

    // sideoutput element due to lateness
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 3001));
    sideExpected.add(new StreamRecord<>(new Tuple2<>("key1", 1), 3001));

    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 25000));
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 25000));
    expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 6999));
    expected.add(new StreamRecord<>(new Tuple2<>("key1", 1), 6999));
    expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 7999));
    expected.add(new StreamRecord<>(new Tuple2<>("key1", 1), 7999));
    expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 8999));
    expected.add(new StreamRecord<>(new Tuple2<>("key1", 1), 8999));

    compareOutput(testHarness, expected);
    compareSideOutput(testHarness, sideExpected);

    assertEquals(0, testHarness.numEventTimeTimers());

    testHarness.close();
  }

  @Test
  public void testCleanupTimerWithEmptyReduceStateForTumblingWindows() throws Exception {
    final int windowSize = 2;
    final long lateness = 1;

    PerKeyWMAggregatingWindowWithProcessFunction<
            String,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>,
            Tuple2<String, Integer>>
        processFunction =
            new PerKeyWMAggregatingWindowWithProcessFunction<>(
                    new KeyedBoundedOutOfOrdernessWatermark(Time.milliseconds(0L)),
                    TumblingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
                    new SumAggregator(),
                    new PassThroughWindowFunction<Tuple2<String, Integer>, String>() {})
                .allowedLateness(Time.milliseconds(lateness));

    OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
        testHarness = createTestHarness(processFunction);

    testHarness.open();

    ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

    // normal element
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1599));
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
    expected.add(new StreamRecord<>(new Tuple2<>("key2", 3), 1999)); // here it fires and purges
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2000));
    expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 3999)); // here is the cleanup timer
    testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 5000));

    compareOutput(testHarness, expected);

    assertEquals(0, testHarness.numEventTimeTimers());

    testHarness.close();
  }

  // ------------------------------------------------------------------------
  //  Helpers
  // ------------------------------------------------------------------------

  private static <OUT>
      OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, OUT> createTestHarness(
          KeyedProcessFunction<String, Tuple2<String, Integer>, OUT> processFunction)
          throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new KeyedProcessOperator<>(Preconditions.checkNotNull(processFunction)),
        new TupleKeySelector(),
        Types.STRING,
        1,
        1,
        0);
  }

  private static void compareOutput(
      OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
          testHarness,
      ConcurrentLinkedQueue<Object> expectedOutput) {
    TestHarnessUtil.assertOutputEqualsSorted(
        "Output was not correct.",
        expectedOutput,
        testHarness.getOutput(),
        new Tuple2ResultSortComparator());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void compareSideOutput(
      OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
          testHarness,
      ConcurrentLinkedQueue<Object> sideExpected) {
    TestHarnessUtil.assertOutputEqualsSorted(
        "SideOutput was not correct.",
        sideExpected,
        (Iterable) testHarness.getSideOutput(lateOutputTag),
        new Tuple2ResultSortComparator());
  }

  // ------------------------------------------------------------------------
  //  UDFs
  // ------------------------------------------------------------------------

  private static class SumAggregator
      implements AggregateFunctionWithTypes<
          Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {
    private static final long serialVersionUID = 1L;

    @Override
    public TypeInformation<Tuple2<String, Integer>> getAccumulatorType() {
      return STRING_INT_TUPLE;
    }

    @Override
    public Tuple2<String, Integer> createAccumulator() {
      return Tuple2.of("", 0);
    }

    @Override
    public Tuple2<String, Integer> add(
        Tuple2<String, Integer> value, Tuple2<String, Integer> accumulator) {
      accumulator.f0 = value.f0;
      accumulator.f1 = accumulator.f1 + value.f1;
      return accumulator;
    }

    @Override
    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
      return accumulator;
    }

    @Override
    public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
      checkArgument(a.f0.equals(b.f0));
      a.f1 = a.f1 + b.f1;
      return a;
    }
  }

  @SuppressWarnings("unchecked")
  private static class Tuple2ResultSortComparator implements Comparator<Object>, Serializable {
    @Override
    public int compare(Object o1, Object o2) {
      if (o1 instanceof Watermark || o2 instanceof Watermark) {
        return 0;
      } else {
        StreamRecord<Tuple2<String, Integer>> sr0 = (StreamRecord<Tuple2<String, Integer>>) o1;
        StreamRecord<Tuple2<String, Integer>> sr1 = (StreamRecord<Tuple2<String, Integer>>) o2;
        if (sr0.getTimestamp() != sr1.getTimestamp()) {
          return (int) (sr0.getTimestamp() - sr1.getTimestamp());
        }
        int comparison = sr0.getValue().f0.compareTo(sr1.getValue().f0);
        if (comparison != 0) {
          return comparison;
        } else {
          return sr0.getValue().f1 - sr1.getValue().f1;
        }
      }
    }
  }

  private static class TupleKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
    private static final long serialVersionUID = 1L;

    @Override
    public String getKey(Tuple2<String, Integer> value) {
      return value.f0;
    }
  }
}
