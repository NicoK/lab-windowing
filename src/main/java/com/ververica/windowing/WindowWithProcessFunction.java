package com.ververica.windowing;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner.WindowAssignerContext;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @param <Key> The Type of the key.
 * @param <IN> The type of the values that are aggregated (input values)
 * @param <OUT> The type of the aggregated result
 */
public class WindowWithProcessFunction<Key, IN, OUT> extends KeyedProcessFunction<Key, IN, OUT>
    implements ResultTypeQueryable<OUT> {

  private static final Logger LOG = LoggerFactory.getLogger(WindowWithProcessFunction.class);

  private final TypeInformation<IN> inputType;
  private TypeInformation<OUT> producedType = null;

  private transient MapState<Long, TimeWindow> windowInfo;
  private transient MapState<Long, List<IN>> windowState;

  private final WindowAssigner<Object, TimeWindow> windowAssigner;
  private final ProcessWindowFunction<IN, OUT, Key> windowProcessFunction;

  private TriggerResult windowFireMode = TriggerResult.FIRE;
  /**
   * {@link OutputTag} to use for late arriving events. Elements for which {@code
   * window.maxTimestamp + allowedLateness} is smaller than the current watermark will be emitted to
   * this.
   */
  protected OutputTag<IN> lateDataOutputTag;

  private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";

  protected transient Counter numLateRecordsDropped;

  /**
   * The allowed lateness for elements. This is used for:
   *
   * <ul>
   *   <li>Deciding if an element should be dropped from a window due to lateness.
   *   <li>Clearing the state of a window if the system time passes the {@code window.maxTimestamp +
   *       allowedLateness} landmark.
   * </ul>
   */
  protected long allowedLateness;

  private transient WindowAssignerContext windowAssignerContext;

  public WindowWithProcessFunction(
      TypeInformation<IN> inputType,
      WindowAssigner<Object, TimeWindow> windowAssigner,
      ProcessWindowFunction<IN, OUT, Key> windowProcessFunction) {
    this.inputType = inputType;
    this.windowProcessFunction = windowProcessFunction;

    checkNotNull(windowAssigner);
    checkArgument(
        windowAssigner instanceof SlidingEventTimeWindows
            || windowAssigner instanceof TumblingEventTimeWindows,
        "unsupported window assigner: not a sliding or tumbling event-time window assigner: {}",
        windowAssigner);
    // check for eventTime should be redundant, but be safe for now
    checkArgument(windowAssigner.isEventTime(), "only event time supported");

    this.windowAssigner = windowAssigner;
  }

  @Override
  public void processElement(IN value, Context ctx, Collector<OUT> out) throws Exception {
    setWindowContext(ctx);

    long currentTimestamp = ctx.timestamp();
    long currentWatermark = ctx.timerService().currentWatermark();
    Collection<TimeWindow> windows =
        windowAssigner.assignWindows(value, currentTimestamp, windowAssignerContext);

    // if element is handled by none of assigned elementWindows
    boolean isSkippedElement = true;
    for (TimeWindow window : windows) {
      // drop if the window is already late
      if (isWindowLate(window, ctx)) {
        continue;
      }
      isSkippedElement = false;

      long stateKey = windowToStateKey(window);
      List<IN> stateEntry = windowState.get(stateKey);
      boolean firstInWindow = stateEntry == null;
      if (firstInWindow) {
        stateEntry = new ArrayList<>();
        windowInfo.put(stateKey, window);
      }
      stateEntry.add(value);
      windowState.put(stateKey, stateEntry);

      boolean cleanupTimerNeeded = firstInWindow && allowedLateness > 0;
      if (window.maxTimestamp() <= currentWatermark) {
        // event within allowed lateness
        emitWindowContents(out, window, stateEntry, ctx);

        if (windowFireMode.isPurge()) {
          windowState.remove(stateKey);
          windowInfo.remove(stateKey);
        } else {
          cleanupTimerNeeded = firstInWindow;
        }
      } else if (firstInWindow) {
        // only register once (avoid state access)
        registerRegularEndTimer(window, ctx);
      }

      if (cleanupTimerNeeded) {
        // only register once (avoid state access)
        registerCleanupTimer(window, ctx);
      }
    }

    // side output input event if
    // - element not handled by any window
    // - late arriving tag has been set
    // - current timestamp + allowed lateness no less than element timestamp
    if (isSkippedElement && isElementLate(ctx)) {
      if (lateDataOutputTag != null) {
        sideOutput(value, ctx);
      } else {
        this.numLateRecordsDropped.inc();
      }
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
    super.onTimer(timestamp, ctx, out);
    if (ctx.timeDomain() == TimeDomain.EVENT_TIME) {
      /*
       * note: the timer could be for either or both of:
       * - the timer for window.maxTime()
       * - the cleanup timer (window.maxTime() + allowedLateness)
       */
      // a window end timer? (assume all windows have the same length - sliding and tumbling
      // windows)
      long windowEndStateKey = regularEndTimeToStateKey(timestamp);
      long cleanupStateKey = cleanupTimeToStateKey(timestamp);
      List<IN> currentState = windowState.get(windowEndStateKey);
      if (currentState != null) {
        emitWindowContents(out, windowInfo.get(windowEndStateKey), currentState, ctx);

        if (windowFireMode.isPurge()) {
          windowState.remove(windowEndStateKey);
          windowInfo.remove(windowEndStateKey);
        }
      }

      // if it exists, this is always a cleanup timer!
      windowState.remove(cleanupStateKey);
      windowInfo.remove(windowEndStateKey);

    } else {
      LOG.error("Timers should only be in event time!");
    }
  }

  /** Emits the contents of the given window using the {@link InternalWindowFunction}. */
  private void emitWindowContents(
      Collector<OUT> out, TimeWindow window, List<IN> contents, Context ctx) throws Exception {
    ((TimestampedCollector<OUT>) out).setAbsoluteTimestamp(window.maxTimestamp());
    windowProcessFunction.process(ctx.getCurrentKey(), window, contents, out);
  }

  /**
   * Write skipped late arriving element to SideOutput.
   *
   * @param element skipped late arriving element to side output
   */
  protected void sideOutput(IN element, Context ctx) {
    ctx.output(lateDataOutputTag, element);
  }

  /**
   * Returns {@code true} if the watermark is after the end timestamp plus the allowed lateness of
   * the given window.
   */
  protected boolean isWindowLate(TimeWindow window, Context ctx) {
    return cleanupTime(window) <= ctx.timerService().currentWatermark();
  }

  /**
   * Decide if a record is currently late, based on current watermark and allowed lateness.
   *
   * @return The element for which should be considered when sideoutputs
   */
  protected boolean isElementLate(Context ctx) {
    return ctx.timestamp() + allowedLateness <= ctx.timerService().currentWatermark();
  }

  /**
   * Registers a timer to the regular window end (when its max time has passed).
   *
   * @param window the window whose state to evaluate
   */
  protected void registerRegularEndTimer(TimeWindow window, Context ctx) {
    ctx.timerService().registerEventTimeTimer(window.maxTimestamp());
  }

  /**
   * Registers a timer to cleanup the content of the window.
   *
   * @param window the window whose state to discard
   */
  protected void registerCleanupTimer(TimeWindow window, Context ctx) {
    long cleanupTime = cleanupTime(window);
    if (cleanupTime == Long.MAX_VALUE) {
      // don't set a GC timer for "end of time"
      return;
    }

    ctx.timerService().registerEventTimeTimer(cleanupTime);
  }

  /**
   * Returns the cleanup time for a window, which is {@code window.maxTimestamp + allowedLateness}.
   * In case this leads to a value greater than {@link Long#MAX_VALUE} then a cleanup time of {@link
   * Long#MAX_VALUE} is returned.
   *
   * @param window the window whose cleanup time we are computing.
   */
  private long cleanupTime(TimeWindow window) {
    long cleanupTime = window.maxTimestamp() + allowedLateness;
    return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
  }

  /**
   * Extracts the state key (window end) from the window.
   *
   * @param window the window
   * @return key to use for accessing its state
   */
  private long windowToStateKey(TimeWindow window) {
    return window.getEnd();
  }

  /**
   * Reverses the calculation from {@link #cleanupTime(TimeWindow)} to calculate the state key for a
   * given timer's time.
   *
   * @param cleanupTime cleanup time (as from the registered timer)
   * @return state key (window end)
   */
  private long cleanupTimeToStateKey(long cleanupTime) {
    checkArgument(cleanupTime != Long.MAX_VALUE);

    long windowMaxTimestamp = cleanupTime - allowedLateness;
    return windowMaxTimestamp + 1;
  }

  /**
   * Reverses the calculation from {@link #registerRegularEndTimer(TimeWindow, Context)} to
   * calculate the state key for a given timer's time.
   *
   * @param windowMaxTime window.maxTimestamp() as used with the registered timer
   * @return state key (window end)
   */
  private long regularEndTimeToStateKey(long windowMaxTime) {
    return windowMaxTime + 1;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.numLateRecordsDropped =
        getRuntimeContext().getMetricGroup().counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);

    windowInfo =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>(
                    "WindowInfo", LongSerializer.INSTANCE, new TimeWindow.Serializer()));
    windowState =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>("WindowState", Types.LONG, Types.LIST(inputType)));
  }

  private void setWindowContext(final Context ctx) {
    if (windowAssignerContext == null) {
      windowAssignerContext =
          new WindowAssignerContext() {
            @Override
            public long getCurrentProcessingTime() {
              return ctx.timerService().currentProcessingTime();
            }
          };
    }
  }

  @SuppressWarnings({"unused", "UnusedReturnValue"})
  public WindowWithProcessFunction<Key, IN, OUT> produces(TypeInformation<OUT> producedType) {
    this.producedType = producedType;
    return this;
  }

  @SuppressWarnings({"unused", "UnusedReturnValue"})
  public WindowWithProcessFunction<Key, IN, OUT> allowedLateness(Time lateness) {
    final long millis = lateness.toMilliseconds();
    checkArgument(millis >= 0, "The allowed lateness cannot be negative.");

    this.allowedLateness = millis;
    return this;
  }

  @SuppressWarnings({"unused", "UnusedReturnValue"})
  public WindowWithProcessFunction<Key, IN, OUT> sideOutputLateData(OutputTag<IN> outputTag) {
    Preconditions.checkNotNull(outputTag, "Side output tag must not be null.");
    this.lateDataOutputTag = outputTag;
    return this;
  }

  @SuppressWarnings({"unused", "UnusedReturnValue"})
  public WindowWithProcessFunction<Key, IN, OUT> triggerMode(TriggerResult windowFireMode) {
    checkArgument(
        windowFireMode == TriggerResult.FIRE || windowFireMode == TriggerResult.FIRE_AND_PURGE,
        "unsupported window fire mode: {}",
        windowFireMode);
    this.windowFireMode = windowFireMode;
    return this;
  }

  @Override
  public TypeInformation<OUT> getProducedType() {
    return producedType != null
        ? producedType
        : getProcessWindowFunctionReturnType(windowProcessFunction);
  }

  private static <IN, OUT, KEY> TypeInformation<OUT> getProcessWindowFunctionReturnType(
      ProcessWindowFunction<IN, OUT, KEY> function) {
    return TypeExtractor.getUnaryOperatorReturnType(
        function, ProcessWindowFunction.class, 0, 1, TypeExtractor.NO_INDEX, null, null, false);
  }
}
