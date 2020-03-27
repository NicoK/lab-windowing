package com.ververica.timestamps;

import java.io.IOException;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Creates a per-key (internal!) watermarks for a {@link KeyedProcessFunction} similar to {@link
 * BoundedOutOfOrdernessTimestampExtractor}. These "watermarks" that lag behind the element with the
 * maximum timestamp (in event time) seen so far by a fixed amount of time, <code>t_late
 * </code>. This can help reduce the number of elements that are ignored due to lateness when
 * computing the final result for a given timer, in the case where we know that elements arrive no
 * later than <code>t_late</code> units of time after the watermark that signals that the system
 * event-time has advanced past their (event-time) timestamp. In contrast to {@link
 * BoundedOutOfOrdernessTimestampExtractor}, this will not generate Flink-Watermarks but only
 * produces these for the encapsulating {@link KeyedProcessFunction}!
 */
public class KeyedBoundedOutOfOrdernessWatermark implements Function {

  private static final long serialVersionUID = 1L;

  /** The timestamp of the last emitted watermark. */
  private long lastWatermark = Long.MIN_VALUE;

  /**
   * The (fixed) interval between the maximum seen timestamp seen in the records and that of the
   * watermark to be emitted.
   */
  private final long maxOutOfOrderness;

  private transient ValueState<Long> watermark;

  public KeyedBoundedOutOfOrdernessWatermark(Time maxOutOfOrderness) {
    if (maxOutOfOrderness.toMilliseconds() < 0) {
      throw new RuntimeException(
          "Tried to set the maximum allowed "
              + "lateness to "
              + maxOutOfOrderness
              + ". This parameter cannot be negative.");
    }
    this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
  }

  @SuppressWarnings("unused")
  public long getMaxOutOfOrdernessInMillis() {
    return maxOutOfOrderness;
  }

  public void init(RuntimeContext ctx) {
    watermark = ctx.getState(new ValueStateDescriptor<>("KeyedWatermark", Types.LONG));
  }

  /**
   * Returns the current watermark for the key of the current context.
   *
   * <p>This method can only be called in code where Flink state can be accessed.
   *
   * @return the current watermark for the context's key
   */
  public final Long getCurrentWatermark() throws IOException {
    Long currentWatermark = watermark.value();
    if (currentWatermark == null) {
      currentWatermark = Long.MIN_VALUE;
    }
    return currentWatermark;
  }

  public final void updateCurrentWatermark(KeyedProcessFunction<?, ?, ?>.Context ctx)
      throws IOException {
    long timestamp = ctx.timestamp();
    // this guarantees that the watermark never goes backwards.
    long potentialWM = timestamp - maxOutOfOrderness;
    if (potentialWM >= lastWatermark) {
      lastWatermark = potentialWM;
    }
    watermark.update(lastWatermark);
  }
}
