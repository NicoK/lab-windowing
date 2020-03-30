package com.ververica.timestamps;

import java.io.IOException;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.Context;
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
  private Long lastWatermark = null;

  /** The currently lowest watermark among all keys of this instance. */
  private transient Long currentLowWatermark = 0L;

  /**
   * The (fixed) interval between the maximum seen timestamp seen in the records and that of the
   * watermark to be emitted.
   */
  private final long maxOutOfOrderness;

  private transient ValueState<Long> watermark;
  private Object lastKey;

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

    ctx.getMetricGroup().gauge("currentLowWatermark", (Gauge<Long>) () -> currentLowWatermark);
  }

  /**
   * Returns the current watermark for the key of the current context.
   *
   * <p>This method can only be called in code where Flink state can be accessed.
   *
   * @param ctx ProcessFunction context
   * @return the current watermark for the context's key
   * @throws IOException if the watermark cannot be retrieved
   */
  public final Long getCurrentWatermark(Context ctx) throws IOException {
    Long currentWatermark = watermark.value();
    if (currentWatermark == null) {
      currentWatermark = Long.MIN_VALUE;
    }
    cacheWatermark(ctx, currentWatermark);
    return currentWatermark;
  }

  /**
   * Updates the per-key watermark (after processing; or at least after retrieving the watermark
   * with {@link #getCurrentWatermark(Context)}!).
   *
   * @param ctx ProcessFunction context
   * @return the new per-key watermark
   * @throws IOException if the watermark cannot be updated
   */
  public final Long updateCurrentWatermark(Context ctx) throws IOException {
    long timestamp = ctx.timestamp();
    // this guarantees that the watermark never goes backwards.
    long potentialWM = timestamp - maxOutOfOrderness;
    Long currentWatermark = getWatermarkCached(ctx);
    if (potentialWM >= currentWatermark) {
      currentWatermark = potentialWM;
      if (currentWatermark >= currentLowWatermark) {
        currentLowWatermark = currentWatermark;
      }
      watermark.update(currentWatermark);
    }
    invalidateWatermarkCache();
    return currentWatermark;
  }

  /**
   * Caches the current watermark (and the key for validation) so that we do not need to access
   * state again (may be slow if RocksDB is used).
   *
   * @param ctx ProcessFunction context
   * @param currentWatermark the current watermark for the context's key
   */
  private void cacheWatermark(Context ctx, Long currentWatermark) {
    lastWatermark = currentWatermark;
    lastKey = ctx.getCurrentKey();
  }

  private Long getWatermarkCached(Context ctx) throws IOException {
    if (lastWatermark == null || lastKey != ctx.getCurrentKey()) {
      return getCurrentWatermark(ctx);
    }
    return lastWatermark;
  }

  /** Invalidates the watermark cache to allow early GC. */
  private void invalidateWatermarkCache() {
    lastWatermark = null;
    lastKey = null;
  }
}
