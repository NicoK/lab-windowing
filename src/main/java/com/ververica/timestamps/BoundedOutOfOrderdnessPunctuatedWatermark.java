package com.ververica.timestamps;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

public abstract class BoundedOutOfOrderdnessPunctuatedWatermark<T>
    implements AssignerWithPunctuatedWatermarks<T> {

  /** The current maximum timestamp seen so far. */
  private long currentMaxTimestamp;

  /**
   * The (fixed) interval between the maximum seen timestamp seen in the records and that of the
   * watermark to be emitted.
   */
  private final long maxOutOfOrderness;

  public BoundedOutOfOrderdnessPunctuatedWatermark(Time maxOutOfOrderness) {
    if (maxOutOfOrderness.toMilliseconds() < 0) {
      throw new RuntimeException(
          "Tried to set the maximum allowed "
              + "lateness to "
              + maxOutOfOrderness
              + ". This parameter cannot be negative.");
    }
    this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
    this.currentMaxTimestamp = Long.MIN_VALUE + this.maxOutOfOrderness;
  }

  @Override
  public long extractTimestamp(T element, long previousElementTimestamp) {
    long timestamp = extractTimestamp(element);
    currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
    return timestamp;
  }

  public abstract long extractTimestamp(T element);

  @Override
  public Watermark checkAndGetNextWatermark(T element, long extractedTimestamp) {
    // emit a watermark with every element
    return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
  }
}
