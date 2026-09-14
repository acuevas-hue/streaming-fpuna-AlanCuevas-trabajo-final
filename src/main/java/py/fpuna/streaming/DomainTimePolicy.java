package py.fpuna.streaming;

import java.util.Optional;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

/** Per-partition event-time watermark: max valid domain timestamp minus five seconds.
 * No wall-clock advancement: deterministic for historical replay. All partitions need heartbeats.
 */
public final class DomainTimePolicy extends TimestampPolicy<String, String> {
  private Instant watermark;
  public DomainTimePolicy(Optional<Instant> previous) {
    watermark = previous.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE);
  }
  @Override public Instant getTimestampForRecord(PartitionContext context, KafkaRecord<String, String> record) {
    try {
      Event e = Event.parse(record.getKV().getKey(), record.getKV().getValue());
      return observe(e.timeMs);
    } catch (IllegalArgumentException ex) {
      return watermark; // Validation transform persists the invalid record; it cannot advance time.
    }
  }
  Instant observe(long timeMs) {
    Instant candidate = new Instant(timeMs - 5000);
    if (candidate.isAfter(watermark)) watermark = candidate;
    return new Instant(timeMs);
  }
  @Override public Instant getWatermark(PartitionContext context) { return watermark; }
}
