package py.fpuna.streaming;

import java.io.Serializable;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;

public final class Transforms {
  private Transforms() {}
  public static final TupleTag<KV<String, Event>> VALID = new TupleTag<KV<String, Event>>() {};
  public static final TupleTag<String> INVALID = new TupleTag<String>() {};
  public static final long LATENESS_SECONDS = 120;

  public static class Validate extends DoFn<KV<String, String>, KV<String, Event>> {
    @ProcessElement public void process(ProcessContext c) {
      Metrics.counter("payments", "consumed").inc();
      try {
        Event e = Event.parse(c.element().getKey(), c.element().getValue());
        if (e.type.equals("payment")) {
          // Timestamp already assigned from this exact event_time by KafkaIO's DomainTimePolicy.
          c.output(KV.of(e.key, e));
          Metrics.counter("payments", "valid").inc();
        } else Metrics.counter("payments", "heartbeats").inc();
      } catch (IllegalArgumentException ex) {
        try {
          String error = Event.JSON.writeValueAsString(java.util.Map.of("reason", ex.getMessage(),
              "key", String.valueOf(c.element().getKey()), "raw", String.valueOf(c.element().getValue())));
          c.output(INVALID, error);
          Metrics.counter("payments", "invalid").inc();
        } catch (java.io.IOException impossible) { throw new IllegalStateException(impossible); }
      }
    }
  }

  public static class Totals implements Serializable {
    public long count, amount;
    @Override public boolean equals(Object o) { return o instanceof Totals t && count==t.count && amount==t.amount; }
    @Override public int hashCode() { return java.util.Objects.hash(count,amount); }
    public Totals() {}
    public Totals(long count, long amount) { this.count = count; this.amount = amount; }
  }
  public static class Accumulator implements Serializable {
    final java.util.Map<String,Long> amounts = new java.util.HashMap<>();
    long amount;
    @Override public boolean equals(Object o) { return o instanceof Accumulator a && amount==a.amount && amounts.equals(a.amounts); }
    @Override public int hashCode() { return java.util.Objects.hash(amounts,amount); }
  }
  /** Mergeable exact deduplication: retain IDs/amounts, never full payment payloads. */
  public static class SumPayments extends Combine.CombineFn<Event, Accumulator, Totals> {
    @Override public Accumulator createAccumulator() { return new Accumulator(); }
    private Accumulator add(Accumulator a, String id, long amount) {
      Long previous = a.amounts.get(id);
      if (previous == null) {
        a.amount = Math.addExact(a.amount, amount); a.amounts.put(id,amount);
      } else if (previous != amount) throw new IllegalArgumentException("Conflicting amount for event_id " + id);
      return a;
    }
    @Override public Accumulator addInput(Accumulator a, Event e) { return add(a,e.id,e.amount); }
    @Override public Accumulator mergeAccumulators(Iterable<Accumulator> all) {
      Accumulator a = createAccumulator();
      for (Accumulator b : all) for (var entry : b.amounts.entrySet()) add(a,entry.getKey(),entry.getValue());
      return a;
    }
    @Override public Totals extractOutput(Accumulator a) { return new Totals(a.amounts.size(),a.amount); }
    @Override public Coder<Accumulator> getAccumulatorCoder(CoderRegistry r, Coder<Event> input) {
      return SerializableCoder.of(Accumulator.class);
    }
    @Override public Coder<Totals> getDefaultOutputCoder(CoderRegistry r, Coder<Event> input) {
      return SerializableCoder.of(Totals.class);
    }
  }
  public static class Result implements Serializable {
    public final String key, timing;
    public final long windowStart, count, amount, pane;
    public Result(String key, long windowStart, long count, long amount, long pane, String timing) {
      this.key = key; this.windowStart = windowStart; this.count = count; this.amount = amount;
      this.pane = pane; this.timing = timing;
    }
    @Override public boolean equals(Object o) { return o instanceof Result r && key.equals(r.key) && windowStart==r.windowStart && count==r.count && amount==r.amount && pane==r.pane && timing.equals(r.timing); }
    @Override public int hashCode() { return java.util.Objects.hash(key,windowStart,count,amount,pane,timing); }
    public String summary() { return key + "|" + windowStart + "|" + count + "|" + amount; }
  }
  public static class Format extends DoFn<KV<String, Totals>, Result> {
    @ProcessElement public void process(ProcessContext c, BoundedWindow window) {
      Totals t = c.element().getValue();
      c.output(new Result(c.element().getKey(), ((IntervalWindow) window).start().getMillis(),
          t.count, t.amount, c.pane().getIndex(), c.pane().getTiming().name()));
      Metrics.counter("payments", "panes").inc();
    }
  }
  public static PCollection<Result> aggregate(PCollection<KV<String, Event>> events) {
    return events
        .apply("MinuteWindows", Window.<KV<String, Event>>into(FixedWindows.of(Duration.standardSeconds(60)))
            .triggering(AfterWatermark.pastEndOfWindow()
                .withEarlyFirings(AfterPane.elementCountAtLeast(1))
                .withLateFirings(AfterPane.elementCountAtLeast(1)))
            .withAllowedLateness(Duration.standardSeconds(LATENESS_SECONDS))
            .accumulatingFiredPanes())
        .apply("IncrementalTotals", Combine.perKey(new SumPayments()))
        .apply("MaterializePane", ParDo.of(new Format())).setCoder(SerializableCoder.of(Result.class));
  }
}
