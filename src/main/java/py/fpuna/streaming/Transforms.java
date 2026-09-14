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

  public static class Deduplicate extends DoFn<KV<String, Event>, KV<String, Event>> {
    @StateId("seen") private final StateSpec<SetState<String>> seen = StateSpecs.set(StringUtf8Coder.of());
    @TimerId("gc") private final TimerSpec gc = TimerSpecs.timer(TimeDomain.EVENT_TIME);
    @ProcessElement public void process(ProcessContext c, BoundedWindow window,
        @StateId("seen") SetState<String> ids, @TimerId("gc") Timer timer) {
      Event e = c.element().getValue();
      if (ids.contains(e.id).read()) {
        Metrics.counter("payments", "duplicates").inc(); return;
      }
      ids.add(e.id);
      timer.withNoOutputTimestamp().set(window.maxTimestamp().plus(Duration.standardSeconds(LATENESS_SECONDS)));
      c.output(c.element());
    }
    @OnTimer("gc") public void clear(@StateId("seen") SetState<String> ids) {
      ids.clear(); Metrics.counter("payments", "state_expired").inc();
    }
  }

  public static class Totals implements Serializable {
    public long count, amount;
    @Override public boolean equals(Object o) { return o instanceof Totals t && count==t.count && amount==t.amount; }
    @Override public int hashCode() { return java.util.Objects.hash(count,amount); }
    public Totals() {}
    public Totals(long count, long amount) { this.count = count; this.amount = amount; }
  }
  public static class SumPayments extends Combine.CombineFn<Event, Totals, Totals> {
    @Override public Totals createAccumulator() { return new Totals(); }
    @Override public Totals addInput(Totals a, Event e) {
      return new Totals(Math.addExact(a.count, 1), Math.addExact(a.amount, e.amount));
    }
    @Override public Totals mergeAccumulators(Iterable<Totals> all) {
      Totals a = new Totals();
      for (Totals b : all) { a.count = Math.addExact(a.count, b.count); a.amount = Math.addExact(a.amount, b.amount); }
      return a;
    }
    @Override public Totals extractOutput(Totals a) { return a; }
    @Override public Coder<Totals> getAccumulatorCoder(CoderRegistry r, Coder<Event> input) {
      return SerializableCoder.of(Totals.class);
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
        .apply("DeduplicateWithinMerchantWindow", ParDo.of(new Deduplicate()))
        .apply("IncrementalTotals", Combine.perKey(new SumPayments()))
        .apply("MaterializePane", ParDo.of(new Format())).setCoder(SerializableCoder.of(Result.class));
  }
}
