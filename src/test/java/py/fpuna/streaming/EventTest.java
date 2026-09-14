package py.fpuna.streaming;

import org.junit.Test;
import static org.junit.Assert.*;
import java.util.List;
import java.util.Optional;

public class EventTest {
  Event event() { return new Event("one", "shop", "payment", 10000, 100); }
  @Test public void roundTrip() {
    Event e=Event.parse("shop",event().json()); assertEquals(100,e.amount); assertEquals(10000,e.timeMs);
  }
  @Test public void rejectsInvalidJson() { assertThrows(IllegalArgumentException.class,()->Event.parse("shop","oops")); }
  @Test public void rejectsKeyMismatch() { assertThrows(IllegalArgumentException.class,()->Event.parse("other",event().json())); }
  @Test public void rejectsSchemaVersion() { assertThrows(IllegalArgumentException.class,()->Event.parse("shop",event().json().replace("\"schema_version\":1","\"schema_version\":2"))); }
  @Test public void rejectsOversizedSchemaVersion() { assertThrows(IllegalArgumentException.class,()->Event.parse("shop",event().json().replace("\"schema_version\":1","\"schema_version\":4294967297"))); }
  @Test public void rejectsNegativePayment() { assertThrows(IllegalArgumentException.class,()->Event.parse("shop",new Event("one","shop","payment",10000,-1).json())); }
  @Test public void rejectsFractionalPayment() { assertThrows(IllegalArgumentException.class,()->Event.parse("shop",event().json().replace("\"amount_minor\":100","\"amount_minor\":1.5"))); }
  @Test public void rejectsNonUtc() { assertThrows(IllegalArgumentException.class,()->Event.parse("shop",event().json().replace("10Z","10+00:00"))); }
  @Test public void rejectsCurrency() { assertThrows(IllegalArgumentException.class,()->Event.parse("shop",event().json().replace("PYG","USD"))); }
  @Test public void heartbeatHasNoMoney() { assertEquals(0,Event.parse("shop",new Event("h","shop","heartbeat",10000,0).json()).amount); }
  @Test public void combineMergesIncrementally() {
    var fn=new Transforms.SumPayments(); var a=fn.addInput(fn.createAccumulator(),event());
    var b=fn.addInput(fn.createAccumulator(),new Event("two","shop","payment",10000,100)); var total=fn.extractOutput(fn.mergeAccumulators(List.of(a,b)));
    assertEquals(2,total.count); assertEquals(200,total.amount);
  }
  @Test public void deduplicationSurvivesAccumulatorMerge() {
    var fn=new Transforms.SumPayments();
    var a=fn.addInput(fn.createAccumulator(),event()); var b=fn.addInput(fn.createAccumulator(),event());
    var total=fn.extractOutput(fn.mergeAccumulators(List.of(a,b)));
    assertEquals(1,total.count); assertEquals(100,total.amount);
  }
  @Test public void conflictingDuplicateFails() {
    var fn=new Transforms.SumPayments(); var a=fn.addInput(fn.createAccumulator(),event());
    assertThrows(IllegalArgumentException.class,()->fn.addInput(a,new Event("one","shop","payment",10000,999)));
  }
  @Test public void overflowFailsExplicitly() {
    var a = new Transforms.Accumulator(); a.amount=Long.MAX_VALUE;
    assertThrows(ArithmeticException.class,()->new Transforms.SumPayments().addInput(a,event()));
  }
  @Test public void watermarkMonotonicUnderDisorder() {
    var policy=new DomainTimePolicy(Optional.empty()); policy.observe(30000); policy.observe(10000);
    assertEquals(25000,policy.getWatermark(null).getMillis()); policy.observe(300000);
    assertEquals(295000,policy.getWatermark(null).getMillis());
  }
  @Test public void demoHasStableDuplicateAndDisorder() {
    var e=Producer.demoEvents(); assertEquals(e.get(0).json(),e.get(2).json());
    assertTrue(e.get(3).timeMs < e.get(1).timeMs);
  }
}
