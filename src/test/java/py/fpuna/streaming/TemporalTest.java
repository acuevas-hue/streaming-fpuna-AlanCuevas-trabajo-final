package py.fpuna.streaming;

import java.util.*;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.*;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.testing.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;

public class TemporalTest {
  @Rule public final TestPipeline pipeline=TestPipeline.create();
  static final KvCoder<String,Event> CODER=KvCoder.of(StringUtf8Coder.of(),SerializableCoder.of(Event.class));
  static TimestampedValue<KV<String,Event>> e(String id,String key,long second,long amount) {
    return TimestampedValue.of(KV.of(key,new Event(id,key,"payment",second*1000,amount)),new Instant(second*1000));
  }
  @Test public void duplicateLateAndExpiredEvents() {
    var stream=TestStream.create(CODER).advanceWatermarkTo(new Instant(0))
        .addElements(e("1","A",10,100),e("2","A",30,200),e("1","A",10,100))
        .advanceWatermarkTo(new Instant(65000))
        .addElements(e("3","A",20,300)) // late, but inside 120-second lateness
        .advanceWatermarkTo(new Instant(181000))
        .addElements(e("expired","A",40,9999)) // past window GC: must not affect totals
        .advanceWatermarkToInfinity();
    var result=Transforms.aggregate(pipeline.apply(stream));
    PAssert.that(result).satisfies(rows->{
      boolean late=false;
      for(var r:rows) {
        assertTrue("duplicate/expired record counted",r.count<=3);
        if(r.count==3) { assertEquals(600,r.amount); if(r.timing.equals("LATE")) late=true; }
      }
      assertTrue("expected a late correction pane",late); return null;
    });
    pipeline.run().waitUntilFinish();
  }
  @Test public void keysAndWindowBoundariesAreIsolated() {
    var stream=TestStream.create(CODER).advanceWatermarkTo(new Instant(0))
        .addElements(e("same","A",59,100),e("same","B",59,700),e("next","A",60,900))
        .advanceWatermarkToInfinity();
    PAssert.that(Transforms.aggregate(pipeline.apply(stream))).satisfies(rows->{
      Set<String> summaries=new HashSet<>(); for(var r:rows) summaries.add(r.summary());
      assertEquals(Set.of("A|0|1|100","B|0|1|700","A|60000|1|900"),summaries); return null;
    });
    pipeline.run().waitUntilFinish();
  }
  @Test public void validationSeparatesInvalidAndHeartbeat() {
    Event e=new Event("1","A","payment",10000,100);
    var values=pipeline.apply(Create.of(KV.of("A",e.json()),KV.of("A","bad"),
        KV.of("A",new Event("h","A","heartbeat",10000,0).json())));
    var result=values.apply(ParDo.of(new Transforms.Validate()).withOutputTags(Transforms.VALID,TupleTagList.of(Transforms.INVALID)));
    result.get(Transforms.VALID).setCoder(CODER);
    result.get(Transforms.INVALID).setCoder(StringUtf8Coder.of());
    PAssert.thatSingleton(result.get(Transforms.VALID).apply(Count.globally())).isEqualTo(1L);
    PAssert.thatSingleton(result.get(Transforms.INVALID).apply(Count.globally())).isEqualTo(1L);
    pipeline.run().waitUntilFinish();
  }
}
