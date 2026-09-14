package py.fpuna.streaming;

import java.time.Instant;
import java.util.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public final class Producer {
  public static final long BASE = Instant.parse("2026-09-14T12:00:00Z").toEpochMilli();
  static Properties properties() {
    Properties p = new Properties();
    p.put("bootstrap.servers", App.env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
    p.put("key.serializer", StringSerializer.class.getName()); p.put("value.serializer", StringSerializer.class.getName());
    p.put("acks", "all"); p.put("enable.idempotence", "true"); p.put("max.in.flight.requests.per.connection", "5");
    p.put("delivery.timeout.ms", "120000"); return p;
  }
  static void send(KafkaProducer<String,String> p, Event e, Integer partition) throws Exception {
    var m = p.send(new ProducerRecord<>("payments.v1", partition, e.timeMs, e.key, e.json())).get();
    System.out.printf("PRODUCED id=%s type=%s time=%s partition=%d offset=%d%n", e.id, e.type,
        Instant.ofEpochMilli(e.timeMs), m.partition(), m.offset());
  }
  public static List<Event> demoEvents() {
    Event first = new Event("demo-1", "merchant-A", "payment", BASE+10000, 100);
    return List.of(first, new Event("demo-2", "merchant-A", "payment", BASE+30000, 200), first,
        new Event("demo-3", "merchant-A", "payment", BASE+20000, 300),
        new Event("demo-4", "merchant-B", "payment", BASE+25000, 700),
        new Event("demo-5", "merchant-A", "payment", BASE+70000, 900));
  }
  static void heartbeat(KafkaProducer<String,String> p, long ms) throws Exception {
    for (int partition = 0; partition < 3; partition++)
      send(p, new Event("heartbeat-"+partition+"-"+ms, "_clock-"+partition, "heartbeat", ms, 0), partition);
  }
  public static void main(String[] args) throws Exception {
    String mode = args.length == 0 ? "demo" : args[0];
    try (KafkaProducer<String,String> p = new KafkaProducer<>(properties())) {
      if (mode.equals("demo")) {
        for (Event e : demoEvents()) send(p, e, null);
        p.send(new ProducerRecord<>("payments.v1", "merchant-A", "{invalid-json")).get();
        // Domain progress markers on EVERY partition close windows, including inactive merchants.
        heartbeat(p, BASE+300000);
      } else if (mode.equals("live")) {
        long seed = args.length > 1 ? Long.parseLong(args[1]) : 42;
        long interval = args.length > 2 ? Long.parseLong(args[2]) : 1000;
        long base = args.length > 3 ? Instant.parse(args[3]).toEpochMilli() : System.currentTimeMillis();
        if (interval < 1) throw new IllegalArgumentException("interval must be positive milliseconds");
        Random random = new Random(seed);
        for (long i = 0; !Thread.currentThread().isInterrupted(); i++) {
          long time = base + i*interval;
          // Every seventh payment arrives with event_time 15 seconds behind the logical clock.
          Event e = new Event("live-"+seed+"-"+base+"-"+i, "merchant-"+(random.nextInt(3)+1),
              "payment", time-(i%7 == 6 ? 15000 : 0), 100+random.nextInt(10000));
          send(p,e,null);
          if (i%10 == 9) send(p,e,null);
          heartbeat(p,time);
          Thread.sleep(interval);
        }
      } else throw new IllegalArgumentException("Use demo or live [seed] [interval_ms] [UTC_base]");
    }
  }
}
