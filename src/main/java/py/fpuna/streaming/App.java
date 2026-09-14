package py.fpuna.streaming;

import java.util.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.apache.kafka.common.serialization.StringDeserializer;

public final class App {
  public static String env(String key, String fallback) { return System.getenv().getOrDefault(key, fallback); }
  public static void main(String[] args) throws Exception {
    String mode = args.length == 0 ? "pipeline" : args[0];
    switch (mode) {
      case "produce" -> Producer.main(Arrays.copyOfRange(args, 1, args.length));
      case "verify" -> Smoke.main(Arrays.copyOfRange(args, 1, args.length));
      case "pipeline" -> runPipeline();
      default -> throw new IllegalArgumentException("Use pipeline, produce, or verify");
    }
  }
  public static void runPipeline() {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.as(StreamingOptions.class).setStreaming(true);
    options.as(DirectOptions.class).setTargetParallelism(2);
    Pipeline p = Pipeline.create(options);
    PCollection<KV<String,String>> input = p.apply("ReadPaymentsKafkaIO", KafkaIO.<String,String>read()
        .withBootstrapServers(env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        .withTopic("payments.v1")
        .withKeyDeserializer(StringDeserializer.class).withValueDeserializer(StringDeserializer.class)
        .withConsumerConfigUpdates(Map.of("group.id", "payments-rebuild", "auto.offset.reset", "earliest", "enable.auto.commit", false))
        .withTimestampPolicyFactory((partition, previous) -> new DomainTimePolicy(previous))
        .withoutMetadata());
    PCollectionTuple parsed = input.apply("ValidateContract", ParDo.of(new Transforms.Validate())
        .withOutputTags(Transforms.VALID, TupleTagList.of(Transforms.INVALID)));
    parsed.get(Transforms.INVALID).setCoder(StringUtf8Coder.of()).apply("PersistInvalid", ParDo.of(new Database.WriteInvalid()));
    PCollection<KV<String, Event>> events = parsed.get(Transforms.VALID)
        .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Event.class)));
    Transforms.aggregate(events).apply("IdempotentPostgres", ParDo.of(new Database.WriteResult()));
    p.run().waitUntilFinish();
  }
}
