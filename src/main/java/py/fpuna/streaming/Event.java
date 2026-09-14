package py.fpuna.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

/** Immutable v1 event; one currency and positive payments only. */
public final class Event implements Serializable {
  static final ObjectMapper JSON = new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  public final String id, key, type;
  public final long timeMs, amount;
  public Event(String id, String key, String type, long timeMs, long amount) {
    this.id = id; this.key = key; this.type = type; this.timeMs = timeMs; this.amount = amount;
  }
  @Override public boolean equals(Object o) { return o instanceof Event e && id.equals(e.id) && key.equals(e.key) && type.equals(e.type) && timeMs==e.timeMs && amount==e.amount; }
  @Override public int hashCode() { return java.util.Objects.hash(id,key,type,timeMs,amount); }
  private static String required(JsonNode n, String name) {
    JsonNode v = n.get(name);
    if (v == null || !v.isTextual() || v.asText().isBlank() || v.asText().length() > 200)
      throw new IllegalArgumentException("invalid " + name);
    return v.asText();
  }
  public static Event parse(String kafkaKey, String raw) {
    try {
      JsonNode n = JSON.readTree(raw);
      if (n == null || !n.isObject() || !n.path("schema_version").isIntegralNumber()
          || !n.path("schema_version").canConvertToInt() || n.path("schema_version").asInt() != 1) throw new IllegalArgumentException("unsupported schema_version");
      String id = required(n, "event_id"), key = required(n, "key"), type = required(n, "type");
      if (!key.equals(kafkaKey)) throw new IllegalArgumentException("Kafka key differs from event key");
      String time = required(n, "event_time");
      if (!time.endsWith("Z")) throw new IllegalArgumentException("event_time must be UTC (Z)");
      long ms = Instant.parse(time).toEpochMilli();
      // Keep the supported temporal domain explicit; avoid Beam's extreme timestamp sentinels.
      if (ms < 0 || ms > 4102444800000L) throw new IllegalArgumentException("timestamp outside 1970..2100");
      JsonNode payload = n.path("payload");
      if (!payload.isObject()) throw new IllegalArgumentException("payload must be an object");
      long amount = 0;
      if (type.equals("payment")) {
        JsonNode v = payload.path("amount_minor");
        if (!v.isIntegralNumber() || !v.canConvertToLong() || v.asLong() <= 0 || v.asLong() > 1000000000000L)
          throw new IllegalArgumentException("invalid amount_minor");
        if (!"PYG".equals(payload.path("currency").asText())) throw new IllegalArgumentException("currency must be PYG");
        amount = v.asLong();
      } else if (!type.equals("heartbeat")) throw new IllegalArgumentException("unsupported type");
      return new Event(id, key, type, ms, amount);
    } catch (IllegalArgumentException ex) { throw ex; }
      catch (Exception ex) { throw new IllegalArgumentException("invalid JSON or timestamp", ex); }
  }
  public String json() {
    try {
      return JSON.writeValueAsString(Map.of("schema_version", 1, "event_id", id, "key", key,
          "type", type, "event_time", Instant.ofEpochMilli(timeMs).toString(),
          "payload", type.equals("payment") ? Map.of("amount_minor", amount, "currency", "PYG") : Map.of()));
    } catch (Exception ex) { throw new IllegalStateException(ex); }
  }
}
