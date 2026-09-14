package py.fpuna.streaming;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.*;
import java.util.HexFormat;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Database {
  private static final Logger LOG = LoggerFactory.getLogger(Database.class);
  private Database() {}
  public static Connection connect() throws SQLException {
    return DriverManager.getConnection(App.env("JDBC_URL", "jdbc:postgresql://localhost:5432/payments"),
        App.env("PGUSER", "payments"), App.env("PGPASSWORD", "local-demo-only"));
  }
  public static final String UPSERT = """
      INSERT INTO payment_windows(merchant_id,window_start,event_count,total_minor,pane_index,timing)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT (merchant_id,window_start) DO UPDATE SET
        event_count=EXCLUDED.event_count, total_minor=EXCLUDED.total_minor,
        pane_index=EXCLUDED.pane_index, timing=EXCLUDED.timing, updated_at=now()
      WHERE EXCLUDED.event_count > payment_windows.event_count
      """;
  public static void upsert(Connection c, Transforms.Result r) throws SQLException {
    try (PreparedStatement p = c.prepareStatement(UPSERT)) {
      p.setString(1, r.key); p.setTimestamp(2, new Timestamp(r.windowStart));
      p.setLong(3, r.count); p.setLong(4, r.amount); p.setLong(5, r.pane); p.setString(6, r.timing);
      p.executeUpdate();
    }
  }
  interface SqlWork { void run(Connection c) throws Exception; }
  static void retry(SqlWork work) throws Exception {
    for (int attempt = 0; ; attempt++) {
      try (Connection c = connect()) { work.run(c); return; }
      catch (SQLException e) {
        String state = e.getSQLState();
        if (attempt >= 3 || state == null || !(state.startsWith("08") || state.startsWith("40"))) throw e;
        Metrics.counter("payments", "db_retries").inc();
        Thread.sleep(250L << attempt);
      }
    }
  }
  public static class WriteResult extends DoFn<Transforms.Result, Void> {
    @ProcessElement public void process(@Element Transforms.Result r) throws Exception {
      retry(c -> upsert(c, r));
      Metrics.counter("payments", "sink_writes").inc();
      LOG.info("RESULT {} pane={} timing={}", r.summary(), r.pane, r.timing);
    }
  }
  public static class WriteInvalid extends DoFn<String, Void> {
    @ProcessElement public void process(@Element String raw) throws Exception {
      String id = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(raw.getBytes(StandardCharsets.UTF_8)));
      retry(c -> {
        try (PreparedStatement p = c.prepareStatement("INSERT INTO invalid_events(error_id,detail) VALUES (?,?::jsonb) ON CONFLICT DO NOTHING")) {
          p.setString(1, id); p.setString(2, raw); p.executeUpdate();
        }
      });
      LOG.warn("INVALID {}", raw);
    }
  }
}
