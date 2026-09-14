package py.fpuna.streaming;

import java.sql.*;
import java.util.*;

/** Executable assertions against the real materialized database. */
public final class Smoke {
  static Map<String,String> read(Connection c) throws SQLException {
    Map<String,String> found = new TreeMap<>();
    try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(
        "SELECT merchant_id,window_start,event_count,total_minor FROM payment_windows")) {
      while (rs.next()) found.put(rs.getString(1)+"|"+rs.getTimestamp(2).getTime(), rs.getLong(3)+"|"+rs.getLong(4));
    }
    return found;
  }
  public static void main(String[] args) throws Exception {
    long base = Producer.BASE;
    Map<String,String> expected = Map.of("merchant-A|"+base,"3|600", "merchant-B|"+base,"1|700",
        "merchant-A|"+(base+60000),"1|900");
    long deadline = System.nanoTime()+180_000_000_000L;
    Map<String,String> actual = Map.of();
    while (System.nanoTime() < deadline) {
      try (Connection c = Database.connect()) {
        actual = read(c);
        long invalid;
        try (Statement s=c.createStatement(); ResultSet rs=s.executeQuery("SELECT count(*) FROM invalid_events")) {
          rs.next(); invalid=rs.getLong(1);
        }
        if (actual.equals(expected) && invalid == 1) {
          // Simulate sink retries and arrival of a stale pane after the latest pane.
          Database.upsert(c, new Transforms.Result("merchant-A",base,3,600,99,"LATE"));
          Database.upsert(c, new Transforms.Result("merchant-A",base,1,100,0,"EARLY"));
          if (!read(c).equals(expected)) throw new AssertionError("UPSERT regressed or duplicated output");
          System.out.println("SMOKE PASS: real KafkaIO output = "+actual+"; invalid=1; stale/retry UPSERT unchanged");
          return;
        }
      }
      Thread.sleep(1000);
    }
    throw new AssertionError("Timed out: expected="+expected+" actual="+actual);
  }
}
