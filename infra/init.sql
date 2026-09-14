CREATE TABLE payment_windows (
  merchant_id text NOT NULL,
  window_start timestamptz NOT NULL,
  event_count bigint NOT NULL CHECK (event_count > 0),
  total_minor bigint NOT NULL CHECK (total_minor > 0),
  pane_index bigint NOT NULL,
  timing text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (merchant_id, window_start)
);
CREATE TABLE invalid_events (
  error_id text PRIMARY KEY,
  detail jsonb NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now()
);
