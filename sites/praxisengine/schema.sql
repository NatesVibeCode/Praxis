CREATE TABLE IF NOT EXISTS subscribers (
  email TEXT PRIMARY KEY NOT NULL,
  first_source TEXT NOT NULL DEFAULT 'landing',
  last_source TEXT NOT NULL DEFAULT 'landing',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  submit_count INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS subscriber_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'landing',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (email) REFERENCES subscribers(email)
);

CREATE INDEX IF NOT EXISTS subscriber_events_email_created_at_idx
ON subscriber_events (email, created_at);

CREATE INDEX IF NOT EXISTS subscribers_updated_at_idx
ON subscribers (updated_at);
