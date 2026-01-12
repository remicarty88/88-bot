BEGIN;

CREATE TABLE IF NOT EXISTS kv (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS subscriptions (
  user_id INTEGER PRIMARY KEY,
  paid_until INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS free_users (
  user_id INTEGER PRIMARY KEY,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
  chat_id INTEGER NOT NULL,
  message_id INTEGER NOT NULL,
  payload_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY(chat_id, message_id)
);

CREATE TABLE IF NOT EXISTS media (
  chat_id INTEGER NOT NULL,
  message_id INTEGER NOT NULL,
  kind TEXT NOT NULL,
  path TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY(chat_id, message_id)
);

CREATE TABLE IF NOT EXISTS forwarded (
  chat_id INTEGER NOT NULL,
  message_id INTEGER NOT NULL,
  tag TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY(chat_id, message_id, tag)
);

CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  username TEXT,
  name TEXT,
  first_seen INTEGER NOT NULL,
  last_seen INTEGER NOT NULL,
  blocked INTEGER NOT NULL DEFAULT 0,
  bot_user INTEGER NOT NULL DEFAULT 0,
  whitelisted INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER,
  action TEXT NOT NULL,
  chat_id INTEGER,
  message_id INTEGER,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS support_state (
  user_id INTEGER PRIMARY KEY,
  pending INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS broadcast_state (
  user_id INTEGER PRIMARY KEY,
  pending INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS access_state (
  user_id INTEGER PRIMARY KEY,
  pending INTEGER NOT NULL DEFAULT 0,
  kind TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS notify_recipients (
  user_id INTEGER PRIMARY KEY,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS business_connections (
  connection_id TEXT PRIMARY KEY,
  owner_user_id INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  notified INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS business_bind_state (
  user_id INTEGER PRIMARY KEY,
  pending INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS owner_chats (
  owner_user_id INTEGER NOT NULL,
  chat_id INTEGER NOT NULL,
  first_seen INTEGER NOT NULL,
  last_seen INTEGER NOT NULL,
  PRIMARY KEY(owner_user_id, chat_id)
);

INSERT INTO kv(key, value) VALUES('stars_price_7d', '15')
  ON CONFLICT(key) DO NOTHING;
INSERT INTO kv(key, value) VALUES('stars_price_14d', '25')
  ON CONFLICT(key) DO NOTHING;
INSERT INTO kv(key, value) VALUES('stars_price_30d', '45')
  ON CONFLICT(key) DO NOTHING;

COMMIT;
