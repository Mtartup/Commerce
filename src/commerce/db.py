from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from commerce.util import now_utc_iso, new_id


SCHEMA_VERSION = 5


class AdsDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        return conn

    def init(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL
                );
                """
            )
            current_version = self._get_schema_version(conn)

            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS connectors (
                  id TEXT PRIMARY KEY,
                  platform TEXT NOT NULL,
                  name TEXT NOT NULL,
                  enabled INTEGER NOT NULL DEFAULT 0,
                  config_json TEXT NOT NULL DEFAULT '{}',
                  capabilities_json TEXT NOT NULL DEFAULT '{}',
                  last_sync_at TEXT,
                  last_error TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS entities (
                  platform TEXT NOT NULL,
                  connector_id TEXT NOT NULL DEFAULT '',
                  account_id TEXT,
                  entity_type TEXT NOT NULL,
                  entity_id TEXT NOT NULL,
                  parent_type TEXT,
                  parent_id TEXT,
                  name TEXT,
                  status TEXT,
                  meta_json TEXT NOT NULL DEFAULT '{}',
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (platform, connector_id, entity_type, entity_id)
                );

                CREATE TABLE IF NOT EXISTS metrics_daily (
                  platform TEXT NOT NULL,
                  connector_id TEXT NOT NULL DEFAULT '',
                  account_id TEXT,
                  entity_type TEXT NOT NULL,
                  entity_id TEXT NOT NULL,
                  date TEXT NOT NULL,
                  spend REAL,
                  impressions INTEGER,
                  clicks INTEGER,
                  conversions REAL,
                  conversion_value REAL,
                  metrics_json TEXT NOT NULL DEFAULT '{}',
                  PRIMARY KEY (platform, connector_id, entity_type, entity_id, date)
                );

                CREATE TABLE IF NOT EXISTS metrics_intraday (
                  platform TEXT NOT NULL,
                  connector_id TEXT NOT NULL DEFAULT '',
                  account_id TEXT,
                  entity_type TEXT NOT NULL,
                  entity_id TEXT NOT NULL,
                  hour_ts TEXT NOT NULL,
                  spend REAL,
                  impressions INTEGER,
                  clicks INTEGER,
                  conversions REAL,
                  conversion_value REAL,
                  metrics_json TEXT NOT NULL DEFAULT '{}',
                  PRIMARY KEY (platform, connector_id, entity_type, entity_id, hour_ts)
                );

                CREATE TABLE IF NOT EXISTS kpi_profiles (
                  id TEXT PRIMARY KEY,
                  name TEXT NOT NULL,
                  platform TEXT,
                  objective TEXT NOT NULL,
                  definition_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS entity_kpi_profiles (
                  platform TEXT NOT NULL,
                  entity_type TEXT NOT NULL,
                  entity_id TEXT NOT NULL,
                  kpi_profile_id TEXT NOT NULL,
                  enabled INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (platform, entity_type, entity_id),
                  FOREIGN KEY (kpi_profile_id) REFERENCES kpi_profiles(id)
                );

                CREATE TABLE IF NOT EXISTS rules (
                  id TEXT PRIMARY KEY,
                  name TEXT NOT NULL,
                  enabled INTEGER NOT NULL DEFAULT 1,
                  platform TEXT,
                  kpi_profile_id TEXT,
                  rule_type TEXT NOT NULL,
                  params_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  FOREIGN KEY (kpi_profile_id) REFERENCES kpi_profiles(id)
                );

                CREATE TABLE IF NOT EXISTS action_proposals (
                  id TEXT PRIMARY KEY,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  status TEXT NOT NULL,
                  platform TEXT NOT NULL,
                  connector_id TEXT,
                  action_type TEXT NOT NULL,
                  account_id TEXT,
                  entity_type TEXT NOT NULL,
                  entity_id TEXT NOT NULL,
                  payload_json TEXT NOT NULL DEFAULT '{}',
                  reason TEXT,
                  risk TEXT NOT NULL DEFAULT 'low',
                  requires_approval INTEGER NOT NULL DEFAULT 1,
                  approved_by TEXT,
                  approved_at TEXT,
                  executed_at TEXT,
                  result_json TEXT,
                  error TEXT,
                  telegram_chat_id INTEGER,
                  telegram_message_id INTEGER,
                  FOREIGN KEY (connector_id) REFERENCES connectors(id)
                );

                CREATE INDEX IF NOT EXISTS idx_action_proposals_status_created
                ON action_proposals(status, created_at);

                CREATE TABLE IF NOT EXISTS executions (
                  id TEXT PRIMARY KEY,
                  proposal_id TEXT NOT NULL,
                  started_at TEXT NOT NULL,
                  finished_at TEXT,
                  status TEXT NOT NULL,
                  before_json TEXT,
                  after_json TEXT,
                  error TEXT,
                  FOREIGN KEY (proposal_id) REFERENCES action_proposals(id)
                );

                CREATE TABLE IF NOT EXISTS tracking_links (
                  code TEXT PRIMARY KEY,
                  destination_url TEXT NOT NULL,
                  channel TEXT,
                  objective TEXT,
                  entity_platform TEXT,
                  entity_type TEXT,
                  entity_id TEXT,
                  meta_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS click_events (
                  id TEXT PRIMARY KEY,
                  code TEXT NOT NULL,
                  date_kst TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  user_agent TEXT,
                  ip_hash TEXT,
                  referer TEXT,
                  query_json TEXT NOT NULL DEFAULT '{}',
                  FOREIGN KEY (code) REFERENCES tracking_links(code)
                );

                CREATE INDEX IF NOT EXISTS idx_click_events_code_date
                ON click_events(code, date_kst, created_at);

                CREATE TABLE IF NOT EXISTS conversion_events (
                  id TEXT PRIMARY KEY,
                  click_id TEXT,
                  date_kst TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  order_id TEXT,
                  value REAL,
                  currency TEXT,
                  source TEXT NOT NULL,
                  extra_json TEXT NOT NULL DEFAULT '{}',
                  UNIQUE(order_id, source),
                  FOREIGN KEY (click_id) REFERENCES click_events(id)
                );

                CREATE INDEX IF NOT EXISTS idx_conversion_events_click_date
                ON conversion_events(click_id, date_kst, created_at);

                CREATE TABLE IF NOT EXISTS store_orders (
                  store TEXT NOT NULL,
                  order_id TEXT NOT NULL,
                  ordered_at TEXT,
                  date_kst TEXT NOT NULL,
                  status TEXT,
                  amount REAL,
                  currency TEXT,
                  order_place_id TEXT,
                  order_place_name TEXT,
                  inflow_path TEXT,
                  inflow_path_detail TEXT,
                  referer TEXT,
                  source_raw TEXT,
                  meta_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (store, order_id)
                );

                CREATE INDEX IF NOT EXISTS idx_store_orders_store_date
                ON store_orders(store, date_kst);

                CREATE INDEX IF NOT EXISTS idx_store_orders_store_inflow
                ON store_orders(store, inflow_path, date_kst);
                """
            )
            if current_version < 5:
                self._migrate_to_v5(conn)
            self._ensure_v5_indexes(conn)
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",
                ("schema_version", str(SCHEMA_VERSION)),
            )

    def _get_schema_version(self, conn: sqlite3.Connection) -> int:
        row = conn.execute(
            "SELECT value FROM meta WHERE key='schema_version'"
        ).fetchone()
        if not row:
            return 0
        try:
            return int(row["value"])
        except Exception:
            return 0

    def _table_exists(self, conn: sqlite3.Connection, table: str) -> bool:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        return row is not None

    def _column_exists(self, conn: sqlite3.Connection, table: str, column: str) -> bool:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return any(str(r["name"]) == column for r in rows)

    def _migrate_to_v5(self, conn: sqlite3.Connection) -> None:
        if self._table_exists(conn, "entities") and not self._column_exists(conn, "entities", "connector_id"):
            conn.executescript(
                """
                ALTER TABLE entities RENAME TO entities_v4_old;
                CREATE TABLE entities (
                  platform TEXT NOT NULL,
                  connector_id TEXT NOT NULL DEFAULT '',
                  account_id TEXT,
                  entity_type TEXT NOT NULL,
                  entity_id TEXT NOT NULL,
                  parent_type TEXT,
                  parent_id TEXT,
                  name TEXT,
                  status TEXT,
                  meta_json TEXT NOT NULL DEFAULT '{}',
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (platform, connector_id, entity_type, entity_id)
                );
                """
            )
            conn.execute(
                """
                INSERT INTO entities(
                  platform, connector_id, account_id, entity_type, entity_id,
                  parent_type, parent_id, name, status, meta_json, updated_at
                )
                SELECT
                  e.platform,
                  COALESCE(pc.connector_id, ''),
                  e.account_id,
                  e.entity_type,
                  e.entity_id,
                  e.parent_type,
                  e.parent_id,
                  e.name,
                  e.status,
                  e.meta_json,
                  e.updated_at
                FROM entities_v4_old e
                LEFT JOIN (
                  SELECT platform, MIN(id) AS connector_id
                  FROM connectors
                  GROUP BY platform
                ) pc ON pc.platform = e.platform
                """
            )
            conn.execute("DROP TABLE entities_v4_old")

        if self._table_exists(conn, "metrics_daily") and not self._column_exists(conn, "metrics_daily", "connector_id"):
            conn.executescript(
                """
                ALTER TABLE metrics_daily RENAME TO metrics_daily_v4_old;
                CREATE TABLE metrics_daily (
                  platform TEXT NOT NULL,
                  connector_id TEXT NOT NULL DEFAULT '',
                  account_id TEXT,
                  entity_type TEXT NOT NULL,
                  entity_id TEXT NOT NULL,
                  date TEXT NOT NULL,
                  spend REAL,
                  impressions INTEGER,
                  clicks INTEGER,
                  conversions REAL,
                  conversion_value REAL,
                  metrics_json TEXT NOT NULL DEFAULT '{}',
                  PRIMARY KEY (platform, connector_id, entity_type, entity_id, date)
                );
                """
            )
            conn.execute(
                """
                INSERT INTO metrics_daily(
                  platform, connector_id, account_id, entity_type, entity_id, date,
                  spend, impressions, clicks, conversions, conversion_value, metrics_json
                )
                SELECT
                  m.platform,
                  COALESCE(pc.connector_id, ''),
                  m.account_id,
                  m.entity_type,
                  m.entity_id,
                  m.date,
                  m.spend,
                  m.impressions,
                  m.clicks,
                  m.conversions,
                  m.conversion_value,
                  m.metrics_json
                FROM metrics_daily_v4_old m
                LEFT JOIN (
                  SELECT platform, MIN(id) AS connector_id
                  FROM connectors
                  GROUP BY platform
                ) pc ON pc.platform = m.platform
                """
            )
            conn.execute("DROP TABLE metrics_daily_v4_old")

        if self._table_exists(conn, "metrics_intraday") and not self._column_exists(conn, "metrics_intraday", "connector_id"):
            conn.executescript(
                """
                ALTER TABLE metrics_intraday RENAME TO metrics_intraday_v4_old;
                CREATE TABLE metrics_intraday (
                  platform TEXT NOT NULL,
                  connector_id TEXT NOT NULL DEFAULT '',
                  account_id TEXT,
                  entity_type TEXT NOT NULL,
                  entity_id TEXT NOT NULL,
                  hour_ts TEXT NOT NULL,
                  spend REAL,
                  impressions INTEGER,
                  clicks INTEGER,
                  conversions REAL,
                  conversion_value REAL,
                  metrics_json TEXT NOT NULL DEFAULT '{}',
                  PRIMARY KEY (platform, connector_id, entity_type, entity_id, hour_ts)
                );
                """
            )
            conn.execute(
                """
                INSERT INTO metrics_intraday(
                  platform, connector_id, account_id, entity_type, entity_id, hour_ts,
                  spend, impressions, clicks, conversions, conversion_value, metrics_json
                )
                SELECT
                  m.platform,
                  COALESCE(pc.connector_id, ''),
                  m.account_id,
                  m.entity_type,
                  m.entity_id,
                  m.hour_ts,
                  m.spend,
                  m.impressions,
                  m.clicks,
                  m.conversions,
                  m.conversion_value,
                  m.metrics_json
                FROM metrics_intraday_v4_old m
                LEFT JOIN (
                  SELECT platform, MIN(id) AS connector_id
                  FROM connectors
                  GROUP BY platform
                ) pc ON pc.platform = m.platform
                """
            )
            conn.execute("DROP TABLE metrics_intraday_v4_old")

    def _ensure_v5_indexes(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_metrics_daily_platform_connector_date
            ON metrics_daily(platform, connector_id, date);

            CREATE INDEX IF NOT EXISTS idx_metrics_intraday_platform_connector_hour
            ON metrics_intraday(platform, connector_id, hour_ts);
            """
        )

    def seed_default_connectors(self) -> None:
        now = now_utc_iso()
        defaults = [
            ("naver", "Naver SearchAd", 1),
            ("meta", "Meta Ads", 0),
            ("google", "Google Ads", 0),
            ("tiktok", "TikTok Ads", 0),
            ("coupang", "Coupang (commerce+ads)", 0),
            ("smartstore", "Naver Smart Store", 0),
            ("cafe24_analytics", "Cafe24 Analytics", 0),
        ]
        with self._connect() as conn:
            for platform, name, enabled in defaults:
                row = conn.execute(
                    "SELECT id, config_json FROM connectors WHERE platform=? AND name=?",
                    (platform, name),
                ).fetchone()
                default_config = {"mode": "import"}
                if platform == "naver":
                    default_config = {
                        "mode": "import",
                        "product_types": ["powerlink", "powercontent", "shoppingsearch"],
                    }
                if row:
                    # Preserve existing config unless it's empty.
                    cfg_raw = (row["config_json"] or "").strip()
                    if cfg_raw in {"", "{}", "null"}:
                        conn.execute(
                            "UPDATE connectors SET enabled=?, config_json=?, updated_at=? WHERE id=?",
                            (enabled, json.dumps(default_config, ensure_ascii=True), now, row["id"]),
                        )
                    else:
                        conn.execute(
                            "UPDATE connectors SET enabled=?, updated_at=? WHERE id=?",
                            (enabled, now, row["id"]),
                        )
                    continue
                conn.execute(
                    """
                    INSERT INTO connectors(
                      id, platform, name, enabled, config_json, capabilities_json,
                      created_at, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_id("con"),
                        platform,
                        name,
                        enabled,
                        json.dumps(default_config, ensure_ascii=True),
                        json.dumps({}),
                        now,
                        now,
                    ),
                )

            profile_id = self._ensure_kpi_profile(conn, now)
            self._ensure_default_rule(conn, now, profile_id)

    def _ensure_kpi_profile(self, conn: sqlite3.Connection, now: str) -> str:
        row = conn.execute(
            "SELECT id FROM kpi_profiles WHERE name=?",
            ("Default: Spend Guardrail",),
        ).fetchone()
        if row:
            return str(row["id"])
        pid = new_id("kpi")
        definition = {
            "description": "Default guardrail profile for early MVP",
            "primary_metrics": ["spend", "conversions", "conversion_value"],
        }
        conn.execute(
            """
            INSERT INTO kpi_profiles(id, name, platform, objective, definition_json, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (pid, "Default: Spend Guardrail", None, "guardrail", json.dumps(definition), now, now),
        )
        return pid

    def _ensure_default_rule(self, conn: sqlite3.Connection, now: str, kpi_profile_id: str) -> None:
        row = conn.execute(
            "SELECT id FROM rules WHERE name=?",
            ("Kill Switch: Spend > 50000 and conversions == 0",),
        ).fetchone()
        if row:
            return
        params = {
            "entity_type": "campaign",
            "spend_threshold": 50000,
            "clicks_threshold": 10,
            "conversion_threshold": 0,
            "auto_execute": False,
        }
        conn.execute(
            """
            INSERT INTO rules(
              id, name, enabled, platform, kpi_profile_id, rule_type, params_json, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                new_id("rule"),
                "Kill Switch: Spend > 50000 and conversions == 0",
                0,
                None,
                kpi_profile_id,
                "kill_switch_spend_no_conv",
                json.dumps(params),
                now,
                now,
            ),
        )

    def list_action_proposals(self, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        sql = "SELECT * FROM action_proposals"
        params: list[Any] = []
        if status:
            sql += " WHERE status=?"
            params.append(status)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    def get_action_proposal(self, proposal_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM action_proposals WHERE id=?",
                (proposal_id,),
            ).fetchone()
            return dict(row) if row else None
