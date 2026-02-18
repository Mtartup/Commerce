from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from commerce.util import now_utc_iso, new_id


DEFAULT_CONNECTOR_ID = ""


class Repo:
    """
    Lightweight repository for worker/web/bot.
    Keeps DB access centralized while staying dependency-free (sqlite3 only).
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        return conn

    @staticmethod
    def _append_connector_filter(
        where: list[str],
        params: list[Any],
        connector_id: str | None,
    ) -> None:
        if connector_id is None:
            return
        where.append("connector_id=?")
        params.append(connector_id or DEFAULT_CONNECTOR_ID)

    def upsert_metric_daily(
        self,
        *,
        platform: str,
        connector_id: str | None = None,
        account_id: str | None,
        entity_type: str,
        entity_id: str,
        day: str,
        spend: float | None,
        impressions: int | None,
        clicks: int | None,
        conversions: float | None,
        conversion_value: float | None,
        metrics_json: dict[str, Any],
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO metrics_daily(
                  platform, connector_id, account_id, entity_type, entity_id, date,
                  spend, impressions, clicks, conversions, conversion_value, metrics_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(platform, connector_id, entity_type, entity_id, date) DO UPDATE SET
                  account_id=excluded.account_id,
                  spend=excluded.spend,
                  impressions=excluded.impressions,
                  clicks=excluded.clicks,
                  conversions=excluded.conversions,
                  conversion_value=excluded.conversion_value,
                  metrics_json=excluded.metrics_json
                """,
                (
                    platform,
                    connector_id or DEFAULT_CONNECTOR_ID,
                    account_id,
                    entity_type,
                    entity_id,
                    day,
                    spend,
                    impressions,
                    clicks,
                    conversions,
                    conversion_value,
                    json.dumps(metrics_json, ensure_ascii=True),
                ),
            )

    def upsert_metric_intraday(
        self,
        *,
        platform: str,
        connector_id: str | None = None,
        account_id: str | None,
        entity_type: str,
        entity_id: str,
        hour_ts: str,
        spend: float | None,
        impressions: int | None,
        clicks: int | None,
        conversions: float | None,
        conversion_value: float | None,
        metrics_json: dict[str, Any],
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO metrics_intraday(
                  platform, connector_id, account_id, entity_type, entity_id, hour_ts,
                  spend, impressions, clicks, conversions, conversion_value, metrics_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(platform, connector_id, entity_type, entity_id, hour_ts) DO UPDATE SET
                  account_id=excluded.account_id,
                  spend=excluded.spend,
                  impressions=excluded.impressions,
                  clicks=excluded.clicks,
                  conversions=excluded.conversions,
                  conversion_value=excluded.conversion_value,
                  metrics_json=excluded.metrics_json
                """,
                (
                    platform,
                    connector_id or DEFAULT_CONNECTOR_ID,
                    account_id,
                    entity_type,
                    entity_id,
                    hour_ts,
                    spend,
                    impressions,
                    clicks,
                    conversions,
                    conversion_value,
                    json.dumps(metrics_json, ensure_ascii=True),
                ),
            )

    def upsert_entity(
        self,
        *,
        platform: str,
        connector_id: str | None = None,
        account_id: str | None,
        entity_type: str,
        entity_id: str,
        parent_type: str | None,
        parent_id: str | None,
        name: str | None,
        status: str | None,
        meta_json: dict[str, Any],
    ) -> None:
        now = now_utc_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO entities(
                  platform, connector_id, account_id, entity_type, entity_id,
                  parent_type, parent_id, name, status, meta_json, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(platform, connector_id, entity_type, entity_id) DO UPDATE SET
                  account_id=excluded.account_id,
                  parent_type=excluded.parent_type,
                  parent_id=excluded.parent_id,
                  name=excluded.name,
                  status=excluded.status,
                  meta_json=excluded.meta_json,
                  updated_at=excluded.updated_at
                """,
                (
                    platform,
                    connector_id or DEFAULT_CONNECTOR_ID,
                    account_id,
                    entity_type,
                    entity_id,
                    parent_type,
                    parent_id,
                    name,
                    status,
                    json.dumps(meta_json, ensure_ascii=True),
                    now,
                ),
            )

    def list_enabled_connectors(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM connectors WHERE enabled=1 ORDER BY platform, name"
            ).fetchall()
            return [dict(r) for r in rows]

    def list_connectors(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM connectors ORDER BY enabled DESC, platform, name"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_connector(self, connector_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM connectors WHERE id=?",
                (connector_id,),
            ).fetchone()
            return dict(row) if row else None

    def set_connector_enabled(self, connector_id: str, enabled: bool) -> None:
        now = now_utc_iso()
        with self.connect() as conn:
            conn.execute(
                "UPDATE connectors SET enabled=?, updated_at=? WHERE id=?",
                (1 if enabled else 0, now, connector_id),
            )

    def update_connector_config(self, connector_id: str, config: dict[str, Any]) -> None:
        now = now_utc_iso()
        with self.connect() as conn:
            conn.execute(
                "UPDATE connectors SET config_json=?, updated_at=? WHERE id=?",
                (json.dumps(config, ensure_ascii=True), now, connector_id),
            )

    def update_connector_sync_status(self, connector_id: str, *, ok: bool, error: str | None) -> None:
        now = now_utc_iso()
        with self.connect() as conn:
            conn.execute(
                "UPDATE connectors SET last_sync_at=?, last_error=?, updated_at=? WHERE id=?",
                (now if ok else None, error, now, connector_id),
            )

    def list_rules(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM rules WHERE enabled=1 ORDER BY name"
            ).fetchall()
            return [dict(r) for r in rows]

    def list_rules_all(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM rules ORDER BY enabled DESC, name"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_rule(self, rule_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM rules WHERE id=?",
                (rule_id,),
            ).fetchone()
            return dict(row) if row else None

    def set_rule_enabled(self, rule_id: str, enabled: bool) -> None:
        now = now_utc_iso()
        with self.connect() as conn:
            conn.execute(
                "UPDATE rules SET enabled=?, updated_at=? WHERE id=?",
                (1 if enabled else 0, now, rule_id),
            )

    def update_rule_params(self, rule_id: str, params_json: str) -> None:
        now = now_utc_iso()
        with self.connect() as conn:
            conn.execute(
                "UPDATE rules SET params_json=?, updated_at=? WHERE id=?",
                (params_json, now, rule_id),
            )

    def list_pending_proposals(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM action_proposals
                WHERE status='proposed'
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_proposal(self, proposal_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM action_proposals WHERE id=?",
                (proposal_id,),
            ).fetchone()
            return dict(row) if row else None

    def set_proposal_status(self, proposal_id: str, status: str, *, actor: str) -> None:
        now = now_utc_iso()
        with self.connect() as conn:
            if status == "approved":
                conn.execute(
                    """
                    UPDATE action_proposals
                    SET status=?, updated_at=?, approved_by=?, approved_at=?
                    WHERE id=?
                    """,
                    (status, now, actor, now, proposal_id),
                )
                return
            conn.execute(
                "UPDATE action_proposals SET status=?, updated_at=? WHERE id=?",
                (status, now, proposal_id),
            )

    def set_proposal_result(
        self,
        proposal_id: str,
        *,
        status: str,
        executed_at: str | None,
        result_json: dict[str, Any] | None,
        error: str | None,
    ) -> None:
        now = now_utc_iso()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE action_proposals
                SET status=?, updated_at=?, executed_at=?, result_json=?, error=?
                WHERE id=?
                """,
                (
                    status,
                    now,
                    executed_at,
                    json.dumps(result_json, ensure_ascii=True) if result_json is not None else None,
                    error,
                    proposal_id,
                ),
            )

    def attach_telegram_message(self, proposal_id: str, chat_id: int, message_id: int) -> None:
        now = now_utc_iso()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE action_proposals
                SET telegram_chat_id=?, telegram_message_id=?, updated_at=?
                WHERE id=?
                """,
                (chat_id, message_id, now, proposal_id),
            )

    def get_latest_metrics_for_entity(
        self,
        *,
        platform: str,
        entity_type: str,
        entity_id: str,
        connector_id: str | None = None,
    ) -> dict[str, Any] | None:
        where = ["platform=?", "entity_type=?", "entity_id=?"]
        params: list[Any] = [platform, entity_type, entity_id]
        self._append_connector_filter(where, params, connector_id)
        sql = "SELECT * FROM metrics_daily WHERE " + " AND ".join(where) + " ORDER BY date DESC LIMIT 1"
        with self.connect() as conn:
            row = conn.execute(sql, params).fetchone()
            return dict(row) if row else None

    def get_latest_metrics_date(
        self,
        *,
        platform: str,
        entity_type: str | None = None,
        connector_id: str | None = None,
    ) -> str | None:
        where = ["platform=?"]
        params: list[Any] = [platform]
        if entity_type:
            where.append("entity_type=?")
            params.append(entity_type)
        self._append_connector_filter(where, params, connector_id)
        sql = "SELECT MAX(date) AS latest_date FROM metrics_daily WHERE " + " AND ".join(where)
        with self.connect() as conn:
            row = conn.execute(sql, params).fetchone()
            latest = row["latest_date"] if row else None
            return str(latest) if latest else None

    def get_latest_store_order_date(self, *, store: str | None = None) -> str | None:
        where: list[str] = []
        params: list[Any] = []
        if store:
            where.append("store=?")
            params.append(store)
        sql = "SELECT MAX(date_kst) AS latest_date FROM store_orders"
        if where:
            sql += " WHERE " + " AND ".join(where)
        with self.connect() as conn:
            row = conn.execute(sql, params).fetchone()
            latest = row["latest_date"] if row else None
            return str(latest) if latest else None

    def list_metrics_daily_for_date(
        self,
        *,
        platform: str,
        entity_type: str,
        day: str,
        connector_id: str | None = None,
    ) -> list[dict[str, Any]]:
        where = ["platform=?", "entity_type=?", "date=?"]
        params: list[Any] = [platform, entity_type, day]
        self._append_connector_filter(where, params, connector_id)
        sql = "SELECT * FROM metrics_daily WHERE " + " AND ".join(where) + " ORDER BY spend DESC"
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    def list_metrics_range_for_date(
        self,
        *,
        platform: str,
        entity_type: str,
        start_day: str,
        end_day: str,
        connector_id: str | None = None,
    ) -> list[dict[str, Any]]:
        where = ["platform=?", "entity_type=?", "date BETWEEN ? AND ?"]
        params: list[Any] = [platform, entity_type, start_day, end_day]
        self._append_connector_filter(where, params, connector_id)
        sql = (
            """
                SELECT
                  entity_id,
                  COALESCE(SUM(spend), 0) AS spend,
                  COALESCE(SUM(impressions), 0) AS impressions,
                  COALESCE(SUM(clicks), 0) AS clicks,
                  COALESCE(SUM(conversions), 0) AS conversions,
                  COALESCE(SUM(conversion_value), 0) AS conversion_value,
                  GROUP_CONCAT(COALESCE(metrics_json, ''), '\u0001') AS metrics_json_list
                FROM metrics_daily
                WHERE """
            + " AND ".join(where)
            + """
                GROUP BY entity_id
                ORDER BY spend DESC
                """
        )
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()

            out: list[dict[str, Any]] = []
            for row in rows:
                mm = dict(row)
                spend = float(mm["spend"] or 0)
                impressions = float(mm["impressions"] or 0)
                clicks = float(mm["clicks"] or 0)
                conv = float(mm["conversions"] or 0)
                value = float(mm["conversion_value"] or 0)
                conversions_all = 0.0
                conversion_value_all = 0.0
                conversions_purchase = 0.0

                raw = mm.get("metrics_json_list") or ""
                for payload in str(raw).split("\x01"):
                    if not payload:
                        continue
                    try:
                        obj = json.loads(payload)
                    except Exception:
                        continue
                    if not isinstance(obj, dict):
                        continue
                    try:
                        conversions_all += float(obj.get("conversions_all") or 0)
                    except Exception:
                        pass
                    try:
                        conversion_value_all += float(obj.get("conversion_value_all") or 0)
                    except Exception:
                        pass
                    try:
                        conversions_purchase += float(obj.get("conversions_purchase") or 0)
                    except Exception:
                        pass

                out.append(
                    {
                        "entity_id": mm["entity_id"],
                        "spend": spend,
                        "impressions": impressions,
                        "clicks": clicks,
                        "conversions": conv,
                        "conversion_value": value,
                        "conversions_all": conversions_all,
                        "conversion_value_all": conversion_value_all,
                        "conversions_purchase": conversions_purchase,
                    }
                )

            return out

    def sum_metrics_daily(
        self,
        *,
        platform: str,
        day: str,
        entity_type: str | None = None,
        connector_id: str | None = None,
    ) -> dict[str, float]:
        where = ["platform=?", "date=?"]
        params: list[Any] = [platform, day]
        if entity_type:
            where.append("entity_type=?")
            params.append(entity_type)
        self._append_connector_filter(where, params, connector_id)
        sql = (
            """
                    SELECT
                      COALESCE(SUM(spend), 0) AS spend,
                      COALESCE(SUM(impressions), 0) AS impressions,
                      COALESCE(SUM(clicks), 0) AS clicks,
                      COALESCE(SUM(conversions), 0) AS conversions,
                      COALESCE(SUM(conversion_value), 0) AS conversion_value,
                      COUNT(*) AS entity_count
                    FROM metrics_daily
                    WHERE """
            + " AND ".join(where)
        )
        with self.connect() as conn:
            row = conn.execute(sql, params).fetchone()
            return {
                "spend": float(row["spend"] or 0),
                "impressions": float(row["impressions"] or 0),
                "clicks": float(row["clicks"] or 0),
                "conversions": float(row["conversions"] or 0),
                "conversion_value": float(row["conversion_value"] or 0),
                "entity_count": float(row["entity_count"] or 0),
            }

    def sum_metrics_range(
        self,
        *,
        platform: str,
        start_day: str,
        end_day: str,
        entity_type: str | None = None,
        connector_id: str | None = None,
    ) -> dict[str, float]:
        where = ["platform=?", "date BETWEEN ? AND ?"]
        params: list[Any] = [platform, start_day, end_day]
        if entity_type:
            where.append("entity_type=?")
            params.append(entity_type)
        self._append_connector_filter(where, params, connector_id)
        sql = (
            """
                    SELECT
                      COALESCE(SUM(spend), 0) AS spend,
                      COALESCE(SUM(impressions), 0) AS impressions,
                      COALESCE(SUM(clicks), 0) AS clicks,
                      COALESCE(SUM(conversions), 0) AS conversions,
                      COALESCE(SUM(conversion_value), 0) AS conversion_value,
                      COUNT(*) AS entity_count
                    FROM metrics_daily
                    WHERE """
            + " AND ".join(where)
        )
        with self.connect() as conn:
            row = conn.execute(sql, params).fetchone()
            return {
                "spend": float(row["spend"] or 0),
                "impressions": float(row["impressions"] or 0),
                "clicks": float(row["clicks"] or 0),
                "conversions": float(row["conversions"] or 0),
                "conversion_value": float(row["conversion_value"] or 0),
                "entity_count": float(row["entity_count"] or 0),
            }

    def get_metrics_daily_for_entity_date(
        self,
        *,
        platform: str,
        entity_type: str,
        entity_id: str,
        day: str,
        connector_id: str | None = None,
    ) -> dict[str, Any] | None:
        where = ["platform=?", "entity_type=?", "entity_id=?", "date=?"]
        params: list[Any] = [platform, entity_type, entity_id, day]
        self._append_connector_filter(where, params, connector_id)
        sql = "SELECT * FROM metrics_daily WHERE " + " AND ".join(where)
        with self.connect() as conn:
            row = conn.execute(sql, params).fetchone()
            return dict(row) if row else None

    def sum_intraday_for_entity_date(
        self,
        *,
        platform: str,
        entity_type: str,
        entity_id: str,
        day: str,
        connector_id: str | None = None,
    ) -> dict[str, float]:
        like = f"{day}%"
        where = ["platform=?", "entity_type=?", "entity_id=?", "hour_ts LIKE ?"]
        params: list[Any] = [platform, entity_type, entity_id, like]
        self._append_connector_filter(where, params, connector_id)
        sql = (
            """
                SELECT
                  COALESCE(SUM(spend), 0) AS spend,
                  COALESCE(SUM(clicks), 0) AS clicks,
                  COALESCE(SUM(conversions), 0) AS conversions,
                  COALESCE(SUM(conversion_value), 0) AS conversion_value
                FROM metrics_intraday
                WHERE """
            + " AND ".join(where)
        )
        with self.connect() as conn:
            row = conn.execute(sql, params).fetchone()
            return {
                "spend": float(row["spend"] or 0),
                "clicks": float(row["clicks"] or 0),
                "conversions": float(row["conversions"] or 0),
                "conversion_value": float(row["conversion_value"] or 0),
            }

    def sum_cafe24_conversions_for_entity_date(
        self, *, entity_platform: str, entity_type: str, entity_id: str, day_kst: str
    ) -> dict[str, float]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT
                  COUNT(*) AS conv_count,
                  COALESCE(SUM(value), 0) AS conv_value
                FROM conversion_events ce
                JOIN click_events clk ON clk.id = ce.click_id
                JOIN tracking_links tl ON tl.code = clk.code
                WHERE ce.date_kst=?
                  AND tl.entity_platform=?
                  AND tl.entity_type=?
                  AND tl.entity_id=?
                """,
                (day_kst, entity_platform, entity_type, entity_id),
            ).fetchone()
            return {
                "conversions": float(row["conv_count"] or 0),
                "conversion_value": float(row["conv_value"] or 0),
            }

    def sum_cafe24_conversions_for_platform_date(
        self, *, entity_platform: str, day_kst: str
    ) -> dict[str, float]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT
                  COUNT(*) AS conv_count,
                  COALESCE(SUM(value), 0) AS conv_value
                FROM conversion_events ce
                JOIN click_events clk ON clk.id = ce.click_id
                JOIN tracking_links tl ON tl.code = clk.code
                WHERE ce.date_kst=?
                  AND tl.entity_platform=?
                """,
                (day_kst, entity_platform),
            ).fetchone()
            return {
                "conversions": float(row["conv_count"] or 0),
                "conversion_value": float(row["conv_value"] or 0),
            }

    def sum_cafe24_conversions_for_entity_date_range(
        self,
        *,
        entity_platform: str,
        entity_type: str,
        entity_id: str,
        start_day_kst: str,
        end_day_kst: str,
    ) -> dict[str, float]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT
                  COUNT(*) AS conv_count,
                  COALESCE(SUM(value), 0) AS conv_value
                FROM conversion_events ce
                JOIN click_events clk ON clk.id = ce.click_id
                JOIN tracking_links tl ON tl.code = clk.code
                WHERE ce.date_kst BETWEEN ? AND ?
                  AND tl.entity_platform=?
                  AND tl.entity_type=?
                  AND tl.entity_id=?
                """,
                (start_day_kst, end_day_kst, entity_platform, entity_type, entity_id),
            ).fetchone()
            return {
                "conversions": float(row["conv_count"] or 0),
                "conversion_value": float(row["conv_value"] or 0),
            }

    def proposal_exists_recent(
        self,
        *,
        platform: str,
        connector_id: str | None = None,
        entity_type: str,
        entity_id: str,
        action_type: str,
        within_hours: int = 24,
    ) -> bool:
        since = (datetime.now(tz=timezone.utc) - timedelta(hours=within_hours)).replace(microsecond=0).isoformat()
        where = [
            "platform=?",
            "entity_type=?",
            "entity_id=?",
            "action_type=?",
            "status IN ('proposed','approved','executed')",
            "created_at >= ?",
        ]
        params: list[Any] = [platform, entity_type, entity_id, action_type, since]
        if connector_id is not None:
            where.append("connector_id=?")
            params.append(connector_id or DEFAULT_CONNECTOR_ID)
        sql = "SELECT id FROM action_proposals WHERE " + " AND ".join(where) + " LIMIT 1"
        with self.connect() as conn:
            row = conn.execute(sql, params).fetchone()
            return row is not None

    def create_action_proposal(
        self,
        *,
        status: str,
        platform: str,
        connector_id: str | None,
        action_type: str,
        account_id: str | None,
        entity_type: str,
        entity_id: str,
        payload: dict[str, Any],
        reason: str | None,
        risk: str = "low",
        requires_approval: bool = True,
        approved_by: str | None = None,
    ) -> str:
        now = now_utc_iso()
        pid = new_id("act")
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO action_proposals(
                  id, created_at, updated_at, status, platform, connector_id,
                  action_type, account_id, entity_type, entity_id,
                  payload_json, reason, risk, requires_approval, approved_by, approved_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pid,
                    now,
                    now,
                    status,
                    platform,
                    connector_id,
                    action_type,
                    account_id,
                    entity_type,
                    entity_id,
                    json.dumps(payload, ensure_ascii=True),
                    reason,
                    risk,
                    1 if requires_approval else 0,
                    approved_by,
                    now if approved_by else None,
                ),
            )
        return pid

    def create_execution(self, proposal_id: str) -> str:
        eid = new_id("exe")
        now = now_utc_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO executions(id, proposal_id, started_at, status)
                VALUES(?, ?, ?, ?)
                """,
                (eid, proposal_id, now, "running"),
            )
        return eid

    def finish_execution(
        self,
        execution_id: str,
        *,
        status: str,
        after_json: dict[str, Any] | None,
        error: str | None,
    ) -> None:
        now = now_utc_iso()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE executions
                SET finished_at=?, status=?, after_json=?, error=?
                WHERE id=?
                """,
                (
                    now,
                    status,
                    json.dumps(after_json, ensure_ascii=True) if after_json is not None else None,
                    error,
                    execution_id,
                ),
            )

    def get_meta(self, key: str) -> str | None:
        with self.connect() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return str(row["value"]) if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",
                (key, value),
            )

    def list_executions(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM executions
                ORDER BY started_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def list_tracking_links(self, limit: int = 200) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM tracking_links
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_tracking_link(self, code: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM tracking_links WHERE code=?",
                (code,),
            ).fetchone()
            return dict(row) if row else None

    def upsert_tracking_link(
        self,
        *,
        code: str,
        destination_url: str,
        channel: str | None,
        objective: str | None,
        entity_platform: str | None,
        entity_type: str | None,
        entity_id: str | None,
        meta_json: dict[str, Any] | None = None,
    ) -> None:
        now = now_utc_iso()
        meta_json = meta_json or {}
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO tracking_links(
                  code, destination_url, channel, objective,
                  entity_platform, entity_type, entity_id, meta_json,
                  created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                  destination_url=excluded.destination_url,
                  channel=excluded.channel,
                  objective=excluded.objective,
                  entity_platform=excluded.entity_platform,
                  entity_type=excluded.entity_type,
                  entity_id=excluded.entity_id,
                  meta_json=excluded.meta_json,
                  updated_at=excluded.updated_at
                """,
                (
                    code,
                    destination_url,
                    channel,
                    objective,
                    entity_platform,
                    entity_type,
                    entity_id,
                    json.dumps(meta_json, ensure_ascii=True),
                    now,
                    now,
                ),
            )

    def record_click_event(
        self,
        *,
        click_id: str,
        code: str,
        date_kst: str,
        user_agent: str | None,
        ip_hash: str | None,
        referer: str | None,
        query: dict[str, Any],
    ) -> None:
        now = now_utc_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO click_events(
                  id, code, date_kst, created_at, user_agent, ip_hash, referer, query_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    click_id,
                    code,
                    date_kst,
                    now,
                    user_agent,
                    ip_hash,
                    referer,
                    json.dumps(query, ensure_ascii=True),
                ),
            )

    def record_conversion_event(
        self,
        *,
        conversion_id: str,
        click_id: str | None,
        date_kst: str,
        order_id: str | None,
        value: float | None,
        currency: str | None,
        source: str,
        extra: dict[str, Any],
    ) -> None:
        now = now_utc_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO conversion_events(
                  id, click_id, date_kst, created_at, order_id, value, currency, source, extra_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    conversion_id,
                    click_id,
                    date_kst,
                    now,
                    order_id,
                    value,
                    currency,
                    source,
                    json.dumps(extra, ensure_ascii=True),
                ),
            )

    def upsert_store_order(
        self,
        *,
        store: str,
        order_id: str,
        ordered_at: str | None,
        date_kst: str,
        status: str | None,
        amount: float | None,
        currency: str | None,
        order_place_id: str | None,
        order_place_name: str | None,
        inflow_path: str | None,
        inflow_path_detail: str | None,
        referer: str | None,
        source_raw: str | None,
        meta_json: dict[str, Any],
    ) -> None:
        now = now_utc_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO store_orders(
                  store, order_id, ordered_at, date_kst, status, amount, currency,
                  order_place_id, order_place_name,
                  inflow_path, inflow_path_detail,
                  referer, source_raw,
                  meta_json, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(store, order_id) DO UPDATE SET
                  ordered_at=excluded.ordered_at,
                  date_kst=excluded.date_kst,
                  status=excluded.status,
                  amount=excluded.amount,
                  currency=excluded.currency,
                  order_place_id=excluded.order_place_id,
                  order_place_name=excluded.order_place_name,
                  inflow_path=excluded.inflow_path,
                  inflow_path_detail=excluded.inflow_path_detail,
                  referer=excluded.referer,
                  source_raw=excluded.source_raw,
                  meta_json=excluded.meta_json,
                  updated_at=excluded.updated_at
                """,
                (
                    store,
                    order_id,
                    ordered_at,
                    date_kst,
                    status,
                    amount,
                    currency,
                    order_place_id,
                    order_place_name,
                    inflow_path,
                    inflow_path_detail,
                    referer,
                    source_raw,
                    json.dumps(meta_json, ensure_ascii=True),
                    now,
                    now,
                ),
            )

    def list_store_orders(
        self,
        *,
        store: str,
        start_date_kst: str | None = None,
        end_date_kst: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM store_orders WHERE store=?"
        params: list[Any] = [store]
        if start_date_kst:
            sql += " AND date_kst >= ?"
            params.append(start_date_kst)
        if end_date_kst:
            sql += " AND date_kst <= ?"
            params.append(end_date_kst)
        sql += " ORDER BY date_kst DESC, ordered_at DESC LIMIT ?"
        params.append(limit)
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    def count_store_orders_by_inflow_path(
        self,
        *,
        store: str,
        start_date_kst: str,
        end_date_kst: str,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                  COALESCE(NULLIF(TRIM(inflow_path), ''), 'unknown') AS inflow_path,
                  COUNT(*) AS orders
                FROM store_orders
                WHERE store=?
                  AND date_kst >= ?
                  AND date_kst <= ?
                GROUP BY inflow_path
                ORDER BY orders DESC
                LIMIT ?
                """,
                (store, start_date_kst, end_date_kst, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    def sum_store_orders(
        self,
        *,
        store: str,
        start_date_kst: str,
        end_date_kst: str,
        exclude_status_tokens: list[str] | None = None,
    ) -> dict[str, Any]:
        sql = """
            SELECT COUNT(*) AS order_count, COALESCE(SUM(amount), 0) AS total_amount
            FROM store_orders
            WHERE store=? AND date_kst >= ? AND date_kst <= ?
        """
        params: list[Any] = [store, start_date_kst, end_date_kst]
        for token in exclude_status_tokens or []:
            t = (token or "").strip()
            if not t:
                continue
            sql += " AND UPPER(COALESCE(status, '')) NOT LIKE ?"
            params.append(f"%{t.upper()}%")
        with self.connect() as conn:
            row = conn.execute(sql, params).fetchone()
            return {"order_count": row["order_count"], "total_amount": row["total_amount"]}

    def list_entities(
        self,
        *,
        platform: str | None = None,
        connector_id: str | None = None,
        entity_type: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM entities"
        params: list[Any] = []
        where: list[str] = []
        if platform:
            where.append("platform=?")
            params.append(platform)
        self._append_connector_filter(where, params, connector_id)
        if entity_type:
            where.append("entity_type=?")
            params.append(entity_type)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY platform, entity_type, name LIMIT ?"
        params.append(limit)
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    def list_kpi_profiles(self, limit: int = 200) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM kpi_profiles
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def create_kpi_profile(
        self,
        *,
        name: str,
        objective: str,
        platform: str | None,
        definition: dict[str, Any],
    ) -> str:
        now = now_utc_iso()
        pid = new_id("kpi")
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO kpi_profiles(id, name, platform, objective, definition_json, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pid,
                    name,
                    platform,
                    objective,
                    json.dumps(definition, ensure_ascii=True),
                    now,
                    now,
                ),
            )
        return pid

    def attach_kpi_profile_to_entity(
        self,
        *,
        platform: str,
        entity_type: str,
        entity_id: str,
        kpi_profile_id: str,
        enabled: bool = True,
    ) -> None:
        now = now_utc_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO entity_kpi_profiles(
                  platform, entity_type, entity_id, kpi_profile_id, enabled, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(platform, entity_type, entity_id) DO UPDATE SET
                  kpi_profile_id=excluded.kpi_profile_id,
                  enabled=excluded.enabled,
                  updated_at=excluded.updated_at
                """,
                (
                    platform,
                    entity_type,
                    entity_id,
                    kpi_profile_id,
                    1 if enabled else 0,
                    now,
                    now,
                ),
            )

    def list_entity_kpi_profiles(self, limit: int = 500) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT ekp.platform, ekp.entity_type, ekp.entity_id, ekp.kpi_profile_id, ekp.enabled,
                       kp.name AS kpi_name, kp.objective AS kpi_objective
                FROM entity_kpi_profiles ekp
                JOIN kpi_profiles kp ON kp.id = ekp.kpi_profile_id
                ORDER BY ekp.updated_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]
