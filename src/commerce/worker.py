from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from commerce.config import Settings
from commerce.db import AdsDB
from commerce.executor import ExecutionError, execute_proposal
from commerce.notify.telegram_bot import notify_auto_pause, notify_new_proposal
from commerce.registry import build_connector
from commerce.repo import Repo


def _parse_kill_switch_params(params_json: str | None) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "spend_threshold": 50000.0,
        "conversion_threshold": 0.0,
        "clicks_threshold": 1.0,
        "entity_type": "campaign",
        "auto_execute": False,
    }
    if not params_json:
        return defaults

    try:
        loaded = json.loads(params_json)
    except Exception:
        return defaults
    if not isinstance(loaded, dict):
        return defaults

    def _to_float(value: Any, default: float) -> float:
        try:
            return float(value)
        except Exception:
            return default

    spend_thr = max(0.0, _to_float(loaded.get("spend_threshold", defaults["spend_threshold"]), 50000.0))
    conv_thr = max(0.0, _to_float(loaded.get("conversion_threshold", defaults["conversion_threshold"]), 0.0))
    min_clicks = max(
        0.0,
        _to_float(loaded.get("clicks_threshold", loaded.get("min_clicks", defaults["clicks_threshold"])), 1.0),
    )
    entity_type = str(loaded.get("entity_type", defaults["entity_type"])).strip().lower() or "campaign"

    auto_raw = loaded.get("auto_execute", defaults["auto_execute"])
    if isinstance(auto_raw, bool):
        auto_execute = auto_raw
    elif isinstance(auto_raw, (int, float)):
        auto_execute = bool(auto_raw)
    elif isinstance(auto_raw, str):
        auto_execute = auto_raw.strip().lower() in {"1", "true", "yes", "y", "on"}
    else:
        auto_execute = False

    return {
        "spend_threshold": spend_thr,
        "conversion_threshold": conv_thr,
        "clicks_threshold": min_clicks,
        "entity_type": entity_type,
        "auto_execute": auto_execute,
    }


async def _tick(settings: Settings) -> None:
    AdsDB(settings.db_path).init()
    repo = Repo(settings.db_path)
    enabled = repo.list_enabled_connectors()
    rules = repo.list_rules()

    now_kst = datetime.now(tz=ZoneInfo(settings.timezone))
    today_kst = now_kst.date().isoformat()
    yesterday_kst = (now_kst.date() - timedelta(days=1)).isoformat()

    for c in enabled:
        try:
            connector = build_connector(
                c["platform"],
                connector_id=c["id"],
                name=c["name"],
                config_json=c["config_json"],
                repo=repo,
                demo_mode=settings.demo_mode,
            )
        except Exception as e:  # noqa: BLE001
            repo.update_connector_sync_status(c["id"], ok=False, error=f"{type(e).__name__}: {e}")
            continue

        try:
            ok, _err = await connector.health_check()
        except Exception as e:  # noqa: BLE001
            repo.update_connector_sync_status(c["id"], ok=False, error=f"{type(e).__name__}: {e}")
            continue
        if not ok and not settings.demo_mode:
            repo.update_connector_sync_status(c["id"], ok=False, error=_err)
            continue

        try:
            await connector.sync_entities()
        except NotImplementedError:
            pass
        except Exception as e:  # noqa: BLE001
            repo.update_connector_sync_status(c["id"], ok=False, error=f"{type(e).__name__}: {e}")
            continue

        try:
            await connector.fetch_metrics_daily(yesterday_kst, today_kst)
        except NotImplementedError:
            # Connector not implemented yet; safe to ignore.
            pass
        except Exception as e:  # noqa: BLE001
            repo.update_connector_sync_status(c["id"], ok=False, error=f"{type(e).__name__}: {e}")
            continue

        # Optional: intraday ingestion (fixture/api later). Safe to ignore if missing.
        try:
            maybe = getattr(connector, "fetch_metrics_intraday", None)
            if callable(maybe):
                await maybe(today_kst)
        except NotImplementedError:
            pass
        except Exception as e:  # noqa: BLE001
            repo.update_connector_sync_status(c["id"], ok=False, error=f"{type(e).__name__}: {e}")
            continue

        repo.update_connector_sync_status(c["id"], ok=True, error=None)

        for r in rules:
            if r["rule_type"] != "kill_switch_spend_no_conv":
                continue
            try:
                platform = "demo" if settings.demo_mode else c["platform"]
                params = _parse_kill_switch_params(r.get("params_json"))
                spend_thr = float(params["spend_threshold"])
                conv_thr = float(params["conversion_threshold"])
                min_clicks = float(params["clicks_threshold"])
                entity_type = str(params["entity_type"])
                auto_execute = bool(params["auto_execute"])

                rows = repo.list_metrics_daily_for_date(
                    platform=platform,
                    connector_id=c["id"],
                    entity_type=entity_type,
                    day=today_kst,
                )
                for m in rows:
                    entity_id = str(m["entity_id"])

                    intr = repo.sum_intraday_for_entity_date(
                        platform=platform,
                        connector_id=c["id"],
                        entity_type=entity_type,
                        entity_id=entity_id,
                        day=today_kst,
                    )
                    spend = intr["spend"] if intr["spend"] > 0 else float(m.get("spend") or 0)
                    clicks = intr["clicks"] if intr["clicks"] > 0 else float(m.get("clicks") or 0)

                    cafe24 = repo.sum_cafe24_conversions_for_entity_date(
                        entity_platform=platform,
                        entity_type=entity_type,
                        entity_id=entity_id,
                        day_kst=today_kst,
                    )
                    conv_cafe24 = cafe24["conversions"]
                    conv_platform = float(m.get("conversions") or 0)
                    conv = conv_cafe24 if conv_cafe24 > 0 else conv_platform

                    if spend < spend_thr:
                        continue
                    if clicks < min_clicks:
                        continue
                    if conv > conv_thr:
                        continue

                    if repo.proposal_exists_recent(
                        platform=platform,
                        connector_id=c["id"],
                        entity_type=entity_type,
                        entity_id=entity_id,
                        action_type="pause_entity",
                        within_hours=24,
                    ):
                        continue

                    reason = (
                        f"AUTO-PAUSE: spend={spend:.0f}>=thr({spend_thr:.0f}) "
                        f"clicks={clicks:.0f}>=min({min_clicks:.0f}) "
                        f"conv={conv:.0f}<=thr({conv_thr:.0f}) "
                        f"date_kst={today_kst}"
                    )
                    payload = {"op": "pause", "reason": "kill_switch_spend_no_conv"}
                    should_execute = (
                        auto_execute
                        and settings.execution_mode == "auto_low_risk"
                    )
                    pid = repo.create_action_proposal(
                        status="approved" if should_execute else "proposed",
                        platform=platform,
                        connector_id=c["id"],
                        action_type="pause_entity",
                        account_id=m.get("account_id"),
                        entity_type=entity_type,
                        entity_id=entity_id,
                        payload=payload,
                        reason=reason,
                        risk="low",
                        requires_approval=not should_execute,
                        approved_by="auto" if should_execute else None,
                    )

                    proposal = repo.get_proposal(pid)
                    if proposal and not should_execute:
                        sent = await notify_new_proposal(settings, proposal)
                        if sent:
                            chat_id, msg_id = sent
                            repo.attach_telegram_message(pid, chat_id, msg_id)
                        continue

                    # Auto execute only if explicitly enabled.
                    if should_execute:
                        try:
                            await execute_proposal(settings, repo=repo, proposal_id=pid, actor="auto")
                            proposal2 = repo.get_proposal(pid)
                            if proposal2:
                                sent = await notify_auto_pause(settings, proposal2)
                                if sent:
                                    chat_id, msg_id = sent
                                    repo.attach_telegram_message(pid, chat_id, msg_id)
                        except ExecutionError:
                            proposal2 = repo.get_proposal(pid)
                            if proposal2:
                                await notify_new_proposal(settings, proposal2)
            except Exception:
                # Rule failures must not stop other rules/connectors.
                continue


def run_tick(settings: Settings) -> None:
    asyncio.run(_tick(settings))


async def _run_forever(settings: Settings) -> None:
    # Basic scheduler: tick every 5 minutes.
    while True:
        try:
            await _tick(settings)
        except Exception as e:  # noqa: BLE001
            print(f"[worker] tick failed: {type(e).__name__}: {e}")
        await asyncio.sleep(300)


def run_worker(settings: Settings) -> None:
    asyncio.run(_run_forever(settings))
