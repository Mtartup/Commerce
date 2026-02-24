from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.parse import urlencode, urlparse, parse_qsl, parse_qs, urlunparse
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn

from commerce.config import Settings
from commerce.db import AdsDB
from commerce.executor import ExecutionError, execute_proposal
from commerce.repo import Repo
from commerce.util import new_id, now_kst_date_str, sha256_hex


STORE_REVENUE_EXCLUDED_STATUS_TOKENS: dict[str, tuple[str, ...]] = {
    "cafe24": ("취소", "반품", "환불"),
    "smartstore": ("CANCEL", "RETURN", "REFUND"),
    "coupang": ("CANCEL", "RETURN", "REFUND"),
}


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return False


def _normalize_rule_params(rule_type: str, raw_json: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw_json or "{}")
    except Exception as e:  # noqa: BLE001
        raise ValueError(f"invalid json: {e}") from e
    if not isinstance(payload, dict):
        raise ValueError("params_json must be a JSON object")

    if rule_type != "kill_switch_spend_no_conv":
        return payload

    def _to_non_negative_float(value: Any, *, default: float) -> float:
        try:
            n = float(value)
        except Exception:
            return default
        return n if n >= 0 else default

    entity_type = str(payload.get("entity_type", "campaign")).strip().lower() or "campaign"
    return {
        "entity_type": entity_type,
        "spend_threshold": _to_non_negative_float(payload.get("spend_threshold", 50000), default=50000.0),
        "clicks_threshold": _to_non_negative_float(
            payload.get("clicks_threshold", payload.get("min_clicks", 1)),
            default=1.0,
        ),
        "conversion_threshold": _to_non_negative_float(payload.get("conversion_threshold", 0), default=0.0),
        "auto_execute": _to_bool(payload.get("auto_execute", False)),
    }


def _safe_next_path(next_url: str) -> str:
    candidate = (next_url or "").strip()
    if not candidate:
        return "/"
    parsed = urlparse(candidate)
    if parsed.scheme or parsed.netloc:
        return "/"
    if not parsed.path.startswith("/") or parsed.path.startswith("//"):
        return "/"
    if parsed.query:
        return f"{parsed.path}?{parsed.query}"
    return parsed.path


def _ui_now(timezone_name: str) -> str:
    return datetime.now(tz=ZoneInfo(timezone_name)).strftime("%Y-%m-%d %H:%M:%S %Z")


def _date_window(end_day: str, days: int) -> list[str]:
    try:
        end_date = datetime.fromisoformat(end_day).date()
    except Exception:
        return [end_day]
    days_i = max(1, int(days))
    start_date = end_date - timedelta(days=days_i - 1)
    out: list[str] = []
    cur = start_date
    while cur <= end_date:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _normalize_series(values: list[float | int], *, min_non_zero: float = 8.0) -> list[float]:
    if not values:
        return []
    max_v = max(float(v or 0) for v in values)
    if max_v <= 0:
        return [0.0 for _ in values]
    out: list[float] = []
    for v in values:
        raw = float(v or 0)
        if raw <= 0:
            out.append(0.0)
            continue
        pct = (raw / max_v) * 100.0
        out.append(max(min_non_zero, round(pct, 2)))
    return out


def _compute_dashboard_basis_day(
    repo: Repo,
    *,
    timezone_name: str,
    connectors: list[dict[str, Any]],
) -> dict[str, Any]:
    today_kst = now_kst_date_str(timezone_name)
    ad_platforms = ("naver", "meta", "google")
    ad_connectors = {c["platform"]: c for c in connectors if c.get("platform") in ad_platforms}

    # Use latest available ad-data day as dashboard basis to avoid false-zero "today" cards.
    ad_day_candidates: list[str] = []
    enabled_ad_platforms = [p for p in ad_platforms if bool(ad_connectors.get(p, {}).get("enabled"))]
    for p in (enabled_ad_platforms or list(ad_platforms)):
        c = ad_connectors.get(p, {})
        cid = str(c.get("id") or "") or None
        latest = (
            repo.get_latest_metrics_date(platform=p, entity_type="campaign", connector_id=cid)
            or repo.get_latest_metrics_date(platform=p, entity_type="campaign")
            or repo.get_latest_metrics_date(platform=p, connector_id=cid)
            or repo.get_latest_metrics_date(platform=p)
        )
        if latest:
            ad_day_candidates.append(latest)

    day = max(ad_day_candidates) if ad_day_candidates else today_kst
    if not ad_day_candidates:
        latest_store = repo.get_latest_store_order_date()
        if latest_store:
            day = latest_store
    return {
        "day": day,
        "today_kst": today_kst,
        "is_today": day == today_kst,
    }


def create_app(settings: Settings) -> FastAPI:
    AdsDB(settings.db_path).init()
    repo = Repo(settings.db_path)
    ui_platforms = {"naver", "meta", "google", "coupang", "smartstore", "cafe24_analytics"}
    platform_names = {
        "naver": "네이버",
        "meta": "메타",
        "google": "구글",
        "coupang": "쿠팡",
        "smartstore": "스마트스토어",
        "cafe24_analytics": "카페24 분석",
    }
    platform_level_labels = {
        "naver": {"campaign": "캠페인", "adgroup": "광고그룹", "keyword": "키워드"},
        "meta": {"campaign": "캠페인", "adset": "광고세트", "ad": "광고"},
        "google": {"campaign": "캠페인", "adgroup": "광고그룹", "keyword": "키워드"},
        "coupang": {},
        "smartstore": {},
        "cafe24_analytics": {"store": "스토어", "product": "상품", "domain": "유입도메인"},
    }
    platform_level_order = {
        "naver": ["campaign", "adgroup", "keyword"],
        "meta": ["campaign", "adset", "ad"],
        "google": ["campaign", "adgroup", "keyword"],
        "coupang": [],
        "smartstore": [],
        "cafe24_analytics": ["store", "product", "domain"],
    }

    def _template_common_context(_request: Request) -> dict[str, Any]:
        basis = _compute_dashboard_basis_day(
            repo,
            timezone_name=settings.timezone,
            connectors=repo.list_connectors(),
        )
        return {
            "dashboard_basis_day": basis["day"],
            "dashboard_basis_today_kst": basis["today_kst"],
            "dashboard_basis_is_today": basis["is_today"],
        }

    base_dir = Path(__file__).resolve().parent
    templates = Jinja2Templates(
        directory=str(base_dir / "templates"),
        context_processors=[_template_common_context],
    )

    app = FastAPI(title="Commerce")
    app.state.sync_lock = Lock()
    app.state.sync_status = {
        "running": False,
        "started_at": None,
        "finished_at": None,
        "last_error": None,
    }
    app.mount("/static", StaticFiles(directory=str(base_dir / "static")), name="static")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request):
        connectors_list = repo.list_connectors()
        all_connectors = {c["platform"]: c for c in connectors_list if c.get("platform") in ui_platforms}
        pending = repo.list_pending_proposals(limit=20)
        ad_platforms = ("naver", "meta", "google")
        basis = _compute_dashboard_basis_day(
            repo,
            timezone_name=settings.timezone,
            connectors=connectors_list,
        )
        day = basis["day"]
        today_kst = basis["today_kst"]
        is_today = basis["is_today"]
        trend_days = _date_window(day, 7)

        def _sum_platform_metrics_day(
            *,
            platform: str,
            connector_id: str | None,
            metric_day: str,
            entity_type: str | None = None,
        ) -> dict[str, float]:
            if entity_type:
                scoped = repo.sum_metrics_daily(
                    platform=platform,
                    connector_id=connector_id,
                    day=metric_day,
                    entity_type=entity_type,
                )
                if scoped["entity_count"] == 0 and connector_id:
                    return repo.sum_metrics_daily(platform=platform, day=metric_day, entity_type=entity_type)
                return scoped

            scoped = repo.sum_metrics_daily(
                platform=platform,
                connector_id=connector_id,
                day=metric_day,
            )
            if scoped["entity_count"] == 0 and connector_id:
                return repo.sum_metrics_daily(platform=platform, day=metric_day)
            return scoped

        # --- Platform cards (ad channels) ---
        platform_cards = []
        total_spend = 0.0
        total_ad_clicks = 0
        for platform in ad_platforms:
            connector = all_connectors.get(platform, {})
            enabled = bool(connector.get("enabled"))
            connector_id = str(connector.get("id") or "") or None
            metrics_campaign = _sum_platform_metrics_day(
                platform=platform,
                connector_id=connector_id,
                metric_day=day,
                entity_type="campaign",
            )
            metrics = metrics_campaign
            if metrics["entity_count"] == 0:
                metrics = _sum_platform_metrics_day(
                    platform=platform,
                    connector_id=connector_id,
                    metric_day=day,
                )
            spend = metrics["spend"] or 0
            clicks = metrics["clicks"] or 0
            conv = metrics["conversions"] or 0
            value = metrics["conversion_value"] or 0
            cvr = (conv / clicks) if clicks else 0
            cpa = (spend / conv) if conv else None
            roas = (value / spend) if spend else None
            spend_series: list[float] = []
            for d in trend_days:
                daily_metrics = _sum_platform_metrics_day(
                    platform=platform,
                    connector_id=connector_id,
                    metric_day=d,
                    entity_type="campaign",
                )
                spend_series.append(float(daily_metrics.get("spend") or 0))
            if enabled:
                total_spend += spend
                total_ad_clicks += int(clicks)
            if not enabled:
                health = "OFF"
                tone = "muted"
            elif spend == 0 and conv == 0:
                health = "데이터 대기"
                tone = "muted"
            elif roas is not None and roas >= 1.5:
                health = "양호"
                tone = "good"
            elif roas is not None and roas >= 1.0:
                health = "주의"
                tone = "warn"
            else:
                health = "문제"
                tone = "danger"
            platform_cards.append(
                {
                    "platform": platform,
                    "label": platform_names.get(platform, platform),
                    "enabled": enabled,
                    "spend": spend,
                    "clicks": clicks,
                    "conversions": conv,
                    "value": value,
                    "cvr": cvr,
                    "cpa": cpa,
                    "roas": roas,
                    "health": health,
                    "tone": tone,
                    "day": day,
                    "entity_count": metrics["entity_count"],
                    "trend_days": trend_days,
                    "spend_series": spend_series,
                    "spend_series_norm": _normalize_series(spend_series),
                }
            )
        max_platform_spend = max((float(c.get("spend") or 0) for c in platform_cards), default=0.0)
        for c in platform_cards:
            spend = float(c.get("spend") or 0)
            c["spend_ratio"] = (spend / max_platform_spend * 100.0) if max_platform_spend > 0 else 0.0

        # --- Store cards (sales channels) — today only ---
        store_cards = []
        total_orders = 0
        total_revenue = 0.0
        for store_name, store_label in [("cafe24", "카페24"), ("coupang", "쿠팡"), ("smartstore", "스마트스토어")]:
            stats = repo.sum_store_orders(
                store=store_name,
                start_date_kst=day,
                end_date_kst=day,
                exclude_status_tokens=list(STORE_REVENUE_EXCLUDED_STATUS_TOKENS.get(store_name, ())),
            )
            total_orders += stats["order_count"]
            total_revenue += float(stats["total_amount"] or 0)
            orders_series: list[float] = []
            for d in trend_days:
                day_stats = repo.sum_store_orders(
                    store=store_name,
                    start_date_kst=d,
                    end_date_kst=d,
                    exclude_status_tokens=list(STORE_REVENUE_EXCLUDED_STATUS_TOKENS.get(store_name, ())),
                )
                orders_series.append(float(day_stats.get("order_count") or 0))
            store_cards.append(
                {
                    "store": store_name,
                    "label": store_label,
                    "orders_series": orders_series,
                    "orders_series_norm": _normalize_series(orders_series),
                    **stats,
                }
            )

        # --- Funnel: cafe24_analytics store-level metrics (visitors/PV) ---
        funnel_metrics = repo.sum_metrics_daily(platform="cafe24_analytics", day=day, entity_type="store")
        funnel_visitors = int(funnel_metrics["impressions"])  # impressions = visitors
        funnel_pv = int(funnel_metrics["clicks"])  # clicks = page views
        pv_per_visit = (funnel_pv / funnel_visitors) if funnel_visitors else 0

        # --- Per-channel cards (자사몰 funnel + 판매채널별 주문) ---
        store_stats_map = {s["store"]: s for s in store_cards}
        cafe24_orders = store_stats_map.get("cafe24", {}).get("order_count", 0)
        cafe24_revenue = float(store_stats_map.get("cafe24", {}).get("total_amount") or 0)
        cafe24_cvr = (cafe24_orders / funnel_visitors * 100) if funnel_visitors else 0
        channel_cards = [
            {
                "store": "cafe24",
                "label": "자사몰(카페24)",
                "visitors": funnel_visitors,
                "pv": funnel_pv,
                "pv_per_visit": pv_per_visit,
                "order_count": cafe24_orders,
                "cvr": cafe24_cvr,
                "total_amount": cafe24_revenue,
                "orders_series": store_stats_map.get("cafe24", {}).get("orders_series", []),
                "orders_series_norm": store_stats_map.get("cafe24", {}).get("orders_series_norm", []),
                "has_funnel": True,
            },
            {
                "store": "smartstore",
                "label": "스마트스토어",
                "order_count": store_stats_map.get("smartstore", {}).get("order_count", 0),
                "total_amount": float(store_stats_map.get("smartstore", {}).get("total_amount") or 0),
                "orders_series": store_stats_map.get("smartstore", {}).get("orders_series", []),
                "orders_series_norm": store_stats_map.get("smartstore", {}).get("orders_series_norm", []),
                "has_funnel": False,
            },
            {
                "store": "coupang",
                "label": "쿠팡",
                "order_count": store_stats_map.get("coupang", {}).get("order_count", 0),
                "total_amount": float(store_stats_map.get("coupang", {}).get("total_amount") or 0),
                "orders_series": store_stats_map.get("coupang", {}).get("orders_series", []),
                "orders_series_norm": store_stats_map.get("coupang", {}).get("orders_series_norm", []),
                "has_funnel": False,
            },
        ]
        max_channel_revenue = max((float(c.get("total_amount") or 0) for c in channel_cards), default=0.0)
        for c in channel_cards:
            revenue = float(c.get("total_amount") or 0)
            c["revenue_ratio"] = (revenue / max_channel_revenue * 100.0) if max_channel_revenue > 0 else 0.0
        # funnel_cvr: cafe24 전용 (기존 total_orders 대신 cafe24_cvr 사용)
        funnel_cvr = cafe24_cvr

        # --- Connector health summary ---
        connector_health = []
        for c in connectors_list:
            p = c.get("platform", "")
            if p not in ui_platforms:
                continue
            enabled = bool(c.get("enabled"))
            label = platform_names.get(p, p)
            if not enabled:
                status = "off"
            elif c.get("last_error"):
                status = "err"
            else:
                status = "ok" if c.get("last_sync_at") else "warn"
                if p in ad_platforms:
                    cid = str(c.get("id") or "") or None
                    latest = (
                        repo.get_latest_metrics_date(platform=p, entity_type="campaign", connector_id=cid)
                        or repo.get_latest_metrics_date(platform=p, entity_type="campaign")
                        or repo.get_latest_metrics_date(platform=p, connector_id=cid)
                        or repo.get_latest_metrics_date(platform=p)
                    )
                    if (not latest) or latest < today_kst:
                        status = "warn"
                elif p == "cafe24_analytics":
                    cid = str(c.get("id") or "") or None
                    latest = (
                        repo.get_latest_metrics_date(platform=p, entity_type="store", connector_id=cid)
                        or repo.get_latest_metrics_date(platform=p, entity_type="store")
                        or repo.get_latest_metrics_date(platform=p, connector_id=cid)
                        or repo.get_latest_metrics_date(platform=p)
                    )
                    if (not latest) or latest < today_kst:
                        status = "warn"
                elif p in {"coupang", "smartstore"}:
                    latest = repo.get_latest_store_order_date(store=p)
                    if (not latest) or latest < today_kst:
                        status = "warn"
            connector_health.append({"platform": p, "label": label, "status": status})

        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "platform_cards": platform_cards,
                "store_cards": store_cards,
                "channel_cards": channel_cards,
                "pending": pending,
                "day": day,
                "today_kst": today_kst,
                "is_today": is_today,
                "dashboard_trend_days": trend_days,
                "total_orders": total_orders,
                "total_revenue": total_revenue,
                "total_spend": total_spend,
                "total_ad_clicks": total_ad_clicks,
                "funnel_visitors": funnel_visitors,
                "funnel_pv": funnel_pv,
                "pv_per_visit": pv_per_visit,
                "funnel_cvr": funnel_cvr,
                "connector_health": connector_health,
            },
        )

    @app.get("/connectors", response_class=HTMLResponse)
    def connectors_page(request: Request, days: int = 1):
        connectors = repo.list_connectors()
        connectors = [c for c in connectors if c.get("platform") in ui_platforms]
        try:
            days_i = int(days)
        except Exception:
            days_i = 1
        days_i = max(1, min(days_i, 30))
        end_day = now_kst_date_str(settings.timezone)
        start_dt = datetime.now(tz=ZoneInfo(settings.timezone)).date() - timedelta(days=days_i - 1)
        start_day = start_dt.isoformat()
        for c in connectors:
            try:
                c["config"] = json.loads(c.get("config_json") or "{}")
            except Exception:
                c["config"] = {}
            platform = (c.get("platform") or "").strip().lower()
            level_order = platform_level_order.get(platform, ["campaign"])
            level_summaries = []
            for level in level_order:
                metrics = repo.sum_metrics_range(
                    platform=platform,
                    start_day=start_day,
                    end_day=end_day,
                    entity_type=level,
                )
                spend = float(metrics.get("spend") or 0)
                clicks = float(metrics.get("clicks") or 0)
                conv = float(metrics.get("conversions") or 0)
                value = float(metrics.get("conversion_value") or 0)
                level_summaries.append(
                    {
                        "entity_type": level,
                        "entity_type_label": platform_level_labels.get(platform, {}).get(level, level),
                        "spend": spend,
                        "clicks": clicks,
                        "conversions": conv,
                        "value": value,
                        "entity_count": int(metrics.get("entity_count") or 0),
                        "cvr": (conv / clicks) if clicks else 0,
                        "cpa": (spend / conv) if conv else None,
                        "roas": (value / spend) if spend else None,
                    }
                )
            total = repo.sum_metrics_range(platform=platform, start_day=start_day, end_day=end_day)
            c["today_summary"] = {
                "spend": float(total.get("spend") or 0),
                "clicks": float(total.get("clicks") or 0),
                "conversions": float(total.get("conversions") or 0),
                "value": float(total.get("conversion_value") or 0),
                "entity_count": int(total.get("entity_count") or 0),
            }
            c["level_summaries"] = level_summaries
            c["day"] = end_day
            c["range_start"] = start_day
        return templates.TemplateResponse(
            "connectors.html",
            {
                "request": request,
                "connectors": connectors,
                "platforms": platform_names,
                "days": days_i,
                "start_day": start_day,
                "end_day": end_day,
            },
        )

    @app.post("/sync")
    async def run_sync(request: Request):
        next_raw = str(request.query_params.get("next") or "").strip()
        if not next_raw:
            ctype = (request.headers.get("content-type") or "").lower()
            if "application/x-www-form-urlencoded" in ctype:
                try:
                    body_text = (await request.body()).decode("utf-8", errors="ignore")
                    parsed = parse_qs(body_text, keep_blank_values=True)
                    next_raw = str((parsed.get("next") or [""])[0] or "").strip()
                except Exception:
                    next_raw = ""
        if not next_raw:
            ref = str(request.headers.get("referer") or "").strip()
            if ref:
                parsed_ref = urlparse(ref)
                next_raw = parsed_ref.path + (f"?{parsed_ref.query}" if parsed_ref.query else "")
        next_url = _safe_next_path(next_raw)
        sync_lock: Lock = request.app.state.sync_lock
        sync_status: dict[str, Any] = request.app.state.sync_status
        if not sync_lock.acquire(blocking=False):
            sync_status["last_error"] = "동기화가 이미 실행 중입니다."
            return RedirectResponse(url=next_url, status_code=303)

        sync_status["running"] = True
        sync_status["started_at"] = _ui_now(settings.timezone)
        sync_status["last_error"] = None
        try:
            from commerce.worker import run_tick

            await run_in_threadpool(run_tick, settings)
        except Exception as e:  # noqa: BLE001
            sync_status["last_error"] = f"{type(e).__name__}: {e}"
        finally:
            sync_status["running"] = False
            sync_status["finished_at"] = _ui_now(settings.timezone)
            sync_lock.release()
        return RedirectResponse(url=next_url, status_code=303)

    @app.post("/connectors/{connector_id}/enable")
    def enable_connector(connector_id: str):
        repo.set_connector_enabled(connector_id, True)
        return RedirectResponse(url="/connectors", status_code=303)

    @app.post("/connectors/{connector_id}/disable")
    def disable_connector(connector_id: str):
        repo.set_connector_enabled(connector_id, False)
        return RedirectResponse(url="/connectors", status_code=303)

    @app.post("/connectors/{connector_id}/config")
    async def update_connector_config(request: Request, connector_id: str):
        form = await request.form()
        mode = (form.get("mode") or "").strip().lower() or "import"
        fixture_dir = (form.get("fixture_dir") or "").strip() or None
        product_types_raw = (form.get("product_types") or "").strip()
        report_tp_raw = (form.get("report_tp") or "").strip()
        customer_id_raw = (form.get("customer_id") or "").strip()
        ad_account_id_raw = (form.get("ad_account_id") or "").strip()
        ingest_levels_raw = (form.get("ingest_levels") or "").strip()
        conversion_action_types_raw = (form.get("conversion_action_types") or "").strip()
        include_today_raw = form.get("include_today")
        api_min_interval_raw = (form.get("api_min_interval_minutes") or "").strip()
        poll_interval_raw = (form.get("report_poll_interval_sec") or "").strip()
        report_timeout_raw = (form.get("report_timeout_sec") or "").strip()

        row = repo.get_connector(connector_id)
        if not row:
            return RedirectResponse(url="/connectors", status_code=303)
        try:
            cfg = json.loads(row.get("config_json") or "{}")
        except Exception:
            cfg = {}
        cfg["mode"] = mode
        if fixture_dir:
            cfg["fixture_dir"] = fixture_dir
        else:
            cfg.pop("fixture_dir", None)
        if row.get("platform") == "naver":
            # Optional: used for CSV imports and future segmentation.
            if product_types_raw:
                pts = [p.strip().lower() for p in product_types_raw.split(",") if p.strip()]
                cfg["product_types"] = pts
            else:
                cfg.pop("product_types", None)

            # API-mode options (safe to persist even when mode != api).
            if report_tp_raw:
                cfg["report_tp"] = report_tp_raw.upper()
            else:
                cfg.pop("report_tp", None)

            if ingest_levels_raw:
                ok: list[str] = []
                for lv in ingest_levels_raw.split(","):
                    s = lv.strip().lower()
                    if s in {"campaign", "adgroup", "keyword", "ad"} and s not in ok:
                        ok.append(s)
                if ok:
                    cfg["ingest_levels"] = ok
                else:
                    cfg.pop("ingest_levels", None)
            else:
                cfg.pop("ingest_levels", None)

            cfg["include_today"] = bool(include_today_raw)

            def _set_float(key: str, raw: str) -> None:
                if not raw:
                    cfg.pop(key, None)
                    return
                try:
                    v = float(raw)
                    if v <= 0:
                        raise ValueError("must be > 0")
                    cfg[key] = v
                except Exception:
                    cfg.pop(key, None)

            _set_float("api_min_interval_minutes", api_min_interval_raw)
            _set_float("report_poll_interval_sec", poll_interval_raw)
            _set_float("report_timeout_sec", report_timeout_raw)
        elif row.get("platform") == "meta":
            # Optional: override env META_AD_ACCOUNT_ID if you want per-connector config.
            if ad_account_id_raw:
                cfg["ad_account_id"] = ad_account_id_raw
            else:
                cfg.pop("ad_account_id", None)

            if ingest_levels_raw:
                ok: list[str] = []
                for lv in ingest_levels_raw.split(","):
                    s = lv.strip().lower()
                    if s in {"campaign", "adset", "ad"} and s not in ok:
                        ok.append(s)
                if ok:
                    cfg["ingest_levels"] = ok
                else:
                    cfg.pop("ingest_levels", None)
            else:
                cfg.pop("ingest_levels", None)

            if conversion_action_types_raw:
                acts = [s.strip() for s in conversion_action_types_raw.split(",") if s.strip()]
                cfg["conversion_action_types"] = acts
            else:
                cfg.pop("conversion_action_types", None)

            cfg["include_today"] = bool(include_today_raw)

            def _set_float(key: str, raw: str) -> None:
                if not raw:
                    cfg.pop(key, None)
                    return
                try:
                    v = float(raw)
                    if v <= 0:
                        raise ValueError("must be > 0")
                    cfg[key] = v
                except Exception:
                    cfg.pop(key, None)

            _set_float("api_min_interval_minutes", api_min_interval_raw)
        elif row.get("platform") == "google":
            # Optional: override env GOOGLE_ADS_CUSTOMER_ID if you want per-connector config.
            if customer_id_raw:
                cfg["customer_id"] = customer_id_raw
            else:
                cfg.pop("customer_id", None)

            if ingest_levels_raw:
                ok: list[str] = []
                for lv in ingest_levels_raw.split(","):
                    s = lv.strip().lower()
                    if s in {"campaign", "adgroup", "keyword"} and s not in ok:
                        ok.append(s)
                if ok:
                    cfg["ingest_levels"] = ok
                else:
                    cfg.pop("ingest_levels", None)
            else:
                cfg.pop("ingest_levels", None)

            cfg["include_today"] = bool(include_today_raw)

            def _set_float(key: str, raw: str) -> None:
                if not raw:
                    cfg.pop(key, None)
                    return
                try:
                    v = float(raw)
                    if v <= 0:
                        raise ValueError("must be > 0")
                    cfg[key] = v
                except Exception:
                    cfg.pop(key, None)

            _set_float("api_min_interval_minutes", api_min_interval_raw)
        elif row.get("platform") == "coupang":
            vendor_id_raw = (form.get("vendor_id") or "").strip()
            access_key_raw = (form.get("access_key") or "").strip()
            secret_key_raw = (form.get("secret_key") or "").strip()
            if vendor_id_raw:
                cfg["vendor_id"] = vendor_id_raw
            else:
                cfg.pop("vendor_id", None)
            if access_key_raw:
                cfg["access_key"] = access_key_raw
            else:
                cfg.pop("access_key", None)
            if secret_key_raw:
                cfg["secret_key"] = secret_key_raw
            else:
                cfg.pop("secret_key", None)

        repo.update_connector_config(connector_id, cfg)
        return RedirectResponse(url="/connectors", status_code=303)

    @app.get("/actions", response_class=HTMLResponse)
    def actions_page(request: Request, status: str = "proposed", error: str | None = None):
        db = AdsDB(settings.db_path)
        proposals = db.list_action_proposals(status=status, limit=100)
        return templates.TemplateResponse(
            "actions.html",
            {"request": request, "proposals": proposals, "status": status, "error": error},
        )

    @app.post("/actions/{proposal_id}/approve")
    def approve_action(proposal_id: str):
        repo.set_proposal_status(proposal_id, "approved", actor="web")
        return RedirectResponse(url="/actions?status=proposed", status_code=303)

    @app.post("/actions/{proposal_id}/reject")
    def reject_action(proposal_id: str):
        repo.set_proposal_status(proposal_id, "rejected", actor="web")
        return RedirectResponse(url="/actions?status=proposed", status_code=303)

    @app.post("/actions/{proposal_id}/execute")
    async def execute_action(proposal_id: str):
        from urllib.parse import quote
        try:
            await execute_proposal(settings, repo=repo, proposal_id=proposal_id, actor="web")
        except ExecutionError as e:
            return RedirectResponse(
                url=f"/actions?status=approved&error={quote(str(e))}",
                status_code=303,
            )
        return RedirectResponse(url="/actions?status=executed", status_code=303)

    @app.get("/rules", response_class=HTMLResponse)
    def rules_page(request: Request, error: str | None = None):
        rules = repo.list_rules_all()
        return templates.TemplateResponse(
            "rules.html",
            {"request": request, "rules": rules, "error": error},
        )

    @app.post("/rules/{rule_id}/enable")
    def enable_rule(rule_id: str):
        repo.set_rule_enabled(rule_id, True)
        return RedirectResponse(url="/rules", status_code=303)

    @app.post("/rules/{rule_id}/disable")
    def disable_rule(rule_id: str):
        repo.set_rule_enabled(rule_id, False)
        return RedirectResponse(url="/rules", status_code=303)

    @app.post("/rules/{rule_id}/update")
    async def update_rule(rule_id: str, request: Request):
        rule = repo.get_rule(rule_id)
        if not rule:
            return RedirectResponse(url="/rules?error=unknown_rule", status_code=303)
        form = await request.form()
        params_json = (form.get("params_json") or "").strip() or "{}"
        try:
            params = _normalize_rule_params(str(rule.get("rule_type") or ""), params_json)
        except ValueError:
            return RedirectResponse(url="/rules?error=invalid_params_json", status_code=303)
        repo.update_rule_params(rule_id, json.dumps(params, ensure_ascii=True))
        return RedirectResponse(url="/rules", status_code=303)

    @app.get("/executions", response_class=HTMLResponse)
    def executions_page(request: Request):
        executions = repo.list_executions(limit=200)
        return templates.TemplateResponse(
            "executions.html",
            {"request": request, "executions": executions},
        )

    @app.get("/metrics", response_class=HTMLResponse)
    def metrics_page(
        request: Request,
        platform: str = "naver",
        entity_type: str = "campaign",
        date: str | None = None,
        days: int = 1,
        conv_metric: str = "primary",
    ):
        try:
            days_i = int(days)
        except Exception:
            days_i = 1
        days_i = max(1, min(days_i, 30))
        platform = (platform or "").strip().lower()
        if platform not in ui_platforms:
            platform = "naver"

        end_day = (date or "").strip()
        if not end_day:
            end_day = repo.get_latest_metrics_date(platform=platform) or now_kst_date_str(settings.timezone)
        try:
            parsed_end = datetime.fromisoformat(end_day).date()
        except Exception:
            parsed_end = datetime.now(tz=ZoneInfo(settings.timezone)).date()
            end_day = parsed_end.isoformat()
        start_dt = parsed_end - timedelta(days=days_i - 1)
        start_day = end_day
        if days_i > 1:
            start_day = start_dt.isoformat()
        alert_rules = {
            "clicks_min": 20,
            "roas_min": 1.0,
            "cpa_max": 30000.0,
            "no_conversion_spend_min": 10000.0,
        }
        platform_entity_types = {
            "naver": ["campaign", "adgroup", "keyword"],
            "meta": ["campaign", "adset", "ad"],
            "google": ["campaign", "adgroup", "keyword"],
            "cafe24_analytics": ["store", "product", "domain"],
        }
        entity_type_labels = {
            "campaign": "캠페인",
            "adgroup": "광고그룹",
            "keyword": "키워드",
            "adset": "광고세트",
            "ad": "광고",
            "store": "스토어",
            "product": "상품",
            "domain": "유입도메인",
        }
        entity_options = platform_entity_types.get(platform, ["campaign", "adgroup", "keyword"])
        if entity_type not in entity_options:
            entity_type = entity_options[0]
        if days_i <= 1:
            rows = repo.list_metrics_daily_for_date(platform=platform, entity_type=entity_type, day=end_day)
        else:
            rows = repo.list_metrics_range_for_date(
                platform=platform,
                entity_type=entity_type,
                start_day=start_day,
                end_day=end_day,
            )
        conv_mode = (conv_metric or "primary").strip().lower()
        enriched: list[dict[str, Any]] = []
        for m in rows:
            if days_i <= 1:
                metrics_extra: dict[str, Any] = {}
                try:
                    raw = m.get("metrics_json")
                    if isinstance(raw, str) and raw.strip():
                        metrics_extra = json.loads(raw)
                except Exception:
                    metrics_extra = {}
                cafe24 = repo.sum_cafe24_conversions_for_entity_date(
                    entity_platform=platform,
                    entity_type=entity_type,
                    entity_id=str(m["entity_id"]),
                    day_kst=end_day,
                )
            else:
                metrics_extra = {
                    "conversions_all": m.get("conversions_all"),
                    "conversion_value_all": m.get("conversion_value_all"),
                    "conversions_purchase": m.get("conversions_purchase"),
                }
                cafe24 = repo.sum_cafe24_conversions_for_entity_date_range(
                    entity_platform=platform,
                    entity_type=entity_type,
                    entity_id=str(m["entity_id"]),
                    start_day_kst=start_day,
                    end_day_kst=end_day,
                )
            mm = dict(m)
            conv_primary = float(m.get("conversions") or 0)
            value_primary = float(m.get("conversion_value") or 0)
            conv_all = float((metrics_extra.get("conversions_all") if days_i <= 1 else m.get("conversions_all")) or 0)
            value_all = float((metrics_extra.get("conversion_value_all") if days_i <= 1 else m.get("conversion_value_all")) or 0)
            conv_calc = conv_all if conv_mode == "all" else conv_primary
            value_calc = value_all if conv_mode == "all" else value_primary
            spend = float(m.get("spend") or 0)
            clicks = float(m.get("clicks") or 0)
            roas = (value_calc / spend) if spend else 0
            cpa = (spend / conv_calc) if conv_calc else None
            alerts: list[str] = []
            if clicks < alert_rules["clicks_min"]:
                alerts.append(f"클릭 < {alert_rules['clicks_min']}")
            if spend > 0 and roas < alert_rules["roas_min"]:
                alerts.append(f"ROAS < {alert_rules['roas_min']:.1f}")
            if conv_calc > 0 and cpa is not None and cpa > alert_rules["cpa_max"]:
                alerts.append(f"CPA > {alert_rules['cpa_max']:,.0f}원")
            if conv_calc == 0 and spend >= alert_rules["no_conversion_spend_min"]:
                alerts.append("전환 0")
            mm["conversions_all"] = metrics_extra.get("conversions_all")
            mm["conversion_value_all"] = metrics_extra.get("conversion_value_all")
            mm["conversions_purchase"] = metrics_extra.get("conversions_purchase")
            mm["cafe24_conversions"] = cafe24["conversions"]
            mm["cafe24_value"] = cafe24["conversion_value"]
            mm["alerts"] = alerts
            mm["alert_count"] = len(alerts)
            mm["derived_roas"] = roas
            mm["derived_cpa"] = cpa
            mm["derived_clicks"] = clicks
            mm["derived_conv"] = conv_calc
            mm["derived_value"] = value_calc
            enriched.append(mm)
        alert_rows = [row for row in enriched if row["alert_count"] > 0]
        alert_rows.sort(key=lambda r: (-r["alert_count"], r["derived_roas"]))
        total_spend = sum(float(r.get("spend") or 0) for r in enriched)
        total_clicks = sum(float(r.get("clicks") or 0) for r in enriched)
        total_conv = sum(float(r.get("derived_conv") or 0) for r in enriched)
        total_value = sum(float(r.get("derived_value") or 0) for r in enriched)
        summary_roas = (total_value / total_spend) if total_spend else None
        summary_cvr = (total_conv / total_clicks * 100.0) if total_clicks else 0.0
        summary_cpa = (total_spend / total_conv) if total_conv else None

        trend_days = _date_window(end_day, days_i)
        trend_rows: list[dict[str, Any]] = []
        for d in trend_days:
            dm = repo.sum_metrics_daily(platform=platform, day=d, entity_type=entity_type)
            spend = float(dm.get("spend") or 0)
            clicks = float(dm.get("clicks") or 0)
            conv = float(dm.get("conversions") or 0)
            value = float(dm.get("conversion_value") or 0)
            trend_rows.append(
                {
                    "day": d,
                    "spend": spend,
                    "clicks": clicks,
                    "conversions": conv,
                    "value": value,
                    "roas": (value / spend) if spend else 0.0,
                }
            )
        spend_norm = _normalize_series([r["spend"] for r in trend_rows], min_non_zero=10.0)
        for i, row in enumerate(trend_rows):
            row["spend_norm"] = spend_norm[i] if i < len(spend_norm) else 0.0

        top_spend_rows = sorted(enriched, key=lambda r: float(r.get("spend") or 0), reverse=True)[:8]
        top_spend_max = max((float(r.get("spend") or 0) for r in top_spend_rows), default=0.0)
        for row in top_spend_rows:
            spend = float(row.get("spend") or 0)
            row["spend_ratio"] = (spend / top_spend_max * 100.0) if top_spend_max > 0 else 0.0

        alert_spend_max = max((float(r.get("spend") or 0) for r in alert_rows[:8]), default=0.0)
        for row in alert_rows:
            spend = float(row.get("spend") or 0)
            row["alert_spend_ratio"] = (spend / alert_spend_max * 100.0) if alert_spend_max > 0 else 0.0

        return templates.TemplateResponse(
            "metrics.html",
            {
                "request": request,
                "rows": enriched,
                "platform": platform,
                "entity_type": entity_type,
                "day": end_day,
                "start_day": start_day,
                "days": days_i,
                "conv_metric": conv_mode,
                "entity_options": entity_options,
                "entity_type_labels": entity_type_labels,
                "alert_rules": alert_rules,
                "alert_count": len(alert_rows),
                "alert_entities_top": alert_rows[:5],
                "summary_spend": total_spend,
                "summary_clicks": total_clicks,
                "summary_conv": total_conv,
                "summary_value": total_value,
                "summary_roas": summary_roas,
                "summary_cvr": summary_cvr,
                "summary_cpa": summary_cpa,
                "trend_rows": trend_rows,
                "top_spend_rows": top_spend_rows,
            },
        )

    @app.get("/store", response_class=HTMLResponse)
    def store_page(
        request: Request,
        store: str = "cafe24",
        days: int = 14,
    ):
        try:
            days_i = int(days)
        except Exception:
            days_i = 14
        days_i = max(1, min(days_i, 180))
        end = now_kst_date_str(settings.timezone)
        start_dt = datetime.now(tz=ZoneInfo(settings.timezone)).date() - timedelta(days=days_i - 1)
        start = start_dt.isoformat()
        exclude_tokens = list(STORE_REVENUE_EXCLUDED_STATUS_TOKENS.get(store, ()))
        totals = repo.sum_store_orders(
            store=store,
            start_date_kst=start,
            end_date_kst=end,
            exclude_status_tokens=exclude_tokens,
        )
        total_orders = int(totals.get("order_count") or 0)
        total_revenue = float(totals.get("total_amount") or 0)
        avg_order_value = (total_revenue / total_orders) if total_orders else None

        summary = repo.count_store_orders_by_inflow_path(
            store=store,
            start_date_kst=start,
            end_date_kst=end,
            limit=50,
        )
        summary_total = sum(int(s.get("orders") or 0) for s in summary)
        summary_enriched: list[dict[str, Any]] = []
        for s in summary:
            orders = int(s.get("orders") or 0)
            ratio = (orders / summary_total * 100.0) if summary_total > 0 else 0.0
            summary_enriched.append(
                {
                    **s,
                    "ratio": ratio,
                }
            )
        if summary_enriched:
            ratio_norm = _normalize_series([s["ratio"] for s in summary_enriched], min_non_zero=12.0)
            for i, s in enumerate(summary_enriched):
                s["ratio_norm"] = ratio_norm[i] if i < len(ratio_norm) else 0.0

        trend_days = _date_window(end, days_i)
        trend_rows: list[dict[str, Any]] = []
        for d in trend_days:
            d_totals = repo.sum_store_orders(
                store=store,
                start_date_kst=d,
                end_date_kst=d,
                exclude_status_tokens=exclude_tokens,
            )
            trend_rows.append(
                {
                    "day": d,
                    "orders": int(d_totals.get("order_count") or 0),
                    "revenue": float(d_totals.get("total_amount") or 0),
                }
            )
        trend_order_norm = _normalize_series([r["orders"] for r in trend_rows], min_non_zero=10.0)
        trend_revenue_norm = _normalize_series([r["revenue"] for r in trend_rows], min_non_zero=10.0)
        for i, r in enumerate(trend_rows):
            r["orders_norm"] = trend_order_norm[i] if i < len(trend_order_norm) else 0.0
            r["revenue_norm"] = trend_revenue_norm[i] if i < len(trend_revenue_norm) else 0.0

        orders = repo.list_store_orders(store=store, start_date_kst=start, end_date_kst=end, limit=200)
        return templates.TemplateResponse(
            "store.html",
            {
                "request": request,
                "store": store,
                "days": days_i,
                "start": start,
                "end": end,
                "summary": summary_enriched,
                "summary_total": summary_total,
                "total_orders": total_orders,
                "total_revenue": total_revenue,
                "avg_order_value": avg_order_value,
                "trend_rows": trend_rows,
                "orders": orders,
            },
        )

    @app.get("/tracking", response_class=HTMLResponse)
    def tracking_page(request: Request):
        links = repo.list_tracking_links(limit=200)
        return templates.TemplateResponse(
            "tracking.html",
            {"request": request, "links": links},
        )

    @app.get("/tracking/snippet/cafe24", response_class=HTMLResponse)
    def tracking_cafe24_snippet(request: Request):
        return templates.TemplateResponse(
            "tracking_cafe24_snippet.html",
            {"request": request},
        )

    @app.post("/tracking/create")
    async def tracking_create(request: Request):
        form = await request.form()
        code = (form.get("code") or "").strip()
        if not code:
            code = new_id("t").replace("t_", "")
        dest = (form.get("destination_url") or "").strip()
        channel = (form.get("channel") or "").strip().lower() or None
        objective = (form.get("objective") or "").strip().lower() or None
        e_platform = (form.get("entity_platform") or "").strip().lower() or None
        e_type = (form.get("entity_type") or "").strip().lower() or None
        e_id = (form.get("entity_id") or "").strip() or None
        if dest:
            repo.upsert_tracking_link(
                code=code,
                destination_url=dest,
                channel=channel,
                objective=objective,
                entity_platform=e_platform,
                entity_type=e_type,
                entity_id=e_id,
                meta_json={},
            )
        return RedirectResponse(url="/tracking", status_code=303)

    @app.get("/kpi", response_class=HTMLResponse)
    def kpi_page(request: Request):
        profiles = repo.list_kpi_profiles(limit=200)
        mappings = repo.list_entity_kpi_profiles(limit=200)
        return templates.TemplateResponse(
            "kpi.html",
            {"request": request, "profiles": profiles, "mappings": mappings},
        )

    @app.post("/kpi/create")
    async def kpi_create(request: Request):
        form = await request.form()
        name = (form.get("name") or "").strip()
        objective = (form.get("objective") or "").strip()
        platform = (form.get("platform") or "").strip().lower() or None
        definition_raw = (form.get("definition_json") or "").strip()
        if not name or not objective:
            return RedirectResponse(url="/kpi", status_code=303)
        try:
            definition = json.loads(definition_raw) if definition_raw else {}
        except Exception:
            definition = {"_raw": definition_raw}
        repo.create_kpi_profile(name=name, objective=objective, platform=platform, definition=definition)
        return RedirectResponse(url="/kpi", status_code=303)

    @app.get("/entities", response_class=HTMLResponse)
    def entities_page(request: Request, platform: str | None = None, entity_type: str | None = None):
        entities = repo.list_entities(platform=platform, entity_type=entity_type, limit=500)
        profiles = repo.list_kpi_profiles(limit=200)
        return templates.TemplateResponse(
            "entities.html",
            {
                "request": request,
                "entities": entities,
                "profiles": profiles,
                "platform": platform,
                "entity_type": entity_type,
            },
        )

    @app.post("/entities/attach-kpi")
    async def entities_attach_kpi(request: Request):
        form = await request.form()
        platform = (form.get("platform") or "").strip().lower()
        entity_type = (form.get("entity_type") or "").strip().lower()
        entity_id = (form.get("entity_id") or "").strip()
        kpi_profile_id = (form.get("kpi_profile_id") or "").strip()
        if platform and entity_type and entity_id and kpi_profile_id:
            repo.attach_kpi_profile_to_entity(
                platform=platform,
                entity_type=entity_type,
                entity_id=entity_id,
                kpi_profile_id=kpi_profile_id,
                enabled=True,
            )
        return RedirectResponse(url="/entities", status_code=303)

    def _append_params(url: str, params: dict[str, str]) -> str:
        u = urlparse(url)
        q = dict(parse_qsl(u.query, keep_blank_values=True))
        q.update(params)
        new_q = urlencode(q, doseq=True)
        return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

    @app.get("/r/{code}")
    def redirect_tracking(code: str, request: Request):
        link = repo.get_tracking_link(code)
        if not link:
            return JSONResponse({"ok": False, "error": "unknown code"}, status_code=404)
        click_id = new_id("clk")
        date_kst = now_kst_date_str(settings.timezone)
        ua = request.headers.get("user-agent")
        ref = request.headers.get("referer")
        ip = (request.client.host if request.client else "") or ""
        ip_hash = sha256_hex(ip) if ip else None
        query = dict(request.query_params)
        repo.record_click_event(
            click_id=click_id,
            code=code,
            date_kst=date_kst,
            user_agent=ua,
            ip_hash=ip_hash,
            referer=ref,
            query=query,
        )
        dest = str(link.get("destination_url") or "")
        passthrough = dict(query)
        passthrough["cid"] = click_id
        dest2 = _append_params(dest, passthrough)
        return RedirectResponse(url=dest2, status_code=302)

    @app.post("/events/conversion")
    async def conversion_event(request: Request):
        try:
            payload = await request.json()
        except Exception:
            return JSONResponse({"ok": False, "error": "invalid json"}, status_code=400)
        if not isinstance(payload, dict):
            return JSONResponse({"ok": False, "error": "bad payload"}, status_code=400)
        _record_conversion(repo, settings, payload)
        return JSONResponse({"ok": True})

    @app.get("/events/conversion.gif")
    def conversion_pixel(
        cid: str | None = None,
        click_id: str | None = None,
        order_id: str | None = None,
        value: float | None = None,
        currency: str | None = "KRW",
        source: str | None = "cafe24_js",
    ):
        payload = {
            "click_id": (click_id or cid or ""),
            "order_id": order_id or "",
            "value": value,
            "currency": currency,
            "source": source,
        }
        _record_conversion(repo, settings, payload)
        # 1x1 transparent gif
        gif_1x1 = (
            b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00"
            b"\xff\xff\xff!\xf9\x04\x01\x00\x00\x00\x00,\x00"
            b"\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02D\x01\x00;"
        )
        return Response(content=gif_1x1, media_type="image/gif")

    return app


def run_web(settings: Settings) -> None:
    app = create_app(settings)
    uvicorn.run(app, host=settings.web_host, port=settings.web_port, log_level="info")


def _record_conversion(repo: Repo, settings: Settings, payload: dict[str, Any]) -> None:
    click_id = (payload.get("click_id") or payload.get("cid") or "").strip() or None
    order_id = (payload.get("order_id") or payload.get("orderId") or "").strip() or None
    value_raw = payload.get("value")
    try:
        value = float(value_raw) if value_raw is not None and str(value_raw).strip() != "" else None
    except Exception:
        value = None
    currency = (payload.get("currency") or "KRW").strip().upper()
    source = (payload.get("source") or "cafe24_js").strip().lower()
    date_kst = now_kst_date_str(settings.timezone)
    repo.record_conversion_event(
        conversion_id=new_id("cvn"),
        click_id=click_id,
        date_kst=date_kst,
        order_id=order_id,
        value=value,
        currency=currency,
        source=source,
        extra=payload,
    )
