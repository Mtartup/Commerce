from __future__ import annotations

import html
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient

from commerce.config import Settings
from commerce.db import AdsDB
from commerce.repo import Repo
from commerce.web.app import create_app
from commerce.util import now_kst_date_str


def _settings_for_db(db_path: Path) -> Settings:
    return Settings(
        db_path=db_path,
        timezone="Asia/Seoul",
        web_host="127.0.0.1",
        web_port=0,
        telegram_bot_token=None,
        telegram_allowed_chat_id=None,
        demo_mode=False,
        execution_mode="manual",
    )


def test_update_connector_config_naver_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "ads.sqlite3"
    AdsDB(db_path).init()
    AdsDB(db_path).seed_default_connectors()

    repo = Repo(db_path)
    naver = next(c for c in repo.list_connectors() if c["platform"] == "naver")
    connector_id = str(naver["id"])

    app = create_app(_settings_for_db(db_path))
    client = TestClient(app)

    resp = client.post(
        f"/connectors/{connector_id}/config",
        data={
            "mode": "api",
            "product_types": "powerlink,powercontent",
            "report_tp": "AD_DETAIL",
            "ingest_levels": "campaign,adgroup,keyword,bad",
            "include_today": "1",
            "api_min_interval_minutes": "5",
            "report_poll_interval_sec": "2.5",
            "report_timeout_sec": "300",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303

    row = repo.get_connector(connector_id)
    assert row is not None
    cfg = json.loads(row.get("config_json") or "{}")
    assert cfg["mode"] == "api"
    assert cfg["product_types"] == ["powerlink", "powercontent"]
    assert cfg["report_tp"] == "AD_DETAIL"
    assert cfg["ingest_levels"] == ["campaign", "adgroup", "keyword"]
    assert cfg["include_today"] is True
    assert cfg["api_min_interval_minutes"] == 5.0
    assert cfg["report_poll_interval_sec"] == 2.5
    assert cfg["report_timeout_sec"] == 300.0


def test_update_connector_config_meta_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "ads.sqlite3"
    AdsDB(db_path).init()
    AdsDB(db_path).seed_default_connectors()

    repo = Repo(db_path)
    meta = next(c for c in repo.list_connectors() if c["platform"] == "meta")
    connector_id = str(meta["id"])

    app = create_app(_settings_for_db(db_path))
    client = TestClient(app)

    resp = client.post(
        f"/connectors/{connector_id}/config",
        data={
            "mode": "api",
            "ad_account_id": "act_123-456-7890",
            "ingest_levels": "campaign,adset,ad,bad",
            "include_today": "1",
            "api_min_interval_minutes": "15",
            "conversion_action_types": "purchase,offsite_conversion.fb_pixel_purchase",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303

    row = repo.get_connector(connector_id)
    assert row is not None
    cfg = json.loads(row.get("config_json") or "{}")
    assert cfg["mode"] == "api"
    assert cfg["ad_account_id"] == "act_123-456-7890"
    assert cfg["ingest_levels"] == ["campaign", "adset", "ad"]
    assert cfg["include_today"] is True
    assert cfg["api_min_interval_minutes"] == 15.0
    assert cfg["conversion_action_types"] == ["purchase", "offsite_conversion.fb_pixel_purchase"]


def test_update_connector_config_clears_optional_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "ads.sqlite3"
    AdsDB(db_path).init()
    AdsDB(db_path).seed_default_connectors()

    repo = Repo(db_path)
    naver = next(c for c in repo.list_connectors() if c["platform"] == "naver")
    connector_id = str(naver["id"])

    app = create_app(_settings_for_db(db_path))
    client = TestClient(app)

    client.post(
        f"/connectors/{connector_id}/config",
        data={
            "mode": "api",
            "product_types": "powerlink",
            "report_tp": "AD_DETAIL",
            "ingest_levels": "campaign",
            "include_today": "1",
            "api_min_interval_minutes": "5",
            "report_poll_interval_sec": "2.5",
            "report_timeout_sec": "300",
        },
        follow_redirects=False,
    )

    resp = client.post(
        f"/connectors/{connector_id}/config",
        data={
            "mode": "api",
            "product_types": "",
            "report_tp": "",
            "ingest_levels": "",
            # omit include_today to set false
            "api_min_interval_minutes": "",
            "report_poll_interval_sec": "",
            "report_timeout_sec": "",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303

    row = repo.get_connector(connector_id)
    assert row is not None
    cfg = json.loads(row.get("config_json") or "{}")
    assert cfg["mode"] == "api"
    assert cfg.get("product_types") is None
    assert cfg.get("report_tp") is None
    assert cfg.get("ingest_levels") is None
    assert cfg["include_today"] is False
    assert cfg.get("api_min_interval_minutes") is None
    assert cfg.get("report_poll_interval_sec") is None
    assert cfg.get("report_timeout_sec") is None


def test_home_and_connectors_do_not_show_disabled_platforms(tmp_path: Path) -> None:
    db_path = tmp_path / "ads.sqlite3"
    AdsDB(db_path).init()
    AdsDB(db_path).seed_default_connectors()

    app = create_app(_settings_for_db(db_path))
    client = TestClient(app)

    home = client.get("/")
    conns = client.get("/connectors")
    metrics = client.get("/metrics?platform=tiktok")

    assert home.status_code == 200
    assert conns.status_code == 200
    assert metrics.status_code == 200

    home_text = home.text.lower()
    conns_text = conns.text.lower()
    metrics_text = metrics.text.lower()

    # tiktok is not in ui_platforms, so it should not appear and metrics should fallback
    assert "tiktok" not in home_text
    assert "tiktok" not in conns_text
    assert "tiktok" not in metrics_text
    assert "네이버" in home_text
    assert "메타" in home_text
    assert "구글" in home_text


def test_connectors_show_channel_level_summary(tmp_path: Path) -> None:
    db_path = tmp_path / "ads.sqlite3"
    AdsDB(db_path).init()
    AdsDB(db_path).seed_default_connectors()
    repo = Repo(db_path)
    day = now_kst_date_str("Asia/Seoul")

    repo.upsert_metric_daily(
        platform="naver",
        account_id="test",
        entity_type="campaign",
        entity_id="n_c_1",
        day=day,
        spend=12345,
        impressions=1000,
        clicks=20,
        conversions=4,
        conversion_value=80000,
        metrics_json={},
    )
    repo.upsert_metric_daily(
        platform="naver",
        account_id="test",
        entity_type="adgroup",
        entity_id="n_g_1",
        day=day,
        spend=8000,
        impressions=600,
        clicks=10,
        conversions=2,
        conversion_value=30000,
        metrics_json={},
    )
    repo.upsert_metric_daily(
        platform="meta",
        account_id="test",
        entity_type="adset",
        entity_id="m_s_1",
        day=day,
        spend=5000,
        impressions=400,
        clicks=5,
        conversions=1,
        conversion_value=15000,
        metrics_json={},
    )

    app = create_app(
        _settings_for_db(db_path),
    )
    client = TestClient(app)
    resp = client.get("/connectors")
    assert resp.status_code == 200
    text = resp.text

    assert "네이버" in text
    assert "메타" in text
    assert "구글" in text
    assert "캠페인" in text
    assert "광고그룹" in text
    assert "광고세트" in text
    assert "/metrics?platform=naver&entity_type=campaign" in text
    assert "/metrics?platform=meta&entity_type=adset" in text


def test_connectors_support_range_days(tmp_path: Path) -> None:
    db_path = tmp_path / "ads.sqlite3"
    AdsDB(db_path).init()
    AdsDB(db_path).seed_default_connectors()
    repo = Repo(db_path)
    tz = ZoneInfo("Asia/Seoul")

    for d in range(7):
        day = (datetime.now(tz=tz).date() - timedelta(days=d)).isoformat()
        repo.upsert_metric_daily(
            platform="google",
            account_id="test",
            entity_type="campaign",
            entity_id=f"g_c_{d}",
            day=day,
            spend=1000,
            impressions=100,
            clicks=10,
            conversions=1,
            conversion_value=5000,
            metrics_json={},
        )

    app = create_app(
        _settings_for_db(db_path),
    )
    client = TestClient(app)
    today = now_kst_date_str("Asia/Seoul")
    start_day = (datetime.now(tz=tz).date() - timedelta(days=6)).isoformat()

    resp = client.get("/connectors?days=7")
    assert resp.status_code == 200
    text = resp.text
    assert f"기간: {start_day} ~ {today}" in text
    assert "최근 7일" in text


def test_metrics_support_range_days(tmp_path: Path) -> None:
    db_path = tmp_path / "ads.sqlite3"
    AdsDB(db_path).init()
    repo = Repo(db_path)
    AdsDB(db_path).seed_default_connectors()

    tz = ZoneInfo("Asia/Seoul")
    today = datetime.now(tz=tz).date()
    today_str = today.isoformat()
    start_day = (today - timedelta(days=6)).isoformat()
    for day_offset in (0, 3):
        d = (today - timedelta(days=day_offset)).isoformat()
        repo.upsert_metric_daily(
            platform="naver",
            account_id="test",
            entity_type="campaign",
            entity_id="campaign_1",
            day=d,
            spend=10000,
            impressions=200,
            clicks=20,
            conversions=2,
            conversion_value=50000,
            metrics_json={},
        )
    repo.upsert_metric_daily(
        platform="naver",
        account_id="test",
        entity_type="campaign",
        entity_id="campaign_2",
        day=today_str,
        spend=5000,
        impressions=100,
        clicks=5,
        conversions=1,
        conversion_value=10000,
        metrics_json={},
    )

    app = create_app(_settings_for_db(db_path))
    client = TestClient(app)
    resp = client.get(f"/metrics?platform=naver&entity_type=campaign&date={today_str}&days=7")
    assert resp.status_code == 200
    text = resp.text
    assert f"기간: {start_day} ~ {today_str}" in text
    assert "최근 7일" in text


def test_metrics_default_ui_uses_period_first(tmp_path: Path) -> None:
    db_path = tmp_path / "ads.sqlite3"
    AdsDB(db_path).init()
    AdsDB(db_path).seed_default_connectors()

    app = create_app(_settings_for_db(db_path))
    client = TestClient(app)
    resp = client.get("/metrics")
    assert resp.status_code == 200

    text = resp.text
    assert "고급: 날짜 직접 입력" in text
    assert "날짜 (KST)" not in text


def test_metrics_alert_card_and_thresholds(tmp_path: Path) -> None:
    db_path = tmp_path / "ads.sqlite3"
    AdsDB(db_path).init()
    AdsDB(db_path).seed_default_connectors()
    repo = Repo(db_path)
    day = now_kst_date_str("Asia/Seoul")

    repo.upsert_metric_daily(
        platform="naver",
        account_id="test",
        entity_type="campaign",
        entity_id="alert_bad",
        day=day,
        spend=50000,
        impressions=120,
        clicks=10,
        conversions=1,
        conversion_value=10000,
        metrics_json={},
    )
    repo.upsert_metric_daily(
        platform="naver",
        account_id="test",
        entity_type="campaign",
        entity_id="safe_ok",
        day=day,
        spend=5000,
        impressions=200,
        clicks=50,
        conversions=10,
        conversion_value=20000,
        metrics_json={},
    )

    app = create_app(_settings_for_db(db_path))
    client = TestClient(app)
    resp = client.get("/metrics?platform=naver&entity_type=campaign")
    assert resp.status_code == 200

    text = html.unescape(resp.text)
    assert "경보(빨간색)" in text
    assert "클릭 < 20" in text
    assert "ROAS < 1.0" in text
    assert "CPA > 30,000원" in text
    assert "alert_bad" in text
    assert "safe_ok" in text
