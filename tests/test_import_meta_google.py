from __future__ import annotations

import sqlite3
from pathlib import Path

from commerce.db import AdsDB
from commerce.importers.google_export import GoogleImportOptions, import_google_ads_csv
from commerce.importers.meta_export import MetaImportOptions, import_meta_ads_csv
from commerce.repo import Repo


def test_import_meta_campaign_basic(tmp_path: Path) -> None:
    db_path = tmp_path / "ads.sqlite3"
    AdsDB(db_path).init()
    repo = Repo(db_path)

    csv_path = tmp_path / "meta.csv"
    csv_path.write_text(
        "Day,Campaign ID,Campaign name,Amount spent (KRW),Impressions,Link clicks,Results,Purchases,Purchases conversion value\n"
        "2026-02-15,c1,MetaCamp,60000,1000,25,5,2,120000\n",
        encoding="utf-8",
    )

    res = import_meta_ads_csv(repo, path=csv_path, opts=MetaImportOptions(level="campaign", account_id="a1"))
    assert res["ok"] is True
    assert res["imported"] == 1

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT platform, entity_type, spend, clicks, conversions, conversion_value FROM metrics_daily"
        ).fetchone()
        assert row is not None
        assert row[0] == "meta"
        assert row[1] == "campaign"
        assert float(row[2]) == 60000.0
        assert int(row[3]) == 25
        assert float(row[4]) == 2.0
        assert float(row[5]) == 120000.0

        mj_raw = conn.execute("SELECT metrics_json FROM metrics_daily").fetchone()
        assert mj_raw is not None
        import json

        mj = json.loads(mj_raw[0])
        assert float(mj["conversions_all"]) == 5.0
        assert float(mj["conversions_purchase"]) == 2.0
        assert float(mj["conversions_results"]) == 5.0


def test_import_google_campaign_basic(tmp_path: Path) -> None:
    db_path = tmp_path / "ads.sqlite3"
    AdsDB(db_path).init()
    repo = Repo(db_path)

    csv_path = tmp_path / "google.csv"
    csv_path.write_text(
        "Date,Campaign ID,Campaign,Cost,Impressions,Clicks,Conversions,All conv.,Conversion value,All conv. value\n"
        "2026-02-15,123,GoogleCamp,70000,2000,50,3,7,210000,250000\n",
        encoding="utf-8",
    )

    res = import_google_ads_csv(repo, path=csv_path, opts=GoogleImportOptions(level="campaign", account_id="g1"))
    assert res["ok"] is True
    assert res["imported"] == 1

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT platform, entity_type, spend, clicks, conversions, conversion_value FROM metrics_daily"
        ).fetchone()
        assert row is not None
        assert row[0] == "google"
        assert row[1] == "campaign"
        assert float(row[2]) == 70000.0
        assert int(row[3]) == 50
        assert float(row[4]) == 3.0
        assert float(row[5]) == 210000.0

        mj_raw = conn.execute("SELECT metrics_json FROM metrics_daily").fetchone()
        assert mj_raw is not None
        import json

        mj = json.loads(mj_raw[0])
        assert float(mj["conversions_all"]) == 7.0
        assert float(mj["conversion_value_all"]) == 250000.0
