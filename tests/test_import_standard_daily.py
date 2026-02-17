from __future__ import annotations

import sqlite3
from pathlib import Path

from commerce.db import AdsDB
from commerce.importers.standard import import_daily_csv
from commerce.repo import Repo


def test_import_standard_daily_basic(tmp_path: Path) -> None:
    db_path = tmp_path / "ads.sqlite3"
    AdsDB(db_path).init()
    repo = Repo(db_path)

    csv_path = tmp_path / "daily.csv"
    csv_path.write_text(
        "platform,entity_type,entity_id,date,spend,clicks,conversions,conversion_value\n"
        "naver,campaign,c1,2026-02-15,50000,10,1,90000\n",
        encoding="utf-8",
    )

    res = import_daily_csv(repo, path=csv_path)
    assert res["ok"] is True
    assert res["imported"] == 1

    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT spend, clicks, conversions, conversion_value FROM metrics_daily").fetchone()
        assert row is not None
        assert float(row[0]) == 50000.0
        assert int(row[1]) == 10
        assert float(row[2]) == 1.0
        assert float(row[3]) == 90000.0

