from __future__ import annotations

import sqlite3
from pathlib import Path

from commerce.db import AdsDB
from commerce.importers.naver_searchad import NaverImportOptions, import_naver_searchad_csv
from commerce.repo import Repo


def test_import_naver_basic(tmp_path: Path) -> None:
    db_path = tmp_path / "ads.sqlite3"
    AdsDB(db_path).init()
    repo = Repo(db_path)

    csv_path = tmp_path / "naver.csv"
    csv_path.write_text(
        "일자,캠페인ID,캠페인명,노출수,클릭수,총비용,전환수,구매전환수,전환매출\n"
        "2026-02-15,c1,Camp1,100,10,60000,5,2,90000\n",
        encoding="utf-8",
    )

    res = import_naver_searchad_csv(
        repo,
        path=csv_path,
        opts=NaverImportOptions(product_type="powerlink", level="campaign", account_id="a1"),
    )
    assert res["ok"] is True
    assert res["imported"] == 1

    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) FROM metrics_daily").fetchone()
        assert row and int(row[0]) == 1

        mj_raw = conn.execute("SELECT metrics_json FROM metrics_daily").fetchone()
        assert mj_raw is not None
        import json

        mj = json.loads(mj_raw[0])
        assert float(mj["conversions_all"]) == 5.0
        assert float(mj["conversions_purchase"]) == 2.0
