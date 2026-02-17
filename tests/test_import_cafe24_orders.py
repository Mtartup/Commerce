from __future__ import annotations

import sqlite3
from pathlib import Path

from commerce.db import AdsDB
from commerce.importers.cafe24_orders import Cafe24OrdersImportOptions, import_cafe24_orders_csv
from commerce.repo import Repo


def test_import_cafe24_orders_basic(tmp_path: Path) -> None:
    db_path = tmp_path / "ads.sqlite3"
    AdsDB(db_path).init()
    repo = Repo(db_path)

    csv_path = tmp_path / "cafe24_orders.csv"
    csv_path.write_text(
        "주문번호,결제일시,결제금액,주문상태,유입경로,유입경로상세\n"
        "20260215-000001,2026-02-15 10:11:12,39800,결제완료,naver,search\n",
        encoding="utf-8",
    )

    res = import_cafe24_orders_csv(
        repo,
        path=csv_path,
        opts=Cafe24OrdersImportOptions(store="cafe24", timezone="Asia/Seoul"),
    )
    assert res["ok"] is True
    assert res["inserted"] == 1

    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT store, order_id, date_kst, inflow_path FROM store_orders").fetchone()
        assert row is not None
        assert row[0] == "cafe24"
        assert row[1] == "20260215-000001"
        assert row[2] == "2026-02-15"
        assert row[3] == "naver"

