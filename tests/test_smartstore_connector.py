from __future__ import annotations

import asyncio
import json
from pathlib import Path

from commerce.connectors.base import ConnectorContext
from commerce.connectors.smartstore import SmartStoreConnector, _to_date_kst
from commerce.db import AdsDB
from commerce.repo import Repo


def test_smartstore_fixture_uses_place_order_date_when_order_date_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "ads.sqlite3"
    AdsDB(db_path).init()
    repo = Repo(db_path)

    fixture_dir = tmp_path / "smartstore_fixture"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    orders = [
        {
            "productOrderId": "po_1",
            "placeOrderDate": "2026-01-15T17:08:45.418+09:00",
            "productOrderStatus": "PURCHASE_DECIDED",
            "totalPaymentAmount": 98000,
            "orderPlaceId": "NAVER_SEARCH",
            "orderPlaceName": "naver search",
        }
    ]
    (fixture_dir / "orders.json").write_text(json.dumps(orders, ensure_ascii=False), encoding="utf-8")

    ctx = ConnectorContext(
        connector_id="con_test",
        platform="smartstore",
        name="SmartStore Test",
        config={"mode": "fixture", "fixture_dir": str(fixture_dir)},
    )
    connector = SmartStoreConnector(ctx, repo)

    asyncio.run(connector.sync_entities())

    rows = repo.list_store_orders(store="smartstore", limit=10)
    assert rows
    row = rows[0]
    assert row["order_id"] == "po_1"
    assert str(row["ordered_at"]).startswith("2026-01-15T17:08:45")
    assert row["date_kst"] == "2026-01-15"


def test_smartstore_fixture_skips_invalid_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "ads.sqlite3"
    AdsDB(db_path).init()
    repo = Repo(db_path)

    fixture_dir = tmp_path / "smartstore_fixture"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    orders = [
        {
            "productOrderId": "",
            "placeOrderDate": "2026-01-01T10:00:00.000+09:00",
            "productOrderStatus": "PURCHASE_DECIDED",
            "totalPaymentAmount": 10000,
        },
        {
            "productOrderId": "po_invalid_date",
            "ordered_at": "n/a",
            "productOrderStatus": "PURCHASE_DECIDED",
            "totalPaymentAmount": 20000,
        },
        {
            "productOrderId": "po_valid",
            "placeOrderDate": "2026-01-15T17:08:45.418+09:00",
            "productOrderStatus": "PURCHASE_DECIDED",
            "totalPaymentAmount": 30000,
        },
    ]
    (fixture_dir / "orders.json").write_text(json.dumps(orders, ensure_ascii=False), encoding="utf-8")

    ctx = ConnectorContext(
        connector_id="con_test",
        platform="smartstore",
        name="SmartStore Test",
        config={"mode": "fixture", "fixture_dir": str(fixture_dir)},
    )
    connector = SmartStoreConnector(ctx, repo)

    asyncio.run(connector.sync_entities())

    rows = repo.list_store_orders(store="smartstore", limit=10)
    assert len(rows) == 1
    row = rows[0]
    assert row["order_id"] == "po_valid"
    assert row["date_kst"] == "2026-01-15"


def test_smartstore_to_date_kst_returns_empty_for_invalid_timestamp() -> None:
    assert _to_date_kst("2026-02-15T00:00:00.000+09:00") == "2026-02-15"
    assert _to_date_kst("bad-timestamp") == ""
    assert _to_date_kst(None) == ""
