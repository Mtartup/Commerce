from __future__ import annotations

import asyncio
import json
from pathlib import Path

from commerce.connectors.base import ConnectorContext
from commerce.connectors.smartstore import SmartStoreConnector
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
