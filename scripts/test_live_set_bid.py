"""Live API test: adgroup set_bid +100 KRW, then revert."""
import asyncio
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from commerce.connectors.base import ConnectorContext
from commerce.connectors.google_ads import GoogleAdsConnector
from commerce.repo import Repo

ADGROUP_ID = "193793658880"
DB_PATH = Path(__file__).resolve().parents[1] / "data" / "ads.sqlite3"


async def main():
    repo = Repo(DB_PATH)
    ctx = ConnectorContext(
        connector_id="live_test",
        platform="google",
        name="Live Test",
        config={"mode": "api"},
    )
    c = GoogleAdsConnector(ctx, repo)

    # 1) 현재 bid 조회
    client = c._google_client()
    cid = c._google_customer_id()
    row = c._query_single(
        client,
        cid,
        f"SELECT ad_group.cpc_bid_micros FROM ad_group WHERE ad_group.id = {ADGROUP_ID}",
    )
    if not row:
        print(f"adgroup {ADGROUP_ID} not found")
        return
    cur_micros = int(getattr(row.ad_group, "cpc_bid_micros", 0) or 0)
    cur_krw = cur_micros // 1_000_000
    new_krw = cur_krw + 100
    print(f"Current bid: {cur_krw} KRW ({cur_micros} micros)")
    print(f"New bid:     {new_krw} KRW")

    # 2) set_bid 호출
    proposal = {
        "action_type": "set_bid",
        "entity_type": "adgroup",
        "entity_id": ADGROUP_ID,
        "payload_json": json.dumps({"bid": new_krw}),
    }
    result = await c.apply_action(proposal)
    print(f"Result: {json.dumps(result, ensure_ascii=False, indent=2)}")

    # 3) 되돌리기
    revert = {
        "action_type": "set_bid",
        "entity_type": "adgroup",
        "entity_id": ADGROUP_ID,
        "payload_json": json.dumps({"bid": cur_krw}),
    }
    rev = await c.apply_action(revert)
    print(f"Reverted: {json.dumps(rev, ensure_ascii=False, indent=2)}")


asyncio.run(main())
