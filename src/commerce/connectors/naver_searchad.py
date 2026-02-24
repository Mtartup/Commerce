from __future__ import annotations

import base64
import csv
import hashlib
import hmac
import json
import os
import time
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from commerce.connectors.base import ConnectorCapabilities, ConnectorContext
from commerce.fixtures import (
    fixture_dir,
    load_entities,
    load_metrics_daily_rows,
    load_metrics_intraday_rows,
)

_DEFAULT_BASE_URL = "https://api.searchad.naver.com"

# Column names for header-less TSV downloads (fileVersion=v2).
# Verified via /stats JSON API cross-reference (2026-02-18):
#   Col 11=impCnt, Col 12=clkCnt, Col 13=salesAmt (≈API value/1.1, VAT-exclusive).
#   Col 14/15 unverified; Col 15 is consistently 0 → mapped to ccnt (safe default).
_AD_DETAIL_COLUMNS = [
    "statDt", "customerId", "nccCampaignId", "nccAdgroupId", "nccKeywordId",
    "nccAdId", "businessChannelId", "timeRange", "reachCnt", "bidAmt",
    "pcMobileType", "impCnt", "clkCnt", "salesAmt", "viewCnt", "ccnt",
]

_KNOWN_HEADER_NAMES = {"nccCampaignId", "impCnt", "clkCnt", "ccnt", "salesAmt"}


def _read_text_best_effort(data: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def _parse_float(v: Any) -> float | None:
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def _parse_int(v: Any) -> int | None:
    f = _parse_float(v)
    if f is None:
        return None
    return int(f)


def _first(row: dict[str, Any], keys: list[str]) -> str | None:
    for k in keys:
        if k in row and str(row.get(k) or "").strip() != "":
            return str(row.get(k)).strip()
    return None


def _daterange_inclusive(date_from: str, date_to: str) -> list[str]:
    d0 = date.fromisoformat(date_from)
    d1 = date.fromisoformat(date_to)
    if d1 < d0:
        d0, d1 = d1, d0
    out: list[str] = []
    cur = d0
    while cur <= d1:
        out.append(cur.isoformat())
        cur = cur + timedelta(days=1)
    return out


def _to_stat_dt(day_iso: str) -> str:
    return day_iso.replace("-", "")


def _to_day_iso(stat_dt: str) -> str:
    if len(stat_dt) == 8 and stat_dt.isdigit():
        return f"{stat_dt[0:4]}-{stat_dt[4:6]}-{stat_dt[6:8]}"
    return stat_dt


def _safe_levels(raw: Any) -> list[str]:
    if isinstance(raw, list):
        levels = [str(x).strip().lower() for x in raw]
    elif isinstance(raw, str) and raw.strip():
        levels = [s.strip().lower() for s in raw.split(",")]
    else:
        levels = ["campaign"]
    ok: list[str] = []
    for lv in levels:
        if lv in {"campaign", "adgroup", "keyword", "ad"} and lv not in ok:
            ok.append(lv)
    return ok or ["campaign"]


class _NaverSearchAdClient:
    def __init__(self, *, base_url: str, api_key: str, secret_key: str, customer_id: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.secret_key = secret_key
        self.customer_id = customer_id

    def _signature(self, timestamp_ms: str, method: str, uri: str) -> str:
        msg = f"{timestamp_ms}.{method}.{uri}"
        digest = hmac.new(
            self.secret_key.encode("utf-8", errors="strict"),
            msg.encode("utf-8", errors="strict"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(digest).decode("ascii", errors="strict")

    def _headers(self, method: str, uri: str) -> dict[str, str]:
        ts = str(int(time.time() * 1000))
        return {
            "Content-Type": "application/json; charset=UTF-8",
            "X-Timestamp": ts,
            "X-API-KEY": self.api_key,
            "X-Customer": str(self.customer_id),
            "X-Signature": self._signature(ts, method, uri),
        }

    async def request_json(
        self,
        *,
        method: str,
        uri: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | list[dict[str, Any]] | None = None,
        timeout: float = 30.0,
    ) -> Any:
        url = f"{self.base_url}{uri}"
        headers = self._headers(method, uri)
        async with httpx.AsyncClient() as client:
            r = await client.request(
                method,
                url,
                params=params,
                json=json_body,
                headers=headers,
                timeout=timeout,
            )
        if r.status_code // 100 != 2:
            body = (r.text or "").strip()
            body = body[:4000]
            raise RuntimeError(f"Naver API {method} {uri} failed: {r.status_code} {body}")
        if not r.content:
            return None
        try:
            return r.json()
        except Exception:
            return r.text

    async def download_report(self, download_url: str) -> bytes:
        # Per official samples, report download signatures use a fixed uri.
        headers = self._headers("GET", "/report-download")
        async with httpx.AsyncClient() as client:
            r = await client.get(download_url, headers=headers, timeout=120)
        if r.status_code // 100 != 2:
            body = (r.text or "").strip()
            body = body[:4000]
            raise RuntimeError(f"Naver report download failed: {r.status_code} {body}")
        return bytes(r.content)


async def _sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)


def _is_no_data_error(exc: RuntimeError) -> bool:
    msg = str(exc)
    return (
        "stat-reports" in msg
        and "400" in msg
        and ("10004" in msg or "지표가 확인되지 않습니다" in msg)
    )


async def _build_and_download_stat_report(
    client: _NaverSearchAdClient,
    *,
    report_tp: str,
    stat_dt: str,
    poll_interval_sec: float = 5.0,
    timeout_sec: float = 120.0,
) -> bytes | None:
    try:
        created = await client.request_json(
            method="POST",
            uri="/stat-reports",
            json_body={"reportTp": report_tp, "statDt": stat_dt},
            timeout=30,
        )
    except RuntimeError as e:
        if _is_no_data_error(e):
            return None
        raise
    if not isinstance(created, dict):
        raise RuntimeError("Unexpected /stat-reports response (not json object)")
    job_id = created.get("reportJobId")
    if job_id is None:
        raise RuntimeError(f"Unexpected /stat-reports response: {created}")

    started = time.time()
    last = created
    status = str(created.get("status") or "").upper()
    while status in {"REGIST", "RUNNING", "WAITING"}:
        if time.time() - started > timeout_sec:
            raise RuntimeError(f"Stat report timeout (job_id={job_id}, status={status})")
        await _sleep(poll_interval_sec)
        last = await client.request_json(method="GET", uri=f"/stat-reports/{job_id}", timeout=30)
        if not isinstance(last, dict):
            raise RuntimeError("Unexpected /stat-reports/{id} response (not json object)")
        status = str(last.get("status") or "").upper()

    if status == "BUILT":
        url = str(last.get("downloadUrl") or "").strip()
        if not url:
            raise RuntimeError(f"Missing downloadUrl on BUILT report: {last}")
        return await client.download_report(url)
    if status in {"NONE", "AGGREGATING"}:
        return None
    if status == "ERROR":
        raise RuntimeError(f"Stat report build failed: {last}")
    return None


def _parse_tsv(
    data: bytes,
    *,
    fieldnames: list[str] | None = None,
) -> list[dict[str, Any]]:
    text = _read_text_best_effort(data)
    lines = [ln for ln in text.splitlines() if ln.strip() != ""]
    if not lines:
        return []
    # Auto-detect: if the first line contains known column headers, let
    # DictReader use it; otherwise the file is header-less (v2 format) and
    # we must supply explicit fieldnames.
    first_cols = set(lines[0].split("\t"))
    has_header = bool(first_cols & _KNOWN_HEADER_NAMES)
    if has_header:
        reader = csv.DictReader(lines, delimiter="\t")
    else:
        reader = csv.DictReader(lines, fieldnames=fieldnames, delimiter="\t")
    return list(reader)


class NaverSearchAdConnector:
    """
    Naver SearchAd connector (API-key + HMAC signature).

    Planned responsibilities:
    - Generate STAT report (daily) and poll until ready, then download.
    - Normalize into metrics_daily + entities.
    - Apply safe actions: pause/resume, budget guardrails (scope depends on product).
    """

    capabilities = ConnectorCapabilities(
        read_metrics=True,
        read_entities=True,
        write_pause=True,
        write_budget=True,
        write_bid=True,
        write_negatives=True,
    )

    def __init__(self, ctx: ConnectorContext, repo):
        self.ctx = ctx
        self.repo = repo

    def _build_client(self) -> _NaverSearchAdClient:
        base_url = os.getenv("NAVER_SEARCHAD_BASE_URL", _DEFAULT_BASE_URL).strip() or _DEFAULT_BASE_URL
        api_key = (os.getenv("NAVER_SEARCHAD_API_KEY") or "").strip()
        secret_key = (os.getenv("NAVER_SEARCHAD_SECRET_KEY") or "").strip()
        customer_id = (os.getenv("NAVER_SEARCHAD_CUSTOMER_ID") or "").strip()
        return _NaverSearchAdClient(
            base_url=base_url, api_key=api_key, secret_key=secret_key, customer_id=customer_id
        )

    def _payload(self, proposal: dict) -> dict:
        raw = proposal.get("payload_json") or "{}"
        if isinstance(raw, dict):
            return raw
        try:
            return json.loads(raw)
        except Exception:
            return {}

    async def _resolve_parent_id(self, proposal: dict, payload: dict, entity_id: str) -> str:
        parent_id = str(payload.get("parent_id") or "").strip()
        if parent_id:
            return parent_id
        with self.repo.connect() as conn:
            row = conn.execute(
                "SELECT parent_id FROM entities WHERE platform='naver' AND entity_type='keyword' AND entity_id=?",
                (entity_id,),
            ).fetchone()
        if row and row[0]:
            return str(row[0]).strip()
        raise ValueError(
            f"Cannot resolve parent adgroup for keyword {entity_id!r}: not in payload or DB"
        )

    async def health_check(self) -> tuple[bool, str | None]:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode in {"import", "fixture"}:
            return True, None
        if mode != "api":
            return False, "bad mode"

        base_url = os.getenv("NAVER_SEARCHAD_BASE_URL", _DEFAULT_BASE_URL).strip() or _DEFAULT_BASE_URL
        api_key = os.getenv("NAVER_SEARCHAD_API_KEY") or ""
        secret_key = os.getenv("NAVER_SEARCHAD_SECRET_KEY") or ""
        customer_id = os.getenv("NAVER_SEARCHAD_CUSTOMER_ID") or ""
        if not base_url:
            return False, "Missing NAVER_SEARCHAD_BASE_URL"
        if not api_key.strip():
            return False, "Missing NAVER_SEARCHAD_API_KEY"
        if not secret_key.strip():
            return False, "Missing NAVER_SEARCHAD_SECRET_KEY"
        if not customer_id.strip():
            return False, "Missing NAVER_SEARCHAD_CUSTOMER_ID"
        return True, None

    async def sync_entities(self) -> None:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode == "import":
            return
        if mode == "fixture":
            d = fixture_dir(self.ctx.platform, self.ctx.config)
            for e in load_entities(d):
                self.repo.upsert_entity(
                    platform=e.get("platform") or self.ctx.platform,
                    account_id=e.get("account_id"),
                    entity_type=e.get("entity_type") or "",
                    entity_id=e.get("entity_id") or "",
                    parent_type=e.get("parent_type"),
                    parent_id=e.get("parent_id"),
                    name=e.get("name"),
                    status=e.get("status"),
                    meta_json=e.get("meta_json") or {},
                )
            return

        # API mode (best-effort: campaigns + adgroups)
        client = self._build_client()
        customer_id = (os.getenv("NAVER_SEARCHAD_CUSTOMER_ID") or "").strip()

        camps = await client.request_json(method="GET", uri="/ncc/campaigns", timeout=30)
        if isinstance(camps, list):
            for c in camps:
                if not isinstance(c, dict):
                    continue
                cid = str(c.get("nccCampaignId") or c.get("campaignId") or "").strip()
                if not cid:
                    continue
                self.repo.upsert_entity(
                    platform="naver",
                    account_id=customer_id,
                    entity_type="campaign",
                    entity_id=cid,
                    parent_type=None,
                    parent_id=None,
                    name=c.get("name"),
                    status=c.get("status"),
                    meta_json={"source": "naver_api"},
                )

        adgs = await client.request_json(method="GET", uri="/ncc/adgroups", timeout=30)
        if isinstance(adgs, list):
            for g in adgs:
                if not isinstance(g, dict):
                    continue
                gid = str(g.get("nccAdgroupId") or g.get("adgroupId") or "").strip()
                if not gid:
                    continue
                parent = str(g.get("nccCampaignId") or "").strip() or None
                self.repo.upsert_entity(
                    platform="naver",
                    account_id=customer_id,
                    entity_type="adgroup",
                    entity_id=gid,
                    parent_type="campaign" if parent else None,
                    parent_id=parent,
                    name=g.get("name"),
                    status=g.get("status"),
                    meta_json={"source": "naver_api"},
                )

    async def fetch_metrics_daily(self, date_from: str, date_to: str) -> None:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode == "import":
            return
        if mode == "fixture":
            d0 = date.fromisoformat(date_from)
            d1 = date.fromisoformat(date_to)
            d = fixture_dir(self.ctx.platform, self.ctx.config)
            for row in load_metrics_daily_rows(d):
                day = str(row.get("date") or "")
                if not day:
                    continue
                dd = date.fromisoformat(day)
                if dd < d0 or dd > d1:
                    continue
                self.repo.upsert_metric_daily(
                    platform=row.get("platform") or self.ctx.platform,
                    account_id=row.get("account_id"),
                    entity_type=row.get("entity_type") or "",
                    entity_id=row.get("entity_id") or "",
                    day=day,
                    spend=row.get("spend"),
                    impressions=row.get("impressions"),
                    clicks=row.get("clicks"),
                    conversions=row.get("conversions"),
                    conversion_value=row.get("conversion_value"),
                    metrics_json=row.get("metrics_json") or {},
                )
            return

        # API mode: Stat Report -> TSV -> rollup
        client = self._build_client()
        customer_id = (os.getenv("NAVER_SEARCHAD_CUSTOMER_ID") or "").strip()

        report_tp = str(self.ctx.config.get("report_tp") or "AD_DETAIL").strip().upper()
        levels = _safe_levels(self.ctx.config.get("ingest_levels"))
        poll_interval = float(self.ctx.config.get("report_poll_interval_sec", 5.0))
        timeout_sec = float(self.ctx.config.get("report_timeout_sec", 120.0))
        include_today = bool(self.ctx.config.get("include_today", False))

        # Avoid creating reports too frequently (worker may tick every 5 minutes).
        min_interval_min = float(self.ctx.config.get("api_min_interval_minutes", 180))
        key = f"naver:{self.ctx.connector_id}:last_fetch_daily"
        last = self.repo.get_meta(key)
        if last:
            try:
                last_dt = datetime.fromisoformat(last)
                now = datetime.now(tz=last_dt.tzinfo) if last_dt.tzinfo else datetime.now()
                if (now - last_dt).total_seconds() < (min_interval_min * 60):
                    return
            except Exception:
                pass

        days = _daterange_inclusive(date_from, date_to)
        if not include_today:
            tz = ZoneInfo(os.getenv("ADS_TIMEZONE", "Asia/Seoul"))
            today_kst = datetime.now(tz=tz).date().isoformat()
            days = [d for d in days if d != today_kst]

        for day_iso in days:
            stat_dt = _to_stat_dt(day_iso)
            blob = await _build_and_download_stat_report(
                client,
                report_tp=report_tp,
                stat_dt=stat_dt,
                poll_interval_sec=poll_interval,
                timeout_sec=timeout_sec,
            )
            if not blob:
                continue
            col_map = {"AD_DETAIL": _AD_DETAIL_COLUMNS}
            rows = _parse_tsv(blob, fieldnames=col_map.get(report_tp))
            if not rows:
                continue
            self._ingest_report_rows(
                rows,
                day_iso=_to_day_iso(stat_dt),
                customer_id=customer_id,
                report_tp=report_tp,
                levels=levels,
            )

        self.repo.set_meta(key, datetime.now().astimezone().replace(microsecond=0).isoformat())

    def _ingest_report_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        day_iso: str,
        customer_id: str,
        report_tp: str,
        levels: list[str],
    ) -> None:
        # Entity fields (header names vary by reportTp; keep heuristics broad)
        camp_id_keys = ["nccCampaignId", "Campaign ID", "캠페인ID", "캠페인 ID"]
        camp_name_keys = ["campaignName", "Campaign", "Campaign name", "캠페인", "캠페인명"]
        grp_id_keys = ["nccAdgroupId", "Adgroup ID", "Ad group ID", "광고그룹ID", "광고그룹 ID", "그룹ID"]
        grp_name_keys = ["adgroupName", "Adgroup", "Ad group", "광고그룹", "광고그룹명"]
        kw_id_keys = ["nccKeywordId", "Keyword ID", "키워드ID", "키워드 ID"]
        kw_name_keys = ["keyword", "Keyword", "키워드", "키워드명"]
        ad_id_keys = ["nccAdId", "Ad ID", "광고ID", "광고 ID"]
        ad_name_keys = ["adName", "Ad", "Ad name", "광고", "광고명"]

        # Metrics fields
        impr_keys = ["impCnt", "Impressions", "노출수", "노출 수"]
        click_keys = ["clkCnt", "Clicks", "클릭수", "클릭 수"]
        spend_keys = ["salesAmt", "cost", "Cost", "총비용", "총 비용", "비용", "광고비", "spend"]
        cpc_keys = ["cpc", "CPC", "평균CPC", "평균 CPC"]

        conv_all_keys = ["ccnt", "Conversions", "전환수", "전환 수", "전체전환수", "전체 전환수"]
        conv_purchase_keys = ["구매전환수", "구매 전환수", "구매수", "구매 수"]
        value_all_keys = ["drtConvValue", "Conv. value", "전환매출", "전환 매출", "전환가치", "전환 가치", "매출"]
        value_purchase_keys = ["구매전환매출", "구매 전환매출", "구매금액", "구매 금액", "구매매출", "구매 매출"]

        agg: dict[tuple[str, str], dict[str, float]] = {}

        def bump(
            entity_type: str,
            entity_id: str,
            *,
            spend: float,
            impr: float,
            clk: float,
            conv: float,
            val: float,
            conv_all: float | None,
            val_all: float | None,
            conv_purchase: float | None,
            val_purchase: float | None,
        ) -> None:
            k = (entity_type, entity_id)
            cur = agg.get(k)
            if not cur:
                cur = {
                    "spend": 0.0,
                    "impressions": 0.0,
                    "clicks": 0.0,
                    "conversions": 0.0,
                    "conversion_value": 0.0,
                    "conversions_all": 0.0,
                    "conversion_value_all": 0.0,
                    "conversions_purchase": 0.0,
                    "conversion_value_purchase": 0.0,
                }
                agg[k] = cur
            cur["spend"] += spend
            cur["impressions"] += impr
            cur["clicks"] += clk
            cur["conversions"] += conv
            cur["conversion_value"] += val
            if conv_all is not None:
                cur["conversions_all"] += float(conv_all or 0)
            if val_all is not None:
                cur["conversion_value_all"] += float(val_all or 0)
            if conv_purchase is not None:
                cur["conversions_purchase"] += float(conv_purchase or 0)
            if val_purchase is not None:
                cur["conversion_value_purchase"] += float(val_purchase or 0)

        for r in rows:
            camp_id = _first(r, camp_id_keys)
            camp_name = _first(r, camp_name_keys)
            grp_id = _first(r, grp_id_keys)
            grp_name = _first(r, grp_name_keys)
            kw_id = _first(r, kw_id_keys)
            kw_name = _first(r, kw_name_keys)
            ad_id = _first(r, ad_id_keys)
            ad_name = _first(r, ad_name_keys)

            impr = float(_parse_int(_first(r, impr_keys)) or 0)
            clk = float(_parse_int(_first(r, click_keys)) or 0)
            spend_v = _parse_float(_first(r, spend_keys))
            if spend_v is None:
                cpc = _parse_float(_first(r, cpc_keys))
                spend_v = (float(cpc or 0) * clk) if cpc is not None else 0.0
            spend = float(spend_v or 0)

            conv_all = _parse_float(_first(r, conv_all_keys))
            conv_purchase = _parse_float(_first(r, conv_purchase_keys))
            conv_primary = conv_purchase if conv_purchase is not None else conv_all
            conv = float(conv_primary or 0)

            val_all = _parse_float(_first(r, value_all_keys))
            val_purchase = _parse_float(_first(r, value_purchase_keys))
            val_primary = val_purchase if val_purchase is not None else val_all
            val = float(val_primary or 0)

            # Upsert entities discovered in report (names help debugging).
            if camp_id:
                self.repo.upsert_entity(
                    platform="naver",
                    account_id=customer_id,
                    entity_type="campaign",
                    entity_id=camp_id,
                    parent_type=None,
                    parent_id=None,
                    name=camp_name,
                    status=None,
                    meta_json={"source": "naver_report", "report_tp": report_tp},
                )
            if grp_id:
                self.repo.upsert_entity(
                    platform="naver",
                    account_id=customer_id,
                    entity_type="adgroup",
                    entity_id=grp_id,
                    parent_type="campaign" if camp_id else None,
                    parent_id=camp_id,
                    name=grp_name,
                    status=None,
                    meta_json={"source": "naver_report", "report_tp": report_tp},
                )
            if kw_id:
                self.repo.upsert_entity(
                    platform="naver",
                    account_id=customer_id,
                    entity_type="keyword",
                    entity_id=kw_id,
                    parent_type="adgroup" if grp_id else ("campaign" if camp_id else None),
                    parent_id=grp_id or camp_id,
                    name=kw_name,
                    status=None,
                    meta_json={"source": "naver_report", "report_tp": report_tp},
                )
            if ad_id:
                self.repo.upsert_entity(
                    platform="naver",
                    account_id=customer_id,
                    entity_type="ad",
                    entity_id=ad_id,
                    parent_type="adgroup" if grp_id else ("campaign" if camp_id else None),
                    parent_id=grp_id or camp_id,
                    name=ad_name,
                    status=None,
                    meta_json={"source": "naver_report", "report_tp": report_tp},
                )

            # Rollup
            if "campaign" in levels and camp_id:
                bump(
                    "campaign",
                    camp_id,
                    spend=spend,
                    impr=impr,
                    clk=clk,
                    conv=conv,
                    val=val,
                    conv_all=conv_all,
                    val_all=val_all,
                    conv_purchase=conv_purchase,
                    val_purchase=val_purchase,
                )
            if "adgroup" in levels and grp_id:
                bump(
                    "adgroup",
                    grp_id,
                    spend=spend,
                    impr=impr,
                    clk=clk,
                    conv=conv,
                    val=val,
                    conv_all=conv_all,
                    val_all=val_all,
                    conv_purchase=conv_purchase,
                    val_purchase=val_purchase,
                )
            if "keyword" in levels and kw_id:
                bump(
                    "keyword",
                    kw_id,
                    spend=spend,
                    impr=impr,
                    clk=clk,
                    conv=conv,
                    val=val,
                    conv_all=conv_all,
                    val_all=val_all,
                    conv_purchase=conv_purchase,
                    val_purchase=val_purchase,
                )
            if "ad" in levels and ad_id:
                bump(
                    "ad",
                    ad_id,
                    spend=spend,
                    impr=impr,
                    clk=clk,
                    conv=conv,
                    val=val,
                    conv_all=conv_all,
                    val_all=val_all,
                    conv_purchase=conv_purchase,
                    val_purchase=val_purchase,
                )

        for (entity_type, entity_id), s in agg.items():
            conv_all_sum = float(s.get("conversions_all", 0.0))
            conv_purchase_sum = float(s.get("conversions_purchase", 0.0))
            val_all_sum = float(s.get("conversion_value_all", 0.0))
            val_purchase_sum = float(s.get("conversion_value_purchase", 0.0))

            conv_primary_sum = conv_purchase_sum if conv_purchase_sum > 0 else conv_all_sum
            val_primary_sum = val_purchase_sum if val_purchase_sum > 0 else val_all_sum

            self.repo.upsert_metric_daily(
                platform="naver",
                account_id=customer_id,
                entity_type=entity_type,
                entity_id=entity_id,
                day=day_iso,
                spend=float(s.get("spend", 0.0)),
                impressions=int(s.get("impressions", 0.0)),
                clicks=int(s.get("clicks", 0.0)),
                conversions=float(conv_primary_sum),
                conversion_value=float(val_primary_sum),
                metrics_json={
                    "source": "naver_api",
                    "report_tp": report_tp,
                    "conversions_all": conv_all_sum,
                    "conversions_purchase": conv_purchase_sum,
                    "conversion_value_all": val_all_sum,
                    "conversion_value_purchase": val_purchase_sum,
                },
            )

    async def fetch_metrics_intraday(self, day: str) -> None:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode != "fixture":
            return
        d = fixture_dir(self.ctx.platform, self.ctx.config)
        for row in load_metrics_intraday_rows(d):
            hour_ts = str(row.get("hour_ts") or "")
            if not hour_ts.startswith(day):
                continue
            self.repo.upsert_metric_intraday(
                platform=row.get("platform") or self.ctx.platform,
                account_id=row.get("account_id"),
                entity_type=row.get("entity_type") or "",
                entity_id=row.get("entity_id") or "",
                hour_ts=hour_ts,
                spend=row.get("spend"),
                impressions=row.get("impressions"),
                clicks=row.get("clicks"),
                conversions=row.get("conversions"),
                conversion_value=row.get("conversion_value"),
                metrics_json=row.get("metrics_json") or {},
            )

    # ------------------------------------------------------------------ #
    # Write helpers                                                        #
    # ------------------------------------------------------------------ #

    async def _apply_pause(
        self, client: _NaverSearchAdClient, proposal: dict, payload: dict
    ) -> dict:
        entity_type = str(proposal.get("entity_type") or "").lower().strip()
        entity_id = str(proposal.get("entity_id") or "").strip()
        op_str = str(payload.get("op") or "pause").lower()
        user_lock = op_str == "pause"

        if entity_type == "campaign":
            before_data = await client.request_json(
                method="GET", uri=f"/ncc/campaigns/{entity_id}", timeout=30
            )
            before_data = before_data if isinstance(before_data, dict) else {}
            after_data = await client.request_json(
                method="PUT",
                uri=f"/ncc/campaigns/{entity_id}",
                params={"fields": "userLock"},
                json_body={"nccCampaignId": entity_id, "userLock": user_lock},
                timeout=30,
            )
        elif entity_type == "adgroup":
            before_data = await client.request_json(
                method="GET", uri=f"/ncc/adgroups/{entity_id}", timeout=30
            )
            before_data = before_data if isinstance(before_data, dict) else {}
            after_data = await client.request_json(
                method="PUT",
                uri=f"/ncc/adgroups/{entity_id}",
                params={"fields": "userLock"},
                json_body={"nccAdgroupId": entity_id, "userLock": user_lock},
                timeout=30,
            )
        elif entity_type == "keyword":
            parent_id = await self._resolve_parent_id(proposal, payload, entity_id)
            before_data = await client.request_json(
                method="GET", uri=f"/ncc/keywords/{entity_id}", timeout=30
            )
            before_data = before_data if isinstance(before_data, dict) else {}
            after_data = await client.request_json(
                method="PUT",
                uri=f"/ncc/keywords/{entity_id}",
                params={"fields": "userLock"},
                json_body={
                    "nccKeywordId": entity_id,
                    "nccAdgroupId": parent_id,
                    "userLock": user_lock,
                },
                timeout=30,
            )
        else:
            raise RuntimeError(f"Unsupported entity_type for pause_entity: {entity_type!r}")

        after_data = after_data if isinstance(after_data, dict) else {}
        return {
            "action": "pause_entity",
            "entity_type": entity_type,
            "entity_id": entity_id,
            "before": {
                "userLock": before_data.get("userLock"),
                "status": before_data.get("status"),
            },
            "after": {
                "userLock": after_data.get("userLock", user_lock),
                "status": after_data.get("status"),
            },
        }

    async def _apply_set_budget(
        self, client: _NaverSearchAdClient, proposal: dict, payload: dict
    ) -> dict:
        entity_id = str(proposal.get("entity_id") or "").strip()
        new_budget = int(payload.get("budget") or 0)

        before_data = await client.request_json(
            method="GET", uri=f"/ncc/campaigns/{entity_id}", timeout=30
        )
        before_data = before_data if isinstance(before_data, dict) else {}
        before_budget = before_data.get("dailyBudget")

        after_data = await client.request_json(
            method="PUT",
            uri=f"/ncc/campaigns/{entity_id}",
            params={"fields": "budget"},
            json_body={
                "nccCampaignId": entity_id,
                "dailyBudget": new_budget,
                "useDailyBudget": True,
            },
            timeout=30,
        )
        after_data = after_data if isinstance(after_data, dict) else {}
        return {
            "action": "set_budget",
            "entity_type": "campaign",
            "entity_id": entity_id,
            "before": {"dailyBudget": before_budget},
            "after": {"dailyBudget": after_data.get("dailyBudget", new_budget)},
        }

    async def _apply_set_bid(
        self, client: _NaverSearchAdClient, proposal: dict, payload: dict
    ) -> dict:
        entity_type = str(proposal.get("entity_type") or "").lower().strip()
        entity_id = str(proposal.get("entity_id") or "").strip()
        new_bid = int(payload.get("bid") or 0)

        if entity_type == "keyword":
            parent_id = await self._resolve_parent_id(proposal, payload, entity_id)
            before_data = await client.request_json(
                method="GET", uri=f"/ncc/keywords/{entity_id}", timeout=30
            )
            before_data = before_data if isinstance(before_data, dict) else {}
            after_data = await client.request_json(
                method="PUT",
                uri=f"/ncc/keywords/{entity_id}",
                params={"fields": "bidAmt"},
                json_body={
                    "nccKeywordId": entity_id,
                    "nccAdgroupId": parent_id,
                    "bidAmt": new_bid,
                    "useGroupBidAmt": False,
                },
                timeout=30,
            )
        elif entity_type == "adgroup":
            before_data = await client.request_json(
                method="GET", uri=f"/ncc/adgroups/{entity_id}", timeout=30
            )
            before_data = before_data if isinstance(before_data, dict) else {}
            after_data = await client.request_json(
                method="PUT",
                uri=f"/ncc/adgroups/{entity_id}",
                params={"fields": "bidAmt"},
                json_body={"nccAdgroupId": entity_id, "bidAmt": new_bid},
                timeout=30,
            )
        else:
            raise RuntimeError(f"Unsupported entity_type for set_bid: {entity_type!r}")

        after_data = after_data if isinstance(after_data, dict) else {}
        return {
            "action": "set_bid",
            "entity_type": entity_type,
            "entity_id": entity_id,
            "before": {"bidAmt": before_data.get("bidAmt")},
            "after": {"bidAmt": after_data.get("bidAmt", new_bid)},
        }

    async def _apply_add_negatives(
        self, client: _NaverSearchAdClient, proposal: dict, payload: dict
    ) -> dict:
        entity_id = str(proposal.get("entity_id") or "").strip()
        keywords = list(payload.get("keywords") or [])

        # Naver restricted-keywords are adgroup-scoped only.
        body = [
            {"keyword": str(kw.get("text") or "").strip()}
            for kw in keywords
            if str(kw.get("text") or "").strip()
        ]
        if not body:
            return {
                "action": "add_negatives",
                "entity_type": "adgroup",
                "entity_id": entity_id,
                "before": {"count": 0},
                "after": {"count": 0, "added": []},
            }

        after_data = await client.request_json(
            method="POST",
            uri=f"/ncc/adgroups/{entity_id}/restricted-keywords",
            json_body=body,
            timeout=30,
        )
        added = after_data if isinstance(after_data, list) else []
        return {
            "action": "add_negatives",
            "entity_type": "adgroup",
            "entity_id": entity_id,
            "before": {"count": 0},
            "after": {"count": len(added), "added": added},
        }

    async def apply_action(self, proposal: dict) -> dict:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode in {"import", "fixture"}:
            return {
                "simulated": True,
                "mode": mode,
                "platform": self.ctx.platform,
                "action_type": proposal.get("action_type"),
                "entity_type": proposal.get("entity_type"),
                "entity_id": proposal.get("entity_id"),
            }

        client = self._build_client()
        action_type = str(proposal.get("action_type") or "").strip()
        payload = self._payload(proposal)

        if action_type == "pause_entity":
            return await self._apply_pause(client, proposal, payload)
        elif action_type == "set_budget":
            return await self._apply_set_budget(client, proposal, payload)
        elif action_type == "set_bid":
            return await self._apply_set_bid(client, proposal, payload)
        elif action_type == "add_negatives":
            return await self._apply_add_negatives(client, proposal, payload)
        else:
            raise ValueError(f"Unsupported action_type for Naver: {action_type!r}")
