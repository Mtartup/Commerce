# Roadmap (Commerce)

> Renamed from "Ads Ops" to "Commerce" on 2026-02-16.

## Phase 0: Skeleton (done)

- SQLite schema: connectors/entities/metrics/proposals/executions
- Telegram bot (polling): approve/reject callbacks
- Mini web app: connectors + action inbox
- Worker: periodic tick + demo rule
- Pluggable connector registry (naver/meta/google/tiktok/coupang stubs)

## Phase 1: Naver (highest ROI first)

1. Read-only ingestion
   - STAT report generation + polling + download
   - Normalize to `metrics_daily` at:
     - account
     - campaign
     - adgroup
     - keyword (optional, heavy)
2. Guardrails (rule-based, safe)
   - Spend kill switch (no conversions)
   - ROAS floor -> budget down (suggest only, approval required)
   - Keyword-level negative suggestions (suggest only)

## Phase 2: Meta (start with safety + insight)

1. Read-only ingestion
   - CSV import (done): campaigns/adsets/ads -> `metrics_daily`
   - API (done): Graph API Insights daily at campaign/adset/ad
2. Guardrails
   - Spend kill switch (LPV/Clicks without downstream signal)
3. Creative analyzer (LLM later)
   - "Top vs bottom creatives" diff report -> proposals for next tests

## Phase 3: Google

1. Read-only ingestion
   - CSV import (done): campaign/adgroup/keyword -> `metrics_daily`
   - API (done): GAQL -> `metrics_daily` (campaign/adgroup/keyword)
2. Guardrails (pause/budget)
3. Extend to negative keywords + bid adjustments

## Phase 4: Cross-channel measurement (blog -> store -> purchase)

- Tracking link service (redirect + click log)
- KPI profiles per objective:
  - direct_purchase (ROAS/CPA)
  - blog_traffic (CPC, landing views, store clicks)
  - retargeting

## Phase 5: Sales Channel Integration

### Cafe24 Analytics (제휴사 승인 필요)
- Connector: `cafe24_analytics.py`
- Endpoints: visitors, pageview, keyword sales, ad attribution, product sales
- Token Bucket rate limit (40 tokens, 2/sec refill)
- OAuth 2.0 (scope: `mall.analytics`)

### Smart Store (Naver Commerce API)
- Connector: `smartstore.py`
- Orders/sales ingestion (read-only)
- Cross-reference with Naver SearchAd spend for true ROAS

### Coupang (Wing API)
- Commerce Open API ingestion (orders/sales/refunds)
- Ads: only if official API is available; otherwise keep export/RPA as optional plugin

## Phase 6: Unified Dashboard

- Cross-channel daily summary: total sales by channel + total ad spend by platform
- True ROAS calculation: ad spend (platform A) → conversion (channel B)
- Telegram daily digest push

## Current Snapshot (2026-02-17)

- Connector-scoped schema migration completed (`schema_version=5`)
- Sales channel ingestion:
  - Cafe24 orders API + CSV import: working
  - SmartStore API: working (date fallback normalized)
  - Coupang API: working (current sync status filter: `ACCEPT`)
  - Cafe24 Analytics API: working
- Dashboard:
  - Basis-day alignment implemented for ad/store lag handling
  - Blended/platform/attributed ROAS split implemented
  - Freshness-aware connector health warnings implemented
