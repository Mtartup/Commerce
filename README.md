# Commerce (Multi-Channel Sales + Ads Operating System)

> Renamed from "Ads Ops" (v0.1) to "Commerce" (v0.2) on 2026-02-16.
> Scope expanded: ads management + sales channel tracking across all platforms.

Goal: a unified commerce dashboard for a 1-person operator:

- **Ad Connectors**: Naver SearchAd / Meta / Google / TikTok / Coupang
- **Sales Channels**: Cafe24 (self-hosted mall) / Smart Store / Coupang
- **Analytics**: Cafe24 Analytics (visitor behavior, keyword/ad attribution)
- **Data ingestion**: raw + normalized into SQLite
- **Guardrails**: rule-based kill-switch (deterministic, not LLM)
- **Proposals**: human approval via Telegram / web inbox
- **Execution + audit log**

Architecture:

- Telegram bot: runtime notifications + approve/reject buttons
- Mini web app: dashboard, connectors health, pending action inbox, rules/profiles, execution logs
- Local SQLite: single-file state + history (first-class for reliability)

## Dashboard Semantics (Important)

To prevent misleading numbers when ad channels update slower than order channels:

- **Dashboard basis day (`day`) is not always today.**
  - Prefer latest available ad metrics day among enabled ad platforms (`naver`, `meta`, `google`).
  - If no ad metrics exist yet, fallback to latest store order day.
  - If neither exists, fallback to today KST.
- **ROAS definitions**
  - `blended_roas = total_revenue / total_spend` (store revenue vs ad spend on the same basis day)
  - `platform_roas = platform_conversion_value / total_spend`
  - `attributed_roas = tracking-linked conversion_value / total_spend`
- **Store revenue excludes cancellation-like statuses**
  - `cafe24`: `취소`, `반품`, `환불`
  - `smartstore` / `coupang`: `CANCEL`, `RETURN`, `REFUND`
- **Connector health `ok/warn/err/off` includes freshness**
  - `warn` can appear even with `last_sync_at` if latest data day is stale vs today.

See `docs/dashboard-data-contract.md` for exact rules.

## Quick Start

```powershell
cd C:\Coding\MCP\Commerce
uv sync
```

Create `.env` from `.env.example` and fill at least Telegram settings:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_ALLOWED_CHAT_ID` (optional; auto-locks to first chat)

Initialize DB and seed default connectors:

```powershell
uv run commerce db init
uv run commerce db seed
```

Run:

```powershell
# Web UI: http://127.0.0.1:8010
uv run commerce web

# Telegram bot (polling)
uv run commerce bot

# Worker (schedules: sync -> propose -> execute if allowed)
uv run commerce worker
```

## Connector Setup

Each connector supports two modes:
- `mode=import` (default): load CSV exports manually
- `mode=api` (live): call platform API automatically

Toggle connectors on/off at `http://127.0.0.1:8010/connectors`.

### Ad Platforms

#### Naver SearchAd

```powershell
# .env
NAVER_SEARCHAD_API_KEY=...
NAVER_SEARCHAD_SECRET_KEY=...
NAVER_SEARCHAD_CUSTOMER_ID=...
```

#### Meta Ads

```powershell
# .env
META_ACCESS_TOKEN=...
META_AD_ACCOUNT_ID=...
META_GRAPH_API_VERSION=v21.0
```

#### Google Ads

```powershell
# .env
GOOGLE_ADS_DEVELOPER_TOKEN=...
GOOGLE_ADS_CUSTOMER_ID=...
GOOGLE_ADS_CLIENT_ID=...
GOOGLE_ADS_CLIENT_SECRET=...
GOOGLE_ADS_REFRESH_TOKEN=...
```

### Sales Channels

| Channel | API | Status |
|---------|-----|--------|
| Cafe24 (self-hosted mall) | REST Admin API (orders) | API + CSV import done |
| Cafe24 Analytics | Analytics API (visitors, keywords, ad attribution) | API done |
| Smart Store | Naver Commerce API (orders) | API done |
| Coupang | Wing API (orders) | API done |

### Why All Channels?

```
Naver ad spend 10만원
  ├→ Smart Store order 3건    ← Smart Store API 연동
  └→ Self-hosted mall order 1건  ← Cafe24 Orders (done)

Meta ad spend 5만원
  └→ Self-hosted mall order 2건  ← Cafe24 Orders (done)

Coupang ad spend 3만원
  └→ Coupang order 4건        ← Coupang API 연동
```

Ad spend happens on one platform, but conversions happen on another.
Without all sales channel data, ROAS calculation is impossible.

## Import Mode (No API)

```powershell
# Standard daily CSV
uv run commerce import daily --file .\imports\commerce_daily.csv

# Naver SearchAd CSV
uv run commerce import naver --file .\imports\naver_campaign.csv --product-type powerlink --level campaign

# Meta Ads CSV
uv run commerce import meta --file .\imports\meta_campaign.csv --level campaign

# Google Ads CSV
uv run commerce import google --file .\imports\google_campaign.csv --level campaign

# Cafe24 Orders CSV
uv run commerce import cafe24-orders --file .\imports\cafe24_orders.csv
```

## Backfill (Historical)

```powershell
uv run commerce backfill --platform naver
uv run commerce backfill --platform meta
uv run commerce backfill --platform google

# If multiple connectors exist for the same platform:
uv run commerce backfill --platform naver --connector-name "Naver SearchAd"
```

## Demo Mode

```powershell
$env:ADS_DEMO_MODE='1'
uv run commerce tick
```

## Project Setup History

- **v0.1 (2026-02 initial)**: "Ads Ops" - ad platform management only
  - Naver SearchAd, Meta, Google connectors (API + CSV import)
  - Telegram bot + web UI + SQLite
  - Rule-based guardrails (kill-switch, ROAS floor)
  - Cafe24 orders CSV import (conversion signal)
- **v0.2 (2026-02-16)**: Renamed to "Commerce" - expanded scope
  - Added sales channel tracking concept (Smart Store, Coupang, Cafe24 Analytics)
  - Module renamed: `src/ads/` → `src/commerce/`
  - CLI renamed: `uv run ads` → `uv run commerce`
  - Env vars kept as `ADS_*` for backward compatibility

## Design Notes

- LLM is not required for the MVP. Can be plugged later as a "proposal generator".
- Execution is safe-by-default: `ADS_EXECUTION_MODE=manual`.
- Each connector declares `capabilities`, so adding platforms is config, not rewrite.
- Env vars use `ADS_*` prefix (legacy from v0.1; works fine, no need to rename).

See `DESIGN.md` for architecture.
See `ROADMAP.md` for implementation phases.
See `docs/dashboard-data-contract.md` for dashboard calculation and freshness policy.
