# Design (Commerce)

> Renamed from "Ads Ops" to "Commerce" on 2026-02-16.

## Intent

Build a unified commerce operating system for a 1-person operator:

- **Ad platforms**: Naver SearchAd / Meta / Google / TikTok / Coupang
- **Sales channels**: Cafe24 (self-hosted mall) / Smart Store / Coupang
- **Analytics**: Cafe24 Analytics (visitor/keyword/ad attribution)
- Local-first reliability (SQLite is the source of truth)
- Safe-by-default execution (human approval required unless explicitly opted-in)
- ADHD-friendly UX: push notifications + minimal decision surface

Non-goals (for now):

- Building a full BI dashboard
- "Fully autonomous" budget/creative changes driven by LLM
- Perfect cross-device attribution (we'll support proxy metrics and optional tracking links)
- Product management, CS, or inventory (those stay in each platform's admin)

Local-only caveat:

- If Commerce runs on `localhost` only, real customer browsers cannot reach it.
- Therefore "custom conversion endpoints" and redirect tracking links are for self-testing unless you host Commerce on a reachable URL.
- MVP optimization should rely on platform-native conversion tracking (Meta/Google/Naver) + importing/pulling platform reports.

## Core Concepts

### Connector

A connector is a platform adapter. It owns:

- Authentication (later)
- Entity sync (accounts/campaign/adgroup/ad/keyword)
- Metrics ingestion (daily; intraday later)
- Execution of a limited set of safe actions (pause/resume/budget/bid/etc)

Connectors are pluggable, declared by `platform` and instantiated via `src/commerce/registry.py`.

### KPI Profile

Per objective, different metrics matter:

- Direct purchase: ROAS/CPA, conversions/value
- Blog-first funnel: CPC to blog, blog engagement, store clicks (proxy), downstream conversion signals if available
- Retargeting: CPM/CTR + assisted conversions

We model this as `kpi_profiles` plus a mapping table `entity_kpi_profiles`.

### Rule

Rules are deterministic guardrails and heuristics, not LLM:

- Kill switch: spend > X and conversions == 0 -> propose pause
- ROAS floor: ROAS < Y for N days -> propose budget down
- Creative fatigue: CTR drops + frequency rises -> propose creative refresh (suggest-only)

Each rule produces `action_proposals`.

### Action Proposal (Human-in-the-loop)

Proposals are the central decision artifact:

- `status`: proposed -> approved -> executed / failed, or rejected
- `risk`: low/med/high
- `requires_approval`: default true
- `payload_json`: connector-specific action payload

Telegram + web inbox operate on proposals.

## System Components

1. SQLite DB
   - Single file in `data/ads.sqlite3` (WAL mode)
2. Worker
   - Periodic scheduler:
     - sync metrics
     - evaluate rules
     - create proposals
     - optionally execute low-risk if configured (future)
3. Telegram bot
   - Runtime UI: alerts + approve/reject buttons
4. Mini web app (FastAPI)
   - Control plane:
     - enable/disable connectors
     - action inbox
     - enable/disable rules
     - execution logs

## Dashboard Data Contract

Dashboard values must be interpreted with a consistent basis day:

- `basis_day` selection:
  1. Latest ad metrics day across enabled ad platforms (`naver`, `meta`, `google`)
  2. Else latest `store_orders.date_kst`
  3. Else today KST
- Home totals (`orders`, `revenue`, `spend`, `ROAS`) are computed on `basis_day`.
- Because channel latency differs, UI should show the basis day explicitly and not imply "today" when stale.

ROAS semantics:

- `blended_roas = total_revenue / total_spend`
- `platform_roas = SUM(metrics_daily.conversion_value on ad platforms) / total_spend`
- `attributed_roas = SUM(conversion_events.value linked via tracking) / total_spend`

Revenue quality guards:

- Exclude cancellation-like statuses from store revenue aggregation.
- SmartStore order date fallback order:
  - `orderDate` -> `placeOrderDate` -> `decisionDate` -> `lastChangedDate`
  - Then derive `date_kst` from chosen `ordered_at`.

## Data Model (SQLite)

Tables (MVP):

- `connectors`: platform adapters + config JSON + enable flag
- `entities`: normalized entity metadata (hierarchy via parent_id, connector-scoped)
- `metrics_daily`: canonical daily metrics + `metrics_json` for raw platform fields (connector-scoped)
- `kpi_profiles`: objective definition + metric mapping config
- `entity_kpi_profiles`: attach KPI profile to a concrete entity (campaign/adgroup/etc)
- `rules`: rule definitions + params
- `action_proposals`: decision artifact + Telegram linkage
- `executions`: execution audit trail

Note: `metrics_daily` keeps canonical columns for common math and stores platform-specific fields in `metrics_json`.
Legacy compatibility note:
- Connector-scoped tables (`entities`, `metrics_daily`, `metrics_intraday`) use `connector_id`.
- Dashboard read paths may fallback to unscoped legacy rows (`connector_id=''`) to preserve old import data visibility.

## Execution Safety Model

Default: `ADS_EXECUTION_MODE=manual`

Planned modes:

- `manual`:
  - Worker only creates proposals.
  - Execution happens only when you explicitly click "Execute" (web) or "/execute" (telegram).
- `auto_low_risk`:
  - Only proposals with `risk=low` AND `requires_approval=0` can be executed automatically.

Additionally:

- Every execution writes `executions.before_json` + `executions.after_json` when possible.
- Connectors must support a "dry-run" preview for risky actions where feasible.

## "No API Yet" Development Strategy

To build the full system without live API calls:

1. Fixture connector mode
   - Each connector supports a `fixture` mode that reads from `fixtures/{platform}/...json`
   - Enables end-to-end testing of:
     - parsing
     - normalization
     - rule outputs
     - proposal/inbox/approval UX
2. Import connectors
   - CLI import of exported CSV/Excel reports into `metrics_daily`
   - Lets you validate KPI profiles with real historical exports before enabling API auth

This avoids refactors when real auth is introduced.

## Platform Notes (Design Constraints)

### Naver SearchAd

- Core ingestion is report-driven (generate -> poll -> download).
- Some products/placements behave differently; treat "connector capabilities" as per-product.
- Strong ROI: keyword/adgroup level guardrails and negative keyword suggestions.

### Meta

- Prefer daily ingestion into SQLite to decouple from any retention/breakdown constraints.
- Start with "pause + budget" actions only. Avoid automating campaign creation early.
- API mode: Graph API Insights daily at campaign/adset/ad (read-only ingestion)

### Google Ads

- Keep the connector isolated; Google Ads API versions change regularly.
- Read-only ingestion is implemented (GAQL -> `metrics_daily` at campaign/adgroup/keyword).
- Only then add safe mutations (pause/budget/bids/negatives).

### TikTok

- Keep as attachable connector; implement when you decide to run spend there.

### Coupang

- Treat commerce API and ads reporting separately.
- Ads API availability is uncertain; plan for import/RPA plugin if needed.
- Multi-vendor supported: each vendor account is a separate connector row; credentials stored in `config_json` (`.env` fallback for primary account).

## Implementation Order (Recommended)

1. Finish control-plane UX (web + telegram) for:
   - connector config + enable/disable
   - rule config + enable/disable
   - proposal execution UI + logs
2. Add fixture/import modes
3. Implement Naver connector (read-only ingestion first)
4. Implement Meta connector
5. Implement Google connector
