# Commerce Project Rules

## Project Identity

- **Name**: Commerce (formerly "Ads Ops")
- **Version**: 0.2.0 (renamed 2026-02-16)
- **Purpose**: Multi-channel sales + ads operating system for 1-person e-commerce operator
- **CLI**: `uv run commerce <command>`
- **Module**: `src/commerce/`

## Architecture

- **Stack**: Python 3.12+, FastAPI, SQLite (WAL), Telegram bot, Typer CLI
- **Pattern**: Pluggable connectors (Protocol-based, not inheritance)
- **State**: Local SQLite in `data/ads.sqlite3` (legacy name, works fine)
- **Safety**: Human-in-the-loop approval for all execution (default: manual mode)

## Connectors

### Ad Platforms (implemented)
| Platform | API | CSV Import | Status |
|----------|-----|------------|--------|
| Naver SearchAd | done | done | active |
| Meta Ads | done | done | active |
| Google Ads | done | done | active |
| TikTok Ads | stub | - | planned |

### Sales Channels
| Channel | API | CSV Import | Status |
|---------|-----|------------|--------|
| Cafe24 Orders | done | done | active (via cafe24_analytics connector) |
| Cafe24 Analytics | done | - | active |
| Smart Store | done | - | active |
| Coupang | done | - | active (1호점 + 2호점, multi-vendor via config_json) |

## Code Conventions

- Async-first (connectors use `async def`)
- All connectors implement `BaseConnector` protocol
- Imports: `from commerce.xxx import ...`
- Env vars use `ADS_*` prefix (legacy, kept for backward compat)
- Config loaded via `Settings.load()` from `.env`

## Safety Rules

- NEVER auto-execute proposals without explicit user opt-in
- ALWAYS write `before_json`/`after_json` to execution audit log
- Connector health check before any sync/execution
- Rate limit compliance per platform (Naver, Meta, Google all have different limits)

## File Structure

```
src/commerce/
  cli.py          # CLI entry point (typer)
  config.py       # Settings from .env
  db.py           # SQLite schema
  repo.py         # Data access layer
  executor.py     # Proposal execution + audit
  worker.py       # Scheduler (sync -> propose -> execute)
  registry.py     # Connector factory
  util.py         # Helpers
  connectors/     # Platform adapters (naver, meta, google, demo, tiktok, coupang)
  importers/      # CSV parsers (naver, meta, google, cafe24_orders, standard)
  notify/         # Telegram bot
  web/            # FastAPI app + templates
```

## Naver SearchAd API 직접 호출 패턴

외부 스크립트에서 API 직접 호출 시:

```python
# 서명: path의 ? 앞만 사용
sign_path = path.split('?')[0]
msg = f"{timestamp}.{method}.{sign_path}"

# 키워드 bid 수정: fields 파라미터 + nccAdgroupId 필수
PUT /ncc/keywords/{kw_id}?fields=bidAmt
body: {"nccKeywordId": kw_id, "nccAdgroupId": ag_id, "bidAmt": 200, "useGroupBidAmt": False}

# 키워드 추가: body는 배열
POST /ncc/keywords?nccAdgroupId={ag_id}
body: [{"nccAdgroupId": ag_id, "keyword": "생착스프레이", "bidAmt": 200, "useGroupBidAmt": False}]

# 캠페인 예산 수정
PUT /ncc/campaigns/{cmp_id}?fields=budget
body: {"nccCampaignId": cmp_id, "dailyBudget": 20000, "useDailyBudget": True}
```

- DB(`entities` 테이블)에 키워드 이름이 ID로만 저장됨 → API 직접 조회 필요
- Python Windows 출력 시 이모지 사용 금지 (cp949 에러) 또는 stdout UTF-8 래핑

## Key Commands

```powershell
uv run commerce web        # Web UI on :8010
uv run commerce bot        # Telegram bot
uv run commerce worker     # Continuous scheduler
uv run commerce tick       # Single sync cycle
uv run commerce backfill   # Historical data pull
uv run commerce import ... # CSV import
```
