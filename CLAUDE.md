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

### Sales Channels (planned)
| Channel | API | CSV Import | Status |
|---------|-----|------------|--------|
| Cafe24 Orders | planned | done | active (import only) |
| Cafe24 Analytics | planned | - | needs partner approval |
| Smart Store | planned | - | planned |
| Coupang | stub | - | planned |

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

## Key Commands

```powershell
uv run commerce web        # Web UI on :8010
uv run commerce bot        # Telegram bot
uv run commerce worker     # Continuous scheduler
uv run commerce tick       # Single sync cycle
uv run commerce backfill   # Historical data pull
uv run commerce import ... # CSV import
```
