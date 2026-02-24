# Test Coverage Analysis — Commerce

**Date**: 2026-02-24
**Overall line coverage**: **38%** (1602 / 4266 lines covered)
**Tests**: 35 collected, 32 passing, 3 failing

---

## Current Coverage by Module

### Well-Covered (>60%)

| Module | Coverage | Lines Missed | Notes |
|--------|----------|-------------|-------|
| `connectors/base.py` | 96% | 1 | Protocol definition, nearly complete |
| `util.py` | 78% | 4 | Small helper module |
| `importers/meta_export.py` | 76% | 29 | Campaign-level tested |
| `importers/cafe24_orders.py` | 75% | 23 | Basic order import tested |
| `importers/naver_searchad.py` | 75% | 30 | Campaign-level tested |
| `importers/google_export.py` | 74% | 32 | Campaign-level tested |
| `db.py` | 70% | 31 | Schema init + seeding tested |
| `importers/standard.py` | 58% | 31 | Daily import tested, intraday not |
| `config.py` | 56% | 14 | Tested indirectly via integration |

### Poorly-Covered (<40%)

| Module | Coverage | Lines Missed | Notes |
|--------|----------|-------------|-------|
| `web/app.py` | 53% | 355 | Routes partly tested, many pages untested |
| `repo.py` | 53% | 171 | Data layer used by tests but not directly tested |
| `connectors/demo.py` | 45% | 18 | No dedicated tests |
| `connectors/smartstore.py` | 41% | 104 | Fixture mode only |
| `connectors/google_ads.py` | 39% | 244 | Only `apply_action` tested |
| `registry.py` | 37% | 32 | No dedicated tests |
| `executor.py` | 26% | 32 | No dedicated tests |
| `fixtures.py` | 24% | 51 | No dedicated tests |
| `connectors/tiktok_ads.py` | 24% | 39 | Stub, no tests |
| `connectors/coupang.py` | 22% | 113 | No tests at all |
| `connectors/cafe24_analytics.py` | 15% | 193 | No tests at all |
| `notify/telegram_bot.py` | 13% | 123 | No tests at all |
| `worker.py` | 12% | 135 | No tests at all |
| `connectors/naver_searchad.py` | 11% | 403 | No tests at all |
| `connectors/meta_ads.py` | 11% | 245 | No tests at all |
| `cli.py` | 0% | 211 | No tests at all |

---

## Existing Test Inventory

| Test File | Tests | What It Covers |
|-----------|-------|----------------|
| `test_smoke.py` | 1 | Package imports without error |
| `test_import_naver.py` | 1 | Naver CSV import happy path (campaign level) |
| `test_import_meta_google.py` | 2 | Meta + Google CSV import happy path (campaign level) |
| `test_import_cafe24_orders.py` | 1 | Cafe24 order CSV import happy path |
| `test_import_standard_daily.py` | 1 | Standard daily CSV import happy path |
| `test_google_ads_apply_action.py` | 8 | Google Ads action execution (pause, budget, bid, negatives, error) |
| `test_smartstore_connector.py` | 3 | SmartStore fixture mode, date extraction, invalid row skipping |
| `test_web_connector_config.py` | 18 | Web UI routes, connector config, metrics display, sync |

### 3 Failing Tests

All in `test_web_connector_config.py` — the connector config POST tests (`test_update_connector_config_naver_fields`, `test_update_connector_config_meta_fields`, `test_update_connector_config_clears_optional_fields`) are failing, likely due to a web route change that the tests haven't been updated to reflect.

---

## Recommended Improvements — Prioritized

### Priority 1: Core Business Logic (highest risk if untested)

#### 1.1 `repo.py` — Data Access Layer (53% covered, 171 lines missing)
The repo is used by nearly everything but has no dedicated unit tests. The uncovered lines include proposal CRUD, execution logging, rule management, and connector queries.

**Proposed tests:**
- `upsert_metric_daily` / `upsert_metric_intraday` — idempotency, conflict resolution
- `get_proposal` / `set_proposal_result` — state transitions
- `create_execution` — audit log creation with before/after JSON
- `list_connectors` / `list_enabled_connectors` — filtering behavior
- `list_rules` / rule evaluation helpers
- Edge cases: empty results, missing fields, JSON roundtrip fidelity

#### 1.2 `executor.py` — Proposal Execution (26% covered, 32 lines missing)
The execution engine that applies changes to ad platforms has almost no test coverage. This is where real money is spent.

**Proposed tests:**
- `execute_proposal()` with manual approval requirement
- Audit logging (before_json / after_json written correctly)
- Error handling when connector fails mid-execution
- Simulated mode vs. live mode behavior

#### 1.3 `worker.py` — Scheduler (12% covered, 135 lines missing)
The automation engine that runs sync-propose-execute cycles is completely untested.

**Proposed tests:**
- Kill switch rule evaluation (should halt execution)
- `tick()` cycle: sync -> propose -> execute pipeline
- Health check failures preventing sync
- Auto-pause triggers
- Error recovery (connector errors don't crash the worker)

### Priority 2: Platform Connectors (highest breadth of untested code)

#### 2.1 `connectors/naver_searchad.py` (11% covered, 403 lines missing)
Largest untested module. Contains API signing, TSV report parsing, entity sync, and bid/budget operations.

**Proposed tests:**
- API signature generation (deterministic, can unit test)
- TSV report parsing (`_parse_tsv_report`)
- Keyword ID generation heuristics
- `sync_entities()` with mocked API responses
- `fetch_metrics_daily()` with mocked report data
- `apply_action()` for bid/budget changes

#### 2.2 `connectors/meta_ads.py` (11% covered, 245 lines missing)

**Proposed tests:**
- `sync_entities()` with mocked Graph API responses
- `fetch_metrics_daily()` — field mapping and aggregation
- `apply_action()` — pause campaign, set budget
- Error handling for expired tokens, rate limits

#### 2.3 `connectors/coupang.py` (22% covered, 113 lines missing)

**Proposed tests:**
- HMAC signature generation (deterministic, unit-testable)
- Order amount calculation logic
- Multi-vendor handling (1호점 + 2호점 via config_json)
- `sync_entities()` with mocked responses

#### 2.4 `connectors/cafe24_analytics.py` (15% covered, 193 lines missing)

**Proposed tests:**
- OAuth token refresh flow
- API pagination handling
- Rate limit compliance
- Metrics mapping from API response

### Priority 3: User-Facing Surfaces

#### 3.1 `cli.py` (0% covered, 211 lines missing)
The entire CLI is untested.

**Proposed tests:**
- Each command parses arguments correctly (`web`, `bot`, `worker`, `tick`, `backfill`, `import`)
- Help text renders without error
- Invalid arguments produce useful error messages
- Import command dispatches to correct importer

#### 3.2 `web/app.py` — Remaining Routes (53% covered, 355 lines missing)
Many web pages and API routes are untested beyond the connector config area.

**Proposed tests:**
- Proposal listing and detail pages
- Execution history pages
- Rule management CRUD routes
- Error pages (404, 500 handling)
- Metric calculation routes (ROAS, spend summaries)

#### 3.3 `notify/telegram_bot.py` (13% covered, 123 lines missing)

**Proposed tests:**
- Message formatting for proposals
- Callback handling for approval/rejection
- Error recovery on network failures
- Update polling with no new messages

### Priority 4: Supporting Modules

#### 4.1 `registry.py` (37% covered)
- Test connector instantiation for each platform
- Test `demo_mode` flag behavior
- Test unknown platform handling

#### 4.2 `fixtures.py` (24% covered)
- Test entity/metric CSV loading
- Test malformed fixture files
- Test missing fixture directory

#### 4.3 Fix the 3 failing tests
The `test_web_connector_config.py` config POST tests need to be updated to match the current route implementation.

### Priority 5: Cross-Cutting Concerns

#### 5.1 Importer Edge Cases (all importers ~75%)
Existing importer tests only cover the happy path for a single campaign-level row.

**Proposed additional tests:**
- Multi-level entity hierarchy (adgroup, keyword, ad)
- Character encoding fallback (`_read_text_best_effort` with cp949, euc-kr)
- Malformed CSV (missing columns, extra columns, empty rows)
- Duplicate date handling (upsert idempotency)
- Large file performance (optional)

#### 5.2 Async/Concurrency
- No tests verify that async connectors behave correctly under concurrent access
- No tests for rate limiting or retry logic
- No tests for timeout behavior

---

## Summary

| Category | Current | Target | Gap |
|----------|---------|--------|-----|
| **Overall line coverage** | 38% | 70%+ | ~1400 lines |
| **Modules with 0% coverage** | 1 (`cli.py`) | 0 | Need CLI tests |
| **Modules below 20%** | 5 | 0 | Need connector + worker tests |
| **Failing tests** | 3 | 0 | Fix web config tests |
| **Error path coverage** | ~10% | 50%+ | Most tests are happy-path only |

The biggest risk areas are **executor.py** (controls real ad spend), **worker.py** (autonomous execution), and the **platform connectors** (API interaction). Prioritize these for testing to reduce the chance of costly bugs in production.
