# Dashboard Data Contract

This document defines how the home dashboard computes numbers so data-latency between channels does not create misleading "today" views.

## Basis Day

`basis_day` is selected in this order:

1. Latest ad metrics day across enabled ad platforms (`naver`, `meta`, `google`)
2. If no ad metrics exist: latest `store_orders.date_kst`
3. If neither exists: today KST

Home totals and ROAS are calculated on `basis_day`.
The UI must show this day explicitly.

## ROAS Definitions

- `blended_roas = total_revenue / total_spend`
- `platform_roas = sum(ad_platform conversion_value) / total_spend`
- `attributed_roas = sum(tracking-linked conversion_events.value) / total_spend`

Where:

- `total_spend` comes from ad-platform metrics (`naver`, `meta`, `google`) on `basis_day`.
- `total_revenue` comes from `store_orders` on `basis_day`, after status filtering.

## Store Revenue Filters

To avoid counting canceled/returned orders:

- `cafe24`: exclude statuses containing `취소`, `반품`, `환불`
- `smartstore`, `coupang`: exclude statuses containing `CANCEL`, `RETURN`, `REFUND` (case-insensitive)

## Connector Health Semantics

`off`: connector disabled  
`err`: connector has `last_error`  
`warn`: connector enabled but data freshness is stale vs today KST  
`ok`: enabled, no error, and freshness is acceptable

Notes:

- Freshness and sync timestamp are separate. A connector may have recent `last_sync_at` but still be `warn` if no fresh rows were ingested.
- Naver no-data response (`/stat-reports` 400 code `10004`) is treated as "no data for that date", not a hard connector error.

## Compatibility Rule (Schema v5)

Metrics/entities are connector-scoped (`connector_id` in primary keys), but dashboard queries may fallback to legacy unscoped rows (`connector_id=''`) to keep old import data visible.

## SmartStore Date Normalization

`ordered_at` fallback order:

1. `orderDate`
2. `placeOrderDate`
3. `decisionDate`
4. `lastChangedDate`

Then `date_kst = ordered_at[:10]`.

