# Naver Commerce API (Smart Store) -- API Reference

## Overview

Naver Commerce API provides programmatic access to Smart Store seller operations
including order management, product management, and settlement data. This project
uses it exclusively for **order retrieval** (read-only).

- **Base URL**: `https://api.commerce.naver.com`
- **Auth method**: bcrypt-based electronic signature + OAuth 2.0 client_credentials
- **Connector file**: `src/commerce/connectors/smartstore.py`
- **Official docs**: https://github.com/commerce-api-naver/commerce-api

## Authentication

### 1. Generate electronic signature (client_secret_sign)

```
password  = "{client_id}_{timestamp_ms}"
hashed    = bcrypt(password.encode(), client_secret.encode())   # client_secret IS the bcrypt salt
signature = base64_urlsafe_encode(hashed)
```

The `client_secret` issued by Naver is already a valid bcrypt salt (starts with `$2a$` or `$2b$`).

### 2. Exchange for access token

```http
POST /external/v1/oauth2/token
Content-Type: application/x-www-form-urlencoded

client_id={client_id}
&timestamp={timestamp_ms}
&client_secret_sign={signature}
&grant_type=client_credentials
&type=SELF
```

**Response** (200 OK):
```json
{
  "access_token": "AAA...ZZZ",
  "expires_in": 10800,
  "token_type": "Bearer"
}
```

Token validity: ~3 hours. Use `Authorization: Bearer {access_token}` on all subsequent requests.

### IP Whitelisting

API call IPs must be registered in the Commerce API console. Unregistered IPs are
progressively restricted.

## Key Endpoints

### GET /v1/pay-order/seller/product-orders/last-changed-statuses

Poll for orders whose status changed within a time window (incremental sync).

**Query params**:
| Param | Type | Description |
|-------|------|-------------|
| `lastChangedFrom` | string | ISO-8601 with offset, e.g. `2026-02-15T00:00:00.000+09:00` |
| `lastChangedTo` | string | ISO-8601 with offset |

**Response** (200):
```json
{
  "data": {
    "lastChangeStatuses": [
      {
        "productOrderId": "2026021512345601",
        "orderId": "2026021512345600",
        "lastChangedType": "PAY_WAITING",
        "lastChangedDate": "2026-02-15T10:30:00.000+09:00"
      }
    ]
  }
}
```

### POST /v1/pay-order/seller/product-orders/query

Fetch full order details for a batch of product order IDs (max 300 per request).

**Request body**:
```json
{ "productOrderIds": ["2026021512345601", "2026021512345602"] }
```

**Response** (200):
```json
{
  "data": [
    {
      "productOrder": {
        "productOrderId": "2026021512345601",
        "orderId": "2026021512345600",
        "placeOrderDate": "2026-02-15T10:00:00.000+09:00",
        "productOrderStatus": "PAYED",
        "totalPaymentAmount": 29900,
        "orderPlaceId": "NAVER_SEARCH",
        "orderPlaceName": "naver search"
      }
    }
  ]
}
```

### GET /v1/pay-order/seller/product-orders/{productOrderId}

Retrieve a single product order by ID (used for spot-checks, not bulk sync).

## Field Mapping

| API field | DB column (`store_orders`) | Notes |
|-----------|---------------------------|-------|
| `productOrderId` | `order_id` | Primary key per order line |
| `orderDate` or `placeOrderDate` | `ordered_at` | ISO-8601 KST (fallback order) |
| `ordered_at[:10]` | `date_kst` | Extracted YYYY-MM-DD |
| `productOrderStatus` | `status` | PAYED, DELIVERING, DELIVERED, etc. |
| `totalPaymentAmount` | `amount` | Integer (KRW, no decimals) |
| `orderPlaceId` | `order_place_id` | Inflow channel code |
| `orderPlaceName` | `order_place_name` | Human-readable channel name |
| *(full object)* | `meta_json` | Raw JSON for audit |

### Date fallback policy (implemented)

SmartStore payloads are not always uniform across endpoints/statuses.
This connector sets `ordered_at` by the first available value in:

1. `orderDate`
2. `placeOrderDate`
3. `decisionDate`
4. `lastChangedDate`

Then `date_kst = ordered_at[:10]`.

## Rate Limits

- Token bucket algorithm per application, measured in 1-second windows
- HTTP 429 returned when exceeded (message: "Too many requests")
- Practical guidance: insert `asyncio.sleep(0.2)` between batch requests
- Exact per-endpoint limits are not publicly documented; monitor 429 responses

## Environment Variables

```bash
SMARTSTORE_CLIENT_ID=        # Commerce API application client ID
SMARTSTORE_CLIENT_SECRET=    # bcrypt salt issued by Naver (starts with $2a$ or $2b$)
```

Connector config `mode` must be set to `"api"` to enable live API calls.

## Sync Strategy

1. Poll `last-changed-statuses` with a sliding window (cursor stored in DB `meta` table)
2. Collect changed `productOrderId` values
3. Batch-query details via `/query` endpoint (300 IDs per request)
4. Upsert into `store_orders` table
5. Persist new cursor timestamp

Default lookback on first run: 30 days.
