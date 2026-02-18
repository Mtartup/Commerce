# Coupang Wing API -- API Reference

## Overview

Coupang Wing Open API provides seller access to order management, product
management, shipping, and returns on the Coupang marketplace. This project
uses it for **order retrieval** (read-only).

- **Base URL**: `https://api-gateway.coupang.com`
- **Auth method**: HMAC-SHA256 signature per request
- **Connector file**: `src/commerce/connectors/coupang.py`
- **Official docs**: https://developers.coupangcorp.com

## Authentication

### HMAC-SHA256 Signature

Every request is signed. No persistent token -- each request carries its own signature.

**Signature construction**:
```
datetime  = now_utc().strftime("%y%m%dT%H%M%SZ")     # e.g. "260216T093000Z"
message   = datetime + method + path + query_string   # concatenated, no separators
signature = hmac_sha256(secret_key, message).hexdigest()
```

- `method`: uppercase HTTP method (`GET`, `POST`, etc.)
- `path`: URI path without host, e.g. `/v2/providers/openapi/apis/api/v4/vendors/A00012345/ordersheets`
- `query_string`: sorted key=value pairs joined with `&`, no leading `?`

### Authorization Header

```
Authorization: CEA algorithm=HmacSHA256, access-key={access_key}, signed-date={datetime}, signature={signature}
```

### Additional Required Header

```
X-Requested-By: {vendor_id}
```

Signatures expire after **5 minutes** from the signed-date.

## Key Endpoints

### GET /v2/providers/openapi/apis/api/v4/vendors/{vendorId}/ordersheets

List purchase orders (POs) with date range and status filtering.

**Query params**:
| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `createdAtFrom` | string | yes | `YYYY-MM-DD+HH:mm` or `YYYY-MM-DDTHH:MM:SS` |
| `createdAtTo` | string | yes | Same format as above |
| `status` | string | yes | `ACCEPT`, `INSTRUCT`, `DEPARTURE`, `DELIVERING`, etc. |
| `maxPerPage` | int | no | Results per page (default varies) |
| `nextToken` | string | no | Pagination cursor from previous response |
| `searchType` | string | no | `timeFrame` for time-range queries |

**Response** (200):
```json
{
  "code": "SUCCESS",
  "message": "",
  "data": [
    {
      "shipmentBoxId": 642538970006401429,
      "orderId": 30000001234567,
      "orderedAt": "2026-02-15T10:30:00",
      "paidAt": "2026-02-15T10:31:00",
      "status": "ACCEPT",
      "shippingPrice": 0,
      "orderPrice": 25900,
      "orderer": {
        "name": "buyer_name",
        "email": "buyer@example.com",
        "safeNumber": "050-1234-5678"
      },
      "receiver": {
        "name": "receiver_name",
        "safeNumber": "050-8765-4321",
        "addr1": "Seoul ...",
        "addr2": "101-dong",
        "postCode": "06100"
      },
      "orderItems": [
        {
          "vendorItemId": 70001234567,
          "vendorItemName": "Product Name - Option",
          "shippingCount": 1,
          "salesPrice": 25900,
          "orderCount": 1
        }
      ]
    }
  ],
  "nextToken": "eyJ..."
}
```

### GET /v2/providers/openapi/apis/api/v4/vendors/{vendorId}/ordersheets/{shipmentBoxId}

Single PO query by shipmentBoxId. Same response structure as above (single item).

### GET /v2/providers/openapi/apis/api/v4/vendors/{vendorId}/ordersheets/{orderId}

Single PO query by orderId.

## Field Mapping

| API field | DB column (`store_orders`) | Notes |
|-----------|---------------------------|-------|
| `orderId` | `order_id` | Primary key |
| `orderedAt` | `ordered_at` | ISO-8601, KST implied |
| `orderedAt[:10]` | `date_kst` | Extracted YYYY-MM-DD |
| `status` | `status` | ACCEPT, INSTRUCT, DEPARTURE, DELIVERING, etc. |
| `orderPrice` | `amount` | Total order amount in KRW |
| *(full object)* | `meta_json` | Raw JSON for audit |

Note: `order_place_id` and `order_place_name` are not provided by Coupang's API
(all orders originate from coupang.com).

## Rate Limits

- **10 requests per second** per API key
- Practical guidance: `asyncio.sleep(0.15)` between paginated requests
- No rate-limit headers are returned; rely on HTTP 429 or 503 for backoff

## Environment Variables

```bash
COUPANG_ACCESS_KEY=     # Wing Open API access key
COUPANG_SECRET_KEY=     # Wing Open API secret key
COUPANG_VENDOR_ID=      # Seller vendor ID (e.g. A00012345)
```

Connector config `mode` must be set to `"api"` to enable live API calls.

## Sync Strategy

1. Query ordersheets with `createdAtFrom`/`createdAtTo` date range
2. Default status filter: `ACCEPT` (newly accepted orders)
3. Paginate with `nextToken` until exhausted
4. Upsert each order into `store_orders` table
5. Store last sync date in DB `meta` table

Default lookback on first run: 14 days.

Implementation note:
- Current connector intentionally pulls only `status=ACCEPT` for stable incremental ingestion.
- If you need full lifecycle reporting (cancellation/returns by status transition), extend the connector to query additional statuses and/or separate status-history tables.

## Wing Console Setup

1. Create a seller account at https://wing.coupang.com
2. Navigate to vendor settings and issue an Open API key
3. No separate approval process required -- key issuance is immediate
