# Cafe24 Analytics API -- API Reference

## Overview

Cafe24 Analytics API (CA API) provides access to customer behavior data for
Cafe24-hosted online stores: visitor counts, pageviews, product sales, and
referral domain tracking. This project uses it for **metrics retrieval** (read-only).

- **Base URL**: `https://ca-api.cafe24data.com`
- **Auth method**: OAuth 2.0 (authorization_code grant)
- **Connector file**: `src/commerce/connectors/cafe24_analytics.py`
- **Official docs**: https://developers.cafe24.com/docs/en/api/cafe24data/
- **Requires**: Partner approval (제휴사 승인) before API access is granted

## Authentication

### OAuth 2.0 Flow

**Step 1 -- Authorization code** (browser redirect):
```
GET https://{mall_id}.cafe24api.com/api/v2/oauth/authorize
  ?response_type=code
  &client_id={client_id}
  &state={csrf_token}
  &redirect_uri={redirect_uri}
  &scope=mall.analytics
```

**Step 2 -- Exchange code for tokens**:
```http
POST https://{mall_id}.cafe24api.com/api/v2/oauth/token
Content-Type: application/x-www-form-urlencoded
Authorization: Basic base64({client_id}:{client_secret})

grant_type=authorization_code&code={auth_code}&redirect_uri={redirect_uri}
```

**Response** (200):
```json
{
  "access_token": "0iqR5nM5EJIq...",
  "expires_at": "2026-02-16T14:00:00.000",
  "refresh_token": "JeTJ7XpnFC0P...",
  "refresh_token_expires_at": "2026-03-02T12:00:00.000",
  "client_id": "BrIfqEKoPxeE...",
  "mall_id": "yourmall",
  "scopes": ["mall.analytics"],
  "issued_at": "2026-02-16T12:00:00.000"
}
```

### Token Lifetimes

| Token | Validity | Renewal |
|-------|----------|---------|
| `access_token` | **2 hours** | Refresh with refresh_token |
| `refresh_token` | **2 weeks** | Re-authorize via browser |

**Step 3 -- Refresh**:
```http
POST https://{mall_id}.cafe24api.com/api/v2/oauth/token
Authorization: Basic base64({client_id}:{client_secret})

grant_type=refresh_token&refresh_token={refresh_token}
```

All CA API requests use `Authorization: Bearer {access_token}`.

## Key Endpoints

### GET /api/v1/malls/{mall_id}/visitors/view

Visitor (unique visit) counts by date range.

**Query params**: `startDate`, `endDate`, `dateType` (`day` | `week` | `month`)

**Response**:
```json
{
  "data": [
    { "date": "2026-02-15", "visitCount": 1432 }
  ]
}
```

### GET /api/v1/malls/{mall_id}/visitors/pageview

Pageview counts by date range.

**Query params**: same as visitors/view

**Response**:
```json
{
  "data": [
    { "date": "2026-02-15", "pageviewCount": 5891 }
  ]
}
```

### GET /api/v1/malls/{mall_id}/products/sales

Product-level sales data (order count, sales amount).

**Query params**: `startDate`, `endDate`, `dateType`

**Response**:
```json
{
  "data": [
    {
      "date": "2026-02-15",
      "productNo": 1234,
      "productName": "Sample Product",
      "orderCount": 12,
      "salesAmount": 358800
    }
  ]
}
```

### GET /api/v1/malls/{mall_id}/visitpaths/domains

Visitor counts by referring domain.

**Query params**: `startDate`, `endDate`, `dateType`

**Response**:
```json
{
  "data": [
    { "date": "2026-02-15", "domain": "search.naver.com", "visitCount": 820 },
    { "date": "2026-02-15", "domain": "m.facebook.com", "visitCount": 215 }
  ]
}
```

## Field Mapping

| Endpoint | API field | DB column (`metrics_daily`) | entity_type |
|----------|-----------|---------------------------|-------------|
| visitors/view | `visitCount` | `impressions` | `store` |
| visitors/pageview | `pageviewCount` | `clicks` | `store` |
| products/sales | `orderCount` | `conversions` | `product` |
| products/sales | `salesAmount` | `conversion_value` | `product` |
| visitpaths/domains | `visitCount` | `clicks` | `domain` |
| visitpaths/domains | `domain` | `entity_id` | `domain` |

Note: `spend` is always NULL for analytics data (no ad spend concept).

## Rate Limits

Token bucket algorithm:

| Parameter | Value |
|-----------|-------|
| Bucket capacity | **40 tokens** |
| Refill rate | **2 tokens/second** (1 token every 500ms) |
| Cost per request | 1 token |
| Burst limit | 40 requests (empty bucket refills in 20 seconds) |

**Response headers**:
| Header | Description |
|--------|-------------|
| `X-RateLimit-Remaining` | Tokens left in bucket |
| `X-RateLimit-Requested-Tokens` | Tokens consumed by this request |
| `X-RateLimit-Burst-Capacity` | Max bucket size (40) |
| `X-RateLimit-Replenish-Rate` | Tokens added per second (2) |

When `X-RateLimit-Remaining < 5`, the connector sleeps for 2 seconds.
HTTP 429 is returned when the bucket is empty.

## Environment Variables

```bash
CAFE24_ANALYTICS_CLIENT_ID=       # OAuth app client ID
CAFE24_ANALYTICS_CLIENT_SECRET=   # OAuth app client secret
CAFE24_ANALYTICS_MALL_ID=         # Store mall ID (e.g. "yourmall")
```

Connector config `mode` must be set to `"api"` to enable live API calls.

## Constraints and Prerequisites

1. **Partner approval required** (제휴사 승인): Submit application at
   https://developers.cafe24.com/data/front/cafe24dataapi/create and await
   Cafe24's review. Without approval, all API calls return 403.

2. **OAuth scope**: Only `mall.analytics` is needed. The authorization flow
   must be completed once via browser; tokens are then stored in the connector's
   `config_json.oauth_tokens` in the DB.

3. **Data usage restriction**: Data obtained through the CA API may only be
   used for the approved channels, media platforms, or purposes declared in
   the partner application. Other uses are prohibited.

4. **API version header**: Requests include `X-Cafe24-Api-Version: 2024-06-01`
   to pin the response schema.
