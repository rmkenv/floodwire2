# API Rate Limits & Quotas

## TheNewsAPI

| Plan | Articles/day | Requests/sec |
|------|-------------|--------------|
| Free dev | Unlimited | ~1/sec (be polite) |
| Paid | 100+/page | higher |

The free tier returns 3 articles per page.  
Paid tiers return up to 100 per page (set `limit=100` in `extract_articles.py`).

## OSM Nominatim

**Policy**: Max 1 request/second. Bulk usage requires self-hosting.

- `rate_limit_sec: 1.0` in config enforces this
- `user_agent` must identify your application (required by OSM policy)
- Self-hosting guide: https://nominatim.org/release-docs/latest/admin/Installation/

## PostGIS

Self-hosted — no external limits.

## Cost Summary

| Articles/day | Geocode calls | OSM time | Cost/month |
|-------------|--------------|----------|-----------|
| 100 | ~300 | ~5 min | $0 |
| 500 | ~1500 | ~25 min | $0 |
| 2000 | ~6000 | ~1.7 hr | $0 (but consider self-hosting Nominatim) |
