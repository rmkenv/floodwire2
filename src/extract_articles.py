"""
extract_articles.py — Fetch flood-related US news articles from TheNewsAPI.com.

Free tier: unlimited dev requests, 3 results per page in dev, 100 in paid.
We page through results to stay within max_articles cap.

Returned article dict schema
----------------------------
{
    "article_id": str,           # TheNewsAPI uuid
    "title":      str,
    "description":str,
    "snippet":    str,           # first ~200 chars of body
    "url":        str,
    "source":     str,           # e.g. "washingtonpost.com"
    "outlet_city":str | None,    # best-effort city from source domain/locale
    "outlet_region": str | None, # state / region
    "published_at": str,         # ISO-8601
    "language":   str,
    "categories": list[str],
}
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Generator

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from .utils import get_logger, load_config

logger = get_logger(__name__)

_BASE_URL = "https://api.thenewsapi.com/v1/news/all"

# Search queries that together capture flash + sunny-day flood events
_FLOOD_QUERIES = [
    "flash flood",
    "flash flooding",
    "sunny day flooding",
    "high tide flooding",
    "king tide flood",
    "tidal flooding",
    "nuisance flooding",
    "flood warning",
    "flood damage",
    "flooded street",
    "flooded road",
    "flooded neighborhood",
    "creek overflow",
    "water rescue flood",
]

# TheNewsAPI locale codes for United States
_US_LOCALE = "us"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_articles(
    cfg: dict[str, Any],
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    max_articles: int | None = None,
) -> list[dict[str, Any]]:
    """Fetch flood articles for the given date range.

    Args:
        cfg:          Loaded config dict.
        start_date:   UTC-aware datetime (default: 24h ago).
        end_date:     UTC-aware datetime (default: now).
        max_articles: Override etl.max_articles from config.

    Returns:
        Deduplicated list of article dicts.
    """
    token: str = cfg["api"]["thenewsapi_token"]
    limit: int = max_articles or cfg.get("etl", {}).get("max_articles", 500)

    now = datetime.now(timezone.utc)
    lookback = cfg.get("etl", {}).get("lookback_days", 1)
    start = start_date or (now - timedelta(days=lookback))
    end = end_date or now

    published_after = start.strftime("%Y-%m-%dT%H:%M:%S")
    published_before = end.strftime("%Y-%m-%dT%H:%M:%S")

    seen_ids: set[str] = set()
    articles: list[dict[str, Any]] = []

    for query in _FLOOD_QUERIES:
        if len(articles) >= limit:
            break
        logger.info("Querying TheNewsAPI: %r (%s → %s)", query, published_after[:10], published_before[:10])
        batch = _fetch_query(
            token=token,
            query=query,
            published_after=published_after,
            published_before=published_before,
            max_count=limit - len(articles),
        )
        added = 0
        for art in batch:
            aid = art["article_id"]
            if aid not in seen_ids:
                seen_ids.add(aid)
                articles.append(art)
                added += 1
        logger.info("  → %d new articles (total so far: %d)", added, len(articles))

    logger.info("Fetch complete. Total unique articles: %d", len(articles))
    return articles


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

@retry(
    retry=retry_if_exception_type(requests.RequestException),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(3),
    reraise=True,
)
def _api_get(url: str, params: dict[str, Any], timeout: int = 15) -> dict[str, Any]:
    """GET with retry/backoff. Raises on non-200."""
    resp = requests.get(url, params=params, timeout=timeout)
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 60))
        logger.warning("Rate limited. Sleeping %ds.", retry_after)
        time.sleep(retry_after)
        resp.raise_for_status()
    resp.raise_for_status()
    return resp.json()  # type: ignore[return-value]


def _fetch_query(
    token: str,
    query: str,
    published_after: str,
    published_before: str,
    max_count: int,
) -> Generator[dict[str, Any], None, None]:
    """Page through TheNewsAPI results for a single query string."""
    page = 1
    fetched = 0

    while fetched < max_count:
        params: dict[str, Any] = {
            "api_token": token,
            "search": query,
            "language": "en",
            "locale": _US_LOCALE,
            "published_after": published_after,
            "published_before": published_before,
            "sort": "published_at",
            "limit": min(25, max_count - fetched),  # free tier max = 3, paid = 100
            "page": page,
        }

        try:
            data = _api_get(_BASE_URL, params)
        except requests.RequestException as exc:
            logger.error("API request failed for query %r page %d: %s", query, page, exc)
            return

        raw_articles: list[dict[str, Any]] = data.get("data", [])
        if not raw_articles:
            break  # no more pages

        for raw in raw_articles:
            yield _normalize(raw)
            fetched += 1
            if fetched >= max_count:
                return

        # Check if there's a next page
        meta = data.get("meta", {})
        if not meta.get("next"):
            break
        page += 1
        # Be polite — free tier has no explicit rate limit, but don't hammer
        time.sleep(0.25)


def _normalize(raw: dict[str, Any]) -> dict[str, Any]:
    """Map TheNewsAPI response fields to our internal schema."""
    source: str = raw.get("source", "") or ""
    outlet_city, outlet_region = _infer_outlet_location(source)

    return {
        "article_id":    raw.get("uuid", ""),
        "title":         raw.get("title", "") or "",
        "description":   raw.get("description", "") or "",
        "snippet":       raw.get("snippet", "") or "",
        "url":           raw.get("url", "") or "",
        "source":        source,
        "outlet_city":   outlet_city,
        "outlet_region": outlet_region,
        "published_at":  raw.get("published_at", "") or "",
        "language":      raw.get("language", "en"),
        "categories":    raw.get("categories", []) or [],
        "keywords":      raw.get("keywords", "") or "",
        # Combined searchable text for NLP
        "_full_text":    " ".join(filter(None, [
            raw.get("title"),
            raw.get("description"),
            raw.get("snippet"),
        ])),
    }


# Heuristic map: source domain keyword → (city, state)
_SOURCE_MAP: dict[str, tuple[str, str]] = {
    "baltimoresun":     ("Baltimore",     "Maryland"),
    "washingtonpost":   ("Washington",    "DC"),
    "nytimes":          ("New York",      "New York"),
    "latimes":          ("Los Angeles",   "California"),
    "chron":            ("Houston",       "Texas"),
    "miamiherald":      ("Miami",         "Florida"),
    "charlotteobserver":("Charlotte",     "North Carolina"),
    "ajc":              ("Atlanta",       "Georgia"),
    "philly":           ("Philadelphia",  "Pennsylvania"),
    "bostonglobe":      ("Boston",        "Massachusetts"),
    "chicagotribune":   ("Chicago",       "Illinois"),
    "denverpost":       ("Denver",        "Colorado"),
    "seattletimes":     ("Seattle",       "Washington"),
    "sfgate":           ("San Francisco", "California"),
    "orlandomsentinel": ("Orlando",       "Florida"),
    "nola":             ("New Orleans",   "Louisiana"),
    "tennessean":       ("Nashville",     "Tennessee"),
    "statesman":        ("Austin",        "Texas"),
    "star-telegram":    ("Fort Worth",    "Texas"),
    "duluthnewstribune":("Duluth",        "Minnesota"),
    "capitalgazette":   ("Annapolis",     "Maryland"),
    "wbaltv":           ("Baltimore",     "Maryland"),
    "wmar":             ("Baltimore",     "Maryland"),
    "wusa":             ("Washington",    "DC"),
}


def _infer_outlet_location(source: str) -> tuple[str | None, str | None]:
    """Best-effort city/state from source domain name."""
    src_lower = source.lower()
    for key, (city, state) in _SOURCE_MAP.items():
        if key in src_lower:
            return city, state
    return None, None
