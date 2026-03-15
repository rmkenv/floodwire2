"""
extract_articles.py — Fetch flood-related US news articles from TheNewsAPI.com.

Changes from v1:
- Tighter queries: multi-word phrases only, no generic single-word terms
- Post-fetch relevance filter: drops figurative uses + international articles
- US state mention check as a secondary signal
"""

from __future__ import annotations

import re
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

from .utils import get_logger

logger = get_logger(__name__)

_BASE_URL = "https://api.thenewsapi.com/v1/news/all"

# ---------------------------------------------------------------------------
# Search queries
# ---------------------------------------------------------------------------
# Rules:
#   - Multi-word phrases only — single words like "flooding" match too broadly
#   - Physical water events only — no metaphorical terms
#   - Each query is specific enough that nearly every result should be relevant
# ---------------------------------------------------------------------------
_FLOOD_QUERIES = [
    # Flash / storm flood
    "flash flood warning",
    "flash flood watch",
    "flash flooding closes",
    "flash flood damage",
    "flash flood rescue",
    "creek overflowed",
    "river overflowed banks",
    "rapid rise flooding",
    "swift water rescue",
    # Sunny day / tidal / coastal
    "sunny day flooding",
    "high tide flooding",
    "king tide flooding",
    "nuisance flooding",
    "tidal flooding streets",
    "sea level flooding",
    # Urban / infrastructure
    "flooded streets closed",
    "flooded road closed",
    "flooded neighborhood evacuated",
    "floodwater entered homes",
    "basement flooding",
    "storm drain overflow",
    # Emergency / damage
    "flood damage homes",
    "flood evacuation order",
    "flood emergency declared",
]

_US_LOCALE = "us"

# ---------------------------------------------------------------------------
# Relevance filter
# ---------------------------------------------------------------------------

# Phrases that indicate a figurative / non-physical use of "flood"
_FIGURATIVE_PATTERNS = re.compile(
    r"\b(flood of (calls|emails|complaints|requests|messages|criticism|tears|"
    r"support|donations|applicants|immigrants|migrants|refugees|tourists|"
    r"visitors|memories|emotions|information|data|traffic|orders|money|cash|"
    r"investment|funding|light|sunshine|color|colour))\b"
    r"|\b(flooded (with (calls|emails|complaints|requests|messages|applicants|"
    r"immigrants|migrants|refugees|tourists|memories|offers|bids)))\b"
    r"|\b(flood (of immigrants|of migrants|of refugees|of tourists|gate))\b",
    re.IGNORECASE,
)

# Must match at least one of these to pass as a physical water flood
_PHYSICAL_FLOOD_PATTERNS = re.compile(
    r"\b(flash flood|flooding (street|road|home|house|basement|neighborhood|"
    r"downtown|highway|interstate|underpass|subway|tunnel|creek|river|bay|"
    r"coast|beach|marina|parking)|flooded (street|road|home|house|basement|"
    r"neighborhood|car|vehicle|highway)|floodwater|flood damage|flood warning|"
    r"flood watch|flood advisory|flood evacuation|storm surge|tidal flood|"
    r"sunny.?day flood|king tide|nuisance flood|high.?tide flood|"
    r"water rescue|swift water|creek overflow|river overflow|"
    r"overflowed (its banks|banks)|flood emergency|flood relief)\b",
    re.IGNORECASE,
)

# Common non-US country/region signals — if present and no US state, likely international
_INTERNATIONAL_SIGNALS = re.compile(
    r"\b(Pakistan|Bangladesh|India|China|Nigeria|Kenya|Ethiopia|Somalia|"
    r"Sudan|Libya|Turkey|Brazil|Colombia|Venezuela|Indonesia|Philippines|"
    r"Vietnam|Thailand|Myanmar|Australia|New Zealand|UK|United Kingdom|"
    r"England|Scotland|Wales|Germany|France|Italy|Spain|Canada|Mexico|"
    r"European|African|Asian|Nairobi|Lagos|Dhaka|Karachi|Mumbai|Beijing|"
    r"Jakarta|Manila|Hanoi|Bangkok|Sydney|Melbourne|London|Paris|Berlin)\b",
    re.IGNORECASE,
)

# US state names and abbreviations as a positive signal
_US_STATE_PATTERN = re.compile(
    r"\b(Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|"
    r"Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|"
    r"Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|"
    r"Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|"
    r"New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|"
    r"Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|"
    r"Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming|"
    r"AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|"
    r"MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|"
    r"TX|UT|VT|VA|WA|WV|WI|WY|D\.?C\.?)\b",
    re.IGNORECASE,
)


def _is_relevant(article: dict[str, Any]) -> tuple[bool, str]:
    """Return (keep, reason). reason is logged when dropping."""
    text = article.get("_full_text", "")

    # 1. Must contain a physical flood term
    if not _PHYSICAL_FLOOD_PATTERNS.search(text):
        return False, "no physical flood term found"

    # 2. Drop clear figurative uses
    if _FIGURATIVE_PATTERNS.search(text):
        return False, "figurative flood language detected"

    # 3. Drop if international signals present AND no US state mentioned
    if _INTERNATIONAL_SIGNALS.search(text) and not _US_STATE_PATTERN.search(text):
        return False, "likely international article"

    return True, ""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_articles(
    cfg: dict[str, Any],
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    max_articles: int | None = None,
) -> list[dict[str, Any]]:
    """Fetch and filter flood articles for the given date range."""
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
    total_dropped = 0

    for query in _FLOOD_QUERIES:
        if len(articles) >= limit:
            break
        logger.info("Querying: %r", query)
        batch = _fetch_query(
            token=token,
            query=query,
            published_after=published_after,
            published_before=published_before,
            max_count=limit - len(articles),
        )
        added = dropped = 0
        for art in batch:
            aid = art["article_id"]
            if aid in seen_ids:
                continue
            seen_ids.add(aid)
            keep, reason = _is_relevant(art)
            if keep:
                articles.append(art)
                added += 1
            else:
                dropped += 1
                total_dropped += 1
                logger.debug("Dropped %r: %s", art.get("title", "")[:60], reason)
        logger.info("  → +%d kept, %d dropped (total: %d)", added, dropped, len(articles))

    logger.info(
        "Fetch complete. Kept: %d  Dropped: %d  Drop rate: %.0f%%",
        len(articles),
        total_dropped,
        100 * total_dropped / max(1, len(articles) + total_dropped),
    )
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
            "limit": min(25, max_count - fetched),
            "page": page,
        }

        try:
            data = _api_get(_BASE_URL, params)
        except requests.RequestException as exc:
            logger.error("API request failed for %r page %d: %s", query, page, exc)
            return

        raw_articles: list[dict[str, Any]] = data.get("data", [])
        if not raw_articles:
            break

        for raw in raw_articles:
            yield _normalize(raw)
            fetched += 1
            if fetched >= max_count:
                return

        meta = data.get("meta", {})
        if not meta.get("next"):
            break
        page += 1
        time.sleep(0.25)


def _normalize(raw: dict[str, Any]) -> dict[str, Any]:
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
        "_full_text":    " ".join(filter(None, [
            raw.get("title"),
            raw.get("description"),
            raw.get("snippet"),
        ])),
    }


_SOURCE_MAP: dict[str, tuple[str, str]] = {
    "baltimoresun":      ("Baltimore",     "Maryland"),
    "washingtonpost":    ("Washington",    "DC"),
    "nytimes":           ("New York",      "New York"),
    "latimes":           ("Los Angeles",   "California"),
    "chron":             ("Houston",       "Texas"),
    "miamiherald":       ("Miami",         "Florida"),
    "charlotteobserver": ("Charlotte",     "North Carolina"),
    "ajc":               ("Atlanta",       "Georgia"),
    "philly":            ("Philadelphia",  "Pennsylvania"),
    "bostonglobe":       ("Boston",        "Massachusetts"),
    "chicagotribune":    ("Chicago",       "Illinois"),
    "denverpost":        ("Denver",        "Colorado"),
    "seattletimes":      ("Seattle",       "Washington"),
    "sfgate":            ("San Francisco", "California"),
    "orlandomsentinel":  ("Orlando",       "Florida"),
    "nola":              ("New Orleans",   "Louisiana"),
    "tennessean":        ("Nashville",     "Tennessee"),
    "statesman":         ("Austin",        "Texas"),
    "star-telegram":     ("Fort Worth",    "Texas"),
    "duluthnewstribune": ("Duluth",        "Minnesota"),
    "capitalgazette":    ("Annapolis",     "Maryland"),
    "wbaltv":            ("Baltimore",     "Maryland"),
    "wmar":              ("Baltimore",     "Maryland"),
    "wusa":              ("Washington",    "DC"),
}


def _infer_outlet_location(source: str) -> tuple[str | None, str | None]:
    src_lower = source.lower()
    for key, (city, state) in _SOURCE_MAP.items():
        if key in src_lower:
            return city, state
    return None, None
