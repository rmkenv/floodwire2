"""
extract_articles.py — Fetch flood news via SerpAPI Google News.

Strategy: group all flood terms into 3 OR queries — one call each.
Total: 3 SerpAPI calls per run (well within 250/month free tier).

Query groups
------------
  Group 1 — Flash / storm flood
  Group 2 — Sunny day / tidal / coastal
  Group 3 — Urban / emergency

Each query uses Google News operators:
  - OR grouping   → fewer calls
  - when:1d       → last 24 hours only
  - gl=us         → US results only
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from .utils import get_logger

logger = get_logger(__name__)

_SERPAPI_URL = "https://serpapi.com/search"

# ---------------------------------------------------------------------------
# Query groups — each becomes ONE SerpAPI call
# Operator reference: https://serpapi.com/google-news-api
# ---------------------------------------------------------------------------
_QUERY_GROUPS = [
    # Group 1: Flash / storm / river flooding
    (
        '"flash flood" OR "flash flooding" OR "creek overflowed" OR '
        '"river overflowed" OR "swift water rescue" OR "water rescue flood" OR '
        '"rapid rise flooding" OR "flood rescue" OR "flood warning issued"'
    ),
    # Group 2: Sunny day / tidal / coastal
    (
        '"sunny day flooding" OR "high tide flooding" OR "king tide flooding" OR '
        '"nuisance flooding" OR "tidal flooding" OR "sea level flooding" OR '
        '"storm surge flooding" OR "coastal flooding"'
    ),
    # Group 3: Urban / infrastructure / emergency
    (
        '"flooded streets" OR "flooded road" OR "floodwater entered" OR '
        '"flood damage homes" OR "flood evacuation" OR "flood emergency" OR '
        '"basement flooding" OR "storm drain overflow" OR "flood advisory"'
    ),
]

# ---------------------------------------------------------------------------
# Post-fetch relevance filter (same as before — catches what Google misses)
# ---------------------------------------------------------------------------

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

_PHYSICAL_FLOOD_PATTERNS = re.compile(
    r"\b(flash flood|flooding (street|road|home|house|basement|neighborhood|"
    r"downtown|highway|interstate|underpass|subway|tunnel|creek|river|bay|"
    r"coast|beach|marina|parking)|flooded (street|road|home|house|basement|"
    r"neighborhood|car|vehicle|highway)|floodwater|flood damage|flood warning|"
    r"flood watch|flood advisory|flood evacuation|flood emergency|storm surge|"
    r"tidal flood|sunny.?day flood|king tide|nuisance flood|high.?tide flood|"
    r"water rescue|swift water|creek overflow|river overflow|"
    r"overflowed (its banks|banks)|flood relief)\b",
    re.IGNORECASE,
)

_INTERNATIONAL_SIGNALS = re.compile(
    r"\b(Pakistan|Bangladesh|India|China|Nigeria|Kenya|Ethiopia|Somalia|"
    r"Sudan|Libya|Turkey|Brazil|Colombia|Venezuela|Indonesia|Philippines|"
    r"Vietnam|Thailand|Myanmar|Australia|New Zealand|UK|United Kingdom|"
    r"England|Scotland|Wales|Germany|France|Italy|Spain|Canada|Mexico|"
    r"European|African|Asian|Nairobi|Lagos|Dhaka|Karachi|Mumbai|Beijing|"
    r"Jakarta|Manila|Hanoi|Bangkok|Sydney|Melbourne|London|Paris|Berlin)\b",
    re.IGNORECASE,
)

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
    text = article.get("_full_text", "")
    if not _PHYSICAL_FLOOD_PATTERNS.search(text):
        return False, "no physical flood term found"
    if _FIGURATIVE_PATTERNS.search(text):
        return False, "figurative flood language detected"
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
    """Fetch and filter flood articles using SerpAPI Google News.

    Uses 3 OR-grouped queries — one API call each.
    """
    api_key: str = cfg["api"]["serpapi_key"]
    limit: int = max_articles or cfg.get("etl", {}).get("max_articles", 500)

    # Build when: operator from date range
    now = datetime.now(timezone.utc)
    lookback = cfg.get("etl", {}).get("lookback_days", 1)
    start = start_date or (now - timedelta(days=lookback))

    # Google News when: operator supports h (hours) or d (days)
    delta_hours = max(1, int((now - start).total_seconds() / 3600))
    when_param = f"{delta_hours}h" if delta_hours <= 72 else f"{int(delta_hours // 24)}d"

    seen_ids: set[str] = set()
    articles: list[dict[str, Any]] = []
    total_dropped = 0

    for i, query in enumerate(_QUERY_GROUPS, 1):
        if len(articles) >= limit:
            break
        logger.info("SerpAPI query group %d/3 (when:%s)", i, when_param)
        batch = _fetch_query(api_key, query, when_param)

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
            if len(articles) >= limit:
                break

        logger.info("  → +%d kept, %d dropped (total: %d)", added, dropped, len(articles))
        # Small pause between calls — SerpAPI is fine with this but be polite
        time.sleep(0.5)

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
def _api_get(params: dict[str, Any], timeout: int = 15) -> dict[str, Any]:
    resp = requests.get(_SERPAPI_URL, params=params, timeout=timeout)
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 60))
        logger.warning("Rate limited by SerpAPI. Sleeping %ds.", retry_after)
        time.sleep(retry_after)
        resp.raise_for_status()
    resp.raise_for_status()
    return resp.json()  # type: ignore[return-value]


def _fetch_query(
    api_key: str,
    query: str,
    when_param: str,
) -> list[dict[str, Any]]:
    """Fetch one query group from SerpAPI Google News. Returns normalized articles."""
    params = {
        "engine":  "google_news",
        "q":       f"{query} when:{when_param}",
        "gl":      "us",
        "hl":      "en",
        "api_key": api_key,
    }

    try:
        data = _api_get(params)
    except requests.RequestException as exc:
        logger.error("SerpAPI request failed: %s", exc)
        return []

    results: list[dict[str, Any]] = []
    for raw in data.get("news_results", []):
        art = _normalize(raw)
        if art:
            results.append(art)
        # Also unpack nested stories (Google News clusters related articles)
        for story in raw.get("stories", []):
            art = _normalize(story)
            if art:
                results.append(art)

    logger.debug("SerpAPI returned %d raw results", len(results))
    return results


def _normalize(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Map SerpAPI Google News result to internal schema."""
    title: str = raw.get("title", "") or ""
    snippet: str = raw.get("snippet", "") or ""
    link: str = raw.get("link", "") or ""

    if not title and not link:
        return None

    # SerpAPI source is nested under "source" dict
    source_info = raw.get("source", {}) or {}
    source_name: str = source_info.get("name", "") or ""
    outlet_city, outlet_region = _infer_outlet_location(source_name)

    # Article ID: use link as stable key (SerpAPI has no uuid)
    article_id = link or title

    # Date: SerpAPI returns iso_date or date string
    published_at: str = raw.get("iso_date", "") or raw.get("date", "") or ""

    return {
        "article_id":    article_id,
        "title":         title,
        "description":   snippet,
        "snippet":       snippet,
        "url":           link,
        "source":        source_name,
        "outlet_city":   outlet_city,
        "outlet_region": outlet_region,
        "published_at":  published_at,
        "language":      "en",
        "categories":    [],
        "keywords":      "",
        "_full_text":    f"{title} {snippet}".strip(),
    }


# ---------------------------------------------------------------------------
# Outlet location heuristics (unchanged)
# ---------------------------------------------------------------------------

_SOURCE_MAP: dict[str, tuple[str, str]] = {
    # Full display names as returned by SerpAPI Google News
    "baltimore sun":        ("Baltimore",     "Maryland"),
    "washington post":      ("Washington",    "DC"),
    "new york times":       ("New York",      "New York"),
    "los angeles times":    ("Los Angeles",   "California"),
    "houston chronicle":    ("Houston",       "Texas"),
    "miami herald":         ("Miami",         "Florida"),
    "boston globe":         ("Boston",        "Massachusetts"),
    "chicago tribune":      ("Chicago",       "Illinois"),
    "denver post":          ("Denver",        "Colorado"),
    "seattle times":        ("Seattle",       "Washington"),
    # Domain keyword fallbacks
    "baltimoresun":         ("Baltimore",     "Maryland"),
    "washingtonpost":       ("Washington",    "DC"),
    "nytimes":              ("New York",      "New York"),
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
    "wbal":              ("Baltimore",     "Maryland"),
    "wtop":              ("Washington",    "DC"),
    "wric":              ("Richmond",      "Virginia"),
    "wavy":              ("Norfolk",       "Virginia"),
    "wtkr":              ("Norfolk",       "Virginia"),
    "wvec":              ("Norfolk",       "Virginia"),
}


def _infer_outlet_location(source: str) -> tuple[str | None, str | None]:
    src_lower = source.lower()
    for key, (city, state) in _SOURCE_MAP.items():
        if key in src_lower:
            return city, state
    return None, None
