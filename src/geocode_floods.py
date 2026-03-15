"""
geocode_floods.py — Flood type classification + location extraction + OSM geocoding.

Pipeline per article
--------------------
1.  classify_flood_type()   → 'flash_flood' | 'sunny_day' | 'riverine' | 'unknown'
2.  extract_locations()     → list of (mention_text, query_string, confidence)
3.  geocode_osm_flood()     → lat, lon, osm_display_name, osm_type

Returned location record schema
--------------------------------
{
    "article_id":   str,
    "mention_text": str,        # raw text snippet that triggered extraction
    "flood_type":   str,
    "confidence":   float,      # 0.0–1.0
    "lat":          float | None,
    "lon":          float | None,
    "osm_display":  str | None,
    "osm_type":     str | None, # e.g. "city", "suburb", "waterway"
    "osm_id":       int | None,
}
"""

from __future__ import annotations

import re
import time
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

# ---------------------------------------------------------------------------
# Flood classification patterns
# ---------------------------------------------------------------------------

FLOOD_PATTERNS: dict[str, str] = {
    "flash_flood": (
        r"\b(flash flood(?:ing|s|ed)?|sudden flood|creek overflow(?:ed)?|"
        r"rapid(?:ly)? ris(?:ing|e)|water rescue|swift water|dam fail|"
        r"heavy rain.{0,60}flood|flood.{0,60}heavy rain)\b"
    ),
    "sunny_day": (
        r"\b(sunny[- ]day flood(?:ing)?|high[- ]tide flood(?:ing)?|"
        r"king tide|nuisance flood(?:ing)?|tidal flood(?:ing)?|"
        r"sea[- ]level rise.{0,60}flood|flood.{0,60}clear sky|"
        r"flood.{0,60}no rain|streets flood.{0,60}without)\b"
    ),
    "riverine": (
        r"\b(river flood(?:ing)?|riverine flood|river overfl(?:ow|owed)|"
        r"river crest(?:ed)?|river stage|levee breach|levee overtop)\b"
    ),
}

_COMPILED: dict[str, re.Pattern[str]] = {
    k: re.compile(v, re.IGNORECASE | re.DOTALL) for k, v in FLOOD_PATTERNS.items()
}


def classify_flood_type(text: str) -> tuple[str, float]:
    """Return (flood_type, confidence).

    Confidence tiers:
      1.0 — specific multi-word phrase matched
      0.7 — generic 'flood' only
      0.0 — no flood match (article shouldn't be here, but guard anyway)
    """
    if not text:
        return "unknown", 0.0

    for ftype, pat in _COMPILED.items():
        m = pat.search(text)
        if m:
            return ftype, 1.0

    # Fallback: generic flooding mention
    if re.search(r"\bflooding?\b", text, re.IGNORECASE):
        return "unknown", 0.7

    return "unknown", 0.0


# ---------------------------------------------------------------------------
# Location extraction
# ---------------------------------------------------------------------------

# spaCy is optional at runtime so we lazy-load and cache
_NLP = None


def _get_nlp() -> Any:
    global _NLP
    if _NLP is None:
        try:
            import spacy  # noqa: PLC0415
            _NLP = spacy.load("en_core_web_sm")
        except OSError:
            logger.warning(
                "spaCy model 'en_core_web_sm' not found. "
                "Run: python -m spacy download en_core_web_sm\n"
                "Falling back to regex-only location extraction."
            )
            _NLP = False  # sentinel: tried and failed
    return _NLP


# Patterns for sub-city locations that add geocoding context
_DISTRICT_PATTERNS: list[tuple[str, str]] = [
    (r"\bdowntown\s+([A-Z][a-zA-Z\s]{2,30})", "downtown, {match}"),
    (r"\b(historic\s+district)\b", "historic district, {city}"),
    (r"\b(waterfront)\b", "waterfront, {city}"),
    (r"\bnear\s+([A-Z][a-zA-Z\s]{2,30}(?:School|Park|Bridge|Highway|Road|Street))", "{match}"),
    (r"\b([A-Z][a-zA-Z\s]{2,30}(?:Creek|River|Lake|Pond|Bay|Harbor))\b", "{match}"),
]


def extract_locations(
    article: dict[str, Any],
) -> list[tuple[str, str, float]]:
    """Extract (mention_text, osm_query_string, confidence) tuples from article.

    Uses spaCy GPE/LOC entities when available, supplemented by regex patterns.
    """
    text: str = article.get("_full_text", "")
    outlet_city: str | None = article.get("outlet_city")
    outlet_region: str | None = article.get("outlet_region")

    results: list[tuple[str, str, float]] = []
    seen_queries: set[str] = set()

    # --- spaCy NER ---
    nlp = _get_nlp()
    if nlp:
        doc = nlp(text[:5000])  # cap to avoid slow processing on long articles
        for ent in doc.ents:
            if ent.label_ in ("GPE", "LOC", "FAC"):
                mention = ent.text.strip()
                if len(mention) < 3:
                    continue
                # Build OSM query with state context
                if outlet_region:
                    query = f"{mention}, {outlet_region}"
                else:
                    query = mention
                _add_unique(results, seen_queries, mention, query, 0.9)

    # --- District/landmark regex patterns ---
    city_ctx = outlet_city or ""
    for pattern, template in _DISTRICT_PATTERNS:
        for m in re.finditer(pattern, text, re.IGNORECASE):
            match_text = m.group(1) if m.lastindex else m.group(0)
            mention = m.group(0).strip()
            query = template.format(match=match_text.strip(), city=city_ctx).strip(", ")
            if outlet_region:
                query = f"{query}, {outlet_region}"
            _add_unique(results, seen_queries, mention, query, 0.75)

    # --- Fallback: outlet city ---
    if not results and outlet_city:
        city_query = f"{outlet_city}, {outlet_region}" if outlet_region else outlet_city
        _add_unique(results, seen_queries, outlet_city, city_query, 0.4)

    return results[:10]  # cap at 10 locations per article


def _add_unique(
    results: list[tuple[str, str, float]],
    seen: set[str],
    mention: str,
    query: str,
    confidence: float,
) -> None:
    key = query.lower().strip()
    if key not in seen and len(key) > 2:
        seen.add(key)
        results.append((mention, query, confidence))


# ---------------------------------------------------------------------------
# OSM Nominatim geocoding
# ---------------------------------------------------------------------------

_NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"


@retry(
    retry=retry_if_exception_type(requests.RequestException),
    wait=wait_exponential(multiplier=1, min=2, max=20),
    stop=stop_after_attempt(3),
    reraise=False,
)
def _nominatim_search(
    query: str,
    user_agent: str,
    timeout: int = 10,
) -> list[dict[str, Any]]:
    """Call Nominatim search endpoint. Returns raw result list."""
    params = {
        "q": query,
        "format": "jsonv2",
        "limit": 1,
        "countrycodes": "us",
        "addressdetails": 1,
    }
    headers = {"User-Agent": user_agent}
    resp = requests.get(_NOMINATIM_URL, params=params, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.json()  # type: ignore[return-value]


def geocode_osm_flood(
    query: str,
    cfg: dict[str, Any],
) -> dict[str, Any]:
    """Geocode a single query string via Nominatim.

    Returns dict with lat, lon, osm_display, osm_type, osm_id.
    All values are None on failure.
    """
    user_agent: str = cfg["api"].get("user_agent", "flood_etl_anonymous")
    timeout: int = int(cfg.get("geocoding", {}).get("timeout_sec", 10))

    try:
        results = _nominatim_search(query, user_agent=user_agent, timeout=timeout)
    except Exception as exc:
        logger.debug("Nominatim error for %r: %s", query, exc)
        return _empty_geo()

    if not results:
        logger.debug("No geocode result for %r", query)
        return _empty_geo()

    hit = results[0]
    return {
        "lat":        float(hit["lat"]),
        "lon":        float(hit["lon"]),
        "osm_display": hit.get("display_name"),
        "osm_type":   hit.get("type") or hit.get("osm_type"),
        "osm_id":     hit.get("osm_id"),
    }


def _empty_geo() -> dict[str, Any]:
    return {"lat": None, "lon": None, "osm_display": None, "osm_type": None, "osm_id": None}


# ---------------------------------------------------------------------------
# Full per-article processing
# ---------------------------------------------------------------------------

def process_article(
    article: dict[str, Any],
    cfg: dict[str, Any],
) -> list[dict[str, Any]]:
    """Run classification + extraction + geocoding for one article.

    When Ollama is available (and cfg.llm.enabled is true), uses LLM results
    for flood type and locations, merging with regex fallback.
    Falls back entirely to regex/spaCy when Ollama is unavailable.

    Returns a list of location records (may be empty if no locations found).
    Sleeps rate_limit_sec between geocode calls per OSM policy.
    """
    rate_limit: float = float(cfg.get("geocoding", {}).get("rate_limit_sec", 1.0))
    text: str = article.get("_full_text", "")
    article_id: str = article["article_id"]

    # --- Flood type classification ---
    # Prefer LLM result if already enriched on the article dict
    llm_type: str | None = article.get("llm_flood_type")
    llm_conf: float = float(article.get("llm_confidence") or 0.0)

    if llm_type and llm_type != "unknown" and llm_conf >= 0.6:
        flood_type, type_confidence = llm_type, llm_conf
        logger.debug("Article %s: using LLM flood type %s (conf=%.2f)", article_id, flood_type, type_confidence)
    else:
        flood_type, type_confidence = classify_flood_type(text)

    # --- Location extraction ---
    # Prefer LLM locations; merge/dedupe with regex results
    locations: list[tuple[str, str, float]] = []
    seen_queries: set[str] = set()

    llm_locs: list[dict[str, Any]] | None = article.get("llm_locations")
    if llm_locs:
        for lloc in llm_locs:
            q = (lloc.get("osm_query") or "").strip()
            if q and q.lower() not in seen_queries:
                seen_queries.add(q.lower())
                locations.append((lloc["mention_text"], q, float(lloc.get("confidence", 0.75))))

    # Always add regex/spaCy results as supplement (fills gaps when LLM misses)
    regex_locs = extract_locations(article)
    for mention, query, conf in regex_locs:
        if query.lower() not in seen_queries:
            seen_queries.add(query.lower())
            locations.append((mention, query, conf))

    if not locations:
        logger.debug("No locations found in article %s", article_id)
        return []

    records: list[dict[str, Any]] = []
    for mention_text, query, loc_confidence in locations:
        geo = geocode_osm_flood(query, cfg)
        time.sleep(rate_limit)  # OSM Nominatim: 1 req/sec max

        records.append({
            "article_id":   article_id,
            "mention_text": mention_text,
            "osm_query":    query,
            "flood_type":   flood_type,
            "confidence":   round(min(type_confidence, loc_confidence), 3),
            "lat":          geo["lat"],
            "lon":          geo["lon"],
            "osm_display":  geo["osm_display"],
            "osm_type":     geo["osm_type"],
            "osm_id":       geo["osm_id"],
            # LLM enrichment passthrough
            "severity":     article.get("llm_severity"),
            "summary":      article.get("llm_summary"),
        })

    logger.debug(
        "Article %s → flood_type=%s, %d locations geocoded (llm=%s)",
        article_id, flood_type, len(records), bool(llm_locs),
    )
    return records
