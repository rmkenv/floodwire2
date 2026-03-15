"""
load_files.py — Append flood locations to GeoJSON and CSV flat files.

Replaces load_postgis.py entirely — no database required.

Output files (created on first run, appended on subsequent runs):
  data/floods.geojson   — GeoJSON FeatureCollection
  data/floods.csv       — CSV with one row per geocoded location

Deduplication:
  Both files are keyed on (article_id, mention_text).
  Re-running for the same date range won't create duplicate rows.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .utils import get_logger

logger = get_logger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"

GEOJSON_PATH = DATA_DIR / "floods.geojson"
CSV_PATH     = DATA_DIR / "floods.csv"

CSV_FIELDS = [
    "article_id",
    "title",
    "source",
    "outlet_city",
    "outlet_region",
    "published_at",
    "url",
    "mention_text",
    "flood_type",
    "confidence",
    "lat",
    "lon",
    "osm_display",
    "osm_type",
    "osm_id",
    "run_at",
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_files(
    articles: list[dict[str, Any]],
    locations: list[dict[str, Any]],
    cfg: dict[str, Any],
) -> dict[str, int]:
    """Append new geocoded flood records to GeoJSON and CSV.

    Args:
        articles:  List of article dicts from extract_articles.
        locations: List of location dicts from geocode_floods.
        cfg:       Loaded config (unused here but kept for interface parity).

    Returns:
        {"geojson_appended": N, "csv_appended": N, "duplicates_skipped": N}
    """
    DATA_DIR.mkdir(exist_ok=True)

    # Build article lookup so we can join fields into location rows
    article_map: dict[str, dict[str, Any]] = {a["article_id"]: a for a in articles}

    # Only write locations that were successfully geocoded
    geo_locs = [loc for loc in locations if loc.get("lat") is not None]
    skipped_no_geo = len(locations) - len(geo_locs)
    if skipped_no_geo:
        logger.info("Skipping %d locations with no coordinates", skipped_no_geo)

    run_at = datetime.now(timezone.utc).isoformat()

    # Build enriched rows (location + joined article fields)
    rows = [_enrich(loc, article_map, run_at) for loc in geo_locs]

    geojson_appended, csv_appended, dupes = _append_geojson(rows)
    csv_app, csv_dupes = _append_csv(rows)

    # Use whichever dupe count is higher (they should match)
    total_dupes = max(dupes, csv_dupes)

    logger.info(
        "Files updated — GeoJSON: +%d rows, CSV: +%d rows, %d duplicates skipped",
        geojson_appended, csv_appended, total_dupes,
    )
    return {
        "geojson_appended": geojson_appended,
        "csv_appended":     csv_appended,
        "duplicates_skipped": total_dupes,
    }


# ---------------------------------------------------------------------------
# GeoJSON
# ---------------------------------------------------------------------------

def _append_geojson(rows: list[dict[str, Any]]) -> tuple[int, int, int]:
    """Append rows to floods.geojson. Returns (appended, total_after, dupes_skipped)."""
    # Load existing features
    existing_keys: set[tuple[str, str]] = set()
    features: list[dict[str, Any]] = []

    if GEOJSON_PATH.exists():
        try:
            fc = json.loads(GEOJSON_PATH.read_text())
            features = fc.get("features", [])
            for f in features:
                props = f.get("properties", {})
                existing_keys.add((props.get("article_id", ""), props.get("mention_text", "")))
        except (json.JSONDecodeError, KeyError) as exc:
            logger.warning("Could not parse existing GeoJSON, starting fresh: %s", exc)
            features = []

    appended = 0
    dupes = 0
    for row in rows:
        key = (row["article_id"], row["mention_text"])
        if key in existing_keys:
            dupes += 1
            continue
        existing_keys.add(key)
        features.append(_to_feature(row))
        appended += 1

    # Write back as a valid FeatureCollection
    fc = {
        "type": "FeatureCollection",
        "features": features,
        "_metadata": {
            "description": "Flood news geocoder — nightly ETL output",
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "total_features": len(features),
        },
    }
    GEOJSON_PATH.write_text(json.dumps(fc, indent=2))
    logger.info("GeoJSON written: %s (%d total features)", GEOJSON_PATH, len(features))
    return appended, len(features), dupes


def _to_feature(row: dict[str, Any]) -> dict[str, Any]:
    """Convert an enriched row dict to a GeoJSON Feature."""
    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [row["lon"], row["lat"]],  # GeoJSON: [lon, lat]
        },
        "properties": {k: v for k, v in row.items() if k not in ("lat", "lon")},
    }


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

def _append_csv(rows: list[dict[str, Any]]) -> tuple[int, int]:
    """Append rows to floods.csv. Returns (appended, dupes_skipped)."""
    existing_keys: set[tuple[str, str]] = set()
    write_header = not CSV_PATH.exists()

    if CSV_PATH.exists():
        try:
            with CSV_PATH.open(newline="", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                for r in reader:
                    existing_keys.add((r.get("article_id", ""), r.get("mention_text", "")))
        except Exception as exc:
            logger.warning("Could not read existing CSV, starting fresh: %s", exc)
            write_header = True

    appended = 0
    dupes = 0
    new_rows: list[dict[str, Any]] = []

    for row in rows:
        key = (row["article_id"], row["mention_text"])
        if key in existing_keys:
            dupes += 1
            continue
        existing_keys.add(key)
        new_rows.append(row)
        appended += 1

    if new_rows:
        with CSV_PATH.open("a", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS, extrasaction="ignore")
            if write_header:
                writer.writeheader()
            writer.writerows(new_rows)
        logger.info("CSV written: %s (+%d rows)", CSV_PATH, appended)

    return appended, dupes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _enrich(
    loc: dict[str, Any],
    article_map: dict[str, dict[str, Any]],
    run_at: str,
) -> dict[str, Any]:
    """Merge location record with parent article fields."""
    art = article_map.get(loc["article_id"], {})
    return {
        # Article fields
        "article_id":    loc["article_id"],
        "title":         art.get("title", ""),
        "source":        art.get("source", ""),
        "outlet_city":   art.get("outlet_city", ""),
        "outlet_region": art.get("outlet_region", ""),
        "published_at":  art.get("published_at", ""),
        "url":           art.get("url", ""),
        # Location fields
        "mention_text":  loc["mention_text"],
        "flood_type":    loc["flood_type"],
        "confidence":    loc["confidence"],
        "lat":           loc["lat"],
        "lon":           loc["lon"],
        "osm_display":   loc.get("osm_display", ""),
        "osm_type":      loc.get("osm_type", ""),
        "osm_id":        loc.get("osm_id", ""),
        "run_at":        run_at,
    }
