"""
gauge_flood_join.py
===================
Proximity join: gauges_current.json  ←→  floods.csv

For each gauge, finds all flood news events within RADIUS_MILES and
attaches them as `nearby_floods`. Writes:

  data/gauges_with_floods.json   — enriched gauge records (full)
  data/gauges_with_floods.csv    — flat joined rows (one row per gauge×flood pair)
  data/gauges_with_floods.geojson — GeoJSON with flood count + nearest headline

Usage:
  python gauge_flood_join.py                        # default 50-mile radius
  python gauge_flood_join.py --radius 25            # tighter radius
  python gauge_flood_join.py --alerts-only          # only gauges at action/flood/major tier
  python gauge_flood_join.py --radius 30 --alerts-only
"""

import json
import csv
import argparse
import logging
from datetime import datetime, timezone
from math import radians, cos, sin, asin, sqrt
from pathlib import Path

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("gauge_flood_join")

# ── Paths ─────────────────────────────────────────────────────
DATA_DIR          = Path("data")
GAUGES_FILE       = DATA_DIR / "gauges_current.json"
FLOODS_CSV        = DATA_DIR / "floods.csv"
OUT_JSON          = DATA_DIR / "gauges_with_floods.json"
OUT_CSV           = DATA_DIR / "gauges_with_floods.csv"
OUT_GEOJSON       = DATA_DIR / "gauges_with_floods.geojson"

# ── Haversine (miles) ─────────────────────────────────────────
def haversine(lat1, lon1, lat2, lon2) -> float:
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 2 * asin(sqrt(a)) * 3958.8


# ── Loaders ───────────────────────────────────────────────────
def load_gauges(path: Path, alerts_only: bool) -> list[dict]:
    if not path.exists():
        log.error(f"Gauge file not found: {path}")
        return []
    with open(path) as f:
        data = json.load(f)
    gauges = data.get("gauges", [])
    if alerts_only:
        gauges = [g for g in gauges if g.get("tier") in ("action", "flood", "major")]
        log.info(f"Alerts-only mode: {len(gauges)} alert gauges")
    else:
        log.info(f"Loaded {len(gauges)} gauges")
    return gauges


def load_floods(path: Path) -> list[dict]:
    if not path.exists():
        log.error(f"Floods CSV not found: {path}")
        return []
    floods = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                lat = float(row.get("lat") or row.get("latitude") or "")
                lon = float(row.get("lon") or row.get("longitude") or "")
                floods.append({**row, "_lat": lat, "_lon": lon})
            except (ValueError, TypeError):
                continue  # skip rows without valid coords
    log.info(f"Loaded {len(floods)} flood events with valid coordinates")
    return floods


# ── Core join ─────────────────────────────────────────────────
def proximity_join(gauges: list[dict], floods: list[dict], radius_miles: float) -> list[dict]:
    """
    Attach nearby flood events to each gauge.
    Each gauge gets a `nearby_floods` list and summary fields.
    """
    enriched = []
    for g in gauges:
        g_lat = g.get("lat")
        g_lon = g.get("lon")
        if g_lat is None or g_lon is None:
            g_enriched = g.copy()
            g_enriched["nearby_floods"] = []
            g_enriched["flood_event_count"] = 0
            g_enriched["nearest_flood_miles"] = None
            g_enriched["nearest_flood_title"] = None
            enriched.append(g_enriched)
            continue

        nearby = []
        for fl in floods:
            dist = haversine(g_lat, g_lon, fl["_lat"], fl["_lon"])
            if dist <= radius_miles:
                entry = {k: v for k, v in fl.items() if not k.startswith("_")}
                entry["distance_miles"] = round(dist, 2)
                nearby.append(entry)

        # Sort closest first
        nearby.sort(key=lambda x: x["distance_miles"])

        g_enriched = g.copy()
        g_enriched["nearby_floods"] = nearby
        g_enriched["flood_event_count"] = len(nearby)
        g_enriched["nearest_flood_miles"] = nearby[0]["distance_miles"] if nearby else None
        g_enriched["nearest_flood_title"] = (
            nearby[0].get("title") or nearby[0].get("headline") or nearby[0].get("description", "")[:80]
            if nearby else None
        )
        enriched.append(g_enriched)

    total_links = sum(g["flood_event_count"] for g in enriched)
    log.info(f"Join complete: {total_links} gauge×flood links across {len(enriched)} gauges "
             f"(radius={radius_miles} mi)")
    return enriched


# ── Writers ───────────────────────────────────────────────────
def write_json(enriched: list[dict], path: Path):
    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "gauge_count": len(enriched),
        "gauges": enriched,
    }
    with open(path, "w") as f:
        json.dump(out, f, indent=2, default=str)
    log.info(f"Wrote {path}")


def write_csv(enriched: list[dict], path: Path):
    """
    Flat join: one row per gauge×flood pair.
    Gauges with zero nearby floods still get one row (with empty flood cols).
    """
    # Collect all flood field names (excluding distance_miles which we always include)
    flood_field_sample = {}
    for g in enriched:
        for fl in g.get("nearby_floods", []):
            flood_field_sample.update(fl)
    flood_fields = [k for k in flood_field_sample if k != "distance_miles"]

    # Gauge fields (scalars only — skip nearby_floods list)
    skip_keys = {"nearby_floods", "flood_event_count", "nearest_flood_miles", "nearest_flood_title"}
    gauge_field_sample = {}
    for g in enriched:
        gauge_field_sample.update({k: "" for k in g if k not in skip_keys})
    gauge_fields = list(gauge_field_sample.keys())

    # Summary fields appended to each gauge row
    summary_fields = ["flood_event_count", "nearest_flood_miles", "nearest_flood_title"]

    # Flood join fields
    flood_join_fields = ["distance_miles"] + flood_fields

    fieldnames = gauge_fields + summary_fields + flood_join_fields

    rows = []
    for g in enriched:
        gauge_base = {k: g.get(k, "") for k in gauge_fields}
        gauge_base["flood_event_count"] = g["flood_event_count"]
        gauge_base["nearest_flood_miles"] = g["nearest_flood_miles"] if g["nearest_flood_miles"] is not None else ""
        gauge_base["nearest_flood_title"] = g["nearest_flood_title"] or ""

        if g["nearby_floods"]:
            for fl in g["nearby_floods"]:
                row = gauge_base.copy()
                row["distance_miles"] = fl.get("distance_miles", "")
                for ff in flood_fields:
                    row[ff] = fl.get(ff, "")
                rows.append(row)
        else:
            row = gauge_base.copy()
            for ff in flood_join_fields:
                row[ff] = ""
            rows.append(row)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    log.info(f"Wrote {path}  ({len(rows)} rows)")


def write_geojson(enriched: list[dict], path: Path):
    tier_colors = {
        "normal": "#2d6a4f",
        "action": "#f48c06",
        "flood":  "#e85d04",
        "major":  "#c1121f",
        "unknown": "#888888",
    }
    features = []
    for g in enriched:
        lat, lon = g.get("lat"), g.get("lon")
        if lat is None or lon is None:
            continue
        props = {k: v for k, v in g.items() if k != "nearby_floods"}
        props["color"] = tier_colors.get(g.get("tier", "unknown"), "#888888")
        # Embed lightweight flood snippet (titles only) to keep file size sane
        props["nearby_flood_titles"] = [
            fl.get("title") or fl.get("headline", "")[:80]
            for fl in g.get("nearby_floods", [])
        ]
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        })
    fc = {"type": "FeatureCollection", "features": features}
    with open(path, "w") as f:
        json.dump(fc, f, indent=2, default=str)
    log.info(f"Wrote {path}")


# ── CLI ───────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Proximity-join gauges to flood news events")
    parser.add_argument("--radius", type=float, default=50.0,
                        help="Search radius in miles (default: 50)")
    parser.add_argument("--alerts-only", action="store_true",
                        help="Only process gauges at action/flood/major tier")
    args = parser.parse_args()

    gauges = load_gauges(GAUGES_FILE, alerts_only=args.alerts_only)
    floods = load_floods(FLOODS_CSV)

    if not gauges:
        log.error("No gauges to process. Run gauge_fetch.py first.")
        return
    if not floods:
        log.error("No flood events loaded. Check data/floods.csv exists and has lat/lon columns.")
        return

    enriched = proximity_join(gauges, floods, radius_miles=args.radius)

    write_json(enriched, OUT_JSON)
    write_csv(enriched, OUT_CSV)
    write_geojson(enriched, OUT_GEOJSON)

    # Quick summary to stdout
    with_events = [g for g in enriched if g["flood_event_count"] > 0]
    log.info(
        f"Summary: {len(with_events)}/{len(enriched)} gauges have nearby flood events "
        f"within {args.radius} mi"
    )
    if with_events:
        top = sorted(with_events, key=lambda x: x["flood_event_count"], reverse=True)[:5]
        log.info("Top gauges by nearby flood event count:")
        for g in top:
            log.info(
                f"  {g.get('site_name', g.get('site_id'))} "
                f"[{g.get('tier')}] → {g['flood_event_count']} events, "
                f"nearest: {g['nearest_flood_miles']} mi"
            )


if __name__ == "__main__":
    main()
