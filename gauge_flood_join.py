"""
gauge_flood_join.py
===================
Proximity join: gauges_current.json  <->  floods.csv

For each gauge, finds all flood news events within RADIUS_MILES and
attaches them as `nearby_floods`. Writes:

  data/gauges_with_floods.json    -- enriched gauge records (full)
  data/gauges_with_floods.csv     -- flat joined rows (one row per gauge x flood pair)
  data/gauges_with_floods.geojson -- GeoJSON with flood count + nearest headline

Usage:
  python gauge_flood_join.py                  # default 50-mile radius
  python gauge_flood_join.py --radius 25      # tighter radius
  python gauge_flood_join.py --alerts-only    # only action/flood/major tier gauges
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
DATA_DIR    = Path("data")
GAUGES_FILE = DATA_DIR / "gauges_current.json"
FLOODS_CSV  = DATA_DIR / "floods.csv"
OUT_JSON    = DATA_DIR / "gauges_with_floods.json"
OUT_CSV     = DATA_DIR / "gauges_with_floods.csv"
OUT_GEOJSON = DATA_DIR / "gauges_with_floods.geojson"


# ── Haversine (miles) ─────────────────────────────────────────
def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 2 * asin(sqrt(a)) * 3958.8


# ── Loaders ───────────────────────────────────────────────────
def load_gauges(path, alerts_only):
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


def load_floods(path):
    if not path.exists():
        log.error(f"Floods CSV not found: {path}")
        return []
    floods = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Try all plausible lat/lon column name variants
            raw_lat = row.get("lat") or row.get("latitude") or row.get("Lat") or row.get("Latitude") or ""
            raw_lon = row.get("lon") or row.get("longitude") or row.get("Lon") or row.get("Longitude") or ""
            try:
                f_lat = float(raw_lat)
                f_lon = float(raw_lon)
            except (ValueError, TypeError):
                continue  # skip rows with missing/bad coords
            # Store parsed floats under mangled keys that can't collide with CSV columns
            row["__parsed_lat__"] = f_lat
            row["__parsed_lon__"] = f_lon
            floods.append(row)
    log.info(f"Loaded {len(floods)} flood events with valid coordinates")
    return floods


# ── Core join ─────────────────────────────────────────────────
def proximity_join(gauges, floods, radius_miles):
    enriched = []
    for g in gauges:
        # Safely coerce gauge coords — JSON may have serialised them as strings
        try:
            g_lat = float(g["lat"])
            g_lon = float(g["lon"])
        except (KeyError, TypeError, ValueError):
            g_lat = g_lon = None

        if g_lat is None or g_lon is None:
            g_out = g.copy()
            g_out["nearby_floods"] = []
            g_out["flood_event_count"] = 0
            g_out["nearest_flood_miles"] = None
            g_out["nearest_flood_title"] = None
            enriched.append(g_out)
            continue

        nearby = []
        for fl in floods:
            f_lat = fl["__parsed_lat__"]
            f_lon = fl["__parsed_lon__"]
            dist = haversine(g_lat, g_lon, f_lat, f_lon)
            if dist <= radius_miles:
                entry = {k: v for k, v in fl.items() if not k.startswith("__parsed_")}
                entry["distance_miles"] = round(dist, 2)
                nearby.append(entry)

        nearby.sort(key=lambda x: x["distance_miles"])

        g_out = g.copy()
        g_out["nearby_floods"] = nearby
        g_out["flood_event_count"] = len(nearby)
        g_out["nearest_flood_miles"] = nearby[0]["distance_miles"] if nearby else None
        g_out["nearest_flood_title"] = (
            (nearby[0].get("title") or nearby[0].get("headline") or
             str(nearby[0].get("description", ""))[:80])
            if nearby else None
        )
        enriched.append(g_out)

    total_links = sum(g["flood_event_count"] for g in enriched)
    log.info(
        f"Join complete: {total_links} gauge x flood links across "
        f"{len(enriched)} gauges (radius={radius_miles} mi)"
    )
    return enriched


# ── Writers ───────────────────────────────────────────────────
def write_json(enriched, path):
    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "gauge_count": len(enriched),
        "gauges": enriched,
    }
    with open(path, "w") as f:
        json.dump(out, f, indent=2, default=str)
    log.info(f"Wrote {path}")


def write_csv(enriched, path):
    """One row per gauge x flood pair; gauges with no nearby floods get one empty row."""
    # Collect all flood field names
    flood_field_names = []
    seen = set()
    for g in enriched:
        for fl in g.get("nearby_floods", []):
            for k in fl:
                if k not in seen and not k.startswith("__parsed_"):
                    flood_field_names.append(k)
                    seen.add(k)

    # Gauge scalar fields
    skip = {"nearby_floods", "flood_event_count", "nearest_flood_miles", "nearest_flood_title"}
    gauge_field_names = []
    seen_g = set()
    for g in enriched:
        for k in g:
            if k not in seen_g and k not in skip:
                gauge_field_names.append(k)
                seen_g.add(k)

    summary_fields = ["flood_event_count", "nearest_flood_miles", "nearest_flood_title"]
    fieldnames = gauge_field_names + summary_fields + flood_field_names

    rows = []
    for g in enriched:
        base = {k: g.get(k, "") for k in gauge_field_names}
        base["flood_event_count"] = g["flood_event_count"]
        base["nearest_flood_miles"] = g["nearest_flood_miles"] if g["nearest_flood_miles"] is not None else ""
        base["nearest_flood_title"] = g["nearest_flood_title"] or ""

        if g["nearby_floods"]:
            for fl in g["nearby_floods"]:
                row = base.copy()
                for ff in flood_field_names:
                    row[ff] = fl.get(ff, "")
                rows.append(row)
        else:
            row = base.copy()
            for ff in flood_field_names:
                row[ff] = ""
            rows.append(row)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    log.info(f"Wrote {path}  ({len(rows)} rows)")


def write_geojson(enriched, path):
    tier_colors = {
        "normal":  "#2d6a4f",
        "action":  "#f48c06",
        "flood":   "#e85d04",
        "major":   "#c1121f",
        "unknown": "#888888",
    }
    features = []
    for g in enriched:
        try:
            lat = float(g["lat"])
            lon = float(g["lon"])
        except (KeyError, TypeError, ValueError):
            continue
        props = {k: v for k, v in g.items() if k != "nearby_floods"}
        props["color"] = tier_colors.get(g.get("tier", "unknown"), "#888888")
        props["nearby_flood_titles"] = [
            fl.get("title") or fl.get("headline") or str(fl.get("description", ""))[:80]
            for fl in g.get("nearby_floods", [])
        ]
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        })
    with open(path, "w") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f, indent=2, default=str)
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

    with_events = [g for g in enriched if g["flood_event_count"] > 0]
    log.info(
        f"Summary: {len(with_events)}/{len(enriched)} gauges have nearby flood "
        f"events within {args.radius} mi"
    )
    if with_events:
        top = sorted(with_events, key=lambda x: x["flood_event_count"], reverse=True)[:5]
        log.info("Top gauges by nearby flood event count:")
        for g in top:
            log.info(
                f"  {g.get('site_name', g.get('site_id'))} "
                f"[{g.get('tier')}] -> {g['flood_event_count']} events, "
                f"nearest: {g['nearest_flood_miles']} mi"
            )


if __name__ == "__main__":
    main()
