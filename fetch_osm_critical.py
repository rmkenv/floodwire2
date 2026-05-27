"""
fetch_osm_critical.py
=====================
FloodWire2 — OSM Critical Infrastructure ETL

Queries the Overpass API for hospitals, clinics, fire stations, police,
emergency services, and major roads within the bounding box of all active
flood events in data/floods.geojson.  Writes data/osm_critical.geojson.

Coverage is dynamic — it expands/contracts nightly as flood events change,
keeping the file small while covering exactly where it matters.

Run
---
  python fetch_osm_critical.py
  python fetch_osm_critical.py --dry-run    # print summary, no write
  python fetch_osm_critical.py --bbox "37,-80,40,-74"  # override bbox
"""

import json
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fetch_osm")

DATA_DIR  = Path("data")
DATA_DIR.mkdir(exist_ok=True)

FLOODS_FILE = DATA_DIR / "floods.geojson"
OUT_FILE    = DATA_DIR / "osm_critical.geojson"

OVERPASS_URL     = "https://overpass-api.de/api/interpreter"
OVERPASS_TIMEOUT = 60
BBOX_BUFFER_DEG  = 0.25    # ~25 km padding around flood event bbox

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "FloodWire2-OSM-ETL/1.0 (github.com/rmkenv/floodwire2)"
})


# ── Bbox from flood events ────────────────────────────────────────────────────

def flood_bbox():
    """
    Returns (min_lat, min_lon, max_lat, max_lon) covering all flood event
    geometries in floods.geojson, padded by BBOX_BUFFER_DEG.
    Falls back to CONUS bbox if file is missing or empty.
    """
    CONUS = (24.0, -125.0, 50.0, -66.0)

    if not FLOODS_FILE.exists():
        log.warning("floods.geojson not found — using CONUS fallback bbox")
        return CONUS

    try:
        gj = json.loads(FLOODS_FILE.read_text())
    except Exception as e:
        log.warning("Could not parse floods.geojson (%s) — using CONUS bbox", e)
        return CONUS

    lats, lons = [], []
    for f in gj.get("features", []):
        g = f.get("geometry") or {}
        coords = []
        if g.get("type") == "Point":
            coords = [g["coordinates"]]
        elif g.get("type") == "LineString":
            coords = g["coordinates"]
        elif g.get("type") == "Polygon":
            coords = g["coordinates"][0]
        for lon, lat in coords:
            lats.append(lat); lons.append(lon)

    if not lats:
        log.warning("No geometries in floods.geojson — using CONUS bbox")
        return CONUS

    min_lat = max(min(lats) - BBOX_BUFFER_DEG, -90)
    max_lat = min(max(lats) + BBOX_BUFFER_DEG,  90)
    min_lon = max(min(lons) - BBOX_BUFFER_DEG, -180)
    max_lon = min(max(lons) + BBOX_BUFFER_DEG,  180)

    log.info("Flood bbox (buffered): %.3f,%.3f → %.3f,%.3f", min_lat, min_lon, max_lat, max_lon)
    return (min_lat, min_lon, max_lat, max_lon)


# ── Overpass query ────────────────────────────────────────────────────────────

def overpass_query(bbox):
    """
    Build and run an Overpass QL query for critical infrastructure.
    Returns list of GeoJSON Feature dicts.
    """
    min_lat, min_lon, max_lat, max_lon = bbox
    bb = f"{min_lat},{min_lon},{max_lat},{max_lon}"

    query = f"""
[out:json][timeout:{OVERPASS_TIMEOUT}];
(
  // Hospitals & medical
  node[amenity=hospital]({bb});
  way[amenity=hospital]({bb});
  node[amenity=clinic]({bb});
  node[amenity=doctors]({bb});
  node[healthcare=hospital]({bb});

  // Emergency services
  node[amenity=fire_station]({bb});
  node[amenity=police]({bb});
  node[amenity=ambulance_station]({bb});
  node[emergency=yes]({bb});

  // Shelters
  node[amenity=shelter]({bb});
  node[social_facility=shelter]({bb});

  // Key roads (ways only — we take centroid)
  way[highway=motorway]({bb});
  way[highway=trunk]({bb});
  way[highway=primary]({bb});

  // Bridges (flood-critical)
  way[bridge=yes][highway]({bb});
);
out center tags;
"""

    log.info("Querying Overpass API (bbox area ~%.0f×%.0f deg)…",
             max_lat - min_lat, max_lon - min_lon)

    for attempt in range(3):
        try:
            res = SESSION.post(OVERPASS_URL, data={"data": query}, timeout=OVERPASS_TIMEOUT + 10)
            if res.status_code == 200:
                return res.json().get("elements", [])
            log.warning("Overpass HTTP %s (attempt %d)", res.status_code, attempt + 1)
        except Exception as e:
            log.warning("Overpass error (attempt %d): %s", attempt + 1, e)
        time.sleep(5 * (attempt + 1))

    return []


# ── Convert Overpass elements → GeoJSON ──────────────────────────────────────

def to_geojson(elements):
    features = []
    for el in elements:
        # Ways come back with `center` lat/lon
        if el["type"] == "way":
            c = el.get("center", {})
            lat, lon = c.get("lat"), c.get("lon")
        else:
            lat, lon = el.get("lat"), el.get("lon")

        if lat is None or lon is None:
            continue

        tags = el.get("tags", {})
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "osm_id":   el.get("id"),
                "osm_type": el["type"],
                "name":     tags.get("name", ""),
                "amenity":  tags.get("amenity", ""),
                "highway":  tags.get("highway", ""),
                "healthcare": tags.get("healthcare", ""),
                "bridge":   tags.get("bridge", ""),
                "emergency": tags.get("emergency", ""),
                "operator": tags.get("operator", ""),
                "addr_city": tags.get("addr:city", ""),
            }
        })
    return features


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--bbox", help="Override bbox as 'min_lat,min_lon,max_lat,max_lon'")
    args = ap.parse_args()

    if args.bbox:
        parts = [float(x) for x in args.bbox.split(",")]
        bbox = tuple(parts)
        log.info("Using override bbox: %s", bbox)
    else:
        bbox = flood_bbox()

    elements = overpass_query(bbox)
    log.info("Overpass returned %d elements", len(elements))

    features = to_geojson(elements)
    log.info("Converted to %d GeoJSON features", len(features))

    gj = {
        "type": "FeatureCollection",
        "generated": datetime.now(timezone.utc).isoformat(),
        "bbox": list(bbox),
        "feature_count": len(features),
        "features": features
    }

    if args.dry_run:
        log.info("Dry run — not writing file")
        for f in features[:5]:
            p = f["properties"]
            print(f"  {p['name'] or '(unnamed)'} [{p['amenity'] or p['highway'] or 'road'}]"
                  f" @ {f['geometry']['coordinates']}")
        return

    OUT_FILE.write_text(json.dumps(gj, separators=(",", ":")))
    log.info("Written: %s (%d bytes, %d features)", OUT_FILE, OUT_FILE.stat().st_size, len(features))


if __name__ == "__main__":
    main()
