"""
fetch_hefs.py
=============
FloodWire2 — HEFS site ETL

Strategy
--------
The HEFS API (api.water.noaa.gov/hefs/v1) returns only opaque numeric
location_ids with no geo data.  To plot sites on a map we need coordinates.

This script uses THREE geo sources in priority order:

  1. NWPS gauges API (api.water.noaa.gov/nwps/v1/gauges/) — preferred, has
     lat/lng + NWS LIDs.  Often 503 in production; handled gracefully.

  2. NWS RFC station list — a stable public CSV hosted on weather.gov that
     maps NWS LIDs to lat/lng.  Used as fallback when NWPS is down.

  3. USGS NWIS site service — used to fill remaining gaps by NWS LID.

The HEFS /locations/ endpoint gives us the set of active location_ids and
which parameters (QINE = streamflow ensemble) they carry.  We filter to only
streamflow-capable sites, then attempt to resolve geo via the above sources.

Output
------
  data/hefs_sites.geojson   — GeoJSON FeatureCollection, one point per site
                              with properties: location_id, nws_lid, name,
                              latitude, longitude, parameters, source

Run
---
  python fetch_hefs.py            # normal run
  python fetch_hefs.py --dry-run  # fetch + print summary, no file write
"""

import json
import csv
import sys
import time
import logging
import argparse
import io
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fetch_hefs")

# ── Paths ─────────────────────────────────────────────────────────────────────
DATA_DIR  = Path("data")
DATA_DIR.mkdir(exist_ok=True)
OUT_FILE  = DATA_DIR / "hefs_sites.geojson"

# ── API endpoints ─────────────────────────────────────────────────────────────
HEFS_BASE      = "https://api.water.noaa.gov/hefs/v1"
HEFS_LOCATIONS = HEFS_BASE + "/locations/"
HEFS_FORECAST  = HEFS_BASE + "/forecasts/"

NWPS_GAUGES    = "https://api.water.noaa.gov/nwps/v1/gauges/"

# NWS RFC station list — published by NWS, rarely changes
# Maps nws_lid -> name, lat, lon, state, rfc
NWS_STATION_CSV = (
    "https://water.noaa.gov/resources/downloads/data/nwsli.csv"
)

# USGS site service — fallback for individual LID lookups
USGS_SITE_URL = "https://waterservices.usgs.gov/nwis/site/"

TIMEOUT   = 20
PAGE_SIZE = 500
MAX_PAGES = 40   # 40 × 500 = 20,000 max locations

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "FloodWire2-HEFS-ETL/1.0 (github.com/rmkenv/floodwire2)"})


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_json(url, params=None, retries=3, backoff=2.0):
    """GET with retry + exponential back-off. Returns parsed JSON or None."""
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            log.warning("HTTP %s for %s", r.status_code, url)
            return None
        except Exception as e:
            log.warning("Attempt %d failed (%s): %s", attempt + 1, url, e)
            if attempt < retries - 1:
                time.sleep(backoff * (attempt + 1))
    return None


def get_text(url, retries=3, backoff=2.0):
    """GET raw text with retry."""
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.text
            log.warning("HTTP %s for %s", r.status_code, url)
            return None
        except Exception as e:
            log.warning("Attempt %d failed (%s): %s", attempt + 1, url, e)
            if attempt < retries - 1:
                time.sleep(backoff * (attempt + 1))
    return None


# ── Step 1: Fetch HEFS location IDs ──────────────────────────────────────────

def fetch_hefs_location_ids():
    """
    Returns list of dicts: {location_id, parameters: [...]}
    Filters to sites that have QINE (streamflow ensemble) parameter.
    Falls back to all sites if QINE is absent everywhere (API may change).
    """
    log.info("Fetching HEFS location list (paginated, page_size=%d)…", PAGE_SIZE)
    all_locs = []

    for page in range(MAX_PAGES):
        offset = page * PAGE_SIZE
        data = get_json(HEFS_LOCATIONS, params={"limit": PAGE_SIZE, "offset": offset})
        if data is None:
            log.error("HEFS locations fetch failed at offset %d — aborting", offset)
            break

        # API returns a bare array
        page_locs = data if isinstance(data, list) else (
            data.get("locations") or data.get("data") or []
        )
        all_locs.extend(page_locs)
        log.info("  page %d: %d locations (total so far: %d)", page, len(page_locs), len(all_locs))

        if len(page_locs) < PAGE_SIZE:
            break   # last page

    log.info("Total HEFS location_ids fetched: %d", len(all_locs))

    # Filter to streamflow sites if QINE present anywhere
    qine_sites = [
        loc for loc in all_locs
        if any(p.get("parameter_id") == "QINE" for p in loc.get("parameters", []))
    ]
    if qine_sites:
        log.info("Filtered to %d QINE (streamflow) sites", len(qine_sites))
        return qine_sites

    # No QINE found — QINE may be named differently; return all for now
    log.warning("No QINE parameter found — returning all %d sites (check API schema)", len(all_locs))
    return all_locs


# ── Step 2a: Try NWPS gauges API for geo lookup ───────────────────────────────

def fetch_nwps_geo_table():
    """
    Returns dict: nws_lid (upper) -> {lat, lon, name, state}
    Uses NWPS /gauges/ paginated endpoint.  Returns {} if unavailable.
    """
    log.info("Trying NWPS gauges API for geo data…")
    table = {}
    limit = 1000

    for page in range(200):   # up to 200k gauges
        offset = page * limit
        data = get_json(NWPS_GAUGES, params={"limit": limit, "offset": offset})
        if data is None:
            log.warning("NWPS gauges API unavailable — will use RFC station CSV fallback")
            return {}

        gauges = data if isinstance(data, list) else (
            data.get("gauges") or data.get("data") or []
        )
        for g in gauges:
            lid = (g.get("nwsLid") or g.get("lid") or g.get("id") or "").upper().strip()
            lat = g.get("latitude") or g.get("lat")
            lon = g.get("longitude") or g.get("lon") or g.get("lng")
            name = g.get("name") or g.get("stationName") or lid
            state = g.get("state") or g.get("stateCd") or ""
            if lid and lat and lon:
                table[lid] = {
                    "lat": float(lat), "lon": float(lon),
                    "name": name, "state": state
                }

        if len(gauges) < limit:
            break
        offset += limit

    log.info("NWPS geo table: %d gauges", len(table))
    return table


# ── Step 2b: NWS RFC station CSV fallback ────────────────────────────────────

def fetch_nws_station_csv():
    """
    Downloads the NWS LID station CSV and returns:
      dict: nws_lid (upper) -> {lat, lon, name, state, rfc}
    """
    log.info("Fetching NWS RFC station CSV…")
    text = get_text(NWS_STATION_CSV)
    if not text:
        log.warning("NWS station CSV unavailable")
        return {}

    table = {}
    reader = csv.DictReader(io.StringIO(text))

    # Column names vary — probe common variants
    def pick(row, *keys):
        for k in keys:
            for rk in row:
                if rk.strip().lower() == k.lower():
                    return row[rk].strip()
        return ""

    for row in reader:
        lid   = pick(row, "nwsli", "nws_lid", "lid", "id").upper()
        lat   = pick(row, "latitude", "lat", "dec_lat_va")
        lon   = pick(row, "longitude", "lon", "lng", "dec_long_va")
        name  = pick(row, "name", "station_name", "site_name", "description")
        state = pick(row, "state", "state_cd", "stateCd")
        rfc   = pick(row, "rfc", "rfc_id", "hb")
        if lid and lat and lon:
            try:
                table[lid] = {
                    "lat": float(lat), "lon": float(lon),
                    "name": name or lid, "state": state, "rfc": rfc
                }
            except ValueError:
                pass

    log.info("NWS station CSV: %d stations", len(table))
    return table


# ── Step 2c: HEFS forecast endpoint reveals NWS LID ──────────────────────────

def probe_hefs_forecast_for_lid(location_id, sample_size=20):
    """
    The HEFS /forecasts/ endpoint may accept NWS LIDs directly.
    Probe a small sample of numeric IDs to see if the response contains
    a nws_lid or similar field that lets us build the mapping.
    Returns dict: location_id -> nws_lid, or {} if not found.
    """
    log.info("Probing HEFS forecast responses for NWS LID field (sample=%d)…", sample_size)
    mapping = {}
    ids = location_id[:sample_size]

    for loc in ids:
        lid_num = loc["location_id"]
        url = f"{HEFS_FORECAST}{lid_num}/quantiles/?type=max&duration=10d"
        data = get_json(url)
        if data is None:
            continue
        # Look for any field that looks like an NWS LID (4–5 alpha chars)
        for key in ("nwsLid", "nws_lid", "lid", "locationId", "location_id",
                    "stationId", "station_id", "gaugeId"):
            val = data.get(key) or (data.get("metadata") or {}).get(key)
            if val and isinstance(val, str) and val.upper() != lid_num:
                mapping[lid_num] = val.upper()
                break
        time.sleep(0.1)   # be polite

    log.info("NWS LID mapping from forecast probing: %d / %d resolved", len(mapping), len(ids))
    return mapping


# ── Step 3: Build GeoJSON ─────────────────────────────────────────────────────

def build_geojson(hefs_locs, geo_table, lid_mapping, source_label):
    """
    Joins HEFS location list to geo_table via lid_mapping.
    Returns GeoJSON FeatureCollection dict.
    """
    features = []
    unresolved = 0

    for loc in hefs_locs:
        loc_id  = loc["location_id"]
        nws_lid = lid_mapping.get(loc_id, "")
        geo     = geo_table.get(nws_lid) if nws_lid else None

        if geo is None:
            unresolved += 1
            continue

        params = [p["parameter_id"] for p in loc.get("parameters", [])]
        last_updated = max(
            (p.get("last_updated_datetime", "") for p in loc.get("parameters", [])),
            default=""
        )

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [geo["lon"], geo["lat"]]
            },
            "properties": {
                "location_id":   loc_id,
                "nws_lid":       nws_lid,
                "name":          geo.get("name", nws_lid),
                "state":         geo.get("state", ""),
                "rfc":           geo.get("rfc", ""),
                "latitude":      geo["lat"],
                "longitude":     geo["lon"],
                "parameters":    params,
                "last_updated":  last_updated,
                "geo_source":    source_label,
            }
        })

    log.info("Built %d features (%d unresolved / no geo match)", len(features), unresolved)
    return {
        "type": "FeatureCollection",
        "generated": datetime.now(timezone.utc).isoformat(),
        "feature_count": len(features),
        "features": features
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Fetch HEFS sites → hefs_sites.geojson")
    ap.add_argument("--dry-run", action="store_true", help="Don't write output file")
    args = ap.parse_args()

    # 1. Get HEFS location IDs
    hefs_locs = fetch_hefs_location_ids()
    if not hefs_locs:
        log.error("No HEFS locations fetched — exiting")
        sys.exit(1)

    # 2. Probe forecast endpoint for NWS LID mapping
    lid_mapping = probe_hefs_forecast_for_lid(hefs_locs, sample_size=30)

    # If probing revealed no LIDs, the HEFS numeric IDs may directly be LIDs
    # (unlikely but possible if API changes) — try treating them as LIDs
    if not lid_mapping:
        log.warning("Forecast probing yielded no LID mapping — treating location_ids as LIDs directly")
        lid_mapping = {loc["location_id"]: loc["location_id"] for loc in hefs_locs}

    # 3. Get geo table — NWPS first, then RFC CSV
    geo_table = fetch_nwps_geo_table()
    source_label = "nwps"

    if not geo_table:
        geo_table = fetch_nws_station_csv()
        source_label = "nws_rfc_csv"

    if not geo_table:
        log.error("All geo sources failed — cannot produce output")
        sys.exit(1)

    # 4. Build GeoJSON
    gj = build_geojson(hefs_locs, geo_table, lid_mapping, source_label)

    log.info("Feature count: %d", gj["feature_count"])

    if gj["feature_count"] == 0:
        log.warning(
            "Zero features resolved. The HEFS numeric location_ids (%s…) "
            "did not match any NWS LIDs in the geo table. "
            "Manual mapping table may be needed — see docs/HEFS_MAPPING.md",
            ", ".join(loc["location_id"] for loc in hefs_locs[:5])
        )

    if args.dry_run:
        log.info("Dry run — skipping file write")
        print(json.dumps(gj, indent=2)[:2000])
        return

    OUT_FILE.write_text(json.dumps(gj, separators=(",", ":")))
    log.info("Written: %s (%d bytes)", OUT_FILE, OUT_FILE.stat().st_size)


if __name__ == "__main__":
    main()
