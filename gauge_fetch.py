"""
gauge_fetch.py
==============
Floodwire2 gauge module — USGS NWIS + NOAA CO-OPS real-time data

Drops into the existing floodwire2 workflow alongside the flood news
ETL pipeline. Run standalone or import fetch_all_gauges() from the
main pipeline script.

Output files (same flat-file pattern as the rest of floodwire2):
  data/gauges_current.json      — latest reading per gauge (overwrites)
  data/gauges_alerts.json       — only gauges at/above action stage
  data/gauges_history.csv       — rolling 7-day time series, appended daily

No database. No Docker. Free APIs. No auth required.

Usage:
  python gauge_fetch.py                 # run standalone
  python gauge_fetch.py --alerts-only   # only write alerts file
  python gauge_fetch.py --refresh-stations  # refresh USGS and NOAA station metadata

GitHub Actions: add a step to your existing nightly.yml:
  - name: Fetch gauge data
    run: python gauge_fetch.py
"""

import json
import csv
import sys
import time
import logging
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from math import radians, cos, sin, asin, sqrt

import requests

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("gauge_fetch")

# ── Output paths (mirrors floodwire2 data/ layout) ───────────
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

CURRENT_FILE = DATA_DIR / "gauges_current.json"
ALERTS_FILE  = DATA_DIR / "gauges_alerts.json"
HISTORY_FILE = DATA_DIR / "gauges_history.csv"
GEOJSON_FILE = DATA_DIR / "floods.geojson"

USGS_STATIONS_FILE = DATA_DIR / "usgs_stations.json"
NOAA_STATIONS_FILE = DATA_DIR / "noaa_stations.json"

# ── API config ────────────────────────────────────────────────
USGS_IV_URL  = "https://waterservices.usgs.gov/nwis/iv/"
NOAA_API_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
USGS_SITE_URL= "https://waterservices.usgs.gov/nwis/site/"
REQUEST_TIMEOUT = 20
RETRY_WAIT      = 3
MAX_RETRIES     = 2

# ── Alert tier thresholds ─────────────────────────────────────
TIERS = ["normal", "action", "flood", "major"]

# ── Utility: Haversine distance ──────────────────────────────
def haversine(lat1, lon1, lat2, lon2):
    """Calculate the great circle distance in miles between two points."""
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 3958.8  # Radius of earth in miles
    return c * r

# ── HTTP GET with retries ─────────────────────────────────────
def get_json(url: str, params: dict = None) -> dict | None:
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT,
                             headers={"User-Agent": "floodwire2-gauge-fetch/1.0"})
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            log.warning("Timeout on %s (attempt %d)", url[:60], attempt + 1)
        except requests.exceptions.HTTPError as e:
            log.warning("HTTP %s for %s", e.response.status_code, url[:60])
            return None
        except Exception as e:
            log.warning("Request error: %s", e)
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_WAIT)
    return None

# ── Download and cache USGS stations metadata ────────────────
def download_usgs_stations():
    log.info("Downloading USGS station metadata...")
    url = "https://waterservices.usgs.gov/nwis/site/?format=rdb&siteType=ST&siteStatus=active"
    r = requests.get(url)
    if r.status_code != 200:
        log.error("Failed to download USGS stations metadata")
        return []

    lines = r.text.splitlines()
    stations = []
    headers = []
    for line in lines:
        if line.startswith("#") or line.strip() == "":
            continue
        if line.startswith("agency_cd"):
            headers = line.split("\t")
            continue
        parts = line.split("\t")
        if len(parts) != len(headers):
            continue
        record = dict(zip(headers, parts))
        try:
            lat = float(record.get("dec_lat_va", ""))
            lon = float(record.get("dec_long_va", ""))
            site_no = record.get("site_no", "")
            if lat and lon and site_no:
                stations.append({"site_id": site_no, "lat": lat, "lon": lon})
        except ValueError:
            continue

    with open(USGS_STATIONS_FILE, "w") as f:
        json.dump(stations, f)
    log.info(f"Saved {len(stations)} USGS stations to {USGS_STATIONS_FILE}")
    return stations

# ── Download and cache NOAA stations metadata ────────────────
def download_noaa_stations():
    log.info("Downloading NOAA station metadata...")
    url = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json?type=water_level"
    data = get_json(url)
    if not data:
        log.error("Failed to download NOAA stations metadata")
        return []

    stations = []
    for s in data.get("stations", []):
        try:
            lat = float(s.get("lat"))
            lon = float(s.get("lng"))
            station_id = s.get("id")
            if lat and lon and station_id:
                stations.append({"station_id": station_id, "lat": lat, "lon": lon})
        except (ValueError, TypeError):
            continue

    with open(NOAA_STATIONS_FILE, "w") as f:
        json.dump(stations, f)
    log.info(f"Saved {len(stations)} NOAA stations to {NOAA_STATIONS_FILE}")
    return stations

# ── Load cached USGS stations or download if missing ─────────
def load_usgs_stations():
    if USGS_STATIONS_FILE.exists():
        with open(USGS_STATIONS_FILE) as f:
            return json.load(f)
    else:
        return download_usgs_stations()

# ── Load cached NOAA stations or download if missing ─────────
def load_noaa_stations():
    if NOAA_STATIONS_FILE.exists():
        with open(NOAA_STATIONS_FILE) as f:
            return json.load(f)
    else:
        return download_noaa_stations()

# ── Find nearest station by lat/lon ──────────────────────────
def find_nearest_station(lat, lon, stations, id_key):
    min_dist = float('inf')
    nearest = None
    for s in stations:
        dist = haversine(lat, lon, s["lat"], s["lon"])
        if dist < min_dist:
            min_dist = dist
            nearest = s
    if nearest:
        return nearest[id_key]
    return None

# ── Load lat/lon points from GeoJSON ──────────────────────────
def load_points_from_geojson(path):
    if not path.exists():
        log.error(f"GeoJSON file not found: {path}")
        return []
    with open(path) as f:
        gj = json.load(f)
    points = []
    for feature in gj.get("features", []):
        coords = feature.get("geometry", {}).get("coordinates", [])
        if len(coords) >= 2:
            lon, lat = coords[0], coords[1]
            points.append({"lat": lat, "lon": lon})
    log.info(f"Loaded {len(points)} points from {path}")
    return points

# ── USGS gauge fetch (unchanged from your original script) ────
def fetch_usgs_gauge(site_id: str) -> dict | None:
    data = get_json(USGS_IV_URL, {
        "format":      "json",
        "sites":       site_id,
        "parameterCd": "00060,00065",
        "siteStatus":  "active",
        "period":      "P7D",
    })
    if not data:
        return None

    ts_list = data.get("value", {}).get("timeSeries", [])
    if not ts_list:
        return None

    source_info = ts_list[0].get("sourceInfo", {})
    site_name   = source_info.get("siteName", site_id)
    geo         = source_info.get("geoLocation", {}).get("geogLocation", {})
    site_lat    = geo.get("latitude")
    site_lon    = geo.get("longitude")

    stage_ts    = next((s for s in ts_list if s["variable"]["variableCode"][0]["value"] == "00065"), None)
    stage       = None
    stage_unit  = "ft"
    stage_dt    = None
    stage_history = []

    if stage_ts:
        vals = stage_ts.get("values", [{}])[0].get("value", [])
        if vals:
            last        = vals[-1]
            raw_val     = last.get("value", "")
            stage_dt    = last.get("dateTime")
            try:
                stage   = float(raw_val)
            except (ValueError, TypeError):
                stage   = None
            for v in vals:
                try:
                    stage_history.append({
                        "dt": v["dateTime"],
                        "stage_ft": float(v["value"]),
                    })
                except (ValueError, TypeError):
                    pass

    flow_ts  = next((s for s in ts_list if s["variable"]["variableCode"][0]["value"] == "00060"), None)
    discharge_cfs = None
    if flow_ts:
        vals = flow_ts.get("values", [{}])[0].get("value", [])
        if vals:
            try:
                discharge_cfs = float(vals[-1]["value"])
            except (ValueError, TypeError):
                pass

    thresholds  = fetch_usgs_thresholds(site_id)
    tier = classify_tier(stage, thresholds)

    return {
        "source":         "USGS",
        "type":           "stream",
        "site_id":        site_id,
        "site_name":      site_name,
        "lat":            site_lat,
        "lon":            site_lon,
        "stage_ft":       stage,
        "stage_unit":     stage_unit,
        "stage_datetime": stage_dt,
        "discharge_cfs":  discharge_cfs,
        "action_stage":   thresholds.get("action"),
        "flood_stage":    thresholds.get("flood"),
        "major_stage":    thresholds.get("major"),
        "tier":           tier,
        "usgs_url":       f"https://waterdata.usgs.gov/monitoring-location/{site_id}/",
        "history":        stage_history,
    }

def fetch_usgs_thresholds(site_id: str) -> dict:
    data = get_json(USGS_SITE_URL, {
        "format":        "rdb",
        "sites":         site_id,
        "siteOutput":    "expanded",
        "hasDataTypeCd": "st",
    })
    return _fetch_waterwatch_thresholds(site_id)

def _fetch_waterwatch_thresholds(site_id: str) -> dict:
    data = get_json("https://waterwatch.usgs.gov/webservices/floodstage",
                    {"format": "json"})
    thresholds = {}
    if not data:
        return thresholds

    for site in data.get("site", []):
        if site.get("site_no") == site_id:
            try:
                if site.get("action_stage") not in (None, "", "-999"):
                    thresholds["action"] = float(site["action_stage"])
                if site.get("flood_stage") not in (None, "", "-999"):
                    thresholds["flood"] = float(site["flood_stage"])
                if site.get("major_flood_stage") not in (None, "", "-999"):
                    thresholds["major"] = float(site["major_flood_stage"])
            except (ValueError, TypeError):
                pass
            break

    return thresholds

# ── NOAA gauge fetch (unchanged from your original script) ────
def fetch_noaa_gauge(station_id: str) -> dict | None:
    now   = datetime.now(timezone.utc)
    begin = now - timedelta(days=7)
    fmt = lambda d: d.strftime("%Y%m%d")

    data = get_json(NOAA_API_URL, {
        "station":    station_id,
        "product":    "water_level",
        "datum":      "MLLW",
        "time_zone":  "gmt",
        "units":      "english",
        "format":     "json",
        "begin_date": fmt(begin),
        "end_date":   fmt(now),
    })

    if not data or "error" in data:
        err = data.get("error", {}).get("message", "unknown") if data else "no response"
        log.debug("NOAA %s: %s", station_id, err)
        return None

    readings = data.get("data", [])
    if not readings:
        return None

    meta         = data.get("metadata", {})
    station_name = meta.get("name", station_id)
    lat          = meta.get("lat")
    lon          = meta.get("lon")

    history = []
    for r in readings:
        try:
            history.append({
                "dt":       r["t"],
                "stage_ft": float(r["v"]),
            })
        except (ValueError, TypeError, KeyError):
            pass

    if not history:
        return None

    latest     = history[-1]
    stage      = latest["stage_ft"]
    stage_dt   = latest["dt"]

    mhhw = _fetch_noaa_datum(station_id, "MHHW")

    thresholds = {}
    if mhhw is not None:
        thresholds = {
            "action": round(mhhw + 1.0, 2),
            "flood":  round(mhhw + 2.0, 2),
            "major":  round(mhhw + 4.0, 2),
        }

    tier = classify_tier(stage, thresholds)

    return {
        "source":         "NOAA",
        "type":           "tidal",
        "site_id":        station_id,
        "site_name":      station_name,
        "lat":            lat,
        "lon":            lon,
        "stage_ft":       stage,
        "stage_unit":     "ft MLLW",
        "stage_datetime": stage_dt,
        "discharge_cfs":  None,
        "action_stage":   thresholds.get("action"),
        "flood_stage":    thresholds.get("flood"),
        "major_stage":    thresholds.get("major"),
        "mhhw_ft":        mhhw,
        "tier":           tier,
        "noaa_url":       f"https://tidesandcurrents.noaa.gov/stationhome.html?id={station_id}",
        "history":        history,
    }

def _fetch_noaa_datum(station_id: str, datum_name: str) -> float | None:
    data = get_json(
        "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/"
        f"{station_id}/datums.json",
        {"units": "english"}
    )
    if not data:
        return None
    for d in data.get("datums", []):
        if d.get("name") == datum_name:
            try:
                return float(d["value"])
            except (ValueError, TypeError):
                return None
    return None

# ── Tier classification ───────────────────────────────────────
def classify_tier(stage: float | None, thresholds: dict) -> str:
    if stage is None:
        return "unknown"
    major  = thresholds.get("major")
    flood  = thresholds.get("flood")
    action = thresholds.get("action")
    if major  is not None and stage >= major:
        return "major"
    if flood  is not None and stage >= flood:
        return "flood"
    if action is not None and stage >= action:
        return "action"
    return "normal"

# ── Main fetch loop using points and nearest stations ─────────
def fetch_all_gauges_from_points(points):
    usgs_stations = load_usgs_stations()
    noaa_stations = load_noaa_stations()

    all_gauges = []
    alert_gauges = []
    run_dt = datetime.now(timezone.utc).isoformat()

    log.info(f"Finding nearest stations and fetching data for {len(points)} points")

    for idx, pt in enumerate(points, 1):
        lat, lon = pt["lat"], pt["lon"]
        usgs_id = find_nearest_station(lat, lon, usgs_stations, "site_id")
        noaa_id = find_nearest_station(lat, lon, noaa_stations, "station_id")

        tiers = []

        if usgs_id:
            log.info(f"Point {idx}: Nearest USGS site {usgs_id} at ({lat},{lon})")
            g = fetch_usgs_gauge(usgs_id)
            if g:
                g["city"] = ""
                g["state"] = ""
                g["fema_zone"] = ""
                g["city_lat"] = lat
                g["city_lon"] = lon
                g["fetched_at"] = run_dt
                all_gauges.append(g)
                tiers.append(g["tier"])
                if g["tier"] in ("action", "flood", "major"):
                    alert_gauges.append(g)
            time.sleep(0.25)

        if noaa_id:
            log.info(f"Point {idx}: Nearest NOAA station {noaa_id} at ({lat},{lon})")
            g = fetch_noaa_gauge(noaa_id)
            if g:
                g["city"] = ""
                g["state"] = ""
                g["fema_zone"] = ""
                g["city_lat"] = lat
                g["city_lon"] = lon
                g["fetched_at"] = run_dt
                all_gauges.append(g)
                tiers.append(g["tier"])
                if g["tier"] in ("action", "flood", "major"):
                    alert_gauges.append(g)
            time.sleep(0.25)

    log.info(f"Fetched {len(all_gauges)} gauges total | {len(alert_gauges)} at/above action stage")
    return all_gauges, alert_gauges

# ── Output writers (unchanged) ────────────────────────────────
def write_current(gauges: list[dict]) -> None:
    slim = []
    for g in gauges:
        rec = {k: v for k, v in g.items() if k != "history"}
        slim.append(rec)

    with open(CURRENT_FILE, "w") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "gauge_count":  len(slim),
            "gauges":       slim,
        }, f, indent=2)
    log.info("Wrote %s  (%d gauges)", CURRENT_FILE, len(slim))

def write_alerts(alert_gauges: list[dict]) -> None:
    slim = []
    for g in alert_gauges:
        rec = {k: v for k, v in g.items() if k != "history"}
        slim.append(rec)

    with open(ALERTS_FILE, "w") as f:
        json.dump({
            "generated_at":  datetime.now(timezone.utc).isoformat(),
            "alert_count":   len(slim),
            "has_alerts":    len(slim) > 0,
            "alerts":        slim,
        }, f, indent=2)
    log.info("Wrote %s  (%d alerts)", ALERTS_FILE, len(slim))

def write_history(gauges: list[dict]) -> None:
    cutoff   = datetime.now(timezone.utc) - timedelta(days=7)
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    fieldnames = [
        "date", "city", "state", "source", "type", "site_id", "site_name",
        "stage_ft", "stage_unit", "stage_datetime", "discharge_cfs",
        "action_stage", "flood_stage", "major_stage", "tier", "fema_zone",
        "lat", "lon",
    ]

    existing = []
    if HISTORY_FILE.exists():
        with open(HISTORY_FILE, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    row_dt = datetime.fromisoformat(row.get("date", "") + "T00:00:00+00:00")
                    if row_dt >= cutoff:
                        existing.append(row)
                except ValueError:
                    existing.append(row)

    today_rows = []
    for g in gauges:
        today_rows.append({
            "date":           run_date,
            "city":           g.get("city", ""),
            "state":          g.get("state", ""),
            "source":         g.get("source", ""),
            "type":           g.get("type", ""),
            "site_id":        g.get("site_id", ""),
            "site_name":      g.get("site_name", ""),
            "stage_ft":       g.get("stage_ft", ""),
            "stage_unit":     g.get("stage_unit", ""),
            "stage_datetime": g.get("stage_datetime", ""),
            "discharge_cfs":  g.get("discharge_cfs", ""),
            "action_stage":   g.get("action_stage", ""),
            "flood_stage":    g.get("flood_stage", ""),
            "major_stage":    g.get("major_stage", ""),
            "tier":           g.get("tier", ""),
            "fema_zone":      g.get("fema_zone", ""),
            "lat":            g.get("lat") or g.get("city_lat", ""),
            "lon":            g.get("lon") or g.get("city_lon", ""),
        })

    existing_keys = {(r["date"], r["site_id"]) for r in existing}
    new_rows = [r for r in today_rows if (r["date"], r["site_id"]) not in existing_keys]

    all_rows = existing + new_rows

    with open(HISTORY_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    log.info("Wrote %s  (%d total rows, %d new today)", HISTORY_FILE, len(all_rows), len(new_rows))

def write_geojson(gauges: list[dict]) -> None:
    tier_colors = {
        "normal":  "#2d6a4f",
        "action":  "#f48c06",
        "flood":   "#e85d04",
        "major":   "#c1121f",
        "unknown": "#888888",
    }

    features = []
    for g in gauges:
        lat = g.get("lat") or g.get("city_lat")
        lon = g.get("lon") or g.get("city_lon")
        if lat is None or lon is None:
            continue
        try:
            lat, lon = float(lat), float(lon)
        except (TypeError, ValueError):
            continue

        tier  = g.get("tier", "unknown")
        color = tier_colors.get(tier, "#888888")

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "site_id":       g.get("site_id"),
                "site_name":     g.get("site_name"),
                "city":          g.get("city"),
                "state":         g.get("state"),
                "source":        g.get("source"),
                "type":          g.get("type"),
                "stage_ft":      g.get("stage_ft"),
                "stage_unit":    g.get("stage_unit"),
                "discharge_cfs": g.get("discharge_cfs"),
                "action_stage":  g.get("action_stage"),
                "flood_stage":   g.get("flood_stage"),
                "major_stage":   g.get("major_stage"),
                "tier":          tier,
                "color":         color,
                "fema_zone":     g.get("fema_zone"),
                "fetched_at":    g.get("fetched_at"),
                "url":           g.get("usgs_url") or g.get("noaa_url"),
            }
        })

    gj = {
        "type": "FeatureCollection",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "features": features,
    }

    out = DATA_DIR / "gauges_current.geojson"
    with open(out, "w") as f:
        json.dump(gj, f, indent=2)
    log.info("Wrote %s  (%d features)", out, len(features))

# ── Main entry point ──────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Floodwire2 gauge fetch")
    parser.add_argument("--alerts-only", action="store_true",
                        help="Only write the alerts file, skip current + history")
    parser.add_argument("--refresh-stations", action="store_true",
                        help="Force refresh USGS and NOAA station metadata")
    args = parser.parse_args()

    if args.refresh_stations:
        download_usgs_stations()
        download_noaa_stations()

    points = load_points_from_geojson(GEOJSON_FILE)
    if not points:
        log.error("No points loaded from GeoJSON, exiting.")
        sys.exit(1)

    all_gauges, alert_gauges = fetch_all_gauges_from_points(points)

    if not args.alerts_only:
        write_current(all_gauges)
        write_history(all_gauges)
        write_geojson(all_gauges)

    write_alerts(alert_gauges)

    tier_counts = {}
    for g in all_gauges:
        t = g.get("tier", "unknown")
        tier_counts[t] = tier_counts.get(t, 0) + 1

    log.info("Run complete. Tier summary: %s", tier_counts)

    if tier_counts.get("major", 0) > 0:
        log.warning("MAJOR FLOOD STAGE detected at %d gauge(s)", tier_counts["major"])
        sys.exit(2)

if __name__ == "__main__":
    main()
