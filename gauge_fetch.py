"""
gauge_fetch.py
==============
Floodwire2 gauge module — USGS NWIS + NOAA CO-OPS real-time data

Usage:
  python gauge_fetch.py                 # run standalone
  python gauge_fetch.py --alerts-only   # only write alerts file
  python gauge_fetch.py --refresh-stations  # refresh USGS and NOAA station metadata
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

# ── Output paths ─────────────────────────────────────────────
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
REQUEST_TIMEOUT = 30
RETRY_WAIT      = 3
MAX_RETRIES     = 2

# ── Utility: Haversine distance ──────────────────────────────
def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    return c * 3958.8 # Miles

# ── HTTP GET with retries ─────────────────────────────────────
def get_json(url: str, params: dict = None) -> dict | None:
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT,
                             headers={"User-Agent": "floodwire2-gauge-fetch/1.0"})
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning(f"Attempt {attempt+1} failed for {url[:50]}: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_WAIT)
    return None

# ── Robust USGS Station Download ──────────────────────────────
def download_usgs_stations():
    log.info("Downloading USGS station metadata (this may take a minute)...")
    url = "https://waterservices.usgs.gov/nwis/site/?format=rdb&siteType=ST&siteStatus=active&hasDataTypeCd=iv"
    try:
        r = requests.get(url, timeout=60, headers={"User-Agent": "floodwire2-gauge-fetch/1.0"})
        r.raise_for_status()
    except Exception as e:
        log.error(f"Failed to download USGS stations: {e}")
        return []

    lines = r.text.splitlines()
    stations = []
    headers = []
    for line in lines:
        if line.startswith("#") or not line.strip(): continue
        if line.startswith("agency_cd"):
            headers = line.split("\t")
            continue
        if line.startswith("5s") or line.startswith("15s"): continue # Skip format line
            
        parts = line.split("\t")
        if len(parts) != len(headers): continue
            
        record = dict(zip(headers, parts))
        try:
            lat, lon = float(record.get("dec_lat_va", "")), float(record.get("dec_long_va", ""))
            site_no = record.get("site_no", "")
            if lat and lon and site_no:
                stations.append({"site_id": site_no, "lat": lat, "lon": lon})
        except (ValueError, TypeError): continue

    with open(USGS_STATIONS_FILE, "w") as f:
        json.dump(stations, f)
    log.info(f"Saved {len(stations)} USGS stations.")
    return stations

# ── Robust NOAA Station Download ──────────────────────────────
def download_noaa_stations():
    log.info("Downloading NOAA station metadata...")
    url = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json"
    data = get_json(url)
    if not data or "stations" not in data:
        log.error("Failed to download NOAA stations metadata")
        return []

    stations = []
    for s in data.get("stations", []):
        try:
            lat, lon = float(s.get("lat")), float(s.get("lng"))
            sid = s.get("id")
            if lat and lon and sid:
                stations.append({"station_id": sid, "lat": lat, "lon": lon})
        except (ValueError, TypeError): continue

    with open(NOAA_STATIONS_FILE, "w") as f:
        json.dump(stations, f)
    log.info(f"Saved {len(stations)} NOAA stations.")
    return stations

def load_usgs_stations():
    if USGS_STATIONS_FILE.exists():
        with open(USGS_STATIONS_FILE) as f: return json.load(f)
    return download_usgs_stations()

def load_noaa_stations():
    if NOAA_STATIONS_FILE.exists():
        with open(NOAA_STATIONS_FILE) as f: return json.load(f)
    return download_noaa_stations()

def find_nearest_station(lat, lon, stations, id_key):
    min_dist, nearest = float('inf'), None
    for s in stations:
        dist = haversine(lat, lon, s["lat"], s["lon"])
        if dist < min_dist:
            min_dist, nearest = dist, s
    return nearest[id_key] if nearest else None

def load_points_from_geojson(path):
    if not path.exists(): return []
    with open(path) as f: gj = json.load(f)
    points = []
    for feature in gj.get("features", []):
        coords = feature.get("geometry", {}).get("coordinates", [])
        if len(coords) >= 2:
            points.append({"lat": coords[1], "lon": coords[0]})
    return points

# ── Fetch Logic ───────────────────────────────────────────────
def fetch_usgs_gauge(site_id: str) -> dict | None:
    data = get_json(USGS_IV_URL, {"format": "json", "sites": site_id, "parameterCd": "00060,00065", "period": "P7D"})
    if not data: return None
    ts_list = data.get("value", {}).get("timeSeries", [])
    if not ts_list: return None

    source_info = ts_list[0].get("sourceInfo", {})
    site_name = source_info.get("siteName", site_id)
    geo = source_info.get("geoLocation", {}).get("geogLocation", {})
    
    stage_ts = next((s for s in ts_list if s["variable"]["variableCode"][0]["value"] == "00065"), None)
    stage, stage_dt, history = None, None, []
    if stage_ts:
        vals = stage_ts.get("values", [{}])[0].get("value", [])
        if vals:
            stage_dt = vals[-1].get("dateTime")
            try: stage = float(vals[-1].get("value", ""))
            except: pass
            for v in vals:
                try: history.append({"dt": v["dateTime"], "stage_ft": float(v["value"])})
                except: pass

    thresholds = _fetch_waterwatch_thresholds(site_id)
    return {
        "source": "USGS", "site_id": site_id, "site_name": site_name,
        "lat": geo.get("latitude"), "lon": geo.get("longitude"),
        "stage_ft": stage, "stage_datetime": stage_dt, "tier": classify_tier(stage, thresholds),
        "action_stage": thresholds.get("action"), "flood_stage": thresholds.get("flood"),
        "major_stage": thresholds.get("major"), "history": history,
        "url": f"https://waterdata.usgs.gov/monitoring-location/{site_id}/"
    }

def _fetch_waterwatch_thresholds(site_id: str) -> dict:
    data = get_json("https://waterwatch.usgs.gov/webservices/floodstage", {"format": "json"})
    if not data: return {}
    for site in data.get("site", []):
        if site.get("site_no") == site_id:
            try:
                return {
                    "action": float(site.get("action_stage")) if site.get("action_stage") not in (None, "", "-999") else None,
                    "flood": float(site.get("flood_stage")) if site.get("flood_stage") not in (None, "", "-999") else None,
                    "major": float(site.get("major_flood_stage")) if site.get("major_flood_stage") not in (None, "", "-999") else None
                }
            except: pass
    return {}

def fetch_noaa_gauge(station_id: str) -> dict | None:
    now = datetime.now(timezone.utc)
    data = get_json(NOAA_API_URL, {
        "station": station_id, "product": "water_level", "datum": "MLLW",
        "time_zone": "gmt", "units": "english", "format": "json",
        "begin_date": (now - timedelta(days=7)).strftime("%Y%m%d"),
        "end_date": now.strftime("%Y%m%d")
    })
    if not data or "data" not in data: return None
    meta = data.get("metadata", {})
    history = []
    for r in data["data"]:
        try: history.append({"dt": r["t"], "stage_ft": float(r["v"])})
        except: pass
    if not history: return None
    
    stage = history[-1]["stage_ft"]
    mhhw = _fetch_noaa_datum(station_id, "MHHW")
    thresholds = {"action": round(mhhw+1,2), "flood": round(mhhw+2,2), "major": round(mhhw+4,2)} if mhhw else {}
    
    return {
        "source": "NOAA", "site_id": station_id, "site_name": meta.get("name"),
        "lat": meta.get("lat"), "lon": meta.get("lon"),
        "stage_ft": stage, "stage_datetime": history[-1]["dt"], "tier": classify_tier(stage, thresholds),
        "action_stage": thresholds.get("action"), "flood_stage": thresholds.get("flood"),
        "major_stage": thresholds.get("major"), "history": history,
        "url": f"https://tidesandcurrents.noaa.gov/stationhome.html?id={station_id}"
    }

def _fetch_noaa_datum(station_id: str, name: str) -> float | None:
    data = get_json(f"https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/{station_id}/datums.json", {"units": "english"})
    if not data: return None
    for d in data.get("datums", []):
        if d.get("name") == name:
            try: return float(d["value"])
            except: pass
    return None

def classify_tier(stage, thresholds):
    if stage is None: return "unknown"
    for t in ["major", "flood", "action"]:
        val = thresholds.get(t)
        if val and stage >= val: return t
    return "normal"

# ── Main Loop with Deduplication ──────────────────────────────
def fetch_all_gauges_from_points(points):
    usgs_stations = load_usgs_stations()
    noaa_stations = load_noaa_stations()
    
    fetched_cache = {} # site_id -> gauge_data
    all_gauges, alert_gauges = [], []
    run_dt = datetime.now(timezone.utc).isoformat()

    log.info(f"Processing {len(points)} points...")
    for idx, pt in enumerate(points, 1):
        u_id = find_nearest_station(pt["lat"], pt["lon"], usgs_stations, "site_id")
        n_id = find_nearest_station(pt["lat"], pt["lon"], noaa_stations, "station_id")

        for sid, fetch_func in [(u_id, fetch_usgs_gauge), (n_id, fetch_noaa_gauge)]:
            if not sid: continue
            if sid in fetched_cache:
                g = fetched_cache[sid]
            else:
                log.info(f"Fetching new gauge {sid}...")
                g = fetch_func(sid)
                fetched_cache[sid] = g
                time.sleep(0.2) # Politeness delay
            
            if g:
                # Clone data but keep original point context
                g_entry = g.copy()
                g_entry.update({"city_lat": pt["lat"], "city_lon": pt["lon"], "fetched_at": run_dt})
                all_gauges.append(g_entry)
                if g_entry["tier"] in ("action", "flood", "major"):
                    alert_gauges.append(g_entry)

    return all_gauges, alert_gauges

# ── Writers ───────────────────────────────────────────────────
def write_outputs(all_gauges, alert_gauges):
    # Current JSON
    with open(CURRENT_FILE, "w") as f:
        json.dump({"generated_at": datetime.now(timezone.utc).isoformat(), "gauges": [{k:v for k,v in g.items() if k != 'history'} for g in all_gauges]}, f, indent=2)
    
    # Alerts JSON
    with open(ALERTS_FILE, "w") as f:
        json.dump({"alerts": [{k:v for k,v in g.items() if k != 'history'} for g in alert_gauges]}, f, indent=2)

    # GeoJSON
    features = []
    colors = {"normal": "#2d6a4f", "action": "#f48c06", "flood": "#e85d04", "major": "#c1121f", "unknown": "#888888"}
    for g in all_gauges:
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [g["lon"], g["lat"]]},
            "properties": {**{k:v for k,v in g.items() if k != 'history'}, "color": colors.get(g["tier"], "#888888")}
        })
    with open(DATA_DIR / "gauges_current.geojson", "w") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f, indent=2)

    log.info(f"Wrote outputs to {DATA_DIR}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh-stations", action="store_true")
    args = parser.parse_args()

    if args.refresh_stations:
        download_usgs_stations()
        download_noaa_stations()

    points = load_points_from_geojson(GEOJSON_FILE)
    if not points:
        log.error("No points found in GeoJSON.")
        return

    all_g, alert_g = fetch_all_gauges_from_points(points)
    write_outputs(all_g, alert_g)

if __name__ == "__main__":
    main()
