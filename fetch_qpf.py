import ftplib
import io
import time
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests

GAUGES_URL = (
    "https://raw.githubusercontent.com/rmkenv/floodwire2/"
    "refs/heads/main/data/gauges_with_floods.geojson"
)
OUTPUT_PATH = Path("data/gauges_with_qpf.geojson")

QPF_FTP_HOST = "ftp.wpc.ncep.noaa.gov"
QPF_DIRS = {
    "qpf_day1_in": "shapefiles/qpf/day1",
    "qpf_day2_in": "shapefiles/qpf/day2",
    "qpf_day3_in": "shapefiles/qpf/day3",
}

NWS_HEADERS = {"User-Agent": "floodwire2/1.0 (github.com/rmkenv/floodwire2)"}


# ── WPC FTP Fetch ─────────────────────────────────────────────────────────────

def fetch_latest_qpf(ftp_dir: str) -> gpd.GeoDataFrame:
    ftp = ftplib.FTP(QPF_FTP_HOST)
    ftp.login()
    files = ftp.nlst(ftp_dir)
    zips = sorted(f for f in files if f.endswith(".zip"))
    if not zips:
        raise FileNotFoundError(f"No zip files found in {ftp_dir}")
    buf = io.BytesIO()
    ftp.retrbinary(f"RETR {zips[-1]}", buf.write)
    ftp.quit()
    buf.seek(0)
    tmp = Path("/tmp/qpf_shp")
    tmp.mkdir(exist_ok=True)
    for f in tmp.glob("*"):
        f.unlink()
    with zipfile.ZipFile(buf) as z:
        z.extractall(tmp)
    shp = list(tmp.glob("*.shp"))
    if not shp:
        raise FileNotFoundError("No .shp in extracted zip.")
    gdf = gpd.read_file(shp[0]).to_crs("EPSG:4326")
    col_map = {c: "QPF" for c in gdf.columns if c.lower() in ("qpf", "globvalue", "grid_code")}
    return gdf.rename(columns=col_map)[["QPF", "geometry"]]


# ── NWS Gridpoints Fallback ───────────────────────────────────────────────────

def get_nws_grid_url(lat: float, lon: float) -> str | None:
    """Resolve a lat/lon to an NWS gridpoint forecastGridData URL."""
    try:
        r = requests.get(
            f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}",
            headers=NWS_HEADERS,
            timeout=10,
        )
        r.raise_for_status()
        return r.json()["properties"]["forecastGridData"]
    except Exception as e:
        print(f"    NWS points lookup failed ({lat},{lon}): {e}")
        return None


def get_nws_qpf_days(grid_url: str) -> dict:
    """
    Fetch grid data and sum QPF into day1/day2/day3 buckets.
    NWS QPF values are in mm over each ISO8601 duration period.
    Returns dict with keys: qpf_day1_in, qpf_day2_in, qpf_day3_in (all mm).
    """
    try:
        r = requests.get(grid_url, headers=NWS_HEADERS, timeout=15)
        r.raise_for_status()
        values = r.json()["properties"]["quantitativePrecipitation"]["values"]
    except Exception as e:
        print(f"    NWS grid fetch failed: {e}")
        return {}

    from datetime import datetime, timezone, timedelta
    import re

    now = datetime.now(timezone.utc)
    buckets = {"qpf_day1_in": 0.0, "qpf_day2_in": 0.0, "qpf_day3_in": 0.0}

    for entry in values:
        # validTime format: "2026-03-30T06:00:00+00:00/PT6H"
        try:
            dt_str, dur_str = entry["validTime"].split("/")
            start = datetime.fromisoformat(dt_str)
            # Parse duration (e.g. PT6H, PT1H, P1D)
            hours = 0
            h_match = re.search(r"(\d+)H", dur_str)
            d_match = re.search(r"(\d+)D", dur_str)
            if h_match:
                hours += int(h_match.group(1))
            if d_match:
                hours += int(d_match.group(1)) * 24
            mid = start + timedelta(hours=hours / 2)
            offset = (mid - now).total_seconds() / 3600
            val_mm = entry["value"] if entry["value"] is not None else 0.0
            if 0 <= offset < 24:
                buckets["qpf_day1_in"] += val_mm
            elif 24 <= offset < 48:
                buckets["qpf_day2_in"] += val_mm
            elif 48 <= offset < 72:
                buckets["qpf_day3_in"] += val_mm
        except Exception:
            continue

    return {k: round(v, 2) for k, v in buckets.items()}


def nws_fallback(gauges: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    For any row where qpf_day1_in is null, hit NWS gridpoints API
    and backfill all three day columns.
    """
    null_mask = gauges["qpf_day1_in"].isna()
    n_null = null_mask.sum()
    if n_null == 0:
        print("  No null QPF rows — skipping NWS fallback.")
        return gauges

    print(f"  {n_null} gauges missing QPF — querying NWS gridpoints API...")

    # Cache grid URLs by WFO grid to avoid redundant /points calls
    grid_cache: dict[tuple, str | None] = {}

    for idx, row in gauges[null_mask].iterrows():
        lat, lon = row.geometry.y, row.geometry.x
        coord_key = (round(lat, 2), round(lon, 2))

        if coord_key not in grid_cache:
            grid_cache[coord_key] = get_nws_grid_url(lat, lon)
            time.sleep(0.5)  # NWS rate limit courtesy

        grid_url = grid_cache[coord_key]
        if grid_url is None:
            continue

        day_vals = get_nws_qpf_days(grid_url)
        time.sleep(0.3)

        for col, val in day_vals.items():
            if col in gauges.columns:
                gauges.at[idx, col] = val

        print(f"    ✓ site_no={row.get('site_no', idx)} → {day_vals}")

    return gauges


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Loading gauge points...")
    gauges = gpd.read_file(GAUGES_URL).to_crs("EPSG:4326")
    gauges = gauges.drop(
        columns=[c for c in gauges.columns if c.startswith("qpf_")], errors="ignore"
    )

    # ── Step 1: WPC shapefile spatial join ───────────────────────────
    for col_name, ftp_dir in QPF_DIRS.items():
        print(f"Joining {col_name} via WPC shapefile...")
        try:
            qpf = fetch_latest_qpf(ftp_dir)
            joined = gpd.sjoin(
                gauges[["site_no", "geometry"]],
                qpf.rename(columns={"QPF": col_name}),
                how="left",
                predicate="within",
            ).drop(columns=["index_right"], errors="ignore")
            gauges = gauges.merge(joined[["site_no", col_name]], on="site_no", how="left")
        except Exception as e:
            print(f"  WARNING: WPC fetch failed for {col_name}: {e}")
            gauges[col_name] = None

    # ── Step 2: NWS gridpoints fallback for nulls ─────────────────────
    gauges = nws_fallback(gauges)

    # ── Step 3: Derived columns ───────────────────────────────────────
    for col in QPF_DIRS:
        inch_col = col.replace("_in", "_inches")
        gauges[inch_col] = (gauges[col] / 25.4).round(2)

    gauges["flood_risk_elevated"] = (
        gauges["qpf_day1_in"].fillna(0) > 25.4
    )
    gauges["qpf_source"] = gauges["qpf_day1_in"].apply(
        lambda v: "wpc_shapefile" if pd.notna(v) else "nws_gridpoints"
    )
    gauges["qpf_updated_utc"] = pd.Timestamp.utcnow().isoformat()

    # ── Step 4: Write output ──────────────────────────────────────────
    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    gauges.to_file(OUTPUT_PATH, driver="GeoJSON")
    print(f"\nDone. {len(gauges)} gauges saved → {OUTPUT_PATH}")
    null_remaining = gauges["qpf_day1_in"].isna().sum()
    if null_remaining:
        print(f"  WARNING: {null_remaining} gauges still have null QPF after fallback.")


if __name__ == "__main__":
    main()
