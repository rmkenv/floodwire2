import ftplib
import io
import json
import zipfile
from pathlib import Path
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape

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


def fetch_latest_qpf(ftp_dir: str) -> gpd.GeoDataFrame:
    """Download the most recent QPF shapefile zip from WPC FTP and return GeoDataFrame."""
    ftp = ftplib.FTP(QPF_FTP_HOST)
    ftp.login()
    files = ftp.nlst(ftp_dir)
    zips = sorted(f for f in files if f.endswith(".zip"))
    if not zips:
        raise FileNotFoundError(f"No zip files found in {ftp_dir}")
    latest = zips[-1]
    print(f"  Fetching: {latest}")
    buf = io.BytesIO()
    ftp.retrbinary(f"RETR {latest}", buf.write)
    ftp.quit()

    buf.seek(0)
    tmp = Path("/tmp/qpf_shp")
    tmp.mkdir(exist_ok=True)
    # Clean any prior extraction
    for f in tmp.glob("*"):
        f.unlink()
    with zipfile.ZipFile(buf) as z:
        z.extractall(tmp)

    shp = list(tmp.glob("*.shp"))
    if not shp:
        raise FileNotFoundError("No .shp file found in extracted zip.")
    gdf = gpd.read_file(shp[0]).to_crs("EPSG:4326")
    # Normalize QPF column name (WPC uses 'QPF' or 'Globvalue')
    col_map = {c: "QPF" for c in gdf.columns if c.lower() in ("qpf", "globvalue", "grid_code")}
    gdf = gdf.rename(columns=col_map)
    return gdf[["QPF", "geometry"]]


def main():
    print("Loading gauge points...")
    gauges = gpd.read_file(GAUGES_URL).to_crs("EPSG:4326")

    # Keep a clean base — drop stale QPF cols if re-running
    drop_cols = [c for c in gauges.columns if c.startswith("qpf_")]
    gauges = gauges.drop(columns=drop_cols, errors="ignore")

    for col_name, ftp_dir in QPF_DIRS.items():
        print(f"Processing {col_name}...")
        try:
            qpf = fetch_latest_qpf(ftp_dir)
            joined = gpd.sjoin(
                gauges[["site_no", "geometry"]],
                qpf.rename(columns={"QPF": col_name}),
                how="left",
                predicate="within",
            ).drop(columns=["index_right"], errors="ignore")
            # Merge the QPF column back onto the main gauges GeoDataFrame
            gauges = gauges.merge(
                joined[["site_no", col_name]], on="site_no", how="left"
            )
        except Exception as e:
            print(f"  WARNING: Could not fetch {col_name}: {e}")
            gauges[col_name] = None

    # Convert QPF from mm to inches for readability and add risk flag
    for col in QPF_DIRS:
        gauges[col.replace("_in", "_inches")] = (gauges[col] / 25.4).round(2)

    # Elevated flood risk: >1 inch QPF in day1 AND gauge exists
    gauges["flood_risk_elevated"] = (
        gauges.get("qpf_day1_in", pd.Series(dtype=float)).fillna(0) > 25.4
    )

    # Add metadata timestamp
    gauges["qpf_updated_utc"] = pd.Timestamp.utcnow().isoformat()

    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    gauges.to_file(OUTPUT_PATH, driver="GeoJSON")
    print(f"Saved {len(gauges)} gauges → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
