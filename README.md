# Flood News Geocoder ETL

**Production-ready Python pipeline for detecting and geolocating flash flood and sunny-day flooding events from US news articles.**

`TheNewsAPI` → spaCy NER + regex → OSM Nominatim → PostGIS

Nightly batch ETL that processes 100–500 articles/day, classifies flood types (flash vs. sunny-day/tidal vs. riverine), and stores precise geometries for querying and visualization.

---

## Architecture

```
TheNewsAPI (flood keyword search)
    ↓  extract_articles.py
Articles (title, description, snippet, outlet metadata)
    ↓  geocode_floods.py
spaCy NER + regex patterns → location candidates
    ↓  geocode_osm_flood()
OSM Nominatim  (1 req/sec, US-scoped)
    ↓  load_postgis.py
PostGIS  flood_articles + flood_locations (Point + 300m buffer)
```

---

## Repository Structure

```
flood-news-geocoder/
├── src/
│   ├── main.py               # Orchestrator
│   ├── extract_articles.py   # TheNewsAPI fetch + normalize
│   ├── geocode_floods.py     # Classification + NER + Nominatim
│   ├── load_postgis.py       # PostGIS upsert
│   └── utils.py              # Config loader + logging
├── sql/
│   ├── schema.sql            # PostGIS tables + indexes + view
│   └── queries.sql           # Sample spatial queries
├── tests/
│   ├── test_extract.py
│   ├── test_geocode.py
│   └── test_integration.py   # Requires real DB (see SETUP.md)
├── dags/
│   └── flood_etl_dag.py      # Airflow DAG (optional)
├── docs/
│   ├── SETUP.md
│   └── API_LIMITS.md
├── config.example.yaml
└── requirements.txt
```

---

## Quick Start

### Prerequisites
- Python 3.9+
- PostgreSQL 13+ with PostGIS 3.0+
- TheNewsAPI.com account (free tier)

```bash
# 1. Clone
git clone https://github.com/yourusername/flood-news-geocoder.git
cd flood-news-geocoder

# 2. Python env
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m spacy download en_core_web_sm

# 3. Database (Ubuntu example)
sudo -u postgres createdb flood_db
psql -U postgres flood_db -f sql/schema.sql

# 4. Config
cp config.example.yaml config.yaml
# Edit config.yaml — add thenewsapi_token, DB credentials, user_agent

# 5. Test run (no DB writes)
python src/main.py --test

# 6. Production
python src/main.py
```

### Cron (nightly at 02:00 UTC)
```
0 2 * * * /path/to/.venv/bin/python /path/to/src/main.py >> /var/log/flood_etl.log 2>&1
```

---

## Configuration (`config.yaml`)

```yaml
api:
  thenewsapi_token: YOUR_TOKEN_HERE
  user_agent: flood_etl_yourname@example.com   # Required by OSM policy

database:
  host: localhost
  port: 5432
  dbname: flood_db
  user: postgres
  password: yourpass

geocoding:
  rate_limit_sec: 1.0      # OSM Nominatim: max 1 req/sec
  buffer_meters: 300       # Flood area buffer
  default_state: MD        # Fallback for ambiguous place names
  timeout_sec: 10

etl:
  lookback_days: 1
  max_articles: 500
  log_level: INFO
  log_file: /var/log/flood_etl.log   # null = stdout only
```

---

## Usage

```bash
# Last 24h (default)
python src/main.py

# Specific date range
python src/main.py --start 2026-03-01 --end 2026-03-07

# Test mode: 10 articles, no DB writes
python src/main.py --test

# Custom config path
python src/main.py --config /etc/flood_etl/config.yaml
```

---

## Flood Classification

| Type | Keywords / Patterns |
|------|---------------------|
| `flash_flood` | flash flood, sudden flood, creek overflow, rapid rise, water rescue, swift water, dam fail, heavy rain + flood |
| `sunny_day` | sunny-day flood, high-tide flood, king tide, nuisance flood, tidal flooding, sea-level rise + flood, flood without rain |
| `riverine` | river flood, river overflow, river crest, levee breach |
| `unknown` | generic "flooding" with no specific pattern match |

Confidence scores: 1.0 (specific pattern), 0.7 (generic flood mention), 0.0 (no flood).

---

## Location Extraction

| Pattern | Example input | Nominatim query |
|---------|--------------|-----------------|
| spaCy GPE entity | "flooding in Northport" | `Northport, Maryland` |
| Downtown prefix | "downtown Baltimore flooded" | `downtown, Baltimore, Maryland` |
| Waterway name | "Jones Falls Creek overflowed" | `Jones Falls Creek, Maryland` |
| Historic district | "the historic district" | `historic district, Baltimore, MD` |
| Near landmark | "near Lincoln High School" | `Lincoln High School, Baltimore` |
| Fallback | no specific place found | `Baltimore, Maryland` (outlet_city) |

---

## Sample Queries

```sql
-- Flash floods in Maryland, last 7 days
SELECT fa.title, fl.mention_text, fa.published_at,
       ST_AsText(fl.point) AS coords
FROM flood_locations fl
JOIN flood_articles fa USING (article_id)
WHERE fl.flood_type = 'flash_flood'
  AND fa.outlet_region = 'Maryland'
  AND fa.published_at > NOW() - INTERVAL '7 days';

-- All floods within 10km of Catonsville, MD
SELECT fa.title, fl.flood_type,
       ROUND(ST_Distance(fl.point::geography,
             ST_MakePoint(-76.7419, 39.2712)::geography) / 1000) AS km
FROM flood_locations fl
JOIN flood_articles fa USING (article_id)
WHERE ST_DWithin(fl.point::geography,
                 ST_MakePoint(-76.7419, 39.2712)::geography, 10000)
ORDER BY km;
```

See `sql/queries.sql` for the full collection including GeoJSON export.

---

## Export to GeoJSON

```bash
ogr2ogr -f GeoJSON floods.geojson PG:"dbname=flood_db" flood_locations
```

---

## Cost

| Service | Free tier | ~200 articles/day | Cost |
|---------|-----------|-------------------|------|
| TheNewsAPI | Unlimited dev | ~200 calls | $0 |
| OSM Nominatim | 1 req/sec | ~600 geocodes (~10 min) | $0 |
| PostGIS | Self-hosted | — | $0 |

**Total: $0/month** for ≤500 articles/day nightly batch.

---

## Tests

```bash
# Unit tests (no external services needed)
pytest tests/test_extract.py tests/test_geocode.py -v

# Integration tests (requires PostGIS)
export FLOOD_TEST_DB="host=localhost dbname=flood_test user=postgres password=pass"
pytest tests/test_integration.py -v
```

---

## Extending

### Add a new flood type

Edit `FLOOD_PATTERNS` in `src/geocode_floods.py`:
```python
FLOOD_PATTERNS = {
    ...
    "urban_flood": r"\b(urban flood|storm drain overflow|sewer backup)\b",
}
```

### Swap Nominatim for Google Maps

In `geocode_osm_flood()` inside `src/geocode_floods.py`:
```python
from googlemaps import Client
gmaps = Client(key=cfg["api"]["google_maps_key"])
result = gmaps.geocode(query, components={"country": "US"})
```

### Airflow

See `dags/flood_etl_dag.py` — runs nightly at 02:00 UTC.

---

## Troubleshooting

**"No articles found"** — Check `thenewsapi_token` in `config.yaml`.  
**"Geocoding timeout"** — Increase `timeout_sec` or `rate_limit_sec` to 1.5.  
**"PostGIS connection refused"** — Check `systemctl status postgresql` and `pg_hba.conf`.  
**spaCy model missing** — Run `python -m spacy download en_core_web_sm`. Pipeline will fall back to regex-only extraction in the meantime.

---

## License

MIT — see `LICENSE`
