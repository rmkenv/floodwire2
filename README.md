# Flood News Geocoder ETL

Nightly pipeline that finds US flood news, verifies relevance with an LLM, geocodes locations, and commits results as GeoJSON + CSV directly to this repo. No database required.

---

## How it works

```
SerpAPI Google News  (3 OR-grouped queries = 3 API calls/day)
    ↓  extract_articles.py
Regex pre-filter     (drops figurative uses, international articles)
    ↓  screen_articles.py
Ollama Cloud LLM     (gpt-oss:120b reads full context, yes/no per article)
    ↓  geocode_floods.py
spaCy NER + OSM Nominatim  (1 req/sec, no API key needed)
    ↓  load_files.py
data/floods.geojson  +  data/floods.csv  (appended, committed to repo)
```

---

## Repository structure

```
.github/workflows/flood_etl.yml   ← GitHub Actions cron (runs nightly at 02:00 ET)
config.example.yaml               ← copy to config.yaml, add your keys
data/
  floods.geojson                  ← append-only GeoJSON FeatureCollection
  floods.csv                      ← append-only CSV
src/
  main.py                         ← orchestrator
  extract_articles.py             ← SerpAPI Google News fetch + regex filter
  screen_articles.py              ← Ollama Cloud LLM relevance screening
  geocode_floods.py               ← flood classification + Nominatim geocoding
  load_files.py                   ← writes GeoJSON + CSV (dedup on article_id)
  utils.py                        ← config loader + logging
tests/
  test_extract.py
  test_geocode.py
  test_screen.py
docs/
  SETUP.md
  GITHUB_ACTIONS_SETUP.md
  API_LIMITS.md
```

---

## Quickstart

### Prerequisites
- Python 3.9+
- Three API keys (all free tiers sufficient — see table below)

### Local setup

```bash
git clone https://github.com/YOUR_USERNAME/flood-news-geocoder.git
cd flood-news-geocoder

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m spacy download en_core_web_sm

cp config.example.yaml config.yaml
# Edit config.yaml — add your three API keys

# Test run (10 articles, no file writes)
python src/main.py --test

# Real run
python src/main.py
```

---

## Configuration

`config.yaml` (copy from `config.example.yaml`):

```yaml
api:
  serpapi_key: YOUR_SERPAPI_KEY          # serpapi.com/manage-api-key
  user_agent: yourname@example.com       # required by OSM Nominatim policy
  ollama_api_key: YOUR_OLLAMA_API_KEY    # ollama.com/settings/keys

geocoding:
  rate_limit_sec: 1.0     # OSM Nominatim: max 1 req/sec
  buffer_meters: 300
  default_state: MD
  timeout_sec: 10

screening:
  rate_limit_sec: 0.5     # pause between Ollama API calls

etl:
  lookback_days: 1        # fetch last N × 24 hours
  max_articles: 500
  log_level: INFO
  log_file: null          # set a path to write logs to a file
```

---

## API keys & costs

| Service | Purpose | Free tier | Where to get it |
|---------|---------|-----------|----------------|
| SerpAPI | Google News search | 250 searches/month | [serpapi.com](https://serpapi.com) |
| Ollama Cloud | LLM relevance screening | Pay-per-use (low volume) | [ollama.com/settings/keys](https://ollama.com/settings/keys) |
| OSM Nominatim | Geocoding | Free, no key needed | Just set `user_agent` in config |

At 1 run/day with ~50 articles/run:
- SerpAPI: 3 calls/day = **90/month** (within 250 free)
- Ollama: ~50 calls/day at `gpt-oss:120b` — check [ollama.com/pricing](https://ollama.com/pricing) for current rates
- Nominatim: free, self-throttled to 1 req/sec

---

## GitHub Actions (fully automated)

### One-time setup

1. Push this repo to GitHub
2. Go to **Settings → Secrets and variables → Actions** and add:

| Secret | Value |
|--------|-------|
| `SERPAPI_KEY` | Your SerpAPI key |
| `OLLAMA_API_KEY` | Your Ollama Cloud key |

3. Go to **Actions → Flood ETL — Nightly → Run workflow** to trigger a test run

After that it runs every night at 02:00 ET automatically, committing updated `data/floods.geojson` and `data/floods.csv` back to the repo.

### Manual dispatch options

From the Actions tab → Run workflow:

| Input | Description |
|-------|-------------|
| `start_date` | Backfill from date (YYYY-MM-DD) |
| `end_date` | Backfill to date (YYYY-MM-DD) |
| `test_mode` | 10 articles, no file writes |
| `skip_screening` | Skip Ollama LLM step (faster) |

---

## Flood classification

| Type | Triggers |
|------|---------|
| `flash_flood` | "flash flood", "swift water rescue", "creek overflow", "rapid rise" |
| `sunny_day` | "sunny day flood", "king tide", "nuisance flood", "high tide flooding" |
| `riverine` | "river flood", "river crest", "levee breach" |
| `unknown` | generic flooding with no specific pattern |

---

## Output format

### GeoJSON (`data/floods.geojson`)

Standard FeatureCollection. Each feature:
```json
{
  "type": "Feature",
  "geometry": { "type": "Point", "coordinates": [-76.61, 39.29] },
  "properties": {
    "article_id": "https://example.com/story",
    "title": "Flash flood warning issued for Baltimore County",
    "source": "WBAL-TV",
    "outlet_city": "Baltimore",
    "outlet_region": "Maryland",
    "published_at": "2026-03-15T06:00:00Z",
    "url": "https://example.com/story",
    "mention_text": "Baltimore County",
    "flood_type": "flash_flood",
    "confidence": 0.9,
    "osm_display": "Baltimore County, Maryland, United States",
    "run_at": "2026-03-15T06:03:12Z"
  }
}
```

### CSV (`data/floods.csv`)

Same fields as GeoJSON properties, plus `lat` and `lon` columns. Open directly in Excel or load with pandas:

```python
import pandas as pd
import geopandas as gpd

# CSV
df = pd.read_csv("data/floods.csv")

# GeoJSON (with spatial support)
gdf = gpd.read_file("data/floods.geojson")
```

---

## CLI reference

```bash
python src/main.py                          # last 24h
python src/main.py --start 2026-03-01       # from date
python src/main.py --start 2026-03-01 --end 2026-03-07
python src/main.py --test                   # 10 articles, no writes
python src/main.py --no-screen              # skip Ollama screening
python src/main.py --config /path/to/cfg.yaml
```

---

## Tests

```bash
pytest tests/ -v
```

40 tests covering extraction, geocoding, and LLM screening — all mocked, no external calls needed.

---

## Troubleshooting

**"Missing required config key: api.serpapi_key"** — copy `config.example.yaml` to `config.yaml` and add your keys.

**"SerpAPI request failed"** — check your `SERPAPI_KEY` secret in GitHub. Verify at [serpapi.com/dashboard](https://serpapi.com/dashboard).

**"ollama_api_key not set — skipping LLM screening"** — add `OLLAMA_API_KEY` to GitHub Secrets. Pipeline still runs, just without the LLM filter.

**"Geocoding timeout"** — increase `timeout_sec` in config, or bump `rate_limit_sec` to 1.5.

**spaCy model missing** — run `python -m spacy download en_core_web_sm`. Pipeline falls back to regex-only location extraction in the meantime.

**No new data committed after a run** — either no flood articles were found in the last 24h (possible on quiet days), or all articles were filtered. Check the Actions log for the drop-rate line.

---

## License

MIT
