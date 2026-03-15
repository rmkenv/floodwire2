# Setup Guide

## 1. PostgreSQL + PostGIS

### Ubuntu / Debian
```bash
sudo apt install postgresql-15 postgresql-15-postgis-3
sudo -u postgres createdb flood_db
sudo -u postgres psql flood_db -c "CREATE EXTENSION postgis;"
psql -U postgres flood_db -f sql/schema.sql
```

### macOS (Homebrew)
```bash
brew install postgresql@15 postgis
createdb flood_db
psql flood_db -c "CREATE EXTENSION postgis;"
psql flood_db -f sql/schema.sql
```

## 2. Python Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

## 3. TheNewsAPI Token

1. Sign up at https://www.thenewsapi.com (free tier)
2. Copy your API token from the dashboard

## 4. Config

```bash
cp config.example.yaml config.yaml
# Edit config.yaml — set thenewsapi_token, DB credentials, user_agent
```

**Important**: `user_agent` is required by OSM Nominatim's usage policy. Use a descriptive string like `flood_etl_yourname@example.com`.

## 5. Test Run

```bash
python src/main.py --test
```

This fetches 10 articles and prints results without writing to the DB.

## 6. Cron (production)

```bash
# Edit crontab
crontab -e

# Add: run at 02:00 UTC daily
0 2 * * * /path/to/.venv/bin/python /path/to/src/main.py >> /var/log/flood_etl.log 2>&1
```

## 7. Unit Tests

```bash
pytest tests/test_extract.py tests/test_geocode.py -v
```

Integration tests require a real PostGIS database:
```bash
export FLOOD_TEST_DB="host=localhost dbname=flood_test user=postgres password=pass"
pytest tests/test_integration.py -v
```
