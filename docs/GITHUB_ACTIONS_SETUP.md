# GitHub Actions + Supabase Setup Guide

This is the complete walkthrough to get the nightly ETL running automatically
on GitHub Actions with Supabase as the free managed PostGIS backend.

---

## Step 1 — Create a Supabase project (free)

1. Go to [supabase.com](https://supabase.com) and sign up (free tier is permanent)
2. Click **New Project**, choose a name (e.g. `flood-etl`), pick a region close to you (e.g. US East)
3. Set a strong database password — **save it**, you'll need it in Step 3
4. Wait ~2 minutes for the project to provision

### Get your connection details

In the Supabase dashboard go to:
**Project Settings → Database → Connection parameters**

You need:
- **Host**: `db.xxxxxxxxxxxx.supabase.co`
- **Database name**: `postgres`
- **User**: `postgres`
- **Password**: the one you set above
- **Port**: `5432`

---

## Step 2 — Initialize the schema

From your local machine (one time only):

```bash
# Install psycopg2 if you haven't already
pip install psycopg2-binary

# Run the init script
python scripts/init_supabase.py \
  --host db.xxxxxxxxxxxx.supabase.co \
  --dbname postgres \
  --user postgres \
  --password YOUR_DB_PASSWORD
```

Expected output:
```
Connecting to db.xxx.supabase.co:5432/postgres ...
Running schema.sql ...
Schema applied successfully.

Tables found: ['flood_articles', 'flood_locations', 'geocode_errors']
✓ Schema ready. Add your DB credentials to GitHub Secrets and push.
```

---

## Step 3 — Add GitHub Secrets

In your GitHub repository go to:
**Settings → Secrets and variables → Actions → New repository secret**

Add all five secrets:

| Secret name | Value |
|-------------|-------|
| `THENEWSAPI_TOKEN` | Your token from thenewsapi.com |
| `DB_HOST` | `db.xxxxxxxxxxxx.supabase.co` |
| `DB_NAME` | `postgres` |
| `DB_USER` | `postgres` |
| `DB_PASSWORD` | Your Supabase DB password |

---

## Step 4 — Push to GitHub

```bash
git add .
git commit -m "Add GitHub Actions nightly ETL"
git push origin main
```

The workflow file at `.github/workflows/flood_etl.yml` will be automatically
detected by GitHub Actions.

---

## Step 5 — Verify

### Trigger a manual test run first

1. Go to your repo on GitHub
2. Click **Actions** tab
3. Click **Flood ETL — Nightly** in the left sidebar
4. Click **Run workflow** → check **Test mode** → click **Run workflow**

This runs with 10 articles and skips DB writes — safe to verify the setup works.

### Then trigger a real run

Same steps, leave **Test mode** unchecked.

### Check the logs

Click the running workflow → click the `etl` job → expand **Run ETL** to see output like:

```
2026-03-15T02:00:01  INFO     main  === EXTRACT ===
2026-03-15T02:00:03  INFO     main  Articles fetched: 87
2026-03-15T02:00:03  INFO     main  === GEOCODE ===
2026-03-15T02:02:41  INFO     main  Geocoding complete: 234 locations found, 12 failed
2026-03-15T02:02:41  INFO     main  === LOAD ===
2026-03-15T02:02:43  INFO     main  === SUMMARY === {'articles_fetched': 87, ...}
```

---

## Step 6 — Query your data

You can query directly from Supabase's built-in SQL editor:
**Dashboard → SQL Editor**

```sql
-- Latest floods
SELECT title, flood_type, mention_text, published_at
FROM v_recent_floods
ORDER BY published_at DESC
LIMIT 20;
```

Or connect from any PostGIS client (QGIS, DBeaver) using the connection string from Step 1.

---

## Schedule

The cron is set to `0 6 * * *` (06:00 UTC = 02:00 ET).
To change it, edit `.github/workflows/flood_etl.yml`:

```yaml
schedule:
  - cron: "0 6 * * *"   # change this line
```

[Crontab.guru](https://crontab.guru) is useful for testing cron expressions.

---

## Supabase Free Tier Limits

| Resource | Free limit | ETL usage |
|----------|-----------|-----------|
| Database size | 500 MB | ~200 articles/day = ~1 MB/day |
| Bandwidth | 5 GB/month | minimal |
| Pausing | Projects pause after 1 week inactivity | GitHub Actions keeps it active |

At 200 articles/day you'll use ~30 MB/month — well within free limits.

---

## Local Development

To run against a local PostGIS (no Supabase needed):

```bash
# Start PostGIS in Docker
docker compose up -d db

# Run ETL against local DB
# First update config.yaml: host: localhost, password: localdev
python src/main.py --test
```
