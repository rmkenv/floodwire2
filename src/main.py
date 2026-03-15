"""
main.py — Nightly ETL orchestrator (flat-file version, no database required).

Pipeline:
  1. Extract  — fetch articles from TheNewsAPI (regex pre-filtered)
  2. Screen   — LLM relevance check via Ollama Cloud (if api key configured)
  3. Geocode  — spaCy NER + OSM Nominatim
  4. Load     — append to data/floods.geojson + data/floods.csv

Usage:
  python src/main.py                            # process last 24h
  python src/main.py --start 2026-03-01 --end 2026-03-07
  python src/main.py --test                     # 10 articles, no file writes
  python src/main.py --no-screen               # skip LLM screening
  python src/main.py --config /path/to/cfg.yaml
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extract_articles import fetch_articles
from src.screen_articles import screen_articles
from src.geocode_floods import process_article
from src.load_files import load_files
from src.utils import get_logger, load_config


def run(
    cfg: dict[str, Any],
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    dry_run: bool = False,
    max_articles: int | None = None,
    skip_screening: bool = False,
) -> dict[str, Any]:
    """Execute the full ETL pipeline. Returns summary dict."""
    logger = get_logger("main", cfg)
    t0 = time.perf_counter()

    # ---- EXTRACT ----
    logger.info("=== EXTRACT ===")
    articles = fetch_articles(
        cfg,
        start_date=start_date,
        end_date=end_date,
        max_articles=max_articles,
    )
    logger.info("Articles after regex filter: %d", len(articles))

    if not articles:
        logger.warning("No articles found. Check TheNewsAPI token / date range.")
        return {"articles_fetched": 0, "articles_after_screening": 0, "locations_geocoded": 0}

    # ---- SCREEN ----
    articles_before_screening = len(articles)
    if skip_screening:
        logger.info("=== SCREEN (skipped via --no-screen) ===")
    else:
        logger.info("=== SCREEN ===")
        articles = screen_articles(articles, cfg)
        logger.info(
            "Articles after LLM screening: %d (dropped %d)",
            len(articles),
            articles_before_screening - len(articles),
        )

    if not articles:
        logger.warning("All articles dropped by LLM screener.")
        return {
            "articles_fetched": articles_before_screening,
            "articles_after_screening": 0,
            "locations_geocoded": 0,
        }

    # ---- GEOCODE ----
    logger.info("=== GEOCODE ===")
    all_locations: list[dict[str, Any]] = []
    geocode_errors: list[dict[str, Any]] = []

    for i, article in enumerate(articles, 1):
        if i % 50 == 0:
            logger.info("Geocoding progress: %d/%d articles", i, len(articles))
        locs = process_article(article, cfg)
        for loc in locs:
            if loc.get("lat") is None:
                geocode_errors.append(loc)
            else:
                all_locations.append(loc)

    logger.info(
        "Geocoding complete: %d locations, %d failed",
        len(all_locations), len(geocode_errors),
    )

    # ---- LOAD ----
    file_stats: dict[str, int] = {}

    if dry_run:
        logger.info("=== DRY RUN — skipping file writes ===")
        _print_sample(articles, all_locations)
    else:
        logger.info("=== LOAD ===")
        file_stats = load_files(articles, all_locations, cfg)

    elapsed = time.perf_counter() - t0
    summary = {
        "articles_fetched":        articles_before_screening,
        "articles_after_screening": len(articles),
        "locations_geocoded":      len(all_locations),
        "geocode_errors":          len(geocode_errors),
        "elapsed_sec":             round(elapsed, 1),
        **file_stats,
    }
    logger.info("=== SUMMARY === %s", summary)
    return summary


def _print_sample(
    articles: list[dict[str, Any]],
    locations: list[dict[str, Any]],
) -> None:
    print("\n--- Sample Articles (after screening) ---")
    for art in articles[:5]:
        hint = art.get("_llm_flood_type_hint", "")
        print(f"  [{art['published_at'][:10]}] {art['title'][:70]}  [{hint}]")
    print("\n--- Sample Locations ---")
    for loc in locations[:5]:
        print(
            f"  {loc['flood_type']:12s}  conf={loc['confidence']:.2f}  "
            f"lat={loc['lat']:.4f} lon={loc['lon']:.4f}  "
            f"'{loc['mention_text'][:40]}'"
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flood News Geocoder ETL")
    parser.add_argument("--config",       default=None,  help="Path to config.yaml")
    parser.add_argument("--start",        default=None,  help="Start date YYYY-MM-DD (UTC)")
    parser.add_argument("--end",          default=None,  help="End date YYYY-MM-DD (UTC)")
    parser.add_argument("--test",         action="store_true",
                        help="Test mode: 10 articles, no file writes")
    parser.add_argument("--no-screen",    action="store_true",
                        help="Skip LLM screening step")
    parser.add_argument("--max-articles", type=int, default=None)
    return parser.parse_args()


def _parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def main() -> None:
    args = _parse_args()
    cfg = load_config(args.config)
    logger = get_logger("main", cfg)

    start_date = _parse_date(args.start) if args.start else None
    end_date   = _parse_date(args.end)   if args.end   else None
    max_arts   = 10 if args.test else args.max_articles

    if args.test:
        logger.info("TEST MODE — 10 articles, no file writes")

    try:
        run(
            cfg,
            start_date=start_date,
            end_date=end_date,
            dry_run=args.test,
            max_articles=max_arts,
            skip_screening=args.no_screen,
        )
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
        sys.exit(0)
    except Exception as exc:
        logger.exception("Fatal ETL error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
