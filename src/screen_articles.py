"""
screen_articles.py — LLM relevance screening via Ollama Cloud API.

Called after the regex filter in extract_articles.py.
Each article is scored by gpt-oss:120b with a structured yes/no prompt.
Results are logged so you can tune the prompt over time.

Screening decision schema
--------------------------
{
    "relevant": bool,
    "reason":   str,   # one sentence
    "flood_type_hint": str | None  # flash_flood | sunny_day | riverine | unknown
}
"""

from __future__ import annotations

import json
import time
from typing import Any

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from .utils import get_logger

logger = get_logger(__name__)

_OLLAMA_API_URL = "https://ollama.com/api/chat"
_MODEL = "gpt-oss:120b"

_SYSTEM_PROMPT = """You are a flood news classifier for a US flood monitoring system.
Your job is to decide whether a news article is about a real, physical water flooding event in the United States.

RELEVANT articles describe:
- Flash floods, flood warnings, flood watches, flood advisories
- Sunny day / high tide / nuisance / tidal flooding
- River flooding, creek overflow, storm surge
- Flood damage to homes, roads, or infrastructure
- Water rescues due to flooding
- Flood evacuations or emergency declarations

NOT RELEVANT articles:
- Figurative use of "flood" (flood of emails, flood of immigrants, etc.)
- International flooding events outside the US
- Opinion pieces or policy debates about flood insurance/funding with no specific event
- Articles primarily about something else that mention flooding only in passing

Respond ONLY with a JSON object, no markdown, no explanation outside the JSON:
{
  "relevant": true or false,
  "reason": "one sentence explanation",
  "flood_type_hint": "flash_flood" or "sunny_day" or "riverine" or "unknown" or null
}"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def screen_articles(
    articles: list[dict[str, Any]],
    cfg: dict[str, Any],
) -> list[dict[str, Any]]:
    """Screen a list of articles with Ollama Cloud. Returns only relevant ones.

    If OLLAMA_API_KEY is not configured, logs a warning and returns all articles
    unchanged so the pipeline still runs.
    """
    api_key: str | None = cfg.get("api", {}).get("ollama_api_key")
    if not api_key:
        logger.warning(
            "ollama_api_key not set in config — skipping LLM screening. "
            "Add it to config.yaml or set OLLAMA_API_KEY secret in GitHub."
        )
        return articles

    rate_limit: float = float(cfg.get("screening", {}).get("rate_limit_sec", 0.5))

    kept: list[dict[str, Any]] = []
    dropped = 0

    for i, article in enumerate(articles, 1):
        decision = _screen_one(article, api_key)
        time.sleep(rate_limit)

        if decision["relevant"]:
            # Attach the hint so geocode_floods.py can use it
            if decision.get("flood_type_hint"):
                article["_llm_flood_type_hint"] = decision["flood_type_hint"]
            kept.append(article)
            logger.debug(
                "[%d/%d] KEEP  %s — %s",
                i, len(articles),
                article.get("title", "")[:60],
                decision["reason"],
            )
        else:
            dropped += 1
            logger.info(
                "[%d/%d] DROP  %s — %s",
                i, len(articles),
                article.get("title", "")[:60],
                decision["reason"],
            )

    logger.info(
        "LLM screening complete. Kept: %d  Dropped: %d  Drop rate: %.0f%%",
        len(kept),
        dropped,
        100 * dropped / max(1, len(articles)),
    )
    return kept


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------

@retry(
    retry=retry_if_exception_type(requests.RequestException),
    wait=wait_exponential(multiplier=1, min=2, max=20),
    stop=stop_after_attempt(3),
    reraise=False,   # on total failure, fall back to keeping the article
)
def _call_ollama(text: str, api_key: str) -> dict[str, Any] | None:
    """Call Ollama Cloud chat API. Returns parsed JSON or None on failure."""
    payload = {
        "model": _MODEL,
        "stream": False,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content": f"Article text:\n\n{text[:1500]}"},
        ],
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    resp = requests.post(_OLLAMA_API_URL, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _screen_one(article: dict[str, Any], api_key: str) -> dict[str, Any]:
    """Screen a single article. Returns a decision dict — always safe to use."""
    text = article.get("_full_text", "")
    title = article.get("title", "")

    try:
        raw = _call_ollama(f"Title: {title}\n\n{text}", api_key)
        if not raw:
            raise ValueError("Empty response from Ollama")

        content: str = raw["message"]["content"].strip()

        # Strip markdown fences if model wraps output despite instructions
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
            content = content.strip()

        decision = json.loads(content)

        # Validate expected keys
        if "relevant" not in decision:
            raise ValueError(f"Missing 'relevant' key in response: {content[:100]}")

        return {
            "relevant":        bool(decision["relevant"]),
            "reason":          str(decision.get("reason", "")),
            "flood_type_hint": decision.get("flood_type_hint"),
        }

    except Exception as exc:
        # On any failure, default to KEEPING the article
        # (better to keep a borderline article than silently drop a real flood)
        logger.warning(
            "LLM screen failed for %r: %s — defaulting to KEEP",
            title[:60], exc,
        )
        return {"relevant": True, "reason": "screening failed — kept by default", "flood_type_hint": None}
