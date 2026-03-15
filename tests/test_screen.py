"""Tests for screen_articles.py"""

from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from src.screen_articles import screen_articles, _screen_one


def _cfg(with_key: bool = True) -> dict:
    return {
        "api": {"ollama_api_key": "test_key" if with_key else None},
        "screening": {"rate_limit_sec": 0},
    }


def _article(title: str = "Flash flood hits Baltimore MD") -> dict:
    return {
        "article_id": "test-001",
        "title": title,
        "_full_text": f"{title}. Floodwater entered homes after creek overflowed.",
    }


def _mock_ollama_response(relevant: bool, reason: str = "test", hint: str = "flash_flood"):
    import json
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = {
        "message": {
            "content": json.dumps({
                "relevant": relevant,
                "reason": reason,
                "flood_type_hint": hint,
            })
        }
    }
    mock.raise_for_status = MagicMock()
    return mock


class TestScreenArticles:
    def test_no_api_key_returns_all(self):
        """Without API key, all articles pass through unchanged."""
        articles = [_article(), _article("Another flood story")]
        result = screen_articles(articles, _cfg(with_key=False))
        assert len(result) == 2

    def test_relevant_article_kept(self):
        with patch("src.screen_articles.requests.post",
                   return_value=_mock_ollama_response(True, "real flood event")):
            result = screen_articles([_article()], _cfg())
        assert len(result) == 1

    def test_irrelevant_article_dropped(self):
        with patch("src.screen_articles.requests.post",
                   return_value=_mock_ollama_response(False, "figurative use")):
            result = screen_articles([_article()], _cfg())
        assert len(result) == 0

    def test_llm_hint_attached_to_article(self):
        with patch("src.screen_articles.requests.post",
                   return_value=_mock_ollama_response(True, "real flood", "sunny_day")):
            result = screen_articles([_article()], _cfg())
        assert result[0].get("_llm_flood_type_hint") == "sunny_day"

    def test_api_failure_defaults_to_keep(self):
        """On Ollama API failure, article should be kept (fail open)."""
        import requests as req
        with patch("src.screen_articles.requests.post",
                   side_effect=req.RequestException("timeout")):
            result = screen_articles([_article()], _cfg())
        assert len(result) == 1

    def test_malformed_json_defaults_to_keep(self):
        """If model returns non-JSON, article should be kept."""
        mock = MagicMock()
        mock.status_code = 200
        mock.json.return_value = {"message": {"content": "Sorry, I cannot help with that."}}
        mock.raise_for_status = MagicMock()
        with patch("src.screen_articles.requests.post", return_value=mock):
            result = screen_articles([_article()], _cfg())
        assert len(result) == 1

    def test_markdown_fenced_json_parsed(self):
        """Model sometimes wraps JSON in ```json fences despite instructions."""
        import json
        mock = MagicMock()
        mock.status_code = 200
        mock.json.return_value = {
            "message": {
                "content": "```json\n" + json.dumps({
                    "relevant": False,
                    "reason": "not a flood",
                    "flood_type_hint": None,
                }) + "\n```"
            }
        }
        mock.raise_for_status = MagicMock()
        with patch("src.screen_articles.requests.post", return_value=mock):
            result = screen_articles([_article()], _cfg())
        assert len(result) == 0

    def test_mixed_batch(self):
        """Multiple articles — some kept, some dropped."""
        responses = [
            _mock_ollama_response(True,  "real flood"),
            _mock_ollama_response(False, "figurative"),
            _mock_ollama_response(True,  "real flood"),
        ]
        articles = [_article(f"Story {i}") for i in range(3)]
        with patch("src.screen_articles.requests.post", side_effect=responses):
            result = screen_articles(articles, _cfg())
        assert len(result) == 2
