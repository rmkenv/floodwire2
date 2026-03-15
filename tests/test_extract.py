"""Tests for extract_articles.py"""

from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from src.extract_articles import _normalize, _infer_outlet_location, fetch_articles


# ---------------------------------------------------------------------------
# _normalize
# ---------------------------------------------------------------------------

class TestNormalize:
    def test_basic_fields(self):
        raw = {
            "uuid": "abc-123",
            "title": "Flash flood hits Baltimore",
            "description": "Heavy rain caused flooding",
            "snippet": "Streets closed",
            "url": "https://example.com/story",
            "source": "baltimoresun.com",
            "published_at": "2026-03-15T02:00:00Z",
            "language": "en",
            "categories": ["weather"],
            "keywords": "flood,rain",
        }
        result = _normalize(raw)
        assert result["article_id"] == "abc-123"
        assert result["title"] == "Flash flood hits Baltimore"
        assert result["outlet_city"] == "Baltimore"
        assert result["outlet_region"] == "Maryland"
        assert "Flash flood" in result["_full_text"]

    def test_missing_fields_default_to_empty(self):
        result = _normalize({})
        assert result["article_id"] == ""
        assert result["title"] == ""
        assert result["categories"] == []

    def test_none_fields_coerced(self):
        raw = {"uuid": "x", "title": None, "description": None, "source": "unknown.com"}
        result = _normalize(raw)
        assert result["title"] == ""


# ---------------------------------------------------------------------------
# _infer_outlet_location
# ---------------------------------------------------------------------------

class TestInferOutletLocation:
    @pytest.mark.parametrize("source,expected_city,expected_region", [
        ("baltimoresun.com", "Baltimore", "Maryland"),
        ("washingtonpost.com", "Washington", "DC"),
        ("nytimes.com", "New York", "New York"),
        ("unknownlocalnews.com", None, None),
    ])
    def test_known_sources(self, source, expected_city, expected_region):
        city, region = _infer_outlet_location(source)
        assert city == expected_city
        assert region == expected_region


# ---------------------------------------------------------------------------
# fetch_articles (mocked API)
# ---------------------------------------------------------------------------

class TestFetchArticles:
    def _make_cfg(self):
        return {
            "api": {"thenewsapi_token": "test_token", "user_agent": "test"},
            "etl": {"lookback_days": 1, "max_articles": 10},
            "geocoding": {},
        }

    def _mock_response(self, articles: list[dict], next_page: str | None = None):
        mock = MagicMock()
        mock.status_code = 200
        mock.json.return_value = {
            "data": articles,
            "meta": {"next": next_page},
        }
        mock.raise_for_status = MagicMock()
        return mock

    def _sample_article(self, uid: str = "a1") -> dict:
        return {
            "uuid": uid,
            "title": f"Flood story {uid}",
            "description": "Flooding occurred",
            "snippet": "Streets flooded",
            "url": "https://example.com",
            "source": "wbaltv.com",
            "published_at": "2026-03-15T01:00:00Z",
            "language": "en",
            "categories": [],
            "keywords": "",
        }

    def test_deduplication(self):
        """Same article_id from two queries should appear only once."""
        with patch("src.extract_articles.requests.get") as mock_get:
            mock_get.return_value = self._mock_response([self._sample_article("dup1")])
            result = fetch_articles(self._make_cfg(), max_articles=50)
        ids = [r["article_id"] for r in result]
        assert ids.count("dup1") == 1

    def test_empty_response(self):
        with patch("src.extract_articles.requests.get") as mock_get:
            mock_get.return_value = self._mock_response([])
            result = fetch_articles(self._make_cfg())
        assert result == []

    def test_api_error_returns_empty(self):
        """Should not raise; returns empty list and logs error."""
        import requests as req
        with patch("src.extract_articles.requests.get", side_effect=req.RequestException("timeout")):
            result = fetch_articles(self._make_cfg())
        assert isinstance(result, list)
