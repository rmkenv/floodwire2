"""Tests for extract_articles.py (SerpAPI version)"""

from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from src.extract_articles import _normalize, _infer_outlet_location, fetch_articles


# ---------------------------------------------------------------------------
# _normalize
# ---------------------------------------------------------------------------

class TestNormalize:
    def _raw(self, **kwargs) -> dict:
        base = {
            "title": "Flash flood warning issued in Maryland",
            "snippet": "Flash flood warning closed flooded streets in Baltimore MD.",
            "link": "https://example.com/story",
            "source": {"name": "WBAL-TV"},
            "iso_date": "2026-03-15T01:00:00Z",
        }
        base.update(kwargs)
        return base

    def test_basic_fields(self):
        result = _normalize(self._raw())
        assert result["title"] == "Flash flood warning issued in Maryland"
        assert result["outlet_city"] == "Baltimore"
        assert result["outlet_region"] == "Maryland"
        assert result["url"] == "https://example.com/story"
        assert "Flash flood" in result["_full_text"]

    def test_missing_title_and_link_returns_none(self):
        result = _normalize({"snippet": "some text"})
        assert result is None

    def test_nested_stories_normalized(self):
        """Stories within a news result should normalize the same way."""
        story = self._raw(title="Creek overflowed near homes", link="https://example.com/s2")
        result = _normalize(story)
        assert result is not None
        assert result["title"] == "Creek overflowed near homes"

    def test_source_name_used_for_outlet(self):
        result = _normalize(self._raw(source={"name": "Washington Post"}))
        assert result["outlet_city"] == "Washington"
        assert result["outlet_region"] == "DC"


# ---------------------------------------------------------------------------
# _infer_outlet_location
# ---------------------------------------------------------------------------

class TestInferOutletLocation:
    @pytest.mark.parametrize("source,expected_city,expected_region", [
        ("WBAL-TV Baltimore",   "Baltimore",  "Maryland"),
        ("Washington Post",     "Washington", "DC"),
        ("New York Times",      "New York",   "New York"),
        ("Unknown Local News",  None,         None),
    ])
    def test_known_sources(self, source, expected_city, expected_region):
        city, region = _infer_outlet_location(source)
        assert city == expected_city
        assert region == expected_region


# ---------------------------------------------------------------------------
# fetch_articles (mocked SerpAPI)
# ---------------------------------------------------------------------------

class TestFetchArticles:
    def _make_cfg(self):
        return {
            "api": {"serpapi_key": "test_key", "user_agent": "test"},
            "etl": {"lookback_days": 1, "max_articles": 50},
            "geocoding": {},
        }

    def _mock_response(self, articles: list[dict]):
        mock = MagicMock()
        mock.status_code = 200
        mock.json.return_value = {"news_results": articles}
        mock.raise_for_status = MagicMock()
        return mock

    def _sample_raw(self, uid: str = "a1") -> dict:
        return {
            "title": f"Flash flood warning issued in Maryland {uid}",
            "snippet": "Flash flood warning closed flooded streets. Floodwater entered homes.",
            "link": f"https://example.com/{uid}",
            "source": {"name": "WBAL-TV"},
            "iso_date": "2026-03-15T01:00:00Z",
        }

    def test_deduplication_across_query_groups(self):
        """Same URL appearing in multiple query groups should appear only once."""
        with patch("src.extract_articles.requests.get") as mock_get:
            mock_get.return_value = self._mock_response([self._sample_raw("dup1")])
            result = fetch_articles(self._make_cfg())
        ids = [r["article_id"] for r in result]
        assert ids.count("https://example.com/dup1") == 1

    def test_empty_response(self):
        with patch("src.extract_articles.requests.get") as mock_get:
            mock_get.return_value = self._mock_response([])
            result = fetch_articles(self._make_cfg())
        assert result == []

    def test_api_error_returns_empty(self):
        import requests as req
        with patch("src.extract_articles.requests.get", side_effect=req.RequestException("timeout")):
            result = fetch_articles(self._make_cfg())
        assert isinstance(result, list)

    def test_nested_stories_extracted(self):
        """Stories nested inside a news_result should also be returned."""
        raw_with_stories = self._sample_raw("parent")
        raw_with_stories["stories"] = [self._sample_raw("child")]
        with patch("src.extract_articles.requests.get") as mock_get:
            mock_get.return_value = self._mock_response([raw_with_stories])
            result = fetch_articles(self._make_cfg())
        urls = [r["url"] for r in result]
        assert "https://example.com/child" in urls

    def test_irrelevant_articles_filtered(self):
        """Articles with no physical flood term should be dropped."""
        irrelevant = {
            "title": "Markets flooded with cheap imports",
            "snippet": "A flood of cheap goods has overwhelmed retailers.",
            "link": "https://example.com/markets",
            "source": {"name": "Reuters"},
            "iso_date": "2026-03-15T01:00:00Z",
        }
        with patch("src.extract_articles.requests.get") as mock_get:
            mock_get.return_value = self._mock_response([irrelevant])
            result = fetch_articles(self._make_cfg())
        assert result == []
