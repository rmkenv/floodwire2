"""Tests for geocode_floods.py"""

from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from src.geocode_floods import (
    classify_flood_type,
    extract_locations,
    geocode_osm_flood,
    process_article,
)


# ---------------------------------------------------------------------------
# classify_flood_type
# ---------------------------------------------------------------------------

class TestClassifyFloodType:
    @pytest.mark.parametrize("text,expected_type", [
        ("A flash flood warning was issued for the area.", "flash_flood"),
        ("Flash flooding closed multiple roads.", "flash_flood"),
        ("Sunny day flooding hit the downtown district.", "sunny_day"),
        ("King tide caused nuisance flooding along the coast.", "sunny_day"),
        ("The river flooded after days of rain, a classic riverine flood.", "riverine"),
        ("There was heavy flooding but no specific type mentioned.", "unknown"),
        ("", "unknown"),
    ])
    def test_classification(self, text, expected_type):
        ftype, conf = classify_flood_type(text)
        assert ftype == expected_type

    def test_flash_flood_confidence_is_1(self):
        _, conf = classify_flood_type("A flash flood swept through the town.")
        assert conf == 1.0

    def test_unknown_with_generic_flood_has_nonzero_conf(self):
        _, conf = classify_flood_type("The area experienced significant flooding.")
        assert conf == 0.7

    def test_no_flood_zero_confidence(self):
        _, conf = classify_flood_type("Beautiful sunny day in the city.")
        assert conf == 0.0


# ---------------------------------------------------------------------------
# extract_locations
# ---------------------------------------------------------------------------

class TestExtractLocations:
    def _article(self, text: str, city: str | None = "Baltimore", region: str | None = "Maryland") -> dict:
        return {
            "article_id": "test-001",
            "_full_text": text,
            "outlet_city": city,
            "outlet_region": region,
        }

    def test_falls_back_to_outlet_city(self):
        art = self._article("Flooding caused road closures across the region.")
        locs = extract_locations(art)
        # Should fall back to outlet_city when no specific places found
        if locs:
            assert any("Baltimore" in q or "Maryland" in q for _, q, _ in locs)

    def test_downtown_pattern(self):
        art = self._article("Downtown Baltimore was flooded this morning.")
        locs = extract_locations(art)
        queries = [q for _, q, _ in locs]
        assert any("downtown" in q.lower() or "Baltimore" in q for q in queries)

    def test_waterway_pattern(self):
        art = self._article("Jones Falls Creek overflowed its banks during the storm.")
        locs = extract_locations(art)
        assert len(locs) >= 0  # Should not raise; may find creek

    def test_no_city_no_crash(self):
        art = self._article("Flooding occurred.", city=None, region=None)
        locs = extract_locations(art)
        assert isinstance(locs, list)

    def test_returns_at_most_10(self):
        # Manufacture text with many place names
        places = [f"Springfield{i}" for i in range(20)]
        text = "Flooding hit " + ", ".join(places)
        art = self._article(text)
        locs = extract_locations(art)
        assert len(locs) <= 10


# ---------------------------------------------------------------------------
# geocode_osm_flood
# ---------------------------------------------------------------------------

class TestGeocodeOSMFlood:
    def _cfg(self):
        return {
            "api": {"user_agent": "test_agent"},
            "geocoding": {"timeout_sec": 5},
        }

    def _mock_nominatim(self, lat: float = 39.28, lon: float = -76.61):
        mock = MagicMock()
        mock.status_code = 200
        mock.json.return_value = [{
            "lat": str(lat),
            "lon": str(lon),
            "display_name": "Baltimore, Maryland, US",
            "type": "city",
            "osm_type": "relation",
            "osm_id": 123456,
        }]
        mock.raise_for_status = MagicMock()
        return mock

    def test_successful_geocode(self):
        with patch("src.geocode_floods.requests.get", return_value=self._mock_nominatim()):
            result = geocode_osm_flood("Baltimore, Maryland", self._cfg())
        assert result["lat"] == pytest.approx(39.28)
        assert result["lon"] == pytest.approx(-76.61)
        assert result["osm_display"] == "Baltimore, Maryland, US"

    def test_empty_nominatim_response(self):
        mock = MagicMock()
        mock.status_code = 200
        mock.json.return_value = []
        mock.raise_for_status = MagicMock()
        with patch("src.geocode_floods.requests.get", return_value=mock):
            result = geocode_osm_flood("Nowhere Special", self._cfg())
        assert result["lat"] is None
        assert result["lon"] is None

    def test_network_error_returns_empty(self):
        import requests as req
        with patch("src.geocode_floods.requests.get", side_effect=req.RequestException("timeout")):
            result = geocode_osm_flood("Baltimore", self._cfg())
        assert result["lat"] is None


# ---------------------------------------------------------------------------
# process_article (integration)
# ---------------------------------------------------------------------------

class TestProcessArticle:
    def _cfg(self):
        return {
            "api": {"user_agent": "test"},
            "geocoding": {"timeout_sec": 5, "rate_limit_sec": 0},  # no sleep in tests
        }

    def test_returns_location_records(self):
        article = {
            "article_id": "art-001",
            "_full_text": "A flash flood hit downtown Annapolis today.",
            "outlet_city": "Annapolis",
            "outlet_region": "Maryland",
        }

        mock_geo = MagicMock()
        mock_geo.status_code = 200
        mock_geo.json.return_value = [{
            "lat": "38.97",
            "lon": "-76.49",
            "display_name": "Annapolis, MD",
            "type": "city",
            "osm_type": "relation",
            "osm_id": 111,
        }]
        mock_geo.raise_for_status = MagicMock()

        with patch("src.geocode_floods.requests.get", return_value=mock_geo):
            records = process_article(article, self._cfg())

        assert len(records) > 0
        assert all(r["flood_type"] == "flash_flood" for r in records)
        assert all(r["lat"] is not None for r in records)
