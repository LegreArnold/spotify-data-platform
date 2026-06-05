"""
Tests unitaires — Fonctions de transformation
==============================================
Ces tests vérifient les fonctions de transformation du catalogue
indépendamment d'Airflow et de PostgreSQL.

Lancement :
    pytest tests/unit/ -v
"""

import pytest
import uuid
from datetime import datetime, timezone


# ─────────────────────────────────────────────────────────────
# IMPORT DES FONCTIONS À TESTER
# Adaptez ces imports selon votre structure src/
# ─────────────────────────────────────────────────────────────

from src.transformations.catalog import (
    normalize_artist_name,
    validate_track_schema,
    deduplicate_artists,
    deduplicate_tracks,
)
from src.transformations.events import (
    enrich_listening_event,
    is_valid_listening_event,
)


# ─────────────────────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────────────────────

@pytest.fixture
def valid_track():
    return {
        "id":          str(uuid.uuid4()),
        "artist_id":   str(uuid.uuid4()),
        "title":       "Test Track",
        "duration_ms": 210_000,
        "genre":       "Pop",
    }

@pytest.fixture
def valid_listening_event():
    return {
        "event_id":    str(uuid.uuid4()),
        "user_id":     str(uuid.uuid4()),
        "track_id":    str(uuid.uuid4()),
        "source_peer": str(uuid.uuid4()),
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "duration_ms": 45_000,
        "completed":   True,
        "device_type": "mobile",
        "geo_country": "FR",
        "event_source": "p2p",
    }

@pytest.fixture
def catalog_with_duplicates():
    """Catalogue contenant des artistes dupliqués (même nom, même label)."""
    artist_id = str(uuid.uuid4())
    return {
        "artists": [
            {"id": artist_id,           "name": "The Beatles", "label": "EMI"},
            {"id": str(uuid.uuid4()),   "name": "the beatles", "label": "EMI"},  # doublon
            {"id": str(uuid.uuid4()),   "name": "Led Zeppelin", "label": "Atlantic"},
        ],
        "tracks": [],
        "albums": [],
    }


# ─────────────────────────────────────────────────────────────
# TESTS — Normalisation des noms d'artistes
# ─────────────────────────────────────────────────────────────

class TestNormalizeArtistName:
    def test_strips_whitespace(self):
        assert normalize_artist_name("  The Beatles  ") == "The Beatles"

    def test_title_case(self):
        assert normalize_artist_name("the beatles") == "The Beatles"

    def test_handles_none(self):
        assert normalize_artist_name(None) is None

    def test_preserves_special_chars(self):
        assert normalize_artist_name("björk") == "Björk"


# ─────────────────────────────────────────────────────────────
# TESTS — Validation du schéma des tracks
# ─────────────────────────────────────────────────────────────

class TestValidateTrackSchema:

    def test_valid_track_passes(self, valid_track):
        errors = validate_track_schema(valid_track)
        assert errors == []

    def test_missing_title_fails(self, valid_track):
        track_no_title = {k: v for k, v in valid_track.items() if k != "title"}
        errors = validate_track_schema(track_no_title)
        assert any("title" in err for err in errors)

    def test_negative_duration_fails(self, valid_track):
        valid_track["duration_ms"] = -1
        errors = validate_track_schema(valid_track)
        assert len(errors) > 0

    def test_too_long_duration_fails(self, valid_track):
        valid_track["duration_ms"] = 36_000_001
        errors = validate_track_schema(valid_track)
        assert len(errors) > 0


# ─────────────────────────────────────────────────────────────
# TESTS — Validation des événements d'écoute
# ─────────────────────────────────────────────────────────────

class TestListeningEventValidation:

    def test_valid_event_passes(self, valid_listening_event):
        assert is_valid_listening_event(valid_listening_event) is True

    def test_missing_user_id_fails(self, valid_listening_event):
        del valid_listening_event["user_id"]
        assert is_valid_listening_event(valid_listening_event) is False

    def test_future_timestamp_fails(self, valid_listening_event):
        valid_listening_event["timestamp"] = "2099-01-01T00:00:00Z"
        assert is_valid_listening_event(valid_listening_event) is False

    def test_bot_pattern_detected(self):
        event = {
            "event_id": "123",
            "user_id": str(uuid.uuid4()),
            "track_id": str(uuid.uuid4()),
            "source_peer": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duration_ms": 100,
            "completed": False,
            "device_type": "mobile",
            "geo_country": "FR",
            "event_source": "p2p",
        }
        assert is_valid_listening_event(event) is False


# ─────────────────────────────────────────────────────────────
# TESTS — Déduplication
# ─────────────────────────────────────────────────────────────

class TestDeduplication:

    def test_removes_duplicate_artists_same_label(self, catalog_with_duplicates):
        result = deduplicate_artists(catalog_with_duplicates["artists"])
        names = [a["name"] for a in result]
        assert names.count("The Beatles") == 1

    def test_keeps_different_labels(self, catalog_with_duplicates):
        artists = [
            {"id": "1", "name": "Artist X", "label": "Label A"},
            {"id": "2", "name": "Artist X", "label": "Label B"},
        ]
        result = deduplicate_artists(artists)
        assert len(result) == 2


# ─────────────────────────────────────────────────────────────
# TESTS READY-TO-RUN — pas de TODO, disponibles immédiatement
# ─────────────────────────────────────────────────────────────

class TestDataGenerator:
    """Tests sur le générateur de données faker — pas de dépendance externe."""

    def test_generate_catalog_structure(self):
        """Le catalogue généré doit avoir la structure attendue par le DAG."""
        from src.data_generator.generate_catalog import generate_label_catalog

        catalog = generate_label_catalog("Test Label", n_artists=2)

        assert "label" in catalog
        assert "artists" in catalog
        assert "albums" in catalog
        assert "tracks" in catalog
        assert len(catalog["artists"]) == 2
        assert len(catalog["tracks"]) > 0

    def test_generated_track_has_required_fields(self):
        """Chaque track généré doit avoir les champs requis par le schéma PostgreSQL."""
        from src.data_generator.generate_catalog import generate_label_catalog

        catalog = generate_label_catalog("Test Label", n_artists=1)
        for track in catalog["tracks"]:
            assert "id" in track
            assert "artist_id" in track
            assert "title" in track
            assert "duration_ms" in track
            assert track["duration_ms"] > 0

    def test_generated_artist_has_label(self):
        """Chaque artiste doit être associé au bon label."""
        from src.data_generator.generate_catalog import generate_label_catalog

        catalog = generate_label_catalog("My Label", n_artists=3)
        for artist in catalog["artists"]:
            assert artist["label"] == "My Label"

    def test_track_ids_are_unique(self):
        """Les IDs des tracks doivent être uniques."""
        from src.data_generator.generate_catalog import generate_label_catalog

        catalog = generate_label_catalog("Test Label", n_artists=5)
        track_ids = [t["id"] for t in catalog["tracks"]]
        assert len(track_ids) == len(set(track_ids)), "IDs de tracks dupliqués détectés"
