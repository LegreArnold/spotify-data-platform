from typing import Any, Dict, Iterable, List


def normalize_artist_name(name: str) -> str | None:
    if name is None:
        return None
    normalized = " ".join(part.capitalize() for part in str(name).strip().split())
    return normalized if normalized != "" else None


def validate_track_schema(track: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    required = ["id", "artist_id", "title", "duration_ms"]

    for field in required:
        if field not in track or track.get(field) in (None, ""):
            errors.append(f"Missing required field: {field}")

    duration = track.get("duration_ms")
    if isinstance(duration, int):
        if duration <= 0:
            errors.append("duration_ms must be positive")
        if duration > 3_600_000:
            errors.append("duration_ms is too long")
    else:
        errors.append("duration_ms must be an integer")

    return errors


def deduplicate_artists(artists: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    unique: Dict[tuple[str, str], Dict[str, Any]] = {}
    for artist in artists:
        name = normalize_artist_name(artist.get("name")) or ""
        label = str(artist.get("label", "")).strip().lower()
        key = (name.lower(), label)
        if key not in unique:
            copy_artist = artist.copy()
            copy_artist["name"] = name
            unique[key] = copy_artist
    return list(unique.values())


def deduplicate_tracks(tracks: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[tuple[str, str, int]] = set()
    unique_tracks: List[Dict[str, Any]] = []
    for track in tracks:
        track_id = track.get("id")
        title = str(track.get("title", "")).strip().lower()
        duration = track.get("duration_ms")
        key = (track_id, title, duration)
        if key not in seen:
            seen.add(key)
            unique_tracks.append(track.copy())
    return unique_tracks
