from datetime import datetime, timedelta, timezone
from typing import Any, Dict


REQUIRED_FIELDS = [
    "event_id",
    "user_id",
    "track_id",
    "timestamp",
    "duration_ms",
    "device_type",
    "geo_country",
    "event_source",
]


def is_valid_listening_event(event: Dict[str, Any]) -> bool:
    if not isinstance(event, dict):
        return False

    for field in REQUIRED_FIELDS:
        if field not in event or event.get(field) in (None, ""):
            return False

    duration = event.get("duration_ms")
    if not isinstance(duration, int) or duration <= 0:
        return False

    timestamp = event.get("timestamp")
    try:
        dt = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
    except Exception:
        return False

    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if dt > now + timedelta(seconds=5):
        return False

    if duration < 5000:
        return False

    if isinstance(event.get("event_source"), str) and event["event_source"] == "fraud":
        return False

    return True


def enrich_listening_event(event: Dict[str, Any], track_metadata: Dict[str, Any]) -> Dict[str, Any]:
    enriched = event.copy()
    enriched.update(track_metadata)
    return enriched
