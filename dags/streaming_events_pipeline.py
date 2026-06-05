"""
DAG : streaming_events_pipeline
=================================
Consomme les événements d'écoute depuis Redis (pub/sub),
les valide, les enrichit avec le catalogue et les stocke.

Planification : toutes les 5 minutes
Catchup       : désactivé (micro-batch temps réel)

Architecture :
    Redis (pub/sub listening_events + p2p_network_events)
        → consume_from_redis()
        → validate_events()          ← invalides → DLQ
        → enrich_events()            ← jointure catalogue PostgreSQL
        → store_to_parquet()         ← MinIO partitionné par heure
        → upsert_to_postgres()       ← table listening_events

TODO :
    [ ] Implémenter consume_from_redis() — accumuler les events sur 5 min
    [ ] Implémenter validate_events() — champs obligatoires, envoyer invalides en DLQ
    [ ] Implémenter enrich_events() — joindre avec le catalogue (track_id → artiste, genre)
    [ ] Implémenter store_to_parquet() — Parquet sur MinIO partitionné par heure
    [ ] Implémenter upsert_to_postgres() — insérer dans listening_events
    [ ] Utiliser TaskFlow API (@task) pour toutes les tâches
    [ ] Ajouter des branches conditionnelles : séparer listening_events et p2p_network_events
    [ ] Ajouter doc_md sur ce DAG
"""

import json
import os
import time
from datetime import datetime, timedelta

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import redis
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## streaming_events_pipeline

### Rôle
Consomme en micro-batch les événements du simulateur P2P depuis Redis,
les valide, les enrichit et les stocke en dual : Parquet (MinIO) + PostgreSQL.

### Sources
- Redis channel `listening_events`
- Redis channel `p2p_network_events`

### Destinations
- Table `listening_events` (PostgreSQL)
- Fichiers Parquet partitionnés sur MinIO : `s3://spotify-parquet/listening_events/date=.../hour=.../`
- Table `dead_letter_events` (pour les events invalides)

### Idempotence
Chaque event est identifié par `event_id` (UUID). L'upsert utilise
`ON CONFLICT (id) DO NOTHING` pour éviter les doublons.

### TODO
Compléter les 5 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_CHANNELS   = ["listening_events", "p2p_network_events"]
BATCH_WINDOW_SEC = 300  # 5 minutes


with DAG(
    dag_id="streaming_events_pipeline",
    default_args=DEFAULT_ARGS,
    description="Micro-batch : Redis → validation → enrichissement → MinIO + PostgreSQL",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "events", "streaming"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="consume_from_redis")
    def consume_from_redis(**context) -> dict:
        """
        Consomme les événements Redis publiés pendant la fenêtre de 5 minutes.
        """
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/1")
        client = redis.from_url(redis_url, decode_responses=True)
        pubsub = client.pubsub()
        pubsub.subscribe(*REDIS_CHANNELS)

        end_time = time.time() + BATCH_WINDOW_SEC
        events = {"listening": [], "p2p_network": []}

        while time.time() < end_time:
            message = pubsub.get_message(timeout=1)
            if message and message.get("type") == "message":
                try:
                    channel = message["channel"]
                    payload = json.loads(message["data"])
                    if channel == "listening_events":
                        events["listening"].append(payload)
                    elif channel == "p2p_network_events":
                        events["p2p_network"].append(payload)
                except json.JSONDecodeError:
                    print("Message Redis non-JSON ignoré")
            time.sleep(0.2)

        pubsub.close()
        print(f"Consommé {len(events['listening'])} listening et {len(events['p2p_network'])} p2p events")
        return events

    @task(task_id="validate_events")
    def validate_events(raw_events: dict, **context) -> dict:
        """
        Valide les événements et isole les invalides en DLQ.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        required_listening = ["event_id", "user_id", "track_id", "timestamp", "duration_ms"]
        required_p2p = ["event_id", "event_type", "peer_id", "timestamp"]

        valid_listening = []
        valid_p2p = []
        errors = 0
        invalid_rows = []

        def check_fields(event, required_fields):
            missing = [field for field in required_fields if field not in event or event.get(field) in (None, "")]
            if missing:
                return False, f"Missing fields: {missing}"
            return True, ""

        def parse_timestamp(value):
            try:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
                return True
            except Exception:
                return False

        for event in raw_events.get("listening", []):
            ok, msg = check_fields(event, required_listening)
            if ok and not parse_timestamp(event.get("timestamp")):
                ok = False
                msg = "Invalid timestamp format"
            if ok and not isinstance(event.get("duration_ms"), int):
                ok = False
                msg = "duration_ms must be integer"
            if ok and event.get("duration_ms", 0) <= 0:
                ok = False
                msg = "duration_ms must be > 0"
            if ok:
                valid_listening.append(event)
            else:
                errors += 1
                invalid_rows.append((
                    "streaming_events_pipeline:listening_events",
                    json.dumps(event, default=str),
                    "validation",
                    msg,
                ))

        for event in raw_events.get("p2p_network", []):
            ok, msg = check_fields(event, required_p2p)
            if ok and not parse_timestamp(event.get("timestamp")):
                ok = False
                msg = "Invalid timestamp format"
            if ok:
                valid_p2p.append(event)
            else:
                errors += 1
                invalid_rows.append((
                    "streaming_events_pipeline:p2p_network_events",
                    json.dumps(event, default=str),
                    "validation",
                    msg,
                ))

        if invalid_rows:
            cursor.executemany(
                "INSERT INTO dead_letter_events (original_topic, payload, error_type, error_message) VALUES (%s, %s::jsonb, %s, %s)",
                invalid_rows,
            )
            conn.commit()

        cursor.close()
        return {
            "valid_listening": valid_listening,
            "valid_p2p": valid_p2p,
            "errors": errors,
        }

    @task(task_id="enrich_events")
    def enrich_events(validated: dict, **context) -> list:
        """
        Enrichit les événements d'écoute avec les données du catalogue.
        """
        listening = validated.get("valid_listening", [])
        if not listening:
            return []

        track_ids = list({event["track_id"] for event in listening if "track_id" in event})
        if not track_ids:
            return []

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, title, artist_id, genre FROM tracks WHERE id = ANY(%s)",
            (track_ids,),
        )
        rows = cursor.fetchall()
        track_map = {row[0]: {"track_title": row[1], "artist_id": row[2], "genre": row[3]} for row in rows}

        enriched = []
        dlq_rows = []
        for event in listening:
            track_meta = track_map.get(event.get("track_id"))
            if not track_meta:
                dlq_rows.append((
                    "streaming_events_pipeline:listening_events",
                    json.dumps(event, default=str),
                    "unknown_track",
                    "track_id inconnu",
                ))
                continue
            copy_event = event.copy()
            copy_event.update(track_meta)
            enriched.append(copy_event)

        if dlq_rows:
            cursor.executemany(
                "INSERT INTO dead_letter_events (original_topic, payload, error_type, error_message) VALUES (%s, %s::jsonb, %s, %s)",
                dlq_rows,
            )
            conn.commit()

        cursor.close()
        return enriched

    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:
        """
        Sauvegarde les événements enrichis en Parquet sur MinIO.
        """
        if not enriched_events:
            return ""

        df = pd.DataFrame(enriched_events)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df = df.dropna(subset=["timestamp"])
        if df.empty:
            return ""

        df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        df["hour"] = df["timestamp"].dt.strftime("%H")

        run_id = context["dag_run"].run_id if context.get("dag_run") else "manual"
        date = df.iloc[0]["date"]
        hour = df.iloc[0]["hour"]
        key = f"listening_events/date={date}/hour={hour}/part-{run_id}.parquet"

        table = pa.Table.from_pandas(df.drop(columns=["date", "hour"]))
        local_path = f"/tmp/{run_id}.parquet"
        pq.write_table(table, local_path)

        endpoint_url = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
        )
        s3.upload_file(local_path, "spotify-parquet", key)

        print(f"Fichier Parquet écrit : s3://spotify-parquet/{key}")
        return f"s3://spotify-parquet/{key}"

    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list, **context) -> dict:
        """
        Insère les événements dans PostgreSQL de façon idempotente.
        """
        if not enriched_events:
            return {"inserted": 0, "skipped": 0}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        query = """
            INSERT INTO listening_events
                (id, user_id, track_id, source_peer_id, timestamp, duration_ms,
                 device_type, geo_country, completed, event_source, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO NOTHING
        """

        rows = []
        for event in enriched_events:
            timestamp = event.get("timestamp")
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            rows.append((
                event.get("event_id"),
                event.get("user_id"),
                event.get("track_id"),
                event.get("source_peer"),
                timestamp,
                event.get("duration_ms"),
                event.get("device_type"),
                event.get("geo_country"),
                event.get("completed", False),
                event.get("event_source"),
            ))

        cursor.executemany(query, rows)
        conn.commit()
        inserted = cursor.rowcount if cursor.rowcount is not None else len(rows)
        cursor.close()
        return {"inserted": inserted, "skipped": max(0, len(rows) - inserted)}

    # ── Orchestration ─────────────────────────────────────────
    raw       = consume_from_redis()
    validated = validate_events(raw)
    enriched  = enrich_events(validated)

    store_to_parquet(enriched)
    upsert_to_postgres(enriched)
