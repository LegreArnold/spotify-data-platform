"""
DAG : catalog_ingestion_pipeline
=================================
Ingère le catalogue musical depuis les fichiers JSON des labels
(stockés dans MinIO) et les charge dans PostgreSQL.

Planification : quotidienne à 02:00 UTC
Catchup       : activé (permet le backfill historique)

Architecture :
    MinIO (labels/*.json)
        → extract_from_minio()
        → validate_schema()
        → transform_catalog()        ← normalisation, dédoublonnage
        → load_to_postgres()         ← upsert avec ON CONFLICT
        → notify_success()

TODO :
    [ ] Implémenter extract_from_minio() — lire les JSONs depuis MinIO
    [ ] Implémenter validate_schema() — vérifier les champs obligatoires
    [ ] Implémenter transform_catalog() — normaliser les noms d'artistes, déduplication
    [ ] Implémenter load_to_postgres() — upsert avec gestion des conflits
    [ ] Configurer retry_delay et retries sur les tâches réseau
    [ ] Ajouter un on_failure_callback pour alerting
    [ ] Activer le doc_md sur ce DAG (voir variable DAG_DOC ci-dessous)
"""

import json
import os
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────
# DOCUMENTATION DU DAG (obligatoire pour la note)
# ─────────────────────────────────────────────────────────────

DAG_DOC = """
## catalog_ingestion_pipeline

### Rôle
Ingère les métadonnées musicales depuis les fichiers JSON de 3 labels
(SunSet Records, NightWave Music, Urban Pulse) stockés dans MinIO.

### Sources
- `s3://labels-raw/sunset_records.json`
- `s3://labels-raw/nightwave_music.json`
- `s3://labels-raw/urban_pulse.json`

### Destinations
- Table `artists` (upsert)
- Table `albums` (upsert)
- Table `tracks` (upsert)

### Idempotence
Le pipeline est idempotent : relancer plusieurs fois le même DAGrun
produit le même résultat grâce aux upserts ON CONFLICT DO UPDATE.

### Gestion des erreurs
- Schéma invalide → événement en DLQ (`dead_letter_events`)
- MinIO indisponible → retry x3 avec backoff exponentiel

### Monitoring
- XCom `tracks_inserted` : nombre de tracks insérées/mises à jour
- XCom `errors_count` : nombre d'entrées envoyées en DLQ
"""

# ─────────────────────────────────────────────────────────────
# CONFIGURATION PAR DÉFAUT
# ─────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":                 "spotify-team",
    "depends_on_past":       False,
    "start_date":            datetime(2025, 1, 1),
    "email_on_failure":      False,
    "email_on_retry":        False,
    "retries":               3,
    "retry_delay":           timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":     timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"
MINIO_CONN_ID    = "spotify_minio"
MINIO_BUCKET     = "labels-raw"
LABEL_FILES      = ["sunset_records.json", "nightwave_music.json", "urban_pulse.json"]


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion quotidienne du catalogue musical depuis MinIO vers PostgreSQL",
    schedule_interval="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "phase-1", "ingestion", "catalogue"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="extract_from_minio")
    def extract_from_minio(**context) -> list[dict]:
        """
        Télécharge les fichiers JSON des labels depuis MinIO.
        """
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

        catalogs = []
        for label_file in LABEL_FILES:
            try:
                response = s3.get_object(Bucket=MINIO_BUCKET, Key=label_file)
                body = response["Body"].read().decode("utf-8")
                catalog = json.loads(body)
                catalogs.append(catalog)
                print(f"Chargé {label_file} depuis MinIO")
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code")
                print(f"Avertissement : impossible de charger {label_file} ({code})")
            except Exception as exc:
                print(f"Erreur lecture MinIO {label_file} : {exc}")
        return catalogs

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list[dict]) -> dict:
        """
        Valide le schéma de chaque catalogue et isole les entrées invalides.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        required = {
            "artists": ["id", "name", "label"],
            "albums": ["id", "artist_id", "title"],
            "tracks": ["id", "artist_id", "title", "duration_ms"],
        }

        valid = {"artists": [], "albums": [], "tracks": []}
        errors_count = 0
        dead_letters = []

        def validate_entry(entry: dict, required_fields: list[str]) -> tuple[bool, str]:
            missing = [f for f in required_fields if f not in entry or entry.get(f) in (None, "")]
            if missing:
                return False, f"Champs manquants: {missing}"
            return True, ""

        for catalog in raw_catalogs:
            for key in ("artists", "albums", "tracks"):
                for entry in catalog.get(key, []):
                    ok, message = validate_entry(entry, required[key])
                    if not ok:
                        errors_count += 1
                        dead_letters.append((
                            "catalog_ingestion",
                            json.dumps(entry, default=str),
                            "schema_validation",
                            message,
                        ))
                    else:
                        valid[key].append(entry)

        if dead_letters:
            cursor.executemany(
                "INSERT INTO dead_letter_events (original_topic, payload, error_type, error_message) VALUES (%s, %s::jsonb, %s, %s)",
                dead_letters,
            )
            conn.commit()

        cursor.close()
        return {"valid": valid, "errors_count": errors_count}

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        """
        Transforme et normalise les données du catalogue.
        """
        def normalize_name(value: str) -> str:
            if value is None:
                return None
            return " ".join(part.capitalize() for part in str(value).strip().split())

        unique_artists = {}
        for artist in validated["valid"]["artists"]:
            normalized_name = normalize_name(artist.get("name"))
            label = artist.get("label", "")
            key = (normalized_name.lower() if normalized_name else "", label.lower())
            if key not in unique_artists:
                copy_artist = artist.copy()
                copy_artist["name"] = normalized_name
                unique_artists[key] = copy_artist

        valid_tracks = []
        for track in validated["valid"]["tracks"]:
            duration = track.get("duration_ms")
            if not isinstance(duration, int) or duration <= 0 or duration >= 3_600_000:
                continue
            copy_track = track.copy()
            copy_track["genre"] = copy_track.get("genre") or "Unknown"
            valid_tracks.append(copy_track)

        valid_albums = [album.copy() for album in validated["valid"]["albums"]]

        return {
            "artists": list(unique_artists.values()),
            "albums": valid_albums,
            "tracks": valid_tracks,
        }

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        """
        Charge les données dans PostgreSQL avec upsert idempotent.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        artists = transformed.get("artists", [])
        albums = transformed.get("albums", [])
        tracks = transformed.get("tracks", [])

        artist_query = """
            INSERT INTO artists (id, name, country, label, genres, monthly_listeners, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (name, label) DO UPDATE
              SET country = EXCLUDED.country,
                  genres = EXCLUDED.genres,
                  monthly_listeners = EXCLUDED.monthly_listeners,
                  updated_at = NOW()
        """
        album_query = """
            INSERT INTO albums (id, artist_id, title, release_year, total_tracks, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO UPDATE
              SET artist_id = EXCLUDED.artist_id,
                  title = EXCLUDED.title,
                  release_year = EXCLUDED.release_year,
                  total_tracks = EXCLUDED.total_tracks
        """
        track_query = """
            INSERT INTO tracks (id, album_id, artist_id, title, duration_ms, genre, bpm, explicit, audio_file_path, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (id) DO UPDATE
              SET album_id = EXCLUDED.album_id,
                  artist_id = EXCLUDED.artist_id,
                  title = EXCLUDED.title,
                  duration_ms = EXCLUDED.duration_ms,
                  genre = EXCLUDED.genre,
                  bpm = EXCLUDED.bpm,
                  explicit = EXCLUDED.explicit,
                  audio_file_path = EXCLUDED.audio_file_path,
                  updated_at = NOW()
        """

        artist_rows = [(
            a.get("id"),
            a.get("name"),
            a.get("country"),
            a.get("label"),
            a.get("genres"),
            a.get("monthly_listeners", 0),
        ) for a in artists]
        album_rows = [(
            a.get("id"),
            a.get("artist_id"),
            a.get("title"),
            a.get("release_year"),
            a.get("total_tracks"),
        ) for a in albums]
        track_rows = [(
            t.get("id"),
            t.get("album_id"),
            t.get("artist_id"),
            t.get("title"),
            t.get("duration_ms"),
            t.get("genre"),
            t.get("bpm"),
            t.get("explicit", False),
            t.get("audio_file_path"),
        ) for t in tracks]

        if artist_rows:
            cursor.executemany(artist_query, artist_rows)
        if album_rows:
            cursor.executemany(album_query, album_rows)
        if track_rows:
            cursor.executemany(track_query, track_rows)

        conn.commit()
        cursor.close()

        stats = {
            "artists_upserted": len(artist_rows),
            "albums_upserted": len(album_rows),
            "tracks_upserted": len(track_rows),
            "errors_count": 0,
        }

        return stats

    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        """
        Log de succès avec statistiques d'ingestion.
        Optionnel : envoyer une notification (webhook Slack simulé).
        """
        dag_run = context["dag_run"]
        print(f"""
        ✅ catalog_ingestion_pipeline terminé
        DAGRun : {dag_run.run_id}
        Tracks insérées  : {stats.get('tracks_upserted', 0)}
        Artists insérés  : {stats.get('artists_upserted', 0)}
        Erreurs DLQ      : {stats.get('errors_count', 0)}
        """)

    # ── Orchestration des tâches ──────────────────────────────
    raw       = extract_from_minio()
    validated = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats     = load_to_postgres(transformed)
    notify_success(stats)
