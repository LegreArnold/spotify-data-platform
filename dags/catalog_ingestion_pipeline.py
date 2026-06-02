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

from datetime import datetime, timedelta

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

        TODO :
            1. Se connecter à MinIO via AwsBaseHook ou boto3
               (endpoint_url = http://minio:9000)
            2. Pour chaque fichier dans LABEL_FILES, télécharger et parser le JSON
            3. Retourner une liste de catalogues : [catalog_label_a, catalog_label_b, ...]
            4. Si un fichier est manquant : logger un warning et continuer
               (pas de crash — on traite ce qu'on a)

        Returns:
            list[dict] : catalogues bruts des labels
        """
        import boto3, json, os
        s3 = boto3.client('s3',
            endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin')
        catalogs = []
        for filename in LABEL_FILES:
            try:
                obj = s3.get_object(Bucket=MINIO_BUCKET, Key=filename)
                catalog = json.loads(obj['Body'].read())
                catalogs.append(catalog)
                print(f"Extrait: {filename} - {len(catalog.get('tracks', []))} tracks")
            except Exception as e:
                print(f"Warning: {filename} manquant: {e}")
        return catalogs

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list[dict]) -> dict:
        """
        Valide le schéma de chaque catalogue et isole les entrées invalides.

        Champs obligatoires pour un artiste  : id, name, label
        Champs obligatoires pour un album    : id, artist_id, title
        Champs obligatoires pour un track    : id, artist_id, title, duration_ms

        TODO :
            1. Parcourir artists, albums, tracks de chaque catalogue
            2. Pour chaque entrée, vérifier la présence des champs obligatoires
            3. Les entrées invalides → insérer dans dead_letter_events avec error_type="schema_validation"
            4. Retourner {"valid": {...}, "errors_count": N}

        Hint : utiliser PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        """
        import json
        REQUIRED_ARTIST = ["id", "name", "label"]
        REQUIRED_ALBUM  = ["id", "artist_id", "title"]
        REQUIRED_TRACK  = ["id", "artist_id", "title", "duration_ms"]
        valid = {"artists": [], "albums": [], "tracks": []}
        errors = []
        for catalog in raw_catalogs:
            for a in catalog.get("artists", []):
                (valid["artists"] if all(k in a for k in REQUIRED_ARTIST) else errors).append(a)
            for al in catalog.get("albums", []):
                (valid["albums"] if all(k in al for k in REQUIRED_ALBUM) else errors).append(al)
            for t in catalog.get("tracks", []):
                (valid["tracks"] if all(k in t for k in REQUIRED_TRACK) else errors).append(t)
        if errors:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn(); cur = conn.cursor()
            for err in errors:
                cur.execute("INSERT INTO dead_letter_events (original_topic, payload, error_type, error_message) VALUES (%s, %s, %s, %s)",
                    ("catalog_ingestion", json.dumps(err), "schema_validation", "champ obligatoire manquant"))
            conn.commit(); cur.close(); conn.close()
        print(f"Valides: {len(valid['artists'])} artistes, {len(valid['tracks'])} tracks | DLQ: {len(errors)}")
        return {"valid": valid, "errors_count": len(errors)}

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        """
        Transforme et normalise les données du catalogue.

        TODO :
            1. Normaliser les noms d'artistes (strip, title case, suppression doublons)
            2. Valider les durées de tracks (duration_ms > 0 et < 3_600_000)
            3. Normaliser les genres (correspondance avec la table genres)
            4. Construire les listes d'upsert : artists[], albums[], tracks[]

        Returns:
            dict avec keys "artists", "albums", "tracks"
        """
        valid = validated["valid"]
        seen = set()
        artists = []
        for a in valid["artists"]:
            a["name"] = a["name"].strip().title()
            key = (a["name"], a["label"])
            if key not in seen:
                seen.add(key)
                artists.append(a)
        tracks = [t for t in valid["tracks"] if 0 < t["duration_ms"] < 3_600_000]
        print(f"Après transformation: {len(artists)} artistes uniques, {len(tracks)} tracks valides")
        return {"artists": artists, "albums": valid["albums"], "tracks": tracks}

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        """
        Charge les données dans PostgreSQL avec upsert idempotent.

        TODO :
            1. Utiliser PostgresHook pour obtenir une connexion
            2. Artists : INSERT ... ON CONFLICT (name, label) DO UPDATE SET ...
            3. Albums  : INSERT ... ON CONFLICT (id) DO UPDATE SET ...
            4. Tracks  : INSERT ... ON CONFLICT (id) DO UPDATE SET updated_at=NOW()
            5. Commit et retourner les stats {tracks_inserted, artists_inserted, ...}
            6. Pousser stats dans XCom pour le monitoring

        Hint : utiliser executemany() avec des listes de tuples pour les performances.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn(); cur = conn.cursor()
        for a in transformed["artists"]:
            cur.execute("""INSERT INTO artists (id, name, country, label, genres, monthly_listeners)
                VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (name, label) DO UPDATE SET
                monthly_listeners=EXCLUDED.monthly_listeners, updated_at=NOW()""",
                (a["id"], a["name"], a.get("country"), a["label"], a.get("genres",[]), a.get("monthly_listeners",0)))
        cur.execute("SELECT id, name, label FROM artists")
        artist_map = {(r[1], r[2]): str(r[0]) for r in cur.fetchall()}
        id_map = {a["id"]: artist_map.get((a["name"], a["label"]), a["id"]) for a in transformed["artists"]}
        for al in transformed["albums"]:
            cur.execute("""INSERT INTO albums (id, artist_id, title, release_year, total_tracks)
                VALUES (%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING""",
                (al["id"], id_map.get(al["artist_id"], al["artist_id"]), al["title"], al.get("release_year"), al.get("total_tracks")))
        for t in transformed["tracks"]:
            cur.execute("""INSERT INTO tracks (id, album_id, artist_id, title, duration_ms, genre, bpm, explicit, audio_file_path)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO UPDATE SET updated_at=NOW()""",
                (t["id"], t.get("album_id"), id_map.get(t["artist_id"], t["artist_id"]), t["title"],
                 t["duration_ms"], t.get("genre"), t.get("bpm"), t.get("explicit", False), t.get("audio_file_path")))
        conn.commit(); cur.close(); conn.close()
        stats = {"artists_inserted": len(transformed["artists"]), "albums_inserted": len(transformed["albums"]), "tracks_inserted": len(transformed["tracks"])}
        print(f"Chargé: {stats}")
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
        Tracks insérées  : {stats.get('tracks_inserted', 0)}
        Artists insérés  : {stats.get('artists_inserted', 0)}
        Erreurs DLQ      : {stats.get('errors_count', 0)}
        """)

    # ── Orchestration des tâches ──────────────────────────────
    raw       = extract_from_minio()
    validated = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats     = load_to_postgres(transformed)
    notify_success(stats)
