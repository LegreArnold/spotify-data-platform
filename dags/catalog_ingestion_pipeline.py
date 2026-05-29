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
        raise NotImplementedError("TODO : implémenter extract_from_minio()")

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
        raise NotImplementedError("TODO : implémenter validate_schema()")

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
        raise NotImplementedError("TODO : implémenter transform_catalog()")

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
        raise NotImplementedError("TODO : implémenter load_to_postgres()")

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
