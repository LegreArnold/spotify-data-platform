"""
DAG : aggregation_pipeline
============================
Calcule les agrégats quotidiens après la fin du streaming_events_pipeline.
Dépend de streaming_events_pipeline via ExternalTaskSensor.

Architecture :
    ExternalTaskSensor (attend streaming_events_pipeline)
        → compute_top_tracks()      ← top 50 du jour → daily_streams
        → compute_artist_stats()    ← streams + unique_listeners → artist_stats
        → compute_p2p_metrics()     ← taux cache_hit, latence moyenne
        → update_aggregates()       ← écriture PostgreSQL

TODO :
    [ ] Implémenter compute_top_tracks()
    [ ] Implémenter compute_artist_stats()
    [ ] Implémenter compute_p2p_metrics()
    [ ] Implémenter update_aggregates()
    [ ] Configurer correctement l'ExternalTaskSensor
    [ ] Stratégie incrémentale : calculer uniquement pour la date d'exécution
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

DAG_DOC = """
## aggregation_pipeline

### Rôle
Calcule les agrégats quotidiens (top tracks, stats artistes, métriques P2P)
après la fin du streaming_events_pipeline.

### Dépendances
Attend la fin de `streaming_events_pipeline` via ExternalTaskSensor.

### Destinations
- Table `daily_streams` : top 50 tracks par jour
- Table `artist_stats` : streams + unique listeners par artiste par jour

### Stratégie
Incrémentale : calcule uniquement pour `execution_date` (le jour courant).
Idempotente : INSERT ... ON CONFLICT (track_id, date) DO UPDATE SET ...

### TODO
Compléter les 4 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"


with DAG(
    dag_id="aggregation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Agrégats quotidiens : top tracks, stats artistes, métriques P2P",
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "aggregation"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_events = ExternalTaskSensor(
        task_id="wait_for_streaming_events",
        external_dag_id="streaming_events_pipeline",
        external_task_id=None,     # attend la fin du DAGRun complet
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="compute_top_tracks")
    def compute_top_tracks(**context) -> list:
        """
        Calcule le top 50 des tracks pour la date d'exécution.
        """
        execution_date = context.get("data_interval_start")
        target_date = execution_date.date() if execution_date else datetime.utcnow().date()

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT track_id,
                   COUNT(*) AS total_streams,
                   COUNT(DISTINCT user_id) AS unique_listeners,
                   SUM(duration_ms) AS total_duration_ms,
                   ARRAY_AGG(DISTINCT geo_country) AS countries
            FROM listening_events
            WHERE DATE(timestamp) = %s
              AND completed = TRUE
            GROUP BY track_id
            ORDER BY total_streams DESC
            LIMIT 50
            """,
            (target_date,)
        )
        rows = cursor.fetchall()
        cursor.close()
        return [
            {
                "track_id": row[0],
                "total_streams": row[1],
                "unique_listeners": row[2],
                "total_duration_ms": row[3],
                "countries": row[4],
                "date": target_date,
            }
            for row in rows
        ]

    @task(task_id="compute_artist_stats")
    def compute_artist_stats(**context) -> list:
        """
        Calcule les statistiques par artiste pour la date d'exécution.
        """
        execution_date = context.get("data_interval_start")
        target_date = execution_date.date() if execution_date else datetime.utcnow().date()

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT t.artist_id,
                   COUNT(*) AS total_streams,
                   COUNT(DISTINCT le.user_id) AS unique_listeners
            FROM listening_events le
            JOIN tracks t ON le.track_id = t.id
            WHERE DATE(le.timestamp) = %s
              AND le.completed = TRUE
            GROUP BY t.artist_id
            """,
            (target_date,),
        )
        stats_rows = cursor.fetchall()

        cursor.execute(
            """
            SELECT artist_id, track_id
            FROM (
              SELECT t.artist_id,
                     le.track_id,
                     ROW_NUMBER() OVER (PARTITION BY t.artist_id ORDER BY COUNT(*) DESC) AS rn
              FROM listening_events le
              JOIN tracks t ON le.track_id = t.id
              WHERE DATE(le.timestamp) = %s
                AND le.completed = TRUE
              GROUP BY t.artist_id, le.track_id
            ) ranked
            WHERE rn = 1
            """,
            (target_date,),
        )
        top_rows = cursor.fetchall()
        cursor.close()

        top_map = {row[0]: row[1] for row in top_rows}
        return [
            {
                "artist_id": row[0],
                "total_streams": row[1],
                "unique_listeners": row[2],
                "top_track_id": top_map.get(row[0]),
                "date": target_date,
            }
            for row in stats_rows
        ]

    @task(task_id="compute_p2p_metrics")
    def compute_p2p_metrics(**context) -> dict:
        """
        Calcule les métriques du réseau P2P pour la date d'exécution.
        """
        execution_date = context.get("data_interval_start")
        target_date = execution_date.date() if execution_date else datetime.utcnow().date()

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE event_source = 'cache')::float / NULLIF(COUNT(*), 0) AS cache_hit_rate,
                COUNT(DISTINCT source_peer_id) AS unique_peers,
                AVG(duration_ms) FILTER (WHERE duration_ms > 0) AS avg_duration_ms
            FROM listening_events
            WHERE DATE(timestamp) = %s
            """,
            (target_date,),
        )
        row = cursor.fetchone()
        cache_hit_rate = row[0] if row else 0.0
        unique_peers = row[1] if row else 0
        avg_duration_ms = row[2] if row else 0

        cursor.execute(
            """
            SELECT device_type, COUNT(*)
            FROM listening_events
            WHERE DATE(timestamp) = %s
            GROUP BY device_type
            """,
            (target_date,),
        )
        device_distribution = {r[0] or 'unknown': r[1] for r in cursor.fetchall()}

        cursor.execute(
            """
            SELECT geo_country, COUNT(*)
            FROM listening_events
            WHERE DATE(timestamp) = %s
            GROUP BY geo_country
            """,
            (target_date,),
        )
        country_distribution = {r[0] or 'unknown': r[1] for r in cursor.fetchall()}

        cursor.close()
        return {
            "date": target_date,
            "cache_hit_rate": cache_hit_rate or 0.0,
            "unique_peers": unique_peers,
            "avg_duration_ms": avg_duration_ms or 0,
            "device_distribution": device_distribution,
            "country_distribution": country_distribution,
        }

    @task(task_id="update_aggregates")
    def update_aggregates(top_tracks: list, artist_stats: list, p2p_metrics: dict, **context):
        """
        Écrit les agrégats dans PostgreSQL de façon idempotente.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        daily_query = """
            INSERT INTO daily_streams (track_id, date, total_streams, unique_listeners, total_duration_ms, countries, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (track_id, date) DO UPDATE
              SET total_streams = EXCLUDED.total_streams,
                  unique_listeners = EXCLUDED.unique_listeners,
                  total_duration_ms = EXCLUDED.total_duration_ms,
                  countries = EXCLUDED.countries,
                  updated_at = NOW()
        """
        artist_query = """
            INSERT INTO artist_stats (artist_id, date, total_streams, unique_listeners, top_track_id, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (artist_id, date) DO UPDATE
              SET total_streams = EXCLUDED.total_streams,
                  unique_listeners = EXCLUDED.unique_listeners,
                  top_track_id = EXCLUDED.top_track_id,
                  updated_at = NOW()
        """

        if top_tracks:
            cursor.executemany(
                daily_query,
                [(
                    item["track_id"],
                    item["date"],
                    item["total_streams"],
                    item["unique_listeners"],
                    item["total_duration_ms"],
                    item["countries"],
                ) for item in top_tracks],
            )

        if artist_stats:
            cursor.executemany(
                artist_query,
                [(
                    item["artist_id"],
                    item["date"],
                    item["total_streams"],
                    item["unique_listeners"],
                    item.get("top_track_id"),
                ) for item in artist_stats],
            )

        conn.commit()
        cursor.close()

        print(f"Agrégats mis à jour : {len(top_tracks)} top tracks, {len(artist_stats)} artist stats")
        print(f"P2P metrics : {p2p_metrics}")

    # ── Orchestration ─────────────────────────────────────────
    top_tracks   = compute_top_tracks()
    artist_stats = compute_artist_stats()
    p2p_metrics  = compute_p2p_metrics()

    wait_for_events >> [top_tracks, artist_stats, p2p_metrics]
    update_aggregates(top_tracks, artist_stats, p2p_metrics)
