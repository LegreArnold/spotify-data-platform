"""
DAG : dlq_reprocessing_pipeline
==================================
Retraite périodiquement les événements défectueux de la Dead Letter Queue.

Planification : toutes les heures
Catchup       : désactivé

Architecture :
    PostgreSQL dead_letter_events (status='pending')
        → fetch_pending_dlq()       ← récupérer les events à retraiter
        → reprocess_events()        ← tenter de corriger et réinjecter
        → update_dlq_status()       ← marquer reprocessed ou abandoned

TODO :
    [ ] Implémenter fetch_pending_dlq()
    [ ] Implémenter reprocess_events()
    [ ] Implémenter update_dlq_status()
    [ ] Tester avec injection de données corrompues
    [ ] Ajouter doc_md sur ce DAG
"""

import json
import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## dlq_reprocessing_pipeline

### Rôle
Retraite les événements défectueux isolés dans `dead_letter_events`.
Tente de corriger les erreurs et de réinjecter les events valides.

### Sources
- Table `dead_letter_events` où `status = 'pending'`

### Logique de retraitement
1. Récupérer les events `pending` avec `retry_count < 3`
2. Tenter la validation et la correction
3. Si succès → réinjecter dans `listening_events` + `status = 'reprocessed'`
4. Si échec après 3 tentatives → `status = 'abandoned'`

### Test d'\''injection
```sql
INSERT INTO dead_letter_events (payload, error_type, original_topic)
VALUES ('{"user_id": null, "track_id": "invalid"}', 'missing_fields', 'listening_events');
```

### TODO
Compléter les 3 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

POSTGRES_CONN_ID = "spotify_postgres"
MAX_RETRIES      = 3
BATCH_SIZE       = 100   # traiter par lots pour ne pas surcharger


with DAG(
    dag_id="dlq_reprocessing_pipeline",
    default_args=DEFAULT_ARGS,
    description="Retraitement horaire des événements Dead Letter Queue",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "dlq", "resilience"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="fetch_pending_dlq")
    def fetch_pending_dlq(**context) -> list:
        """
        Récupère les événements en attente de retraitement.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, payload, error_type, retry_count, original_topic
            FROM dead_letter_events
            WHERE status = 'pending'
              AND retry_count < %s
            ORDER BY created_at ASC
            LIMIT %s
            """,
            (MAX_RETRIES, BATCH_SIZE),
        )
        rows = cursor.fetchall()
        cursor.close()
        print(f"{len(rows)} événements DLQ pending trouvés")
        return [
            {
                "id": row[0],
                "payload": row[1],
                "error_type": row[2],
                "retry_count": row[3],
                "original_topic": row[4],
            }
            for row in rows
        ]

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list, **context) -> dict:
        """
        Tente de corriger et réinjecter chaque événement défectueux.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        reprocessed = []
        failed = []

        for record in pending_events:
            payload = record.get("payload")
            try:
                event = json.loads(payload)
            except Exception as exc:
                failed.append({"id": record["id"], "reason": f"JSON invalide : {exc}"})
                continue

            if not event.get("user_id"):
                failed.append({"id": record["id"], "reason": "user_id manquant"})
                continue

            timestamp = event.get("timestamp")
            try:
                event_time = datetime.fromisoformat(timestamp.replace("Z", "+00:00")) if timestamp else None
            except Exception:
                event_time = None

            if not event_time:
                event_time = datetime.utcnow()

            track_id = event.get("track_id")
            cursor.execute("SELECT 1 FROM tracks WHERE id = %s", (track_id,))
            if cursor.fetchone() is None:
                failed.append({"id": record["id"], "reason": "track_id inconnu"})
                continue

            if not isinstance(event.get("duration_ms"), int) or event.get("duration_ms", 0) <= 0:
                failed.append({"id": record["id"], "reason": "duration_ms invalide"})
                continue

            reprocessed.append({
                "id": record["id"],
                "event": {
                    "event_id": event.get("event_id") or str(uuid.uuid4()),
                    "user_id": event["user_id"],
                    "track_id": track_id,
                    "source_peer": event.get("source_peer"),
                    "timestamp": event_time.isoformat(),
                    "duration_ms": event["duration_ms"],
                    "device_type": event.get("device_type"),
                    "geo_country": event.get("geo_country"),
                    "completed": event.get("completed", False),
                    "event_source": event.get("event_source", "p2p"),
                },
            })

        cursor.close()
        return {"reprocessed": reprocessed, "failed": failed}

    @task(task_id="update_dlq_status")
    def update_dlq_status(results: dict, **context) -> dict:
        """
        Met à jour le statut des événements dans dead_letter_events.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        reprocessed = results.get("reprocessed", [])
        failed = results.get("failed", [])
        reprocessed_count = 0

        insert_query = """
            INSERT INTO listening_events
                (id, user_id, track_id, source_peer_id, timestamp, duration_ms,
                 device_type, geo_country, completed, event_source, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO NOTHING
        """

        for row in reprocessed:
            event = row["event"]
            cursor.execute(insert_query, (
                event.get("event_id"),
                event.get("user_id"),
                event.get("track_id"),
                event.get("source_peer"),
                event.get("timestamp"),
                event.get("duration_ms"),
                event.get("device_type"),
                event.get("geo_country"),
                event.get("completed", False),
                event.get("event_source"),
            ))
            cursor.execute(
                "UPDATE dead_letter_events SET status = 'reprocessed', resolved_at = NOW() WHERE id = %s",
                (row["id"],),
            )
            reprocessed_count += 1

        for row in failed:
            cursor.execute(
                "UPDATE dead_letter_events SET retry_count = retry_count + 1, last_retry_at = NOW(), status = CASE WHEN retry_count + 1 >= %s THEN 'abandoned' ELSE 'pending' END WHERE id = %s",
                (MAX_RETRIES, row["id"]),
            )

        conn.commit()
        cursor.execute("SELECT COUNT(*) FROM dead_letter_events WHERE status = 'pending'")
        pending_count = cursor.fetchone()[0]
        cursor.close()

        print(f"DLQ : {reprocessed_count} retraités, {len(failed)} échecs, {pending_count} encore pending")
        return {
            "reprocessed": reprocessed_count,
            "failed": len(failed),
            "pending": pending_count,
        }

    # ── Orchestration ─────────────────────────────────────────
    pending = fetch_pending_dlq()
    results = reprocess_events(pending)
    update_dlq_status(results)
