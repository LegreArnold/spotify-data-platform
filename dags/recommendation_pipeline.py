"""
DAG : recommendation_pipeline
================================
Génère les recommandations personnalisées via collaborative filtering
et les stocke dans Redis + PostgreSQL.

Dépend de aggregation_pipeline via ExternalTaskSensor.

TODO :
    [ ] Implémenter build_user_track_matrix()
    [ ] Implémenter compute_recommendations()
    [ ] Implémenter store_recommendations()
    [ ] Ajouter doc_md sur ce DAG
"""

import json
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import redis
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from sklearn.metrics.pairwise import cosine_similarity

DAG_DOC = """
## recommendation_pipeline

### Rôle
Génère un top-10 de recommandations par utilisateur actif
via collaborative filtering (similarité cosinus entre profils d'écoute).

### Dépendances
Attend la fin de `aggregation_pipeline` via ExternalTaskSensor.

### Destinations
- Redis : clé `reco:{user_id}` → liste de track_ids (TTL 24h)
- PostgreSQL : table `recommendations`

### Algorithme
Collaborative filtering simplifié :
1. Construire la matrice user × track (écoutes des 7 derniers jours)
2. Calculer la similarité cosinus entre utilisateurs
3. Pour chaque user, recommander les tracks aimés par ses voisins

### TODO
Compléter les 3 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_URL        = "redis://redis:6379/1"
RECO_TTL_SECONDS = 86400   # 24 heures
TOP_N_RECO       = 10
LOOKBACK_DAYS    = 7


with DAG(
    dag_id="recommendation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Collaborative filtering → recommandations Redis + PostgreSQL",
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "recommendation", "ml"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_aggregation = ExternalTaskSensor(
        task_id="wait_for_aggregation",
        external_dag_id="aggregation_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="build_user_track_matrix")
    def build_user_track_matrix(**context) -> dict:
        """
        Construit la matrice user × track des écoutes des 7 derniers jours.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT user_id, track_id, COUNT(*) AS play_count
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '7 days'
              AND completed = TRUE
            GROUP BY user_id, track_id
            """
        )
        rows = cursor.fetchall()
        cursor.close()

        df = pd.DataFrame(rows, columns=["user_id", "track_id", "play_count"])
        if df.empty:
            return {"matrix": {}, "users": []}

        counts = df.groupby("user_id")["track_id"].nunique()
        active_users = counts[counts >= 3].index.tolist()
        df = df[df["user_id"].isin(active_users)]

        matrix = {}
        for user_id, sub in df.groupby("user_id"):
            matrix[user_id] = {row.track_id: int(row.play_count) for row in sub.itertuples(index=False)}

        return {"matrix": matrix, "users": active_users}

    @task(task_id="compute_recommendations")
    def compute_recommendations(matrix_data: dict, **context) -> dict:
        """
        Calcule les recommandations par similarité cosinus.
        """
        matrix = matrix_data.get("matrix", {})
        users = matrix_data.get("users", [])
        if not matrix or len(users) < 2:
            return {}

        all_tracks = sorted({track for tracks in matrix.values() for track in tracks})
        user_ids = users
        user_index = {user: idx for idx, user in enumerate(user_ids)}
        track_index = {track: idx for idx, track in enumerate(all_tracks)}

        data = np.zeros((len(user_ids), len(all_tracks)), dtype=float)
        for user_id, tracks in matrix.items():
            if user_id not in user_index:
                continue
            for track_id, count in tracks.items():
                data[user_index[user_id], track_index[track_id]] = count

        similarity = cosine_similarity(data)
        recommendations = {}

        for i, user_id in enumerate(user_ids):
            user_vector = data[i]
            neighbors = np.argsort(-similarity[i])
            neighbors = [n for n in neighbors if n != i][:5]
            scores = {}
            for neighbor in neighbors:
                weight = similarity[i, neighbor]
                if weight <= 0:
                    continue
                neighbor_tracks = data[neighbor]
                for j, count in enumerate(neighbor_tracks):
                    if count > 0 and user_vector[j] == 0:
                        scores[all_tracks[j]] = scores.get(all_tracks[j], 0.0) + weight * count

            recommended = sorted(scores.items(), key=lambda item: -item[1])[:TOP_N_RECO]
            recommendations[user_id] = [track_id for track_id, _ in recommended]

        return recommendations

    @task(task_id="store_recommendations")
    def store_recommendations(recommendations: dict, **context) -> dict:
        """
        Stocke les recommandations dans Redis et PostgreSQL.
        """
        if not recommendations:
            return {"users_with_recos": 0, "total_recommendations": 0}

        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO recommendations (user_id, track_id, score, generated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (user_id, track_id) DO UPDATE
              SET score = EXCLUDED.score,
                  generated_at = NOW()
        """

        total = 0
        for user_id, track_ids in recommendations.items():
            redis_client.setex(f"reco:{user_id}", RECO_TTL_SECONDS, json.dumps(track_ids))
            for idx, track_id in enumerate(track_ids):
                score = max(0.0, float(len(track_ids) - idx))
                cursor.execute(insert_query, (user_id, track_id, score))
                total += 1

        conn.commit()
        cursor.close()

        return {"users_with_recos": len(recommendations), "total_recommendations": total}

    # ── Orchestration ─────────────────────────────────────────
    matrix        = build_user_track_matrix()
    recommendations = compute_recommendations(matrix)

    wait_for_aggregation >> matrix
    store_recommendations(recommendations)
