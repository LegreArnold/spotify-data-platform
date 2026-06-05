"""
Spark Job : streaming_trends_job
==================================
Consomme le topic Kafka `listening_events` et produit en continu
les tendances musicales temps réel.

Outputs :
    - PostgreSQL → table `realtime_top_tracks` (top 10 par fenêtre de 5 min)
    - Redis      → clé `top_tracks:live` (top genres par sliding window)

Lancement :
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.1 \\
        spark_jobs/streaming_trends_job.py

TODO :
    [ ] Implémenter la lecture du topic Kafka avec readStream
    [ ] Désérialiser les messages JSON avec le bon schéma
    [ ] Implémenter les fenêtres tumbling de 5 minutes
    [ ] Implémenter les sliding windows pour les genres (15 min / 5 min)
    [ ] Configurer le checkpoint sur MinIO
    [ ] Écrire les résultats dans PostgreSQL et Redis
"""

import json
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka-1:9092")
KAFKA_TOPIC      = "listening_events"
CHECKPOINT_PATH  = "s3a://spotify-checkpoints/streaming_trends"
POSTGRES_URL     = os.getenv("SPOTIFY_POSTGRES_URL",
                             "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS   = {
    "user":   "spotify",
    "password": "spotify",
    "driver": "org.postgresql.Driver",
}

# ─────────────────────────────────────────────────────────────
# SCHÉMA DES ÉVÉNEMENTS D'ÉCOUTE
# ─────────────────────────────────────────────────────────────

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),    False),
    StructField("user_id",     StringType(),    False),
    StructField("track_id",    StringType(),    False),
    StructField("source_peer", StringType(),    True),
    StructField("timestamp",   StringType(),    False),  # ISO 8601 → à caster en Timestamp
    StructField("duration_ms", IntegerType(),   True),
    StructField("device_type", StringType(),    True),
    StructField("geo_country", StringType(),    True),
    StructField("completed",   BooleanType(),   True),
    StructField("event_source",StringType(),    True),
])


# ─────────────────────────────────────────────────────────────
# INITIALISATION SPARK
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Crée et configure la SparkSession avec les dépendances nécessaires.

    TODO : vérifier que les packages kafka et postgresql sont disponibles
    """
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-trends")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint",             "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",           "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",           "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access",    "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# LECTURE KAFKA
# ─────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    """
    Lit le topic Kafka `listening_events` en streaming.
    """
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = raw_df.select(
        F.col("key").cast("string"),
        F.from_json(F.col("value").cast("string"), LISTENING_EVENT_SCHEMA).alias("data"),
    ).select("data.*")

    events_df = parsed_df.withColumn("event_time", F.to_timestamp("timestamp"))
    return events_df


# ─────────────────────────────────────────────────────────────
# AGRÉGATIONS STREAMING
# ─────────────────────────────────────────────────────────────

def compute_top_tracks_tumbling(events_df):
    """
    Top 10 des tracks par tumbling window de 5 minutes.
    """
    def write_batch(df, batch_id):
        if df.rdd.isEmpty():
            return
        jdbc_url = POSTGRES_URL
        df.write.jdbc(url=jdbc_url, table="realtime_top_tracks", mode="append", properties=POSTGRES_PROPS)

    top_tracks = (
        events_df
        .withWatermark("event_time", "10 minutes")
        .groupBy(F.window("event_time", "5 minutes"), "track_id")
        .agg(
            F.count("*").alias("stream_count"),
            F.countDistinct("user_id").alias("unique_listeners"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "track_id",
            "stream_count",
            "unique_listeners",
        )
    )

    query = (
        top_tracks.writeStream
        .outputMode("update")
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_PATH + "/top_tracks")
        .start()
    )
    return query


def compute_genre_listeners_sliding(events_df, catalog_df):
    """
    Listeners uniques par genre en sliding window (15 min glissant toutes les 5 min).
    """
    if catalog_df is None:
        return None

    joined = events_df.join(catalog_df.select("id", "genre"), events_df.track_id == catalog_df.id, "left")
    sliding = (
        joined
        .withWatermark("event_time", "20 minutes")
        .groupBy(F.window("event_time", "15 minutes", "5 minutes"), "genre")
        .agg(F.countDistinct("user_id").alias("unique_listeners"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "genre",
            "unique_listeners",
        )
    )

    def write_genre_batch(df, batch_id):
        import redis as redis_py

        if df.rdd.isEmpty():
            return

        redis_client = redis_py.Redis(host="redis", port=6379, db=1, decode_responses=True)
        genre_scores = {
            row["genre"]: row["unique_listeners"]
            for row in df.collect()
        }
        redis_client.set("genre_listeners:live", json.dumps(genre_scores))

    query = (
        sliding.writeStream
        .outputMode("update")
        .foreachBatch(write_genre_batch)
        .option("checkpointLocation", CHECKPOINT_PATH + "/genre_listeners")
        .start()
    )
    return query


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage streaming_trends_job...")
    print(f"Kafka : {KAFKA_BOOTSTRAP} → topic : {KAFKA_TOPIC}")
    print(f"Checkpoint : {CHECKPOINT_PATH}")

    # Lecture Kafka
    events_df = read_kafka_stream(spark)

    # Chargement du catalogue (jointure statique — Phase 2, seq 2.3)
    # catalog_df = spark.read.jdbc(POSTGRES_URL, "tracks", properties=POSTGRES_PROPS)

    # Agrégations
    query_top_tracks = compute_top_tracks_tumbling(events_df)
    # query_genres     = compute_genre_listeners_sliding(events_df, catalog_df)

    # Attendre l'arrêt gracieux
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
