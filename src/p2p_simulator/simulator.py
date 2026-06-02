"""
SPOTIFY — Simulateur P2P
========================
Ce simulateur génère des événements réalistes d'un réseau peer-to-peer
de streaming musical. Il publie dans Redis pub/sub (Phase 1) et dans
Kafka (Phase 2, après décommentage).

Usage :
    python -m src.p2p_simulator.simulator --peers 10 --rate 5
    python -m src.p2p_simulator.simulator --mode fraud --peers 5
    python -m src.p2p_simulator.simulator --mode late_events

TODO Phase 1 :  Compléter _generate_listening_event() et _publish_to_redis()
TODO Phase 2 :  Activer _publish_to_kafka() et le mode fraude
"""

import argparse
import json
import logging
import random
import signal
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional

import redis

# Phase 2 — décommenter quand Kafka est prêt
# from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("p2p_simulator")


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

REDIS_URL = "redis://localhost:6379/1"
KAFKA_BOOTSTRAP = "kafka-1:9092"       # Phase 2

TOPICS = {
    "listening":   "listening_events",
    "p2p_network": "p2p_network_events",
}

DEVICE_TYPES = ["mobile", "desktop", "smart_speaker", "web", "tv"]
GEO_COUNTRIES = ["FR", "DE", "US", "GB", "ES", "IT", "BR", "JP", "KR", "AU"]
EVENT_SOURCES = ["p2p", "p2p", "p2p", "direct", "cache"]  # pondéré : 60% P2P


# ─────────────────────────────────────────────────────────────
# DONNÉES SIMULÉES
# ─────────────────────────────────────────────────────────────

# Ces UUIDs seront remplacés par les vrais IDs depuis PostgreSQL
# Une fois votre base peuplée, charger dynamiquement avec _load_catalog()
SAMPLE_TRACKS = [
    {"id": str(uuid.uuid4()), "title": f"Track {i}", "duration_ms": random.randint(120000, 300000)}
    for i in range(50)
]

SAMPLE_USERS = [str(uuid.uuid4()) for _ in range(200)]
SAMPLE_PEERS = [str(uuid.uuid4()) for _ in range(20)]


# ─────────────────────────────────────────────────────────────
# SIMULATEUR PRINCIPAL
# ─────────────────────────────────────────────────────────────

class P2PSimulator:
    """
    Simulateur du réseau P2P SPOTIFY.

    Génère deux types d'événements :
    - listening_events   : un utilisateur écoute un morceau via un peer
    - p2p_network_events : connexion/déconnexion/transfert entre peers
    """

    def __init__(
        self,
        n_peers: int = 10,
        events_per_second: float = 5.0,
        mode: str = "normal",
    ):
        self.n_peers = n_peers
        self.events_per_second = events_per_second
        self.mode = mode
        self.running = True
        self.event_count = 0

        # Connexion Redis
        self.redis = redis.from_url(REDIS_URL, decode_responses=True)

        # Phase 2 — Kafka producer
        # self.kafka_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

        # Peers actifs simulés
        self.active_peers = [str(uuid.uuid4()) for _ in range(n_peers)]

        # Charger les vrais tracks depuis PostgreSQL
        self.tracks = self._load_catalog()

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        logger.info(f"Simulateur démarré | mode={mode} | peers={n_peers} | rate={events_per_second} evt/s")

    def _load_catalog(self) -> list:
        import glob, json
        try:
            tracks = []
            for f in glob.glob("data/labels/*.json"):
                with open(f, "r", encoding="utf-8") as fp:
                    catalog = json.load(fp)
                    tracks.extend(catalog.get("tracks", []))
            if tracks:
                logger.info(f"Catalogue chargé: {len(tracks)} tracks depuis data/labels/")
                return [{"id": t["id"], "title": t["title"], "duration_ms": t["duration_ms"]} for t in tracks]
        except Exception as e:
            logger.warning(f"Catalogue introuvable, utilisation SAMPLE_TRACKS: {e}")
        return SAMPLE_TRACKS

    def run(self):
        """Boucle principale : génère et publie des événements en continu."""
        interval = 1.0 / self.events_per_second

        while self.running:
            try:
                # Alterner listening et réseau P2P (80% / 20%)
                if random.random() < 0.8:
                    event = self._generate_listening_event()
                    self._publish_event("listening", event)
                else:
                    event = self._generate_p2p_network_event()
                    self._publish_event("p2p_network", event)

                self.event_count += 1

                if self.event_count % 100 == 0:
                    logger.info(f"Événements publiés : {self.event_count}")

                time.sleep(interval)

            except Exception as e:
                logger.error(f"Erreur lors de la génération d'événement : {e}")
                time.sleep(1)

    # ── Génération d'événements ──────────────────────────────

    def _generate_listening_event(self) -> dict:
        """
        Génère un événement d'écoute.

        TODO : compléter ce squelette pour générer un événement réaliste.
        Champs attendus :
            - event_id     : UUID unique
            - user_id      : UUID utilisateur (depuis SAMPLE_USERS)
            - track_id     : UUID du morceau (depuis SAMPLE_TRACKS)
            - source_peer  : UUID du peer qui sert le morceau
            - timestamp    : ISO 8601 (datetime.utcnow())
            - duration_ms  : durée écoutée (entre 30 000 et track.duration_ms)
            - device_type  : depuis DEVICE_TYPES
            - geo_country  : depuis GEO_COUNTRIES
            - completed    : bool (True si duration_ms > 30s)
            - event_source : depuis EVENT_SOURCES

        En mode "fraud" (Phase 2) :
            - 30% des events : duration_ms < 5000 (écoute trop courte = bot)
            - 10% : même user_id sur 20 tracks en <10 secondes

        En mode "late_events" (Phase 2) :
            - timestamp décalé de -5 à -30 minutes dans le passé
        """
        track = random.choice(self.tracks)

        duration_ms = random.randint(30_000, track["duration_ms"])
        event = {
            "event_id":    str(uuid.uuid4()),
            "user_id":     random.choice(SAMPLE_USERS),
            "track_id":    track["id"],
            "source_peer": random.choice(self.active_peers),
            "timestamp":   datetime.utcnow().isoformat() + "Z",
            "duration_ms":  duration_ms,
            "device_type":  random.choice(DEVICE_TYPES),
            "geo_country":  random.choice(GEO_COUNTRIES),
            "completed":    duration_ms > 30_000,
            "event_source": random.choice(EVENT_SOURCES),
        }


        # Mode fraud (Phase 2) — décommenter
        # if self.mode == "fraud" and random.random() < 0.3:
        #     event["duration_ms"] = random.randint(100, 4999)
        #     event["completed"] = False

        # Mode late_events (Phase 2) — décommenter
        # if self.mode == "late_events" and random.random() < 0.4:
        #     delay_minutes = random.randint(5, 30)
        #     ts = datetime.utcnow() - timedelta(minutes=delay_minutes)
        #     event["timestamp"] = ts.isoformat() + "Z"

        return event

    def _generate_p2p_network_event(self) -> dict:
        """
        Génère un événement réseau P2P.

        TODO : compléter pour générer des événements de type :
            - peer_connect    : un peer rejoint le réseau
            - peer_disconnect : un peer quitte le réseau
            - chunk_transfer  : transfert d'un chunk audio entre peers
            - cache_hit       : le morceau était en cache local
            - cache_miss      : téléchargement depuis un autre peer nécessaire
        """
        event_type = random.choice([
            "peer_connect", "peer_disconnect",
            "chunk_transfer", "cache_hit", "cache_miss"
        ])

        event = {
            "event_id":   str(uuid.uuid4()),
            "event_type": event_type,
            "peer_id":    random.choice(self.active_peers),
            "timestamp":  datetime.utcnow().isoformat() + "Z",
        }
        if event_type == "chunk_transfer":
            event["track_id"]    = random.choice(SAMPLE_TRACKS)["id"]
            event["chunk_size"]  = random.randint(64_000, 512_000)
            event["target_peer"] = random.choice(self.active_peers)
        elif event_type in ("cache_hit", "cache_miss"):
            event["track_id"] = random.choice(SAMPLE_TRACKS)["id"]
        return event


    # ── Publication ──────────────────────────────────────────

    def _publish_event(self, topic_key: str, event: dict):
        """Publie un événement dans Redis et (Phase 2) dans Kafka."""
        payload = json.dumps(event)
        channel = TOPICS[topic_key]

        self._publish_to_redis(channel, payload)
        # Phase 2 — décommenter
        # self._publish_to_kafka(channel, event.get("user_id", ""), payload)

    def _publish_to_redis(self, channel: str, payload: str):
        """
        TODO : publier payload dans le channel Redis via pub/sub.
        Utiliser self.redis.publish(channel, payload)
        Gérer l'exception si Redis est indisponible (log + skip).
        """
        try:
            self.redis.publish(channel, payload)
            self.redis.lpush(channel + "_buffer", payload)
            self.redis.ltrim(channel + "_buffer", 0, 9999)
        except Exception as e:
            logger.error(f"Redis indisponible sur {channel} : {e}")


    # def _publish_to_kafka(self, topic: str, key: str, payload: str):
    #     """
    #     TODO Phase 2 : publier payload dans le topic Kafka.
    #     - key     : utilisé pour le partitionnement (user_id ou peer_id)
    #     - acks    : 'all' pour la durabilité
    #     - Gérer le callback de confirmation (delivery_report)
    #     """
    #     raise NotImplementedError("TODO Phase 2 : implémenter _publish_to_kafka()")

    def _shutdown(self, signum, frame):
        logger.info(f"Arrêt du simulateur (signal {signum}) — {self.event_count} événements publiés")
        self.running = False


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SPOTIFY P2P Simulator")
    parser.add_argument("--peers",  type=int,   default=10,     help="Nombre de peers simulés")
    parser.add_argument("--rate",   type=float, default=5.0,    help="Événements par seconde")
    parser.add_argument("--mode",   type=str,   default="normal",
                        choices=["normal", "fraud", "late_events", "chaos"],
                        help="Mode de simulation")
    args = parser.parse_args()

    simulator = P2PSimulator(
        n_peers=args.peers,
        events_per_second=args.rate,
        mode=args.mode,
    )
    simulator.run()


if __name__ == "__main__":
    main()
