"""
=============================================================================
 DAG : earthquake_pipeline
 Description : Pipeline séismes temps réel
               USGS Earthquake API → Kafka (earthquake-raw)
               → Flink SQL → Kafka (earthquake-stats / earthquake-alerts)

 Séance 7 - TP Data Engineering Temps Réel
=============================================================================

 RAPPEL — Qu'est-ce qu'un fichier DAG ?
 ----------------------------------------
 Un DAG (Directed Acyclic Graph) Airflow est un fichier Python placé dans
 /opt/airflow/dags/. Le Scheduler le parse à intervalle régulier pour :
   1. Découvrir les tâches et leurs dépendances
   2. Créer des DagRuns selon le schedule défini
   3. Soumettre les TaskInstances à l'Executor

 Le code au niveau MODULE est exécuté à chaque parsing.
 Les fonctions (callables) sont exécutées uniquement quand la tâche tourne.

 SOURCE DE DONNÉES : USGS Earthquake API
 ----------------------------------------
 API publique, sans clé, maintenue par le U.S. Geological Survey.
 Documentation : https://earthquake.usgs.gov/fdsnws/event/1/
 Format de réponse : GeoJSON (RFC 7946)

 Exemple d'appel :
   GET /fdsnws/event/1/query?format=geojson
       &starttime=2026-01-01T00:00:00
       &endtime=2026-01-01T01:00:00
       &minmagnitude=2.5
=============================================================================
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ===========================================================================
#  CONFIGURATION
#
#  Évaluées au PARSING du DAG — garder ce bloc léger (pas de réseau, pas de DB).
# ===========================================================================

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

KAFKA_TOPIC_RAW       = "earthquake-raw"
KAFKA_TOPIC_ALERTS    = "earthquake-alerts"

# ===========================================================================
#  DEFAULT ARGS
# ===========================================================================

default_args = {
    "owner": "etudiant",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

# ===========================================================================
#  DAG DEFINITION
# ===========================================================================

with DAG(
    dag_id="earthquake_pipeline",
    default_args=default_args,
    description="Pipeline seismes : USGS API -> Kafka -> Flink SQL",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["earthquake", "kafka", "flink", "tp7"],
) as dag:

    # ===================================================================
    #  TASK 1 : health_check
    #  Vérifie que Kafka et Flink répondent avant de démarrer le pipeline.
    # ===================================================================
    health_check = BashOperator(
        task_id="health_check",
        bash_command="""
            echo "=== Vérification Kafka ==="
            docker exec kafka kafka-topics \
                --bootstrap-server localhost:9092 --list

            echo "=== Vérification Flink ==="
            curl -sf http://flink-jobmanager:8081/overview

            echo "Health check OK"
        """,
    )

    # ===================================================================
    #  TASK 2 : create_topics
    #  Crée les 3 topics Kafka nécessaires au pipeline.
    #
    #  CONCEPT — Idempotence :
    #  --if-not-exists garantit qu'on peut relancer cette tâche sans erreur.
    #  C'est la règle d'or en orchestration : chaque tâche doit supporter
    #  les retries et les re-runs sans créer de doublons ou d'erreurs.
    #
    #  Partitions :
    #  - earthquake-raw      : 3 partitions (parallélisme ingestion)
    #  - earthquake-enriched : 1 partition (ordre temporel requis)
    #  - earthquake-alerts   : 1 partition (alertes, ordre temporel requis)
    # ===================================================================
    create_topics = BashOperator(
        task_id="create_topics",
        bash_command="""
            for TOPIC in earthquake-raw earthquake-enriched earthquake-alerts; do
                PARTITIONS=3
                if [ "$TOPIC" != "earthquake-raw" ]; then PARTITIONS=1; fi

                docker exec kafka kafka-topics \
                    --bootstrap-server localhost:9092 \
                    --create --if-not-exists \
                    --topic "$TOPIC" \
                    --partitions $PARTITIONS \
                    --replication-factor 1

                echo "Topic $TOPIC pret."
            done
        """,
    )

    # ===================================================================
    #  TASK 3 : run_flink_job
    #  Soumet le fichier SQL au Flink SQL Client.
    #
    #  CONCEPT — Airflow orchestre, Flink traite :
    #  Airflow ne traite pas la donnée lui-même — il délègue au cluster Flink
    #  via "docker exec flink-sql-client bin/sql-client.sh -f <fichier>".
    #  Le fichier SQL est monté dans le conteneur via le volume flink-jobs/.
    # ===================================================================
    run_flink_job = BashOperator(
        task_id="run_flink_job",
        bash_command="""
            set -eo pipefail

            echo "=== Vérification des jobs Flink existants ==="
            RUNNING=$(curl -sf http://flink-jobmanager:8081/jobs/overview \
                | python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
running = [j for j in jobs if j.get('state') == 'RUNNING']
print(len(running))
")
            echo "Jobs actuellement RUNNING : $RUNNING"

            if [ "$RUNNING" -ge 2 ]; then
                echo "Les jobs Flink tournent déjà. Aucune soumission nécessaire."
                exit 0
            fi

            echo "=== Soumission du fichier SQL à Flink ==="
            OUTPUT=$(docker exec flink-sql-client \
                bin/sql-client.sh -f /opt/flink/jobs/earthquake_processing.sql 2>&1)
            echo "$OUTPUT"

            if echo "$OUTPUT" | grep -q '\[ERROR\]'; then
                echo "Erreur détectée dans la sortie Flink SQL." >&2
                exit 1
            fi

            echo "Job Flink SQL soumis avec succès."
        """,
    )

    # ===================================================================
    #  TASK 4 : start_tableau_sink
    #  Vérifie que le service Docker kafka-to-postgres est en vie.
    #  Le service est déclaré dans docker-compose.yml avec
    #  restart: unless-stopped — il démarre automatiquement avec la stack.
    # ===================================================================
    start_tableau_sink = BashOperator(
        task_id="start_tableau_sink",
        bash_command="""
            docker ps \
                --filter "name=^/kafka-to-postgres$" \
                --filter "status=running" \
            | grep -q kafka-to-postgres \
            && echo "kafka-to-postgres service OK." \
            || (echo "kafka-to-postgres non démarré -- vérifier docker compose." && exit 1)
        """,
    )

    # ===================================================================
    #  TASK 5 : verify_output
    #  Vérifie que le pipeline Flink produit bien dans earthquake-enriched
    #  et que PostgreSQL earthquake_events est peuplé par le producer.
    # ===================================================================
    verify_output = BashOperator(
        task_id="verify_output",
        bash_command="""
            echo "=== 1/2 Vérification Kafka earthquake-enriched ==="
            OUTPUT=$(docker exec kafka kafka-console-consumer \
                --bootstrap-server localhost:9092 \
                --topic earthquake-enriched \
                --from-beginning \
                --max-messages 3 \
                --timeout-ms 30000 2>&1)
            echo "$OUTPUT"
            echo "$OUTPUT" | grep -q "Processed a total of [1-9]" || exit 1
            echo "Kafka earthquake-enriched OK."

            echo "=== 2/2 Vérification PostgreSQL earthquake_events ==="
            COUNT=$(docker exec airflow-postgres psql \
                -U airflow -d airflow -t -c \
                "SELECT COUNT(*) FROM earthquake_events;" 2>&1 | tr -d ' ')
            echo "Lignes dans earthquake_events : $COUNT"
            [ "$COUNT" -gt 0 ] || exit 1
            echo "PostgreSQL OK - $COUNT séisme(s) disponible(s) pour Tableau."
        """,
    )

    # ===================================================================
    #  DÉPENDANCES
    #
    #  Le producer (service Docker) alimente Kafka + PostgreSQL en continu.
    #  Airflow orchestre uniquement la création des topics et Flink.
    #
    #  health_check → create_topics → run_flink_job → start_tableau_sink → verify_output
    # ===================================================================
    health_check >> create_topics >> run_flink_job >> start_tableau_sink >> verify_output