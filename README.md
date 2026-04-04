# Projet Big Data

Pipeline hybride de suivi des transports en Ile-de-France.

Le projet combine :

- des donnees batch GTFS
- des snapshots JSON quasi temps reel depuis l'API IDFM
- plusieurs etapes Spark pour preparer, croiser et agreger les donnees
- une publication optionnelle dans Hive
- une orchestration cluster via Oozie

## Structure du depot

```text
.
|-- README.md
|-- project.md
|-- requirements.txt
|-- hive/
|   `-- create_tables.hql
|-- oozie/
|   `-- workflow.xml
|-- scripts/
|   |-- run_hive_tables.sh
|   |-- run_local_pipeline.sh
|   |-- run_spark_step_hdfs.sh
|   |-- submit_oozie_workflow.sh
|   `-- upload_raw_to_hdfs_cluster.sh
`-- src/
    |-- build_analytics.py
    |-- build_hybrid_table.py
    |-- collect_realtime.py
    |-- config.py
    |-- prepare_gtfs.py
    |-- prepare_realtime.py
    `-- spark_session.py
```

## Fichiers principaux

- `src/collect_realtime.py` : collecte de snapshots JSON IDFM
- `src/prepare_gtfs.py` : preparation des donnees GTFS statiques
- `src/prepare_realtime.py` : preparation des passages temps reel
- `src/build_hybrid_table.py` : jointure GTFS + temps reel
- `src/build_analytics.py` : indicateurs finaux
- `scripts/run_local_pipeline.sh` : execution locale complete
- `scripts/submit_oozie_workflow.sh` : packaging et soumission Oozie
- `oozie/workflow.xml` : definition du workflow cluster
- `hive/create_tables.hql` : publication Hive des sorties
- `project.md` : brief du projet fourni pour le rendu

## Installation locale

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Execution locale

Si les donnees brutes sont deja presentes dans `data/raw/` :

```bash
./scripts/run_local_pipeline.sh
```

Pour recolter d'abord de nouveaux snapshots :

```bash
COLLECT_REALTIME_FIRST=true ./scripts/run_local_pipeline.sh
```

Pour lancer uniquement la collecte :

```bash
python src/collect_realtime.py
```

## Sorties attendues

- `data/processed/gtfs/gtfs_schedule.parquet`
- `data/processed/realtime/realtime_passages.parquet`
- `data/final/hybrid_transport_monitoring.parquet`
- `data/analytics/line_performance`
- `data/analytics/stop_performance`
- `data/analytics/disruption_reasons`
- `data/analytics/network_overview`

## Execution cluster avec Oozie

Depuis le cluster, a la racine du projet :

```bash
export GROUP=ece_2026_spring_gr01
export HDFS_BASE_DIR=/education/ece_2026_spring_gr01/$USER/project
export NAME_NODE=hdfs://au
export JOB_TRACKER=yarn-rm-1.au.adaltas.cloud:8032
export OOZIE_URL=http://oozie-1.au.adaltas.cloud:11000/oozie

bash scripts/submit_oozie_workflow.sh
```

Le workflow execute :

1. upload HDFS des donnees brutes
2. `prepare_gtfs.py`
3. `prepare_realtime.py`
4. `build_hybrid_table.py`
5. `build_analytics.py`
6. publication Hive

Variables utiles :

- `QUEUE_NAME`
- `OOZIE_APP_HDFS_DIR`
- `HIVE_JDBC_URL`
- `HIVE_USERNAME`
- `PYSPARK_PYTHON`
- `PYSPARK_DRIVER_PYTHON`

## Verification sur le cluster

Suivi du job Oozie :

```bash
oozie jobs -oozie "$OOZIE_URL" -jobtype wf -filter user=$USER
oozie job -oozie "$OOZIE_URL" -info <JOB_ID>
oozie job -oozie "$OOZIE_URL" -log <JOB_ID>
```

Verification HDFS :

```bash
hdfs dfs -ls "$HDFS_BASE_DIR/final/hybrid_transport_monitoring.parquet"
hdfs dfs -ls "$HDFS_BASE_DIR/analytics"
```

Verification Hive :

```bash
HIVE_USERNAME=${USER//[.-]/_}_transport
beeline -e "SHOW TABLES IN ${GROUP};"
beeline -e "SELECT * FROM ${GROUP}.${HIVE_USERNAME}_network_overview LIMIT 10;"
```

## Etat du projet

- les scripts Spark peuvent etre testes individuellement sur le cluster avec `scripts/run_spark_step_hdfs.sh`
- la publication Hive est geree par `scripts/run_hive_tables.sh`
- l'orchestration Oozie est centralisee dans `oozie/workflow.xml`
