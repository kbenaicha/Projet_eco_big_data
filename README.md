# Projet Big Data

Pipeline hybride de suivi des transports en Ile-de-France.

Le projet croise :

- des donnees GTFS statiques
- des snapshots JSON temps reel
- plusieurs traitements Spark
- une couche de restitution simple a lire

L'objectif n'est pas seulement de produire des Parquet, mais de sortir des resultats compréhensibles :

- quelles lignes sont les plus perturbees
- quels arrets sont les plus sensibles
- quelle part du reseau est retardee ou annulee
- quels motifs de perturbation dominent

## Structure utile

```text
.
|-- README.md
|-- project.md
|-- requirements.txt
|-- data/
|   `-- raw/
|-- hive/
|   `-- create_tables.hql
|-- reports/
|   |-- results_dashboard.html
|   |-- results_summary.md
|   |-- assets/
|   `-- exports/
|-- scripts/
|   |-- run_cluster_pipeline.sh
|   |-- run_hive_tables.sh
|   |-- run_local_pipeline.sh
|   |-- run_spark_step_hdfs.sh
|   `-- upload_raw_to_hdfs_cluster.sh
`-- src/
    |-- build_analytics.py
    |-- build_hybrid_table.py
    |-- build_results_report.py
    |-- collect_realtime.py
    |-- config.py
    |-- prepare_gtfs.py
    |-- prepare_realtime.py
    `-- spark_session.py
```

## Fichiers principaux

- `src/prepare_gtfs.py` : preparation des donnees GTFS
- `src/prepare_realtime.py` : preparation des snapshots temps reel
- `src/build_hybrid_table.py` : jointure GTFS + temps reel
- `src/build_analytics.py` : calcul des indicateurs
- `src/build_results_report.py` : generation du resume, du dashboard HTML, des CSV et des SVG
- `scripts/run_local_pipeline.sh` : pipeline complete en local
- `scripts/run_cluster_pipeline.sh` : pipeline complete sur le cluster

## Sorties produites

Sorties techniques :

- `data/processed/gtfs/gtfs_schedule.parquet`
- `data/processed/realtime/realtime_passages.parquet`
- `data/final/hybrid_transport_monitoring.parquet`
- `data/analytics/line_performance`
- `data/analytics/stop_performance`
- `data/analytics/disruption_reasons`
- `data/analytics/network_overview`

Sorties lisibles :

- `reports/results_summary.md`
- `reports/results_dashboard.html`
- `reports/exports/network_overview.csv`
- `reports/exports/top_lines.csv`
- `reports/exports/top_stops.csv`
- `reports/exports/disruption_reasons.csv`
- `reports/exports/sample_disruptions.csv`
- `reports/assets/top_lines.svg`
- `reports/assets/top_stops.svg`
- `reports/assets/disruption_reasons.svg`

## Installation locale

Depuis la racine du projet :

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

lancementlancer Spark localement sur macOS :

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
```

## Execution locale complete

Prerequis :

- avoir `data/raw/gtfs/IDFM-gtfs/*.txt`
- avoir `data/raw/realtime/*.json`

Commande principale :

```bash
source .venv/bin/activate
bash scripts/run_local_pipeline.sh
```

Ce script enchaine :

1. `prepare_gtfs.py`
2. `prepare_realtime.py`
3. `build_hybrid_table.py`
4. `build_analytics.py`
5. `build_results_report.py`

## Regenerer seulement le dashboard et le resume

Si les sorties `data/final` et `data/analytics` existent deja :

```bash
source .venv/bin/activate
python src/build_results_report.py
```

## Voir les resultats en local

Ouvrir directement les fichiers :

```bash
open reports/results_dashboard.html
open reports/results_summary.md
```

Ou via `localhost` :

```bash
python3 -m http.server 8000
```

Puis ouvrir :

```text
http://localhost:8000/reports/results_dashboard.html
```

## Execution sur le cluster

### 1. Transferer les fichiers utiles

Depuis ton Mac :

```bash
rsync -av \
  "/Users/kawtarbenaicha/Desktop/ING4_sem8 /Cours big data /Projet_big_data/src" \
  "/Users/kawtarbenaicha/Desktop/ING4_sem8 /Cours big data /Projet_big_data/scripts" \
  "/Users/kawtarbenaicha/Desktop/ING4_sem8 /Cours big data /Projet_big_data/data/raw" \
  "/Users/kawtarbenaicha/Desktop/ING4_sem8 /Cours big data /Projet_big_data/hive" \
  b.kawtar-ece@edge-1.au.adaltas.cloud:~/Projet_big_data/
```

### 2. Se connecter

```bash
ssh b.kawtar-ece@edge-1.au.adaltas.cloud
cd ~/Projet_big_data
```

### 3. Lancer la pipeline cluster

Commande la plus simple :

```bash
bash scripts/run_cluster_pipeline.sh
```

Variantes utiles :

```bash
UPLOAD_RAW_FIRST=false bash scripts/run_cluster_pipeline.sh
```

```bash
PUBLISH_HIVE=true bash scripts/run_cluster_pipeline.sh
```

Si le groupe n'est pas detecte correctement :

```bash
GROUP=ece_2026_spring_gr01 bash scripts/run_cluster_pipeline.sh
```

### 4. Verifier les sorties HDFS

```bash
export GROUP=ece_2026_spring_gr01
export HDFS_BASE_DIR=/education/$GROUP/$USER/project

hdfs dfs -ls "$HDFS_BASE_DIR/final/hybrid_transport_monitoring.parquet"
hdfs dfs -ls "$HDFS_BASE_DIR/analytics"
```

### 5. Recuperer les rapports sur ton Mac

Depuis ton Mac :

```bash
rsync -av \
  b.kawtar-ece@edge-1.au.adaltas.cloud:~/Projet_big_data/reports/ \
  "/Users/kawtarbenaicha/Desktop/ING4_sem8 /Cours big data /Projet_big_data/reports/"
```

## Comment presenter les resultats

Le rendu le plus simple et le plus convaincant est :

1. montrer `reports/results_dashboard.html`
2. faire une capture d'ecran
3. reprendre 3 ou 4 constats depuis `reports/results_summary.md`
4. ajouter au besoin un CSV dans `reports/exports/`

Exemples de phrases a reutiliser :

- "Le reseau observe couvre X lignes et Y arrets."
- "Z % des observations sont perturbees."
- "La ligne la plus sensible est ..."
- "L'arret le plus sensible est ..."
- "Le motif principal de perturbation est ..."

## Verification rapide

En local :

```bash
ls reports
ls reports/exports
ls reports/assets
```

Sur le cluster :

```bash
hdfs dfs -ls "$HDFS_BASE_DIR/final"
hdfs dfs -ls "$HDFS_BASE_DIR/analytics"
```
