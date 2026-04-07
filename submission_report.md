# Big Data Ecosystem Project Report

## Project Title

Hybrid Transport Monitoring in Ile-de-France

## 1. Context and Use Case

Ce projet a pour objectif de suivre la qualite de service des transports en Ile-de-France en croisant :

- des donnees GTFS statiques
- des snapshots JSON temps reel

L'idee est simple : comparer les horaires theoriques avec les observations temps reel pour identifier les retards, les annulations et les perturbations sur le reseau.

Le cas d'usage est realiste et facile a comprendre :

- un usager veut savoir si une ligne fonctionne normalement
- un gestionnaire veut identifier les lignes ou arrets les plus sensibles
- un analyste veut mesurer le niveau global de perturbation du reseau

Ce sujet est pertinent pour un projet Big Data car il combine :

- des donnees de reference structurees
- des donnees temps reel semi-structurees
- plusieurs etapes de transformation et d'agregation

Le projet repose donc sur une **architecture hybride**.

## 2. Data: Origin and Structure

### 2.1 Data origin

Les donnees proviennent de sources publiques de transport :

- les fichiers **GTFS** pour les horaires et arrets planifies
- des **snapshots JSON** issus des donnees temps reel IDFM

Les GTFS representent la vision theorique du reseau.
Les snapshots temps reel representent l'etat observe du service.

### 2.2 Data structure

Le projet manipule deux familles de donnees :

1. **Donnees structurees**
   - `routes.txt`
   - `trips.txt`
   - `stops.txt`
   - `stop_times.txt`

2. **Donnees semi-structurees**
   - fichiers JSON temps reel
   - contenant des champs imbriques qu'il faut extraire et aplatir

### 2.3 Main fields used

Exemples de champs utilises :

Depuis GTFS :

- `route_short_name`
- `trip_id`
- `stop_name`
- `departure_time`

Depuis le temps reel :

- `recorded_at_time`
- `line_ref`
- `stop_point_name`
- `expected_departure_time`
- `departure_status`
- `arrival_status`

Dans la table finale :

- `line_code`
- `stop_point_name`
- `delay_minutes`
- `is_delayed`
- `is_cancelled`
- `is_disrupted`
- `disruption_reason`

### 2.4 Estimated volume and frequency

Le pipeline combine deux rythmes de donnees :

- les donnees GTFS sont traitees en **batch**
- les snapshots JSON arrivent de maniere plus frequente et jouent le role de donnees **quasi temps reel**

Sur l'echantillon valide, la table hybride finale contient :

- **373 observations**
- **23 lignes**
- **8 arrets**

Cela confirme le caractere hybride du projet :

- une couche statique de reference
- une couche dynamique issue des snapshots

## 3. Your Big Data Architecture

### 3.1 Processing model

Le projet suit une architecture **hybride** :

- batch pour les donnees GTFS
- quasi temps reel pour les snapshots JSON

### 3.2 Components used

Les composants principaux sont :

- **Python** pour l'orchestration
- **PySpark** pour les traitements
- **HDFS** pour le stockage distribue sur le cluster
- **Hive** en publication optionnelle
- **HTML / CSV / Markdown** pour la restitution finale

### 3.3 Pipeline stages

Le pipeline se decompose en 5 etapes principales :

1. **Ingestion**
   - lecture des fichiers GTFS et des snapshots JSON

2. **Preparation GTFS**
   - `src/prepare_gtfs.py`
   - production de `gtfs_schedule.parquet`

3. **Preparation temps reel**
   - `src/prepare_realtime.py`
   - production de `realtime_passages.parquet`

4. **Construction de la table hybride**
   - `src/build_hybrid_table.py`
   - rapprochement entre horaires planifies et observations reelles
   - production de `hybrid_transport_monitoring.parquet`

5. **Analytics et restitution**
   - `src/build_analytics.py`
   - `src/build_results_report.py`
   - production de tables d'analyse, CSV, SVG, dashboard HTML et resume Markdown

### 3.4 End-to-end data flow

Le flux de donnees complet est :

`raw GTFS + raw realtime -> processed GTFS + processed realtime -> hybrid parquet -> analytics -> report`

Cette architecture reste volontairement simple.
L'objectif n'est pas la complexite technique, mais la clarte du flux de bout en bout.

## 4. Testing and Validation

### 4.1 Local execution

Le projet peut etre teste localement avec :

```bash
bash scripts/run_local_pipeline.sh
```

Le rapport final peut aussi etre regenere seul avec :

```bash
python src/build_results_report.py
```

### 4.2 Cluster execution

Le projet peut etre teste sur le cluster avec :

```bash
bash scripts/run_cluster_pipeline.sh
```

Ce script :

- charge les donnees brutes dans HDFS si necessaire
- lance les etapes Spark
- genere les sorties finales
- peut publier les resultats dans Hive si besoin

### 4.3 Validation of outputs

Les sorties techniques validees sont :

- `data/final/hybrid_transport_monitoring.parquet`
- `data/analytics/line_performance`
- `data/analytics/stop_performance`
- `data/analytics/disruption_reasons`
- `data/analytics/network_overview`

Les sorties lisibles validees sont :

- `reports/results_summary.md`
- `reports/results_dashboard.html`
- `reports/exports/*.csv`
- `reports/assets/*.svg`

### 4.4 Example results

Sur l'echantillon valide :

- **373 observations** ont ete analysees
- **0.80 %** des observations sont perturbees
- **0.54 %** sont annulees
- le retard moyen observe est de **0.03 minute**

Le dashboard met aussi en avant :

- l'arret **La Defense** comme arret sensible sur l'echantillon
- **cancelled_service** comme motif principal de perturbation

Ces resultats sont volontairement presentes sous une forme simple a lire, avec :

- un dashboard HTML
- un resume texte
- des exports CSV

## Conclusion

Ce projet respecte bien l'objectif du cours :

- il s'appuie sur un cas d'usage simple et realiste
- il combine plusieurs types de donnees
- il montre un flux de donnees complet de l'ingestion jusqu'a l'exploitation
- il justifie le choix d'une architecture hybride

La valeur ajoutee du projet est de ne pas s'arreter aux traitements techniques.
Les resultats sont aussi rendus lisibles pour un humain grace a un resume, un dashboard et des exports reutilisables.
