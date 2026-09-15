# 🚢 Smart Maritime Vessel Traffic & Port Intelligence Platform

A containerised, distributed analytics platform that ingests NOAA MarineCadastre historical AIS (Automatic Identification System) vessel-position archives, streams the records through Apache Kafka into a Spark Structured Streaming pipeline for real-time enrichment and archival, and then runs a daily Spark batch job (orchestrated by Apache Airflow) to compute fleet KPIs, port dwell times, traffic density grids, and K-Means vessel-behaviour clusters — all served from a PostGIS spatial database and visualised in Apache Superset dashboards.

> **Ingestion mode:** historical CSV replay.  
> The `ais-producer` container replays seven NOAA AIS CSV files (25–31 December 2024) at full throughput into Kafka. A future live-feed upgrade using the aisstream.io WebSocket API is plumbed in `.env` (`AISSTREAM_API_KEY`, `AIS_BOUNDING_BOXES`) but not yet implemented.

---

## Architecture

```mermaid
flowchart LR
    subgraph INGEST["1. Ingestion & Streaming"]
        direction TB
        PROD["AIS Producer<br/>(Historical CSV Replay)"] -->|raw_ais_positions| KAFKA["Apache Kafka<br/>(KRaft Broker)"]
        KAFKA -->|Kafka Stream| SPARK_STR["Spark Structured Streaming<br/>(streaming_maritime_processor.py)"]
        SPARK_STR -->|Sink A: UPSERT| PG_ACTIVE[("PostGIS<br/>active_fleet_state")]
        SPARK_STR -->|Sink B: Speed Alerts| KAFKA_ALERTS["Kafka Topic<br/>vessel_speed_alerts"]
        SPARK_STR -->|Sink C: Parquet Archive| HDFS_RAW[("HDFS /raw/ais_historical/<br/>date=YYYY-MM-DD/")]
        SPARK_STR -->|Sink D: Rejects| HDFS_REJ[("HDFS /rejected/<br/>ais_streaming")]
    end

    subgraph BATCH["2. Batch Analytics & Orchestration"]
        direction TB
        AIRFLOW["Apache Airflow<br/>(maritime_batch_kpi_pipeline)"] -->|1. Partition Check| HDFS_RAW
        AIRFLOW -->|2. spark-submit (Batch KPIs)| SPARK_BATCH["Spark Batch KPI Processor<br/>(batch_port_kpi_processor.py)"]
        HDFS_RAW -->|Historical Partition Read| SPARK_BATCH
        PG_REF[("PostGIS<br/>port_reference")] -->|Port Centroids| SPARK_BATCH
        SPARK_BATCH -->|Port Dwell Times| PG_DWELL[("PostGIS<br/>port_dwell_times")]
        SPARK_BATCH -->|Fleet Speed KPIs| PG_KPIS[("PostGIS<br/>fleet_daily_kpis")]
        SPARK_BATCH -->|Route Density Grid| PG_GRID[("PostGIS<br/>route_density_grid")]
        SPARK_BATCH -->|Materialized Alerts| PG_ALERTS[("PostGIS<br/>vessel_speed_alerts")]
    end

    subgraph LAYER5["3. Analytics & AI Layer"]
        direction TB
        AIRFLOW -->|3. spark-submit (Clustering)| SPARK_ML["PySpark MLlib KMeans<br/>(vessel_clustering.py)"]
        HDFS_RAW -->|StandardScaler & VectorAssembler| SPARK_ML
        SPARK_ML -->|Persist Model Artifacts| HDFS_MODELS[("HDFS /models/<br/>vessel_clustering/date=YYYY-MM-DD/")]
        SPARK_ML -->|Clustered Vessels & Anomalies| PG_CLUSTERS[("PostGIS<br/>vessel_behavior_clusters")]
        AIRFLOW -->|4. Verify Row Counts| PG_CLUSTERS

        ZEPPELIN["Apache Zeppelin Notebooks<br/>(Interactive Exploration :8091)"] -.->|Ad-hoc Analytics| HDFS_RAW
        ZEPPELIN -.->|Cluster Validation| HDFS_MODELS
    end

    subgraph SERVE["4. Visualization & BI Layer"]
        direction TB
        PG_ACTIVE --> SUPERSET["Apache Superset<br/>(Interactive BI Dashboards)"]
        PG_DWELL --> SUPERSET
        PG_KPIS --> SUPERSET
        PG_GRID --> SUPERSET
        PG_ALERTS --> SUPERSET
        PG_CLUSTERS --> SUPERSET
    end
```

---

## Quick Start

### Prerequisites

| Requirement | Minimum version |
|---|---|
| Docker Engine | 24.0+ |
| Docker Compose | 2.20+ |
| RAM | 16 GB (all services combined) |
| CPU cores | 4+ |
| OS | Windows 10/11, Linux, or macOS |

### Environment configuration

Before starting the stack, copy `.env.example` to `.env` and customize any passwords or ports as needed:

```bash
cp .env.example .env
```

### Getting the AIS source data

The historical replay producer (`ais-producer`) replays 7 consecutive daily AIS vessel traffic CSV archives from NOAA Marine Cadastre:

1. **Source portal:**
   Download the daily archives for **25–31 December 2024** from the NOAA Marine Cadastre AIS Data Handler:
   * NOAA Marine Cadastre AIS Portal: [https://marinecadastre.gov/ais/](https://marinecadastre.gov/ais/)
   * 2024 Data Index: [https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024/index.html](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024/index.html)
2. **Exact 7 expected filenames:**
   * `AIS_2024_12_25.csv`
   * `AIS_2024_12_26.csv`
   * `AIS_2024_12_27.csv`
   * `AIS_2024_12_28.csv`
   * `AIS_2024_12_29.csv`
   * `AIS_2024_12_30.csv`
   * `AIS_2024_12_31.csv`
3. **File placement:**
   Unzip each downloaded `.zip` archive and place the resulting flat `.csv` files into `data/historical/` (e.g. `data/historical/AIS_2024_12_25.csv`).
4. **Disk space considerations:**
   * Downloaded archives (.zip): ~2.5 GB total (~350 MB/day).
   * Uncompressed CSVs: ~18 GB total (~2.5 GB/day, ~6–8 million AIS pings per day).

### Start the stack

```bash
docker compose up -d
```

The first run downloads ~5 GB of images and builds two custom images (`bigdata-lab-spark-master` and `bigdata-lab-airflow`). Subsequent starts take ~30 seconds.

A convenience script for Windows/PowerShell is also provided:

```powershell
.\setup_and_run.ps1
```

This script additionally polls for Kafka/PostGIS readiness, disengages HDFS SafeMode, and installs the `pg8000` PostgreSQL driver into the Spark containers.

### Service endpoints

| Service | URL | Default credentials |
|---|---|---|
| Apache Airflow | http://localhost:8082 | `admin` / `changeme` (see `.env`) |
| Apache Superset | http://localhost:8089 | `admin` / `changeme` (see `.env`) |
| Spark Master UI | http://localhost:8080 | — |
| Spark Worker UI | http://localhost:8081 | — |
| Kafka UI | http://localhost:8090 | — |
| HDFS NameNode UI | http://localhost:9870 | — |
| Zeppelin Notebooks | http://localhost:8091 | — |
| Jupyter Lab | http://localhost:8888 | Token: `lab` |

> [!WARNING]
> All default credentials are for **local development only**. See the Security notes section before exposing any service beyond localhost.

### Run the full pipeline for a specific date

```bash
# 1. Start the AIS producer (replays 7 CSV files into Kafka)
docker compose start ais-producer

# 2. Trigger the Airflow batch pipeline for a date that has been archived to HDFS
docker compose exec airflow-scheduler \
  airflow dags trigger -e 2024-12-25 maritime_batch_kpi_pipeline

# 3. Check run status
docker compose exec airflow-scheduler \
  airflow dags list-runs -d maritime_batch_kpi_pipeline
```

### Verify data landed in PostGIS

```bash
docker compose exec postgis \
  psql -U maritime -d maritime \
  -c "SELECT kpi_date, count(*) FROM fleet_daily_kpis GROUP BY kpi_date;"
```

---

## Project structure

| Folder | Purpose |
|---|---|
| `ais/` | AIS Kafka producer: replays NOAA CSV archives into the `raw_ais_positions` topic |
| `spark/` | PySpark applications: Structured Streaming processor, Batch KPI processor, MLlib clustering job |
| `airflow/` | Airflow DAGs (`maritime_batch_kpi_pipeline`) and container build files |
| `notebooks/` | Zeppelin notebooks and workspace for interactive ML and SQL exploration |
| `db/` | PostGIS DDL init scripts (`/docker-entrypoint-initdb.d/`) |
| `superset/` | Superset config (`superset_config.py`) and provisioning scripts |
| `kafka/` | Topic creation shell script run by the `kafka-topics-init` container |
| `hadoop/` | Hadoop environment config (`hadoop.env`) |
| `data/` | Mount point for AIS CSV files (`data/historical/`) — not committed to git |
| `jupyter/` | Jupyter Lab Dockerfile and notebooks |

---

## Batch pipeline (Airflow DAG: `maritime_batch_kpi_pipeline`)

The production DAG runs five sequential tasks:

1. **`check_hdfs_partition_exists`** — Fails fast if the HDFS date partition `/raw/ais_historical/date=YYYY-MM-DD/` does not exist or contains no Parquet files.
2. **`run_batch_kpi_job`** — `spark-submit batch_port_kpi_processor.py` computing port dwell times, fleet speed KPIs, route density grids, and speed alerts.
3. **`run_vessel_clustering_job`** — `spark-submit vessel_clustering.py` fitting a K-Means model (k=5), detecting 95th-percentile anomalies, persisting model artifacts to HDFS, and writing results to `vessel_behavior_clusters`. *The K-Means model and StandardScaler are fit once, on the first date processed, and persisted as the 'reference' model/scaler in HDFS; every subsequent day reuses that same fitted model unless `--refit-model` is passed. This keeps cluster IDs comparable day-over-day but means cluster semantics reflect whichever date trained the reference model.*
4. **`verify_postgres_rows_written`** — Asserts non-zero rows landed in `fleet_daily_kpis` and `vessel_behavior_clusters`; raises `AirflowFailException` otherwise.
5. **`pipeline_health_check`** — Logs structured diagnostics for Kafka, Spark, HDFS, and PostGIS into the task log.

### Manual spark-submit (outside Airflow)

```bash
# Batch KPI processor
# worker=6G, 6 cores; streaming=1.5G/3 cores, batch=1.5G/3 cores — both can run concurrently
docker compose exec \
  -e POSTGIS_HOST -e POSTGIS_PORT -e POSTGIS_DB -e POSTGIS_USER -e POSTGIS_PASSWORD \
  spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0 \
  --py-files /opt/spark-apps/common.zip \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.executor.memory=1536m \
  --conf spark.driver.memory=768m \
  --conf spark.cores.max=3 \
  /opt/spark-apps/batch_port_kpi_processor.py --exec-date 2024-12-25

# Vessel behaviour clustering
# worker=6G, 6 cores; streaming=1.5G/3 cores, batch=1.5G/3 cores — both can run concurrently
docker compose exec \
  -e POSTGIS_HOST -e POSTGIS_PORT -e POSTGIS_DB -e POSTGIS_USER -e POSTGIS_PASSWORD \
  spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0 \
  --py-files /opt/spark-apps/common.zip \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.executor.memory=1536m \
  --conf spark.driver.memory=768m \
  --conf spark.cores.max=3 \
  /opt/spark-apps/ml/vessel_clustering.py \
  --exec-date 2024-12-25 --k 5 --anomaly-threshold-pct 95
```

---

## Apache Superset dashboards

Open http://localhost:8089 (`admin` / `changeme` or as configured in `.env`).

Automated provisioning:

```bash
docker compose exec superset python3 /opt/superset-scripts/setup_superset.py
# or
bash superset/setup_superset.sh
```

Four pre-configured dashboards are available after provisioning:

1. **Maritime Fleet Operations & AI Analytics** (Primary Unified Dashboard) — Complete executive overview bringing together real-time fleet positions, high-speed alerts, port dwell times, speed profiles by vessel type, route density heatmaps, and PySpark MLlib K-Means vessel behavior clusters.
2. **Real-time Fleet Tracking** — Geospatial scatterplot of active vessels (30 s auto-refresh).
3. **Speed Alerts & Safety Violations** — Real-time and historical alerts for vessels exceeding 20 knots.
4. **Port Congestion & Operational Metrics** — Dwell time rankings, speed profile distributions, and route density grids.

---

## PostGIS schema highlights

| Table | Description |
|---|---|
| `active_fleet_state` | Latest position per MMSI (upserted by streaming job) |
| `fleet_daily_kpis` | Per-vessel-type daily speed/activity KPIs |
| `port_dwell_times` | Individual port visit records with dwell duration |
| `route_density_grid` | 0.05° grid cells with ping counts per date |
| `vessel_speed_alerts` | Individual over-speed events (>20 kn) |
| `vessel_behavior_clusters` | K-Means cluster assignments and anomaly flags |
| `port_reference` | Static port centroids used for catchment calculations |

---

## Roadmap / Planned Future Capabilities

The following features have schema tables and Kafka topics provisioned in the repository, but active stream processing logic is planned for future releases:
- **Pairwise Collision Risk & CPA Analysis**: Tables `collision_risk_alerts` and topic `collision_risk_telemetry`.
- **Port Geofencing & Event Triggers**: Polygon boundaries in `geofence_boundaries`, event logging in `port_geofence_events`.
- **Automated Port Congestion Indexing**: Table `port_congestion`.

---

## Known limitations

- **Single-broker Kafka**: The cluster runs one broker in KRaft mode with `replication_factor=1`. There is no fault tolerance; a broker restart drops any in-flight messages.
- **Single-node Spark cluster**: One master and one worker. The worker is allocated 6 GB RAM and 6 vCPUs to comfortably run concurrent streaming and batch workloads.
- **Replay-only ingestion**: The AIS producer replays seven pre-downloaded NOAA CSV files. There is no continuous live feed. A live WebSocket upgrade via aisstream.io is stubbed in `.env` (`AISSTREAM_API_KEY`, `AIS_BOUNDING_BOXES`) but not yet implemented.
- **No Kafka authentication or TLS**: All Kafka listeners use `PLAINTEXT`. Suitable for a local lab only.
- **Airflow LocalExecutor**: All tasks run in a single process. Parallelism is limited and there is no worker autoscaling.
- **Airflow host Docker socket mount**: Airflow mounts `/var/run/docker.sock` to trigger `spark-submit` and `hdfs dfs` commands inside sibling containers (`spark-master`, `namenode`). This grants root-equivalent control over the host Docker daemon. In hardened enterprise environments, replace `docker exec` with native network operators like `SparkSubmitOperator` via Apache Livy or `KubernetesPodOperator` with RBAC policies.
- **No Fernet key**: `AIRFLOW__CORE__FERNET_KEY` is left empty; connection passwords stored in Airflow's metadata DB are not encrypted at rest.

---

## Security notes

> [!CAUTION]
> The default credentials and secret keys below are for **local development only**.  
> **Do not expose any service port to the internet or a shared network with these defaults.**

> [!WARNING]
> **Git History Advisory**: An earlier commit in git history (`1b2eb36`) contained a `.env` file with local passwords. While `.env` is now untracked and excluded in `.gitignore` (with `.env.example` provided as a clean template), all credentials must be rotated before deploying to any non-local or production environment. See [SECURITY.md](file:///c:/Users/user/Desktop/maritime-lab/SECURITY.md) for details and `git filter-repo` instructions to purge git history if required.

| Setting | Default value | .env variable |
|---|---|---|
| Airflow admin password | `changeme` | `AIRFLOW_ADMIN_PASSWORD` |
| Airflow webserver secret key | `change-me-to-a-secure-random-key` | `AIRFLOW_SECRET_KEY` |
| PostGIS user/password | `maritime` / `changeme` | `POSTGIS_USER` / `POSTGIS_PASSWORD` |
| Superset admin password | `changeme` | `SUPERSET_ADMIN_PASSWORD` |
| Superset secret key | `change-me-please-superset-secret` | `SUPERSET_SECRET_KEY` |
| Jupyter Lab token | `lab` | (hardcoded in docker-compose) |

Before any non-local deployment:
1. Replace all passwords and secret keys in `.env` with strong, randomly generated values.
2. Enable Kafka TLS and SASL authentication.
3. Set `AIRFLOW__CORE__FERNET_KEY` to a generated Fernet key (`python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`).
4. Set `AIRFLOW__WEBSERVER__WTF_CSRF_ENABLED=True`.
5. Restrict port bindings to `127.0.0.1` or place all services behind a reverse proxy with TLS termination.
6. Note on Zeppelin Notebooks: `notebooks/conf/interpreter.json` contains static JDBC credentials (`default.user: maritime`, `default.password: maritime`). If `POSTGIS_PASSWORD` is rotated, this file must be manually updated to match.
7. Decouple Airflow from the host Docker socket (`/var/run/docker.sock`) by transitioning to network-based execution (e.g. Apache Livy REST API, `SparkSubmitOperator`, or Kubernetes Executor).

---

## Tech stack

| Layer | Technology |
|---|---|
| Ingestion | Python, Apache Kafka 3.9 (KRaft mode) |
| Stream processing | Apache Spark 3.3 Structured Streaming |
| Batch processing | Apache Spark 3.3 |
| ML / clustering | PySpark MLlib KMeans; Apache Zeppelin (interactive) |
| Storage — data lake | Hadoop HDFS 3.2 |
| Storage — serving | PostgreSQL 15 + PostGIS 3.3 |
| Orchestration | Apache Airflow 2.9 (LocalExecutor) |
| Visualization | Apache Superset 3.1 |
| Infrastructure | Docker Compose |
| Languages | Python 3.11, SQL |
