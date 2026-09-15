"""
Smart Maritime Vessel Traffic & Port Intelligence Platform
Layer 5: Analytics & AI Layer (Vessel Behavior Clustering)
============================================================
Automated production PySpark MLlib clustering pipeline:
  1. Ingests daily archived AIS Parquet partitions from HDFS
  2. Filters nulls, invalid coordinates, and impossible speeds
  3. Assembles and standardizes features (SOG, COG, LAT, LON) via StandardScaler
  4. Trains KMeans (k=5) to profile vessel navigation behaviors
  5. Computes Euclidean distance to cluster centroid for each ping
  6. Calculates per-cluster 95th percentile threshold to detect anomalies
  7. Persists fitted KMeansModel artifact to HDFS for auditability/reproducibility
  8. Materializes latest vessel behavior fix per MMSI to satisfy UNIQUE (kpi_date, mmsi, model_run_ts)
  9. Idempotently writes clustered results to PostGIS (vessel_behavior_clusters)
  10. Populates PostGIS spatial Point geometries (SRID 4326, lon-lat order)
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone, timedelta
import logging
import math
import os
import sys

import pg8000.dbapi as pg_driver
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import StandardScaler, StandardScalerModel, VectorAssembler
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

# Add parent directories to sys.path so common.schemas can be imported
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, "/opt/spark-apps")

try:
    from common.schemas import AIS_HISTORICAL_SCHEMA
except ImportError:
    AIS_HISTORICAL_SCHEMA = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("vessel_clustering")


# =============================================================================
# ENVIRONMENT CONFIGURATION
# =============================================================================

HDFS_NAMENODE = os.environ.get("HDFS_NAMENODE", "hdfs://namenode:9000")
HDFS_ARCHIVE_BASE = os.environ.get(
    "ARCHIVE_PATH", f"{HDFS_NAMENODE}/raw/ais_historical"
)
HDFS_MODELS_BASE = os.environ.get(
    "MODELS_PATH", f"{HDFS_NAMENODE}/models/vessel_clustering"
)

POSTGIS_HOST = os.environ.get("POSTGIS_HOST", "postgis")
POSTGIS_PORT = int(os.environ.get("POSTGIS_PORT", "5432"))
POSTGIS_DB = os.environ.get("POSTGIS_DB", "maritime")
POSTGIS_USER = os.environ.get("POSTGIS_USER", "maritime")
POSTGIS_PASSWORD = os.environ.get("POSTGIS_PASSWORD", "maritime")

JDBC_URL = f"jdbc:postgresql://{POSTGIS_HOST}:{POSTGIS_PORT}/{POSTGIS_DB}"
JDBC_PROPERTIES = {
    "user": POSTGIS_USER,
    "password": POSTGIS_PASSWORD,
    "driver": "org.postgresql.Driver",
}


# =============================================================================
# POSTGIS HELPERS
# =============================================================================

def get_pg_connection(retries: int = 4, delay: float = 2.0):
    """Create a pg8000 DB connection with retries for transient DNS/connection blips."""
    for attempt in range(1, retries + 1):
        try:
            return pg_driver.connect(
                host=POSTGIS_HOST,
                port=POSTGIS_PORT,
                database=POSTGIS_DB,
                user=POSTGIS_USER,
                password=POSTGIS_PASSWORD,
            )
        except Exception as exc:
            if attempt == retries:
                log.error("All %d connection attempts to PostGIS failed: %s", retries, exc)
                raise
            import time
            log.warning("PostGIS connection attempt %d failed (%s); retrying in %s s...", attempt, exc, delay)
            time.sleep(delay)


def delete_partition_date(table_name: str, date_col: str, exec_date: str) -> None:
    """Execute scoped DELETE to guarantee idempotent delete-then-insert per partition day."""
    log.info("Deleting existing records from %s WHERE %s = '%s'", table_name, date_col, exec_date)
    conn = None
    cur = None
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        query = f"DELETE FROM {table_name} WHERE {date_col} = %s"
        cur.execute(query, (exec_date,))
        conn.commit()
        log.info("Deleted %d existing rows from %s", cur.rowcount, table_name)
    except Exception as exc:
        if conn:
            conn.rollback()
        log.error("Failed to delete partition date from %s: %s", table_name, exc)
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def update_geometries(exec_date: str) -> None:
    """Populate PostGIS geom column with ST_SetSRID(ST_MakePoint(lon, lat), 4326)."""
    log.info("Updating PostGIS geometries for %s (longitude first)...", exec_date)
    conn = None
    cur = None
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        query = """
            UPDATE vessel_behavior_clusters
            SET geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326)
            WHERE kpi_date = %s AND geom IS NULL
        """
        cur.execute(query, (exec_date,))
        conn.commit()
        log.info("Successfully updated PostGIS geometries for %d rows", cur.rowcount)
    except Exception as exc:
        if conn:
            conn.rollback()
        log.error("Failed to update PostGIS geometries: %s", exc)
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Smart Maritime Layer 5: Vessel Behavior Clustering & Anomaly Detection"
    )
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    parser.add_argument(
        "--exec-date",
        type=str,
        default=yesterday,
        help="Target processing date partition (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--k",
        type=int,
        default=5,
        help="Number of clusters for KMeans (default: 5)",
    )
    parser.add_argument(
        "--anomaly-threshold-pct",
        type=float,
        default=95.0,
        help="Per-cluster percentile threshold for anomaly detection (default: 95.0)",
    )
    parser.add_argument(
        "--refit-model",
        action="store_true",
        default=False,
        help="Force re-fitting KMeans model and updating reference model (default: False)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    exec_date = args.exec_date
    k = args.k
    anomaly_threshold_pct = args.anomaly_threshold_pct
    refit_model = args.refit_model

    log.info("=================================================================")
    log.info("STARTING VESSEL BEHAVIOR CLUSTERING PIPELINE FOR %s", exec_date)
    log.info("Parameters: k=%d, anomaly_threshold_pct=%.1f%%, refit_model=%s", k, anomaly_threshold_pct, refit_model)
    log.info("=================================================================")

    spark = (
        SparkSession.builder
        .appName(f"Maritime-Vessel-Clustering-{exec_date}")
        .config("spark.sql.shuffle.partitions", "8")
        # worker=6G; streaming=1.5G executor + 0.75G driver, batch=1.5G executor + 0.75G driver, leaving headroom for OS/JVM overhead — both can run concurrently
        .config("spark.executor.memory", "1536m")
        .config("spark.driver.memory", "768m")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Read input partition from HDFS
        input_partition = f"{HDFS_ARCHIVE_BASE}/date={exec_date}"
        log.info("Reading HDFS partition: %s", input_partition)
        raw_df = spark.read.parquet(input_partition)
        raw_count = raw_df.count()
        log.info("Loaded %d raw pings from HDFS partition %s", raw_count, input_partition)

        if raw_count == 0:
            log.warning("No records found in partition %s. Exiting gracefully.", input_partition)
            return

        # 2. Select and clean features
        base_df = raw_df.select(
            F.col("MMSI").cast(LongType()).alias("mmsi"),
            F.col("BaseDateTime").alias("base_date_time"),
            F.col("SOG").cast(DoubleType()).alias("sog_knots"),
            F.col("COG").cast(DoubleType()).alias("cog_degrees"),
            F.col("LAT").cast(DoubleType()).alias("lat"),
            F.col("LON").cast(DoubleType()).alias("lon"),
        )

        valid_df = base_df.filter(
            F.col("mmsi").isNotNull() & (F.col("mmsi") > 0)
            & F.col("sog_knots").isNotNull() & (F.col("sog_knots") >= 0.0) & (F.col("sog_knots") <= 100.0)
            & F.col("cog_degrees").isNotNull() & (F.col("cog_degrees") >= 0.0) & (F.col("cog_degrees") <= 360.0)
            & F.col("lat").isNotNull() & (F.col("lat") >= -90.0) & (F.col("lat") <= 90.0)
            & F.col("lon").isNotNull() & (F.col("lon") >= -180.0) & (F.col("lon") <= 180.0)
        )
        valid_count = valid_df.count()
        dropped_count = raw_count - valid_count
        log.info("Feature cleaning complete: %d valid pings retained, %d invalid pings dropped", valid_count, dropped_count)

        if valid_count == 0:
            log.warning("No valid pings remaining after cleaning for %s. Exiting.", exec_date)
            return

        # Circular encoding for COG: avoid 0°/360° Euclidean discontinuity
        encoded_df = (
            valid_df
            .withColumn("cog_sin", F.sin(F.radians(F.col("cog_degrees"))))
            .withColumn("cog_cos", F.cos(F.radians(F.col("cog_degrees"))))
        )

        # 3. VectorAssembler & Reference Model Resolution
        assembler = VectorAssembler(
            inputCols=["sog_knots", "cog_sin", "cog_cos", "lat", "lon"],
            outputCol="features",
        )
        assembled_df = assembler.transform(encoded_df)

        ref_model_path = f"{HDFS_MODELS_BASE}/reference_model"
        ref_scaler_path = f"{HDFS_MODELS_BASE}/reference_scaler"

        model = None
        scaler_model = None

        if not refit_model:
            try:
                log.info("Checking for reference KMeans model and scaler at %s ...", ref_model_path)
                model = KMeansModel.load(ref_model_path)
                scaler_model = StandardScalerModel.load(ref_scaler_path)
                log.info("Successfully loaded existing reference model and scaler.")
            except Exception as exc:
                log.info("No valid reference model/scaler found (%s). Fitting new reference model.", exc)
                model = None
                scaler_model = None

        if model is None or scaler_model is None:
            log.info("Fitting new StandardScaler and KMeans model (k=%d, seed=42)...", k)
            scaler = StandardScaler(
                inputCol="features",
                outputCol="scaled_features",
                withMean=True,
                withStd=True,
            )
            scaler_model = scaler.fit(assembled_df)
            scaled_df = scaler_model.transform(assembled_df)

            kmeans = KMeans(
                k=k,
                seed=42,
                featuresCol="scaled_features",
                predictionCol="cluster_id",
            )
            model = kmeans.fit(scaled_df)

            # Persist newly fitted model and scaler as reference artifacts
            try:
                log.info("Saving reference model and scaler to %s ...", ref_model_path)
                model.write().overwrite().save(ref_model_path)
                scaler_model.write().overwrite().save(ref_scaler_path)
                log.info("Successfully saved reference model and scaler to HDFS.")
            except Exception as exc:
                log.warning("Could not persist reference model/scaler to HDFS: %s", exc)
        else:
            scaled_df = scaler_model.transform(assembled_df)

        # 4. KMeans Predictions & Centroid Telemetry
        clustered_df = model.transform(scaled_df)

        centers = [c.tolist() for c in model.clusterCenters()]
        centers_bc = spark.sparkContext.broadcast(centers)
        log.info("KMeans clustering complete. Centroid coordinates in scaled feature space:")
        for idx, center in enumerate(centers):
            log.info("  Centroid %d: %s", idx, [round(val, 4) for val in center])

        # 5. Distance to Centroid Calculation
        # ARCHITECTURAL DECISION & PERFORMANCE TRADE-OFF:
        # Distance and percentile anomaly thresholds are evaluated across all valid pings
        # before vessel deduplication. This ensures that active navigation trajectories
        # contribute proportional statistical weight to the behavioral distribution,
        # yielding accurate, representative 95th-percentile anomaly boundaries.
        #
        # SCALING RECOMMENDATION:
        # For hyper-scale workloads (>10M pings/day), Python UDF serialization overhead can
        # be minimized by vectorizing Euclidean distance with native Spark SQL Catalyst expressions
        # or downsampling stationary pings prior to clustering.
        @F.udf(returnType=DoubleType())
        def calc_distance(cluster_id, scaled_vec):
            if cluster_id is None or scaled_vec is None:
                return None
            center = centers_bc.value[int(cluster_id)]
            vec = scaled_vec.toArray()
            return float(math.sqrt(sum((float(a) - float(b)) ** 2 for a, b in zip(vec, center))))

        df_with_dist = clustered_df.withColumn(
            "distance_to_centroid",
            calc_distance(F.col("cluster_id"), F.col("scaled_features")),
        )

        # 6. Per-Cluster Anomaly Threshold Detection
        quantile_pct = float(anomaly_threshold_pct) / 100.0
        thresholds_df = (
            df_with_dist.groupBy("cluster_id")
            .agg(F.expr(f"percentile_approx(distance_to_centroid, {quantile_pct})").alias("threshold_distance"))
        )
        log.info("Per-cluster anomaly distance thresholds (percentile %.1f%%):", anomaly_threshold_pct)
        for row in thresholds_df.collect():
            log.info("  Cluster %d threshold: %.4f", row["cluster_id"], row["threshold_distance"] or 0.0)

        df_flagged = (
            df_with_dist.join(thresholds_df, on="cluster_id", how="left")
            .withColumn(
                "is_anomaly",
                F.coalesce(F.col("distance_to_centroid") > F.col("threshold_distance"), F.lit(False)),
            )
        )

        # 7. Persist Model Artifact to HDFS
        model_hdfs_path = f"{HDFS_MODELS_BASE}/date={exec_date}"
        log.info("Persisting KMeansModel artifact to HDFS: %s", model_hdfs_path)
        try:
            model.write().overwrite().save(model_hdfs_path)
            log.info("Successfully persisted model artifact to %s", model_hdfs_path)
        except Exception as exc:
            log.warning("Could not persist model artifact to %s: %s", model_hdfs_path, exc)

        # 8. Prepare Output DataFrame for PostGIS
        # Deduplicate to latest fix per vessel for the day, guaranteeing UNIQUE (kpi_date, mmsi, model_run_ts)
        window_spec = Window.partitionBy("mmsi").orderBy(F.col("base_date_time").desc_nulls_last())
        vessel_latest_df = (
            df_flagged
            .withColumn("row_num", F.row_number().over(window_spec))
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )
        vessel_count = vessel_latest_df.count()
        log.info("Fleet daily profiling: %d unique vessels materialized for %s", vessel_count, exec_date)

        model_run_ts = datetime.now(timezone.utc)
        output_df = vessel_latest_df.select(
            F.to_date(F.lit(exec_date)).alias("kpi_date"),
            F.col("mmsi"),
            F.col("cluster_id").cast(IntegerType()),
            F.round(F.col("lat"), 6).alias("lat"),
            F.round(F.col("lon"), 6).alias("lon"),
            F.round(F.col("sog_knots"), 2).alias("sog_knots"),
            F.round(F.col("cog_degrees"), 2).alias("cog_degrees"),
            F.round(F.col("distance_to_centroid"), 4).alias("distance_to_centroid"),
            F.col("is_anomaly").cast(BooleanType()).alias("is_anomaly"),
            F.lit(model_run_ts).cast(TimestampType()).alias("model_run_ts"),
        )

        # Structured cluster summary
        summary_stats = output_df.groupBy("cluster_id").agg(
            F.count("*").alias("total_vessels"),
            F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("anomalies"),
            F.round(F.avg("sog_knots"), 2).alias("avg_sog"),
            F.round(F.avg("distance_to_centroid"), 4).alias("avg_dist"),
        ).orderBy("cluster_id").collect()

        total_anomalies = 0
        log.info("=================================================================")
        log.info("VESSEL BEHAVIOR CLUSTER SUMMARY (%s)", exec_date)
        log.info("=================================================================")
        for row in summary_stats:
            c_id = row["cluster_id"]
            total = row["total_vessels"]
            anom = row["anomalies"]
            total_anomalies += anom
            avg_s = row["avg_sog"]
            avg_d = row["avg_dist"]
            pct = (anom / total * 100.0) if total > 0 else 0.0
            log.info("  Cluster %d: %d vessels, %d anomalies (%.1f%%), avg SOG %.2f kts, avg dist %.4f",
                     c_id, total, anom, pct, avg_s, avg_d)
        log.info("Total anomalous vessels flagged: %d out of %d total active fleet", total_anomalies, vessel_count)

        # 9. Idempotent Write to PostGIS (delete-then-insert scoped to exec_date)
        delete_partition_date("vessel_behavior_clusters", "kpi_date", exec_date)

        log.info("Writing clustered vessel records to PostGIS (table: vessel_behavior_clusters)...")
        (
            output_df.write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", "vessel_behavior_clusters")
            .options(**JDBC_PROPERTIES)
            .mode("append")
            .save()
        )
        log.info("Successfully wrote %d rows to PostGIS vessel_behavior_clusters", vessel_count)

        # 10. Populate PostGIS spatial geometry
        update_geometries(exec_date)

        log.info("=================================================================")
        log.info("VESSEL CLUSTERING PIPELINE FOR %s COMPLETED SUCCESSFULLY", exec_date)
        log.info("=================================================================")

    except Exception as exc:
        log.error("Fatal error during vessel clustering pipeline: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()