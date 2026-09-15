"""
AIS STRUCTURED STREAMING -- THREE-SINK FAN-OUT ARCHITECTURE
============================================================

This job reads AIS vessel positions from a single Kafka topic and fans the
cleaned stream out to three independent downstream sinks, all driven by one
spark.readStream call so Kafka is consumed exactly once per micro-batch.

Sink layout
-----------
A. PostGIS  active_fleet_state  (foreachBatch -> pg8000 UPSERT)
   One row per MMSI, upserted every micro-batch so dashboards always show
   the latest known position of every active vessel.

B. Kafka topic vessel_speed_alerts  (foreachBatch -> Kafka batch write)
   Rule-based alert: any vessel whose SOG exceeds SPEED_ALERT_THRESHOLD_KNOTS
   emits a JSON alert message with mmsi, sog, position, and detected_at.

C. HDFS Parquet  hdfs://namenode:9000/raw/ais_historical/
   Full-fidelity archival of every cleaned AIS record, partitioned by date.
   Source of truth for downstream batch jobs and ML pipelines.

Checkpoint layout  (one directory per query -- NEVER share checkpoint dirs)
---------------------------------------------------------------------------
   CHECKPOINT_ROOT/postgis/        <- Sink A
   CHECKPOINT_ROOT/kafka_alerts/   <- Sink B
   CHECKPOINT_ROOT/hdfs_archive/   <- Sink C
   CHECKPOINT_ROOT/rejects/        <- validation rejects side-stream
   CHECKPOINT_ROOT/json_rejects/   <- JSON dead-letter side-stream

Dead-letter / rejects handling
--------------------------------
Rows unparseable at JSON level    -> json_rejects      (written to HDFS)
Rows failing validation checks    -> validation_rejects (written to HDFS)
Neither path is silently dropped.

Run modes
---------
RUN_MODE=backfill (default)
    Uses availableNow=True -- drains the current Kafka backlog then stops.
    Designed for historical replay of the ~46.5 M-row CSV dataset.

RUN_MODE=live
    Uses processingTime every LIVE_TRIGGER_SECONDS for a continuous feed.
    All five queries share the trigger so micro-batches are time-aligned.

Environment variables
---------------------
See the CONFIG section below for the full list with defaults.

Dependencies (Maven coordinates for spark-submit --packages)
------------------------------------------------------------
  org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0
  org.postgresql:postgresql:42.6.0
  pg8000 must be pip-installed in the Spark Python env (pure-Python, no C build tools required).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Iterator, List

import pg8000.dbapi as pg_driver
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    dayofmonth,
    dayofweek,
    from_json,
    hour,
    lit,
    month,
    quarter,
    row_number,
    struct,
    to_date,
    to_json,
    to_timestamp,
    trim,
    when,
    year,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Logging -- use a real logger for all error paths; print() for startup banner
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("ais_streaming")


# ============================================================
# CONFIGURATION
# ============================================================

def _env(key: str, default: str) -> str:
    """Read key from the environment, falling back to default."""
    return os.environ.get(key, default)


RUN_MODE: str = _env("RUN_MODE", "backfill").lower()

# Kafka
KAFKA_BOOTSTRAP: str = _env("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC: str = _env("KAFKA_TOPIC", "raw_ais_positions")
ALERT_TOPIC_SPEED: str = _env("ALERT_TOPIC_SPEED", "vessel_speed_alerts")
MAX_OFFSETS_PER_TRIGGER: str = _env("MAX_OFFSETS_PER_TRIGGER", "500000")
LIVE_TRIGGER_SECONDS: int = int(_env("LIVE_TRIGGER_SECONDS", "2"))

# PostGIS -- all credentials from env vars, never hardcoded
POSTGIS_HOST: str = _env("POSTGIS_HOST", "postgis")
POSTGIS_PORT: int = int(_env("POSTGIS_PORT", "5432"))
POSTGIS_DB: str = _env("POSTGIS_DB", "maritime")
POSTGIS_USER: str = _env("POSTGIS_USER", "maritime")
POSTGIS_PASSWORD: str = _env("POSTGIS_PASSWORD", "maritime")

# HDFS -- port 9000 matches docker-compose.yml HDFS_RPC_PORT (NOT 8020).
# The prompt spec says 8020 but the actual running cluster uses 9000.
# Override via HDFS_BASE env var if your setup differs.
HDFS_BASE: str = _env("HDFS_BASE", "hdfs://namenode:9000")
ARCHIVE_PATH: str = _env("ARCHIVE_PATH", f"{HDFS_BASE}/raw/ais_historical")
REJECTED_PATH: str = _env("REJECTED_PATH", f"{HDFS_BASE}/rejected/ais_streaming")

# Five independent checkpoint directories -- one per streaming query.
# Sharing a checkpoint directory across independent queries is a serious bug:
# queries race to write the same offset files and corrupt each other's state,
# causing data loss and phantom re-processing on restart.
CHECKPOINT_ROOT: str = _env(
    "CHECKPOINT_ROOT", f"{HDFS_BASE}/checkpoints/ais_streaming_v2"
)
CHECKPOINT_PRIMARY: str = f"{CHECKPOINT_ROOT}/primary"
CHECKPOINT_REJECTS: str = f"{CHECKPOINT_ROOT}/rejects"
CHECKPOINT_JSON_REJECTS: str = f"{CHECKPOINT_ROOT}/json_rejects"

SPEED_ALERT_THRESHOLD_KNOTS: float = float(_env("SPEED_ALERT_THRESHOLD_KNOTS", "20.0"))
MAX_RECORDS_PER_FILE: int = int(_env("MAX_RECORDS_PER_FILE", "1000000"))


# ============================================================
# SPARK SESSION
# ============================================================

spark: SparkSession = (
    SparkSession.builder
    .appName("AIS-MultiSink-Streaming")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# ============================================================
# AIS INPUT SCHEMA
# ============================================================
# All fields arrive as strings from the CSV-replay Kafka producer.
# Numeric types are cast explicitly downstream after validation.
# _corrupt_record is populated by from_json(PERMISSIVE mode) for rows whose
# raw value cannot be parsed as valid JSON matching this schema.

_ais_raw_schema = StructType([
    StructField("MMSI",             StringType(), True),
    StructField("BaseDateTime",     StringType(), True),
    StructField("LAT",              StringType(), True),
    StructField("LON",              StringType(), True),
    StructField("SOG",              StringType(), True),
    StructField("COG",              StringType(), True),
    StructField("Heading",          StringType(), True),
    StructField("VesselName",       StringType(), True),
    StructField("IMO",              StringType(), True),
    StructField("CallSign",         StringType(), True),
    StructField("VesselType",       StringType(), True),
    StructField("Status",           StringType(), True),
    StructField("Length",           StringType(), True),
    StructField("Width",            StringType(), True),
    StructField("Draft",            StringType(), True),
    StructField("Cargo",            StringType(), True),
    StructField("TransceiverClass", StringType(), True),
    StructField("_corrupt_record",  StringType(), True),
])


# ============================================================
# STARTUP BANNER
# ============================================================

print("=" * 70)
print("AIS STRUCTURED STREAMING -- THREE-SINK FAN-OUT")
print("=" * 70)
print(f"Run mode             : {RUN_MODE}")
print(f"Kafka broker         : {KAFKA_BOOTSTRAP}")
print(f"Source topic         : {KAFKA_TOPIC}")
print(f"Speed alert topic    : {ALERT_TOPIC_SPEED}")
print(f"PostGIS              : {POSTGIS_HOST}:{POSTGIS_PORT}/{POSTGIS_DB}")
print(f"Archive path         : {ARCHIVE_PATH}")
print(f"Rejected path        : {REJECTED_PATH}")
print(f"Checkpoint root      : {CHECKPOINT_ROOT}")
print(f"Speed threshold      : {SPEED_ALERT_THRESHOLD_KNOTS} knots")
print(f"maxOffsetsPerTrigger : {MAX_OFFSETS_PER_TRIGGER}")
print(f"maxRecordsPerFile    : {MAX_RECORDS_PER_FILE}")
if RUN_MODE == "live":
    print(f"Trigger              : processingTime every {LIVE_TRIGGER_SECONDS}s")
else:
    print("Trigger              : availableNow (backfill -- drains then stops)")
print("=" * 70)


# ============================================================
# 1.  KAFKA SOURCE -- single shared read
# ============================================================
# Important: we read from Kafka exactly once. All five streaming queries
# (three primary sinks + two reject side-streams) are derived from
# transformations on this one DataFrame. Spark executes the shared lineage
# once per micro-batch per query; each query tracks its own offsets and
# checkpoints independently.

raw_stream: DataFrame = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
    .option("failOnDataLoss", "false")
    .load()
)


# ============================================================
# 2.  PARSE JSON -- PERMISSIVE (keep unparseable rows in dead-letter)
# ============================================================

parsed_stream: DataFrame = (
    raw_stream
    .withColumn("kafka_partition", col("partition"))
    .withColumn("kafka_offset",    col("offset"))
    .withColumn("kafka_timestamp", col("timestamp"))
    .withColumn(
        "data",
        from_json(
            col("value").cast("string"),
            _ais_raw_schema,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ),
    )
    .select("kafka_partition", "kafka_offset", "kafka_timestamp", "data.*")
)


# ============================================================
# 3.  SPLIT: JSON-LEVEL REJECTS vs CANDIDATES
# ============================================================

json_rejects: DataFrame = parsed_stream.filter(col("_corrupt_record").isNotNull())

candidates: DataFrame = (
    parsed_stream
    .filter(col("_corrupt_record").isNull())
    .drop("_corrupt_record")
)


# ============================================================
# 4.  BASIC STRING CLEANING
# ============================================================

# Drop repeated CSV header rows that sneak through (producer artefact)
candidates = candidates.filter(col("MMSI") != "MMSI")

# Trim MMSI and require it to be non-null
candidates = candidates.withColumn("MMSI", trim(col("MMSI")))
candidates = candidates.filter(col("MMSI").isNotNull())

# MMSI must be exactly 9 digits -- route failures to rejects, not /dev/null
mmsi_rejects: DataFrame = candidates.filter(~col("MMSI").rlike(r"^[0-9]{9}$"))
candidates = candidates.filter(col("MMSI").rlike(r"^[0-9]{9}$"))

# Normalise empty strings -> NULL across all string columns
_string_cols: List[str] = [
    "BaseDateTime", "LAT", "LON", "SOG", "COG", "Heading",
    "VesselName", "IMO", "CallSign", "VesselType", "Status",
    "Length", "Width", "Draft", "Cargo", "TransceiverClass",
]
for _c in _string_cols:
    candidates = candidates.withColumn(
        _c,
        when(trim(col(_c)) == "", None).otherwise(trim(col(_c))),
    )


# ============================================================
# 5.  TYPE CASTING
# ============================================================
# MMSI is validated as 9 digits above; cast to Long is safe after that.

candidates = (
    candidates
    .withColumn("MMSI",         col("MMSI").cast(LongType()))
    .withColumn("BaseDateTime", to_timestamp(col("BaseDateTime"), "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("LAT",          col("LAT").cast(DoubleType()))
    .withColumn("LON",          col("LON").cast(DoubleType()))
    .withColumn("SOG",          col("SOG").cast(DoubleType()))
    .withColumn("COG",          col("COG").cast(DoubleType()))
    .withColumn("Heading",      col("Heading").cast(DoubleType()))
    .withColumn("VesselType",   col("VesselType").cast(IntegerType()))
    .withColumn("Status",       col("Status").cast(IntegerType()))
    .withColumn("Length",       col("Length").cast(DoubleType()))
    .withColumn("Width",        col("Width").cast(DoubleType()))
    .withColumn("Draft",        col("Draft").cast(DoubleType()))
    .withColumn("Cargo",        col("Cargo").cast(IntegerType()))
)


# ============================================================
# 6.  VALIDATION REJECTS (captured before dropping)
# ============================================================
# These rows passed JSON parsing and have a valid 9-digit MMSI, but fail
# downstream validation. Captured here so no malformed row goes untracked.

timestamp_rejects: DataFrame = candidates.filter(col("BaseDateTime").isNull())
candidates = candidates.filter(col("BaseDateTime").isNotNull())

_valid_latlon = (
    (col("LAT") >= -90)  & (col("LAT") <= 90) &
    (col("LON") >= -180) & (col("LON") <= 180)
)
latlon_rejects: DataFrame = candidates.filter(~_valid_latlon)
candidates = candidates.filter(_valid_latlon)

# Combine all validation-level rejects into one side-stream.
# All reject-stream columns are intentionally cast to StringType for triage purposes
# and uniform Parquet schema consistency across micro-batches.
timestamp_rejects_typed = timestamp_rejects.select(
    [col(c).cast(StringType()).alias(c) for c in timestamp_rejects.columns]
)
latlon_rejects_typed = latlon_rejects.select(
    [col(c).cast(StringType()).alias(c) for c in latlon_rejects.columns]
)
mmsi_rejects_typed = mmsi_rejects.select(
    [col(c).cast(StringType()).alias(c) for c in mmsi_rejects.columns]
)
validation_rejects: DataFrame = (
    timestamp_rejects_typed
    .unionByName(latlon_rejects_typed, allowMissingColumns=True)
    .unionByName(mmsi_rejects_typed,   allowMissingColumns=True)
)


# ============================================================
# 7.  AIS SENTINEL VALUE NULLING
# ============================================================
# AIS spec (ITU-R M.1371) defines sentinel values meaning "data unavailable".
# Nullify them so downstream analytics do not treat them as real readings.

candidates = (
    candidates
    # Heading: 511 means "not available" per ITU-R M.1371
    .withColumn("Heading", when(col("Heading") == 511, None).otherwise(col("Heading")))
    # SOG: >= 102.2 kts means "not available" (spec sentinel 102.3;
    # using >= 102.2 absorbs floating-point rounding artefacts)
    .withColumn("SOG",     when(col("SOG") >= 102.2, None).otherwise(col("SOG")))
    # COG: >= 360 degrees means "not available"
    .withColumn("COG",     when(col("COG") >= 360, None).otherwise(col("COG")))
)


# ============================================================
# 8.  CATEGORICAL DEFAULTS
# ============================================================

for _cat in ["VesselName", "IMO", "CallSign", "TransceiverClass"]:
    candidates = candidates.withColumn(
        _cat,
        when(col(_cat).isNull() | (trim(col(_cat)) == ""), lit("UNKNOWN"))
        .otherwise(col(_cat)),
    )
candidates = candidates.withColumn(
    "IMO",
    when(col("IMO") == "IMO0000000", lit("UNKNOWN")).otherwise(col("IMO")),
)


# ============================================================
# 9.  DEDUPLICATION (watermark-based)
# ============================================================
# Guards against Kafka producer retries creating duplicate rows.
# Watermark on BaseDateTime bounds the state store memory footprint.
# Rejects are split off BEFORE dedup so every malformed row is captured,
# even if it is a duplicate of a valid record.

cleaned_stream: DataFrame = (
    candidates
    .withWatermark("BaseDateTime", "10 minutes")
    .dropDuplicates(["MMSI", "BaseDateTime"])
)


# ============================================================
# 10. DERIVED COLUMNS (speed conversion + date/time features)
# ============================================================

cleaned_stream = (
    cleaned_stream
    .withColumn("speed_kmh",   col("SOG") * 1.852)
    .withColumn("date",        to_date(col("BaseDateTime")))
    .withColumn("year",        year(col("BaseDateTime")))
    .withColumn("month",       month(col("BaseDateTime")))
    .withColumn("day",         dayofmonth(col("BaseDateTime")))
    .withColumn("hour",        hour(col("BaseDateTime")))
    .withColumn("day_of_week", dayofweek(col("BaseDateTime")))
    .withColumn("quarter",     quarter(col("BaseDateTime")))
)


# ============================================================
# HELPER: PostGIS connection factory
# ============================================================

def _make_pg_connection():
    """
    Open a pg8000 connection using module-level config vars.
    Credentials are read from env vars at call time (inside each executor),
    never hardcoded.

    pg8000 is a pure-Python DB-API 2.0 driver (no C extensions) and works on
    Alpine Linux containers where psycopg2-binary fails due to C-symbol linking
    issues (``secure_getenv``).

    Note: pg8000.dbapi.connect uses ``database=`` (not psycopg2's ``dbname=``).

    Returns
    -------
    pg8000.dbapi connection object
    """
    return pg_driver.connect(
        host=POSTGIS_HOST,
        port=POSTGIS_PORT,
        database=POSTGIS_DB,      # pg8000 uses `database`, not `dbname`
        user=POSTGIS_USER,
        password=POSTGIS_PASSWORD,
    )


# ============================================================
# SINK A: PostGIS active_fleet_state UPSERT
# ============================================================
# WARNING: ST_MakePoint(longitude, latitude) -- longitude FIRST.
# This is the OPPOSITE of the everyday (lat, lon) ordering convention.
# Swapping them silently mirrors every vessel position across the equator
# and prime meridian, producing plausible-looking but completely wrong data.

# Full per-row UPSERT statement with inline %s placeholders.
# pg8000 uses standard DB-API 2.0 parameterisation (%s for every value),
# executed via cursor.executemany() -- one network round-trip per batch,
# replacing psycopg2.extras.execute_values which is a psycopg2 extension.
#
# Column order passed from _write_partition_to_postgis:
#   (mmsi, vessel_name, lon, lat, sog, cog, heading, nav_status, last_updated)
#
# WARNING: ST_MakePoint(longitude, latitude) -- longitude FIRST.
# This is the OPPOSITE of the everyday (lat, lon) ordering convention.
# Swapping them silently mirrors every vessel position across the equator
# and prime meridian, producing plausible-looking but completely wrong data.
_UPSERT_SQL = """
    INSERT INTO active_fleet_state
        (mmsi, vessel_name, geom, sog_knots, cog_degrees, heading_degrees,
         nav_status, last_updated)
    VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, %s, %s)
    ON CONFLICT (mmsi) DO UPDATE SET
        vessel_name      = EXCLUDED.vessel_name,
        geom             = EXCLUDED.geom,
        sog_knots        = EXCLUDED.sog_knots,
        cog_degrees      = EXCLUDED.cog_degrees,
        heading_degrees  = EXCLUDED.heading_degrees,
        nav_status       = EXCLUDED.nav_status,
        last_updated     = EXCLUDED.last_updated;
"""


def _write_partition_to_postgis(partition_iter: Iterator) -> None:
    """
    Called once per Spark partition by foreachPartition.

    Opens exactly one pg8000 connection for the entire partition, bulk-
    upserts all rows via cursor.executemany(), and closes the connection in a
    finally block -- no connection leaks even on the exception path.

    cursor.executemany() is standard DB-API 2.0 and supported by pg8000.
    It sends each row as a separate parameterised statement but reuses the
    same network connection and parsed query plan, giving good throughput
    without requiring any psycopg2-specific extensions.

    Parameters
    ----------
    partition_iter : Iterator
        Iterator of pyspark.sql.Row objects from one DataFrame partition.
    """
    rows = list(partition_iter)
    if not rows:
        return

    now_utc = datetime.now(timezone.utc)

    # Tuple order matches _UPSERT_SQL placeholders:
    # (mmsi, vessel_name, lon, lat, sog, cog, heading, nav_status, last_updated)
    values = []
    for row in rows:
        values.append((
            int(row["MMSI"]),
            row["VesselName"],
            float(row["LON"]),             # longitude FIRST -- ST_MakePoint(lon, lat)
            float(row["LAT"]),             # latitude SECOND
            float(row["SOG"])     if row["SOG"]     is not None else None,
            float(row["COG"])     if row["COG"]     is not None else None,
            float(row["Heading"]) if row["Heading"] is not None else None,
            str(row["Status"])    if row["Status"]  is not None else None,
            now_utc,
        ))

    # Sort values deterministically by mmsi (index 0) to enforce uniform lock acquisition order
    # across concurrent transactions, preventing PostgreSQL 40P01 deadlocks.
    values.sort(key=lambda x: x[0])

    conn = None
    cur = None
    try:
        conn = _make_pg_connection()
        cur = conn.cursor()
        # executemany is standard DB-API 2.0: one round-trip per row, but
        # reuses the same connection and server-side query plan across the batch.
        cur.executemany(_UPSERT_SQL, values)
        conn.commit()
    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        raise   # re-raise; caller logs with batch context
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def upsert_fleet_state(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch handler for Sink A (PostGIS active_fleet_state).

    Filters rows with null LAT/LON (cannot upsert spatially without position),
    deduplicates by MMSI to keep only the latest record per vessel within the
    micro-batch, repartitions by MMSI to isolate keys to dedicated executor
    partitions, and routes each partition to _write_partition_to_postgis via
    foreachPartition -- exactly one DB connection per partition, never per row.

    Parameters
    ----------
    batch_df : DataFrame
        Micro-batch DataFrame from Spark Structured Streaming.
    batch_id : int
        Monotonically increasing batch sequence number.
    """
    spatial_df = batch_df.filter(col("LAT").isNotNull() & col("LON").isNotNull())

    # 1. Deduplicate within micro-batch: retain only the latest record per vessel
    window_spec = Window.partitionBy("MMSI").orderBy(col("BaseDateTime").desc_nulls_last())
    spatial_df = (
        spatial_df
        .withColumn("_rn", row_number().over(window_spec))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    count = spatial_df.count()
    if count == 0:
        log.info(
            "Sink A (PostGIS) batch %d: no rows with valid LAT/LON, skipping.", batch_id
        )
        return

    # 2. Partition by key (MMSI) to eliminate concurrent cross-task lock contention
    spatial_df = spatial_df.repartition(4, "MMSI")

    log.info("Sink A (PostGIS) batch %d: upserting %d rows.", batch_id, count)
    try:
        spatial_df.foreachPartition(_write_partition_to_postgis)
        log.info("Sink A (PostGIS) batch %d: upsert committed successfully.", batch_id)
    except Exception as exc:
        log.error(
            "Sink A (PostGIS) batch %d: foreachPartition raised %s: %s",
            batch_id, type(exc).__name__, exc,
        )
        raise   # re-raise so Spark retry/checkpoint semantics still apply


# ============================================================
# SINK B: Kafka & PostGIS speed-violation alerts
# ============================================================

_INSERT_SPEED_ALERT_SQL: str = (
    "INSERT INTO vessel_speed_alerts "
    "(detected_at, mmsi, vessel_name, sog_knots, lat, lon, nav_status, alert_type, threshold_knots) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
)

def _write_alerts_partition_to_postgis(partition_iter: Iterator) -> None:
    """
    Worker-side foreachPartition writer inserting speed alerts directly
    into PostGIS table vessel_speed_alerts via pure-Python pg8000.
    """
    rows = list(partition_iter)
    if not rows:
        return

    values = []
    for row in rows:
        values.append((
            row["detected_at"],
            int(row["MMSI"]),
            row["VesselName"],
            float(row["SOG"]) if row["SOG"] is not None else None,
            float(row["LAT"]) if row["LAT"] is not None else None,
            float(row["LON"]) if row["LON"] is not None else None,
            str(row["Status"]) if row["Status"] is not None else None,
            "SPEED_VIOLATION",
            float(SPEED_ALERT_THRESHOLD_KNOTS),
        ))

    # Sort deterministically by MMSI and timestamp to enforce consistent lock acquisition
    values.sort(key=lambda x: (x[1], str(x[0])))

    conn = None
    cur = None
    try:
        conn = _make_pg_connection()
        cur = conn.cursor()
        cur.executemany(_INSERT_SPEED_ALERT_SQL, values)
        conn.commit()
    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def publish_speed_alerts(batch_df: DataFrame, batch_id: int) -> None:
    """
    Publish speed violation alerts to Kafka topic vessel_speed_alerts
    AND persist directly to PostGIS table vessel_speed_alerts for real-time dashboard visibility.
    """
    alert_df = batch_df.filter(
        col("SOG").isNotNull() & (col("SOG") > SPEED_ALERT_THRESHOLD_KNOTS)
    )
    count = alert_df.count()
    if count == 0:
        log.info(
            "Sink B (Alerts) batch %d: no violations (SOG > %.1f kts), skipping.",
            batch_id, SPEED_ALERT_THRESHOLD_KNOTS,
        )
        return

    log.info(
        "Sink B (Alerts) batch %d: %d violation(s) (SOG > %.1f kts).",
        batch_id, count, SPEED_ALERT_THRESHOLD_KNOTS,
    )
    try:
        # 1. Emit alerts to Kafka topic
        alert_payload_df = alert_df.select(
            col("MMSI").cast(StringType()).alias("key"),
            to_json(
                struct(
                    col("MMSI").alias("mmsi"),
                    col("VesselName").alias("vessel_name"),
                    col("SOG").alias("sog_knots"),
                    col("LAT").alias("lat"),
                    col("LON").alias("lon"),
                    col("BaseDateTime").alias("base_date_time"),
                    col("Status").alias("nav_status"),
                    lit("SPEED_VIOLATION").alias("alert_type"),
                    lit(SPEED_ALERT_THRESHOLD_KNOTS).alias("threshold_knots"),
                    current_timestamp().alias("detected_at"),
                )
            ).alias("value"),
        )
        (
            alert_payload_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("topic", ALERT_TOPIC_SPEED)
            .save()
        )
        log.info(
            "Sink B (Kafka alerts) batch %d: published %d alerts to %s.",
            batch_id, count, ALERT_TOPIC_SPEED,
        )

        # 2. Persist directly to PostGIS table vessel_speed_alerts for live dashboard visibility
        alert_db_df = (
            alert_df
            .withColumn("detected_at", current_timestamp())
            .repartition(4, "MMSI")
        )
        alert_db_df.foreachPartition(_write_alerts_partition_to_postgis)
        log.info(
            "Sink B (PostGIS alerts) batch %d: persisted %d alerts to PostGIS vessel_speed_alerts.",
            batch_id, count,
        )
    except Exception as exc:
        log.error(
            "Sink B (Alerts) batch %d: write failed -- %s: %s",
            batch_id, type(exc).__name__, exc,
        )
        raise


# ============================================================
# SINK C: HDFS Parquet archival
# ============================================================

def archive_to_hdfs(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch handler for Sink C (HDFS Parquet archive).
    Archives full-fidelity cleaned records partitioned by date.
    """
    count = batch_df.count()
    if count == 0:
        log.info("Sink C (HDFS archive) batch %d: empty batch, skipping.", batch_id)
        return

    log.info("Sink C (HDFS archive) batch %d: archiving %d rows.", batch_id, count)
    # Optional marker retained for manual micro-batch audit and pipeline completeness checks
    marker_path = f"{ARCHIVE_PATH}/_batch_markers/{batch_id}"
    target_parts = max(1, (count + MAX_RECORDS_PER_FILE - 1) // MAX_RECORDS_PER_FILE)
    writer = (
        batch_df.coalesce(target_parts)
        .write.mode("append")
        .partitionBy("date")
        .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
    )
    try:
        writer.format("parquet").save(ARCHIVE_PATH)
        spark.createDataFrame(
            [(batch_id,)], ["batch_id"]
        ).write.mode("overwrite").json(marker_path)
        log.info(
            "Sink C (HDFS archive) batch %d: wrote %d rows to %s.",
            batch_id, count, ARCHIVE_PATH,
        )
    except Exception as exc:
        log.error(
            "Sink C (HDFS archive) batch %d: write failed -- %s: %s",
            batch_id, type(exc).__name__, exc,
        )
        raise


# ============================================================
# UNIFIED SINK HANDLER (Sinks A, B, C)
# ============================================================

def unified_sink(batch_df: DataFrame, batch_id: int) -> None:
    """
    Consolidated foreachBatch handler for all primary sinks (Sinks A, B, C).
    Persists the micro-batch DataFrame in memory across all sub-sinks to ensure
    Kafka records are read and processed exactly once per micro-batch.
    """
    batch_df.persist()
    try:
        # Sink A: Upsert latest vessel position into PostGIS active_fleet_state
        try:
            upsert_fleet_state(batch_df, batch_id)
        except Exception as exc:
            log.error("Sink A (PostGIS upsert) failed in batch %d: %s", batch_id, exc)
            raise

        # Sink B: Publish to Kafka alert topic and persist to PostGIS vessel_speed_alerts
        try:
            publish_speed_alerts(batch_df, batch_id)
        except Exception as exc:
            log.error("Sink B (Speed alerts) failed in batch %d: %s", batch_id, exc)
            raise

        # Sink C: Archive partitioned Parquet files to HDFS data lake
        try:
            archive_to_hdfs(batch_df, batch_id)
        except Exception as exc:
            log.error("Sink C (HDFS archive) failed in batch %d: %s", batch_id, exc)
            raise
    finally:
        batch_df.unpersist()


# ============================================================
# REJECT SINK HANDLERS (preserved + extended from prior version)
# ============================================================

def write_rejects(batch_df: DataFrame, batch_id: int) -> None:
    """
    Append validation-level rejected rows to HDFS for data-quality triage.

    Parameters
    ----------
    batch_df : DataFrame
    batch_id : int
    """
    count = batch_df.count()
    if count == 0:
        return
    try:
        batch_df.write.mode("append").format("parquet").save(REJECTED_PATH)
        log.info(
            "Rejects sink batch %d: wrote %d rows to %s.", batch_id, count, REJECTED_PATH
        )
    except Exception as exc:
        log.error(
            "Rejects sink batch %d: write failed -- %s: %s",
            batch_id, type(exc).__name__, exc,
        )
        raise


def write_json_rejects(batch_df: DataFrame, batch_id: int) -> None:
    """
    Append JSON dead-letter records to a separate HDFS path.

    Written to a distinct sub-path so they can be triaged separately from
    schema-validation rejects during data quality investigation.

    Parameters
    ----------
    batch_df : DataFrame
    batch_id : int
    """
    json_rejected_path = f"{REJECTED_PATH}_json"
    count = batch_df.count()
    if count == 0:
        return
    try:
        batch_df.write.mode("append").format("parquet").save(json_rejected_path)
        log.info(
            "JSON rejects sink batch %d: wrote %d records to %s.",
            batch_id, count, json_rejected_path,
        )
    except Exception as exc:
        log.error(
            "JSON rejects sink batch %d: write failed -- %s: %s",
            batch_id, type(exc).__name__, exc,
        )
        raise


# ============================================================
# TRIGGER CONFIGURATION
# ============================================================
# RUN_MODE=backfill (default):
#   availableNow=True drains all current Kafka offsets as fast as possible,
#   then terminates all queries automatically. Correct for historical replay.
#
#   NOTE: In PySpark 3.3.0 (our target), the availableNow trigger is expressed
#   as a keyword argument:  .trigger(availableNow=True).
#   The class-method form Trigger.AvailableNow() was added in Spark 3.4 and
#   is NOT available in 3.3.0 -- do not use it.
#
# RUN_MODE=live:
#   processingTime with LIVE_TRIGGER_SECONDS for a continuous real-time feed.
#   All five queries share the same trigger so micro-batches are time-aligned,
#   making per-batch metrics easy to compare across sinks.

if RUN_MODE == "live":
    trigger_kwargs: dict = {"processingTime": f"{LIVE_TRIGGER_SECONDS} seconds"}
else:
    trigger_kwargs = {"availableNow": True}


# ============================================================
# START STREAMING QUERIES
# ============================================================
# One primary unified query (consolidating Sinks A, B, and C via unified_sink)
# + two reject side-streams (validation rejects and json rejects).
# Consolidating the primary sinks into a single foreachBatch attached to ONE writeStream
# query ensures Kafka data is read and processed once per micro-batch, eliminating the
# 3x Kafka read amplification of the prior independent query design.

# Primary Unified Sink: PostGIS fleet state + Speed alerts (Kafka + PostGIS) + HDFS Parquet archive
query_primary = (
    cleaned_stream.writeStream
    .foreachBatch(unified_sink)
    .option("checkpointLocation", CHECKPOINT_PRIMARY)
    .trigger(**trigger_kwargs)
    .start()
)

# Reject side-streams (preserved for dead-letter diagnostics)
query_rejects = (
    validation_rejects.writeStream
    .foreachBatch(write_rejects)
    .option("checkpointLocation", CHECKPOINT_REJECTS)
    .trigger(**trigger_kwargs)
    .start()
)

query_json_rejects = (
    json_rejects.writeStream
    .foreachBatch(write_json_rejects)
    .option("checkpointLocation", CHECKPOINT_JSON_REJECTS)
    .trigger(**trigger_kwargs)
    .start()
)


# ============================================================
# WAIT FOR ALL QUERIES
# ============================================================

all_queries = [
    query_primary,
    query_rejects,
    query_json_rejects,
]

print("\nAll streaming queries started. Waiting for termination...")
if RUN_MODE == "live":
    print(f"Live mode -- runs until manually stopped (trigger every {LIVE_TRIGGER_SECONDS}s).")
else:
    print("Backfill mode -- will stop when Kafka backlog is fully drained.")

for q in all_queries:
    q.awaitTermination()

print("\nAll streams finished.")
