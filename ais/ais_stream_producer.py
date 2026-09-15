"""
Historical AIS CSV → Kafka Producer
=====================================

Ingestion mode: **CSV replay** (historical data only).

This producer reads NOAA MarineCadastre AIS CSV archives
(AIS_2024_12_25.csv … AIS_2024_12_31.csv) from AIS_DATA_DIR and publishes
every row to a Kafka topic in the same flat-dict schema that the Spark
Structured Streaming job (streaming_maritime_processor.py) expects.

There is intentionally no live WebSocket connection.  A future upgrade to
real-time ingestion via the aisstream.io WebSocket API would replace this
file; the AISSTREAM_API_KEY environment variable is already plumbed in
.env and docker-compose.yml as a placeholder for that upgrade.

Performance notes
-----------------
* One worker process per file (up to CSV_WORKERS at a time) so all
  physical cores are used.
* orjson is used when available (falls back to stdlib json silently).
* csv.reader + zip() avoids building a duplicate intermediate dict per row.
* Each worker flushes once at the end of its file instead of after every row.

Environment variables
---------------------
KAFKA_BOOTSTRAP   Kafka broker address          (default: kafka:9092)
KAFKA_TOPIC       Destination topic             (default: raw_ais_positions)
AIS_DATA_DIR      Directory containing CSV files (default: /data/historical)
MAX_ROWS          Max rows per file, 0 = all   (default: 0)
CSV_WORKERS       Parallel file workers         (default: min(files, CPUs))

Optional speed dependency:
    pip install orjson
"""

import csv
import json
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path

from kafka import KafkaProducer

try:
    import orjson
    def dumps(v):
        return orjson.dumps(v)
    JSON_LIB = "orjson"
except ImportError:
    def dumps(v):
        return json.dumps(v).encode("utf-8")
    JSON_LIB = "json (stdlib) -- install orjson for a big speedup"


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw_ais_positions")

AIS_DATA_DIR = Path(os.environ.get("AIS_DATA_DIR", "/data/historical"))

MAX_ROWS = int(os.environ.get("MAX_ROWS", "0"))

CSV_FILES = [
    "AIS_2024_12_25.csv",
    "AIS_2024_12_26.csv",
    "AIS_2024_12_27.csv",
    "AIS_2024_12_28.csv",
    "AIS_2024_12_29.csv",
    "AIS_2024_12_30.csv",
    "AIS_2024_12_31.csv",
]

# How many files to process in parallel. Default: all cores, but never
# more than the number of files (more workers than files is wasted).
CSV_WORKERS = int(os.environ.get("CSV_WORKERS", str(min(len(CSV_FILES), os.cpu_count() or 4))))


# ============================================================
# Per-worker Kafka producer
# ============================================================
# NOTE: KafkaProducer instances are NOT fork-safe / picklable, so each
# worker process must create its own producer after it starts -- it
# cannot be created once at module level and shared like the original.

def make_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=dumps,
        key_serializer=lambda v: v.encode("utf-8") if v else None,
        batch_size=256 * 1024,
        linger_ms=20,
        buffer_memory=64 * 1024 * 1024,
        compression_type="lz4",
        max_in_flight_requests_per_connection=5,
    )


# ============================================================
# Process a single CSV file (runs inside a worker process)
# ============================================================

def process_file(filename):

    filepath = AIS_DATA_DIR / filename
    print(f"\nStarting: {filename}", flush=True)

    if not filepath.exists():
        print(f"File not found: {filepath}", flush=True)
        return filename, 0

    producer = make_producer()
    start_time = time.time()
    count = 0

    with open(
        filepath,
        "r",
        encoding="utf-8",
        errors="replace",
        newline="",
    ) as file:

        reader = csv.reader(file)

        try:
            header = next(reader)
        except StopIteration:
            producer.close()
            return filename, 0

        try:
            mmsi_idx = header.index("MMSI")
        except ValueError:
            print(f"{filename}: no MMSI column found, skipping", flush=True)
            producer.close()
            return filename, 0

        n_cols = len(header)

        for row in reader:

            # ------------------------------------------------
            # Remove repeated headers inside CSV
            # ------------------------------------------------
            if len(row) <= mmsi_idx:
                continue

            mmsi = row[mmsi_idx].strip()

            if not mmsi or mmsi == "MMSI":
                continue

            # ------------------------------------------------
            # Pad/truncate ragged rows to match header length,
            # then zip into a dict (same shape as the original
            # DictReader-based output).
            # ------------------------------------------------
            if len(row) < n_cols:
                row = row + [""] * (n_cols - len(row))
            elif len(row) > n_cols:
                row = row[:n_cols]

            data = {
                key: (value.strip() if value else value)
                for key, value in zip(header, row)
            }

            producer.send(KAFKA_TOPIC, key=mmsi, value=data)
            count += 1

            if MAX_ROWS > 0 and count >= MAX_ROWS:
                break

            if count % 100_000 == 0:
                elapsed = time.time() - start_time
                rate = count / elapsed if elapsed > 0 else 0
                print(
                    f"{filename}: {count:,} rows | {rate:,.0f} rows/sec",
                    flush=True,
                )

    producer.flush()
    producer.close()

    elapsed = time.time() - start_time
    rate = count / elapsed if elapsed > 0 else 0

    print(
        f"Finished: {filename} | rows={count:,} | rate={rate:,.0f} rows/sec",
        flush=True,
    )

    return filename, count


# ============================================================
# Main
# ============================================================

def main():

    print("=" * 60)
    print("HISTORICAL AIS -> KAFKA PRODUCER (fast)")
    print("=" * 60)
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Data directory: {AIS_DATA_DIR}")
    print(f"MAX_ROWS per file: {MAX_ROWS}")
    print(f"JSON serializer: {JSON_LIB}")
    print(f"Workers: {CSV_WORKERS} (of {os.cpu_count()} CPUs detected)")
    print("=" * 60)

    total = 0
    overall_start = time.time()

    # One process per file, up to CSV_WORKERS at a time. Kafka brokers
    # handle many concurrent producers fine, so this is safe to run
    # against a single-broker dev cluster too -- just lower CSV_WORKERS
    # if you see the broker struggling.
    with mp.Pool(processes=CSV_WORKERS) as pool:
        for filename, count in pool.imap_unordered(process_file, CSV_FILES):
            total += count

    elapsed = time.time() - overall_start
    rate = total / elapsed if elapsed > 0 else 0

    print("\n" + "=" * 60)
    print(f"ALL FILES FINISHED | total={total:,} | rate={rate:,.0f} rows/sec")
    print("=" * 60)

    if total == 0:
        missing_files = []
        empty_files = []
        for filename in CSV_FILES:
            filepath = AIS_DATA_DIR / filename
            if not filepath.exists():
                missing_files.append(filename)
            elif filepath.stat().st_size == 0:
                empty_files.append(filename)
            else:
                empty_files.append(f"{filename} (no valid rows parsed)")

        print("\n" + "!" * 60, file=sys.stderr)
        print("ERROR: Zero rows produced across all expected AIS CSV files!", file=sys.stderr)
        print(f"Target directory: {AIS_DATA_DIR}", file=sys.stderr)
        if missing_files:
            print(f"Missing files ({len(missing_files)}/{len(CSV_FILES)}): {', '.join(missing_files)}", file=sys.stderr)
        if empty_files:
            print(f"Empty/unparseable files: {', '.join(empty_files)}", file=sys.stderr)
        print("Please download the NOAA Marine Cadastre AIS datasets for 25-31 Dec 2024", file=sys.stderr)
        print("and place the unzipped CSVs in data/historical/. See README.md for details.", file=sys.stderr)
        print("!" * 60 + "\n", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()