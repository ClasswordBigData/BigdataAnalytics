import json
import os
import time
from datetime import datetime

import psycopg
from smart_meter_simulator import dataset_info, iter_readings


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://eric:kigali@10.32.87.163:5432/smart_energy_grid"
)

BATCH_SIZE = 50_000
PROGRESS_INTERVAL = 250_000

FIELDS = (
    "meter_id",
    "event_time",
    "received_time",
    "power_kw",
    "voltage_v",
    "current_a",
    "frequency_hz",
    "energy_kwh",
)

COPY_SQL = """
    COPY energy_readings (
        meter_id,
        event_time,
        received_time,
        power_kw,
        voltage_v,
        current_a,
        frequency_hz,
        energy_kwh
    ) FROM STDIN
"""


def get_expected_count(info):
    """Read expected_counts.emitted_readings from dict/object metadata."""
    if isinstance(info, dict):
        counts = info.get("expected_counts", {})
        if isinstance(counts, dict):
            return counts.get("emitted_readings")

    counts = getattr(info, "expected_counts", None)
    if isinstance(counts, dict):
        return counts.get("emitted_readings")

    return getattr(counts, "emitted_readings", None)


def process_batch(conn, batch):
    """Bulk-copy one batch and commit it."""
    with conn.cursor() as cur:
        with cur.copy(COPY_SQL) as copy:
            for reading in batch:
                copy.write_row(tuple(reading[field] for field in FIELDS))

    conn.commit()


def main():
    # Total timing begins before metadata retrieval and generation.
    total_start = time.perf_counter()
    load_start = None

    dictionaries_consumed = 0
    committed_rows = 0
    batch = []

    print("=" * 65)
    print("SMART ENERGY GRID: COMPLETE STREAM INGESTION")
    print("=" * 65)

    # Step 1: Obtain the simulator's expected counts.
    print("\nSTEP 1: Reading dataset metadata...")
    info = dataset_info()

    print(json.dumps(info, indent=2, default=str))

    expected = get_expected_count(info)

    if expected is None:
        raise RuntimeError(
            "Could not find expected_counts.emitted_readings "
            "in dataset_info(). Inspect the printed metadata and "
            "update get_expected_count() to match its structure."
        )

    expected = int(expected)
    print(f"\nExpected emitted readings: {expected:,}")

    with psycopg.connect(DATABASE_URL) as conn:
        # Never accidentally append to an earlier full-stream load.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('public.energy_readings')"
            )
            if cur.fetchone()[0] is None:
                raise RuntimeError(
                    "Table public.energy_readings does not exist."
                )

            cur.execute("SELECT COUNT(*) FROM energy_readings")
            existing_rows = cur.fetchone()[0]

        if existing_rows != 0:
            raise RuntimeError(
                f"energy_readings already contains {existing_rows:,} rows. "
                "This program requires an initially empty table. "
                "Verify and clear the previous test data separately "
                "before running again."
            )

        print("\nSTEP 2: Starting a fresh iter_readings() iterator...")
        print(f"Batch size: {BATCH_SIZE:,}")
        print("Duplicates will be preserved.")
        print("Starting stream generation and ingestion...\n")

        iterator = iter_readings()

        try:
            for reading in iterator:
                dictionaries_consumed += 1
                batch.append(reading)

                if len(batch) >= BATCH_SIZE:
                    if load_start is None:
                        load_start = time.perf_counter()

                    process_batch(conn, batch)
                    committed_rows += len(batch)
                    batch.clear()

                    if committed_rows % PROGRESS_INTERVAL == 0:
                        elapsed = time.perf_counter() - total_start
                        print(
                            f"Consumed={dictionaries_consumed:,} | "
                            f"Committed={committed_rows:,} | "
                            f"Expected={expected:,} | "
                            f"Elapsed={elapsed:.1f}s"
                        )

            # Write the final partial batch.
            if batch:
                if load_start is None:
                    load_start = time.perf_counter()

                final_batch_size = len(batch)
                process_batch(conn, batch)
                committed_rows += final_batch_size
                batch.clear()

        except Exception:
            conn.rollback()
            print(
                "\nERROR: Ingestion failed. "
                "The current uncommitted batch was rolled back."
            )
            print(f"Dictionaries consumed: {dictionaries_consumed:,}")
            print(f"Previously committed rows: {committed_rows:,}")
            raise

        # The context manager commits/closes cleanly on successful exit.
        # Query the database before leaving the connection context.
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM energy_readings")
            stored_rows = cur.fetchone()[0]

            cur.execute("""
                SELECT
                    COUNT(DISTINCT meter_id),
                    MIN(event_time),
                    MAX(event_time)
                FROM energy_readings
            """)
            distinct_meters, earliest, latest = cur.fetchone()

            cur.execute("""
                SELECT
                    table_bytes,
                    index_bytes,
                    toast_bytes,
                    total_bytes
                FROM hypertable_detailed_size('energy_readings')
            """)
            sizes = cur.fetchone()

    final_commit_time = time.perf_counter()

    total_seconds = final_commit_time - total_start
    load_seconds = (
        final_commit_time - load_start
        if load_start is not None
        else 0.0
    )

    table_bytes, index_bytes, toast_bytes, total_bytes = sizes

    print("\n" + "=" * 65)
    print("STEP 3: FINAL INGESTION REPORT")
    print("=" * 65)
    print(f"Expected emitted readings: {expected:,}")
    print(f"Dictionaries consumed:     {dictionaries_consumed:,}")
    print(f"Rows committed by loader:  {committed_rows:,}")
    print(f"Rows stored in database:   {stored_rows:,}")

    all_match = (
        dictionaries_consumed == expected
        and committed_rows == expected
        and stored_rows == expected
    )

    print(f"All counts match:          {'YES' if all_match else 'NO'}")

    print("\nTIMING")
    print("Ingestion route: Direct PostgreSQL COPY in batches")
    print(f"Total elapsed seconds: {total_seconds:.2f}")
    print(f"Database load seconds: {load_seconds:.2f}")

    print("\nDATABASE CONTENT")
    print(f"Distinct meters: {distinct_meters:,}")
    print(f"Earliest event_time: {earliest}")
    print(f"Latest event_time:   {latest}")

    print("\nHYPERTABLE SIZE")
    print(f"Table data: {table_bytes:,} bytes "
          f"({table_bytes / 1024**2:.2f} MiB)")
    print(f"Indexes:    {index_bytes:,} bytes "
          f"({index_bytes / 1024**2:.2f} MiB)")
    print(f"TOAST:      {toast_bytes:,} bytes "
          f"({toast_bytes / 1024**2:.2f} MiB)")
    print(f"Total:      {total_bytes:,} bytes "
          f"({total_bytes / 1024**2:.2f} MiB; "
          f"{total_bytes / 1024**3:.3f} GiB)")

    print("\n" + "=" * 65)

    if all_match:
        print("SUCCESS: Expected, consumed, committed, and stored counts agree.")
        print("Keep your screenshots before removing any intermediate files.")
    else:
        print("WARNING: Counts do not all agree.")
        print("Do not clean up or claim successful completion yet.")

    print("=" * 65)


if __name__ == "__main__":
    main()
