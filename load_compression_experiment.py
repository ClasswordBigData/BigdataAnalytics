
import csv
import math
import os
import socket
import statistics
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import psycopg
from psycopg import sql

# ============================================================
# DATABASE CONFIGURATION
# ============================================================

DATABASE_URL = (
    "postgresql://eric:kigali@10.32.87.163:5432/smart_energy_grid"
)

CAT = ZoneInfo("Africa/Kigali")
MACHINE = socket.gethostname()

# ============================================================
# COMPRESSION LOAD CONFIGURATION
# ============================================================

SOURCE_TABLE = "energy_readings"

COMPRESSION_TARGETS = [
    "energy_time_compressed",
    "energy_meter_compressed",
]

COLUMNS = (
    "meter_id",
    "event_time",
    "received_time",
    "power_kw",
    "voltage_v",
    "current_a",
    "frequency_hz",
    "energy_kwh",
)

# ============================================================
# QUERY BENCHMARK CONFIGURATION
# ============================================================

QUERY_TABLES = [
    "energy_readings",
    "energy_readings_3h",
    "energy_readings_week",
]

RUNS_PER_QUERY = 5

QUERY_SQL = {
    "Q1": """
        SELECT
            date_trunc(
                'hour',
                event_time AT TIME ZONE 'Africa/Kigali'
            ) AS cat_hour,
            AVG(power_kw) AS avg_power_kw
        FROM {table}
        WHERE event_time >=
              TIMESTAMPTZ '2026-09-21 00:00:00 Africa/Kigali'
          AND event_time <
              TIMESTAMPTZ '2026-09-22 00:00:00 Africa/Kigali'
        GROUP BY 1
        ORDER BY 1
    """,

    "Q2": """
        SELECT
            date_bin(
                INTERVAL '15 minutes',
                event_time AT TIME ZONE 'Africa/Kigali',
                TIMESTAMP '2000-01-01 00:00:00'
            ) AS cat_interval,
            SUM(power_kw) AS total_power_kw
        FROM {table}
        WHERE event_time >=
              TIMESTAMPTZ '2026-09-22 00:00:00 Africa/Kigali'
          AND event_time <
              TIMESTAMPTZ '2026-09-29 00:00:00 Africa/Kigali'
        GROUP BY 1
        ORDER BY total_power_kw DESC, cat_interval ASC
        LIMIT 10
    """,

    "Q3": """
        SELECT
            meter_id,
            SUM(energy_kwh) AS total_energy_kwh
        FROM {table}
        GROUP BY meter_id
        ORDER BY meter_id
    """,

    "Q4": """
        SELECT
            SUM(energy_kwh) AS total_energy_kwh,
            AVG(power_kw) AS avg_power_kw,
            MAX(power_kw) AS peak_power_kw
        FROM {table}
        WHERE meter_id = 1000000000
          AND event_time >=
              TIMESTAMPTZ '2026-09-22 00:00:00 Africa/Kigali'
          AND event_time <
              TIMESTAMPTZ '2026-09-29 00:00:00 Africa/Kigali'
    """,

    "Q5": """
        SELECT
            COUNT(*) AS row_count,
            SUM(energy_kwh) AS total_energy_kwh,
            AVG(power_kw) AS avg_power_kw,
            MIN(power_kw) AS min_power_kw,
            MAX(power_kw) AS max_power_kw
        FROM {table}
    """,
}

# ============================================================
# SHARED HELPERS
# ============================================================

def cat_now():
    return datetime.now(CAT)


def csv_export(filename, rows):
    if not rows:
        print(f"WARNING: No rows to export to {filename}")
        return

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"CSV created: {os.path.abspath(filename)}")


def qualified_table(table_name):
    # Identifiers are safely quoted rather than interpolated raw.
    return sql.Identifier("public", table_name)


def get_chunks(cur, hypertable):
    cur.execute(
        """
        SELECT chunk_schema, chunk_name, range_start, range_end
        FROM timescaledb_information.chunks
        WHERE hypertable_name = %s
          AND hypertable_schema = 'public'
        ORDER BY range_start
        """,
        (hypertable,),
    )
    return cur.fetchall()


def same_results(left, right):
    if len(left) != len(right):
        return False

    for row_a, row_b in zip(left, right):
        if len(row_a) != len(row_b):
            return False

        for a, b in zip(row_a, row_b):
            if a is None or b is None:
                if a is not b:
                    return False

            elif isinstance(a, (int, float)) and not isinstance(a, bool):
                if not isinstance(b, (int, float)):
                    return False

                if not math.isclose(
                    float(a), float(b),
                    rel_tol=0.0,
                    abs_tol=0.001,
                ):
                    return False

            elif a != b:
                return False

    return True


def execute_timed(conn, query):
    start_cat = cat_now()
    start = time.perf_counter()

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    elapsed_ms = (time.perf_counter() - start) * 1000
    end_cat = cat_now()

    return rows, elapsed_ms, start_cat, end_cat


# ============================================================
# PHASE 1: LOAD DATA AND COMPRESS ELIGIBLE CHUNKS
# ============================================================

def run_compression_load(conn):
    print("\n" + "=" * 70)
    print("PHASE 1: COMPRESSION LOAD")
    print("=" * 70)

    log_rows = []

    # Use one fixed cutoff for the entire experiment.
    experiment_start = cat_now()
    cutoff = experiment_start - timedelta(hours=48)

    print("Machine:", MACHINE)
    print("Experiment start CAT:", experiment_start.isoformat())
    print("Fixed cutoff CAT:", cutoff.isoformat())

    with conn.cursor() as cur:
        source_chunks = get_chunks(cur, SOURCE_TABLE)

        if not source_chunks:
            raise RuntimeError(
                "No source chunks found for public.energy_readings. "
                "Check the hypertable and schema."
            )

        # Check all destination tables before writing anything.
        for target in COMPRESSION_TARGETS:
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}").format(
                    qualified_table(target)
                )
            )
            count = cur.fetchone()[0]

            if count != 0:
                raise RuntimeError(
                    f"public.{target} already has {count} rows. "
                    "Inspect the destination before restarting."
                )

    conn.commit()

    column_sql = sql.SQL(", ").join(
        sql.Identifier(column) for column in COLUMNS
    )

    for schema, chunk_name, range_start, range_end in source_chunks:
        for target in COMPRESSION_TARGETS:
            load_start = cat_now()
            t0 = time.perf_counter()

            # Preserve the original chunk-by-chunk loading approach.
            # Source time ranges select the rows to copy.
            with conn.cursor() as cur:
                insert_query = sql.SQL("""
                    INSERT INTO {} ({})
                    SELECT {}
                    FROM {}
                    WHERE event_time >= %s
                      AND event_time < %s
                """).format(
                    qualified_table(target),
                    column_sql,
                    column_sql,
                    qualified_table(SOURCE_TABLE),
                )

                cur.execute(
                    insert_query,
                    (range_start, range_end),
                )
                inserted = cur.rowcount

            conn.commit()

            load_ms = (time.perf_counter() - t0) * 1000
            load_end = cat_now()

            entry = {
                "machine": MACHINE,
                "target": target,
                "source_chunk": f"{schema}.{chunk_name}",
                "range_start": str(range_start),
                "range_end": str(range_end),
                "cutoff_cat": cutoff.isoformat(),
                "inserted_rows": inserted,
                "load_start_cat": load_start.isoformat(),
                "load_end_cat": load_end.isoformat(),
                "load_ms": round(load_ms, 3),
                "compression_start_cat": "",
                "compression_end_cat": "",
                "compression_ms": "",
                "compressed": False,
            }

            log_rows.append(entry)

            # Locate the destination chunk containing this time range.
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        chunk_schema,
                        chunk_name,
                        range_start,
                        range_end
                    FROM timescaledb_information.chunks
                    WHERE hypertable_schema = 'public'
                      AND hypertable_name = %s
                      AND range_start <= %s
                      AND range_end >= %s
                    ORDER BY range_start
                    """,
                    (target, range_start, range_end),
                )
                destination_chunk = cur.fetchone()

            if destination_chunk is None:
                raise RuntimeError(
                    f"Could not locate destination chunk for {target}: "
                    f"{range_start} to {range_end}"
                )

            dest_schema, dest_name, dest_start, dest_end = (
                destination_chunk
            )

            # Compress only chunks whose end precedes the fixed cutoff.
            if dest_end < cutoff:
                compression_start = cat_now()
                c0 = time.perf_counter()

                chunk_regclass = sql.SQL("{}.{}").format(
                    sql.Identifier(dest_schema),
                    sql.Identifier(dest_name),
                )

                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL(
                            "SELECT convert_to_columnstore(%s::regclass)"
                        ),
                        (chunk_regclass.as_string(conn),),
                    )
                    cur.fetchone()

                conn.commit()

                compression_ms = (time.perf_counter() - c0) * 1000
                compression_end = cat_now()

                entry.update({
                    "compression_start_cat":
                        compression_start.isoformat(),
                    "compression_end_cat":
                        compression_end.isoformat(),
                    "compression_ms": round(compression_ms, 3),
                    "compressed": True,
                })

            print(
                f"{target} | {range_start} -> {range_end} | "
                f"inserted={inserted} | "
                f"load_ms={load_ms:.3f} | "
                f"compressed={entry['compressed']} | "
                f"compression_ms={entry['compression_ms']}"
            )

    csv_export("compression_load_log.csv", log_rows)


# ============================================================
# PHASE 2: QUERY PERFORMANCE BENCHMARK
# ============================================================

def run_query_benchmark(conn):
    print("\n" + "=" * 70)
    print("PHASE 2: QUERY PERFORMANCE BENCHMARK")
    print("=" * 70)

    run_rows = []
    summary_rows = []
    results = {}

    overall_start = cat_now()

    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'Africa/Kigali'")

    for table in QUERY_TABLES:
        # Validate table names against the explicit configuration.
        if table not in QUERY_TABLES:
            raise ValueError(f"Unexpected table: {table}")

        with conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass(%s)",
                (f"public.{table}",),
            )
            if cur.fetchone()[0] is None:
                raise RuntimeError(
                    f"Required benchmark table public.{table} "
                    "does not exist."
                )

        for qname, template in QUERY_SQL.items():
            query = template.format(
                table=f'"public"."{table}"'
            )

            pair_start = time.perf_counter()

            # One warm-up run, excluded from timing statistics.
            warm_rows, warm_ms, warm_start, warm_end = execute_timed(
                conn, query
            )

            run_rows.append({
                "query": qname,
                "table": table,
                "run_type": "warmup",
                "run_number": 0,
                "machine": MACHINE,
                "start_cat": warm_start.isoformat(),
                "end_cat": warm_end.isoformat(),
                "duration_ms": f"{warm_ms:.3f}",
            })

            measured = []
            last_rows = None

            for run_number in range(1, RUNS_PER_QUERY + 1):
                rows, elapsed_ms, start_cat, end_cat = execute_timed(
                    conn, query
                )

                measured.append(elapsed_ms)
                last_rows = rows

                run_rows.append({
                    "query": qname,
                    "table": table,
                    "run_type": "measured",
                    "run_number": run_number,
                    "machine": MACHINE,
                    "start_cat": start_cat.isoformat(),
                    "end_cat": end_cat.isoformat(),
                    "duration_ms": f"{elapsed_ms:.3f}",
                })

            results[(qname, table)] = last_rows

            median_ms = statistics.median(measured)
            session_seconds = time.perf_counter() - pair_start

            summary_rows.append({
                "query": qname,
                "table": table,
                "machine": MACHINE,
                "median_ms": f"{median_ms:.3f}",
                "min_ms": f"{min(measured):.3f}",
                "max_ms": f"{max(measured):.3f}",
                "session_seconds_including_warmup":
                    f"{session_seconds:.3f}",
                "result_rows": len(last_rows),
            })

            print(
                f"{qname:2} {table:24} "
                f"median={median_ms:.3f} ms "
                f"min={min(measured):.3f} ms "
                f"max={max(measured):.3f} ms "
                f"rows={len(last_rows)}"
            )

    comparison_rows = []

    print("\nRESULT EQUIVALENCE CHECKS")

    for qname in QUERY_SQL:
        baseline = results[(qname, "energy_readings")]

        for table in QUERY_TABLES[1:]:
            compared = results[(qname, table)]
            matches = same_results(baseline, compared)

            comparison_rows.append({
                "query": qname,
                "baseline_table": "energy_readings",
                "compared_table": table,
                "results_match_within_0_001": matches,
                "baseline_rows": len(baseline),
                "compared_rows": len(compared),
            })

            print(
                f"{qname} vs {table}: "
                f"{'PASS' if matches else 'FAIL'}"
            )

    overall_end = cat_now()

    csv_export("benchmark_runs.csv", run_rows)
    csv_export("benchmark_summary.csv", summary_rows)
    csv_export("benchmark_result_checks.csv", comparison_rows)

    print("Benchmark start CAT:", overall_start.isoformat())
    print("Benchmark end CAT:", overall_end.isoformat())


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 70)
    print("SMART ENERGY GRID EXPERIMENT")
    print("=" * 70)
    print("Machine:", MACHINE)
    print("Database host: 10.32.87.163")
    print("Database: smart_energy_grid")
    print("Timezone: Africa/Kigali")

    # Run both phases using the same database connection.
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("SET TIME ZONE 'Africa/Kigali'")

        run_compression_load(conn)
        run_query_benchmark(conn)

    print("\n" + "=" * 70)
    print("ALL EXPERIMENTS COMPLETED")
    print("=" * 70)
    print("Output files:")
    print("  compression_load_log.csv")
    print("  benchmark_runs.csv")
    print("  benchmark_summary.csv")
    print("  benchmark_result_checks.csv")


if __name__ == "__main__":
    main()
