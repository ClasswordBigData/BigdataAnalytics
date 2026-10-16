
import csv
import math
import os
import socket
import statistics
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg

# ============================================================
# DATABASE CONFIGURATION
# ============================================================

DATABASE_URL = (
    "postgresql://eric:kigali@10.32.87.163:5432/smart_energy_grid"
)

CAT = ZoneInfo("Africa/Kigali")

TABLES = [
    "energy_readings",
    "energy_readings_3h",
    "energy_readings_week",
]

# ============================================================
# BENCHMARK CONFIGURATION
# ============================================================

RUNS_PER_QUERY = 5
PROGRESS_INTERVAL = 100
MACHINE = socket.gethostname()

# Only the source table changes between configurations.
SQL = {
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
# RESULTS
# ============================================================

run_rows = []
summary_rows = []
comparison_rows = []
results = {}

overall_start = None
overall_end = None


def cat_now():
    """Return the current Kigali time."""
    return datetime.now(CAT).isoformat(timespec="milliseconds")


def same_results(left, right):
    """Compare query results with a numeric tolerance of 0.001."""
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
                    float(a),
                    float(b),
                    rel_tol=0.0,
                    abs_tol=0.001,
                ):
                    return False

            elif a != b:
                return False

    return True


def execute_timed(conn, query):
    """Execute a query and return rows and timing information."""
    start_cat = cat_now()
    start = time.perf_counter()

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    elapsed_ms = (time.perf_counter() - start) * 1000.0
    end_cat = cat_now()

    return rows, elapsed_ms, start_cat, end_cat


def export_csv(filename, rows):
    """Write benchmark results to CSV."""
    if not rows:
        print(f"Skipping empty results file: {filename}")
        return

    with open(filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"Created: {os.path.abspath(filename)}")


def main():
    global overall_start, overall_end

    print("=" * 65)
    print("SMART ENERGY GRID DATABASE BENCHMARK")
    print("=" * 65)
    print("Machine:", MACHINE)
    print("Database: smart_energy_grid")
    print("Database host: 10.32.87.163")
    print("Timezone: Africa/Kigali")
    print("Measured runs per query:", RUNS_PER_QUERY)
    print("Tables:", ", ".join(TABLES))
    print()

    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("SET TIME ZONE 'Africa/Kigali'")

        # Confirm that all benchmark tables exist.
        with conn.cursor() as cur:
            for table in TABLES:
                cur.execute(
                    "SELECT to_regclass(%s)",
                    (f"public.{table}",),
                )

                if cur.fetchone()[0] is None:
                    raise RuntimeError(
                        f"Table public.{table} does not exist."
                    )

        overall_start = cat_now()

        for table in TABLES:
            for qname, template in SQL.items():
                query = template.format(table=table)
                pair_start = time.perf_counter()

                # One warm-up execution, excluded from statistics.
                warm_rows, warm_ms, warm_start, warm_end = (
                    execute_timed(conn, query)
                )

                run_rows.append({
                    "query": qname,
                    "table": table,
                    "run_type": "warmup",
                    "run_number": 0,
                    "machine": MACHINE,
                    "start_cat": warm_start,
                    "end_cat": warm_end,
                    "duration_ms": f"{warm_ms:.3f}",
                })

                measured = []
                measured_results = []

                for run_number in range(1, RUNS_PER_QUERY + 1):
                    rows, elapsed_ms, start_cat, end_cat = (
                        execute_timed(conn, query)
                    )

                    measured.append(elapsed_ms)
                    measured_results.append(rows)

                    run_rows.append({
                        "query": qname,
                        "table": table,
                        "run_type": "measured",
                        "run_number": run_number,
                        "machine": MACHINE,
                        "start_cat": start_cat,
                        "end_cat": end_cat,
                        "duration_ms": f"{elapsed_ms:.3f}",
                    })

                # Preserve the final measured result for comparisons.
                results[(qname, table)] = measured_results[-1]

                median_ms = statistics.median(measured)

                pair_session_seconds = (
                    time.perf_counter() - pair_start
                )

                summary_rows.append({
                    "query": qname,
                    "table": table,
                    "machine": MACHINE,
                    "median_ms": f"{median_ms:.3f}",
                    "min_ms": f"{min(measured):.3f}",
                    "max_ms": f"{max(measured):.3f}",
                    "session_seconds_including_warmup":
                        f"{pair_session_seconds:.3f}",
                    "result_rows": len(results[(qname, table)]),
                })

                print(
                    f"{qname:2} {table:24} "
                    f"median={median_ms:.3f} ms "
                    f"min={min(measured):.3f} ms "
                    f"max={max(measured):.3f} ms "
                    f"rows={len(results[(qname, table)])}"
                )

        overall_end = cat_now()

    # ========================================================
    # COMPARE RESULTS ACROSS TABLES
    # ========================================================

    print("\n" + "=" * 65)
    print("RESULT EQUIVALENCE CHECKS")
    print("=" * 65)

    for qname in SQL:
        baseline = results[(qname, "energy_readings")]

        for table in TABLES[1:]:
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

    # ========================================================
    # EXPORT CSV FILES
    # ========================================================

    export_csv("benchmark_runs.csv", run_rows)
    export_csv("benchmark_summary.csv", summary_rows)
    export_csv("benchmark_result_checks.csv", comparison_rows)

    print("\n" + "=" * 65)
    print("BENCHMARK COMPLETE")
    print("=" * 65)
    print("Machine:", MACHINE)
    print("Client: Python + psycopg 3")
    print("Timer: time.perf_counter()")
    print("Cache: warm-cache; one warm-up before five measured runs")
    print("Overall CAT start:", overall_start)
    print("Overall CAT end:", overall_end)
    print("Output directory:", os.getcwd())


if __name__ == "__main__":
    main()
