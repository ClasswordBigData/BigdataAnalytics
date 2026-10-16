
#!/usr/bin/env python3
import os
import sys
import csv
import time
import argparse
import statistics
from pathlib import Path
from datetime import date, datetime
from decimal import Decimal

import psycopg
from psycopg.rows import dict_row


START = "2026-09-22 00:00:00 Africa/Kigali"
END = "2026-09-29 00:00:00 Africa/Kigali"
METER_ID = 1000000000
REGION_DAYS = 7
READINGS_PER_DAY = 96

OUT = Path("section7_results")
OUT.mkdir(exist_ok=True)


# Every query returns consistent names for matching metrics.
PANELS = {
    "hourly_regional": {
        "keys": ["time", "region"],
        "metrics": ["energy_kwh", "reading_count"],
        "energy_tolerance": 0.001,
        "queries": {
            "Raw": """
                SELECT
                    time_bucket(
                        INTERVAL '1 hour', e.event_time,
                        'Africa/Kigali'
                    ) AS time,
                    COALESCE(m.region, 'Unknown') AS region,
                    SUM(e.energy_kwh) AS energy_kwh,
                    COUNT(*) AS reading_count
                FROM energy_readings e
                JOIN meters m ON m.meter_id = e.meter_id
                WHERE e.event_time >=
                    TIMESTAMPTZ '2026-09-22 00:00:00 Africa/Kigali'
                  AND e.event_time <
                    TIMESTAMPTZ '2026-09-29 00:00:00 Africa/Kigali'
                GROUP BY 1, 2
                ORDER BY 1, 2
            """,
            "Aggregate": """
                SELECT
                    time_bucket(
                        INTERVAL '1 hour', bucket,
                        'Africa/Kigali'
                    ) AS time,
                    COALESCE(region, 'Unknown') AS region,
                    SUM(sum_energy_kwh) AS energy_kwh,
                    SUM(reading_count) AS reading_count
                FROM cagg_15m_region
                WHERE bucket >=
                    TIMESTAMPTZ '2026-09-22 00:00:00 Africa/Kigali'
                  AND bucket <
                    TIMESTAMPTZ '2026-09-29 00:00:00 Africa/Kigali'
                GROUP BY 1, 2
                ORDER BY 1, 2
            """,
        },
    },

    "daily_network": {
        "keys": ["time"],
        "metrics": ["energy_kwh", "reading_count"],
        "energy_tolerance": 0.001,
        "queries": {
            "Raw": """
                SELECT
                    time_bucket(
                        INTERVAL '1 day', event_time,
                        'Africa/Kigali'
                    ) AS time,
                    SUM(energy_kwh) AS energy_kwh,
                    COUNT(*) AS reading_count
                FROM energy_readings
                WHERE event_time >=
                    TIMESTAMPTZ '2026-09-22 00:00:00 Africa/Kigali'
                  AND event_time <
                    TIMESTAMPTZ '2026-09-29 00:00:00 Africa/Kigali'
                GROUP BY 1
                ORDER BY 1
            """,
            "Aggregate": """
                SELECT
                    time_bucket(
                        INTERVAL '1 day', bucket,
                        'Africa/Kigali'
                    ) AS time,
                    SUM(sum_energy_kwh) AS energy_kwh,
                    SUM(reading_count) AS reading_count
                FROM cagg_hour_region
                WHERE bucket >=
                    TIMESTAMPTZ '2026-09-22 00:00:00 Africa/Kigali'
                  AND bucket <
                    TIMESTAMPTZ '2026-09-29 00:00:00 Africa/Kigali'
                GROUP BY 1
                ORDER BY 1
            """,
        },
    },

    "weekly_network": {
        "keys": ["period"],
        "metrics": ["energy_kwh", "reading_count"],
        "energy_tolerance": 0.001,
        "queries": {
            "Raw": """
                WITH readings AS (
                    SELECT
                        (event_time AT TIME ZONE 'Africa/Kigali')::date
                            AS cat_date,
                        energy_kwh
                    FROM energy_readings
                    WHERE event_time >=
                        TIMESTAMPTZ '2026-09-22 00:00:00 Africa/Kigali'
                      AND event_time <
                        TIMESTAMPTZ '2026-09-29 00:00:00 Africa/Kigali'
                )
                SELECT
                    DATE '2026-09-01'
                      + ((cat_date - DATE '2026-09-01') / 7) * 7
                        AS period,
                    SUM(energy_kwh) AS energy_kwh,
                    COUNT(*) AS reading_count
                FROM readings
                GROUP BY 1
                ORDER BY 1
            """,
            "Aggregate": """
                WITH readings AS (
                    SELECT
                        DATE '2026-09-01'
                          + (
                              (
                                (bucket AT TIME ZONE 'Africa/Kigali')::date
                                - DATE '2026-09-01'
                              ) / 7
                            ) * 7 AS period,
                        sum_energy_kwh,
                        reading_count
                    FROM cagg_hour_region
                    WHERE bucket >=
                        TIMESTAMPTZ '2026-09-22 00:00:00 Africa/Kigali'
                      AND bucket <
                        TIMESTAMPTZ '2026-09-29 00:00:00 Africa/Kigali'
                )
                SELECT
                    period,
                    SUM(sum_energy_kwh) AS energy_kwh,
                    SUM(reading_count) AS reading_count
                FROM readings
                GROUP BY period
                ORDER BY period
            """,
        },
    },

    "selected_meter": {
        "keys": ["time", "meter_id"],
        "metrics": ["energy_kwh", "reading_count"],
        "energy_tolerance": 0.001,
        "queries": {
            "Raw": """
                SELECT
                    time_bucket(
                        INTERVAL '1 day', event_time,
                        'Africa/Kigali'
                    ) AS time,
                    meter_id,
                    SUM(energy_kwh) AS energy_kwh,
                    COUNT(*) AS reading_count
                FROM energy_readings
                WHERE meter_id = 1000000000
                  AND event_time >=
                    TIMESTAMPTZ '2026-09-22 00:00:00 Africa/Kigali'
                  AND event_time <
                    TIMESTAMPTZ '2026-09-29 00:00:00 Africa/Kigali'
                GROUP BY 1, 2
                ORDER BY 1
            """,
            "Aggregate": """
                SELECT
                    bucket AS time,
                    meter_id,
                    SUM(sum_energy_kwh) AS energy_kwh,
                    SUM(reading_count) AS reading_count
                FROM cagg_day_meter
                WHERE meter_id = 1000000000
                  AND bucket >=
                    TIMESTAMPTZ '2026-09-22 00:00:00 Africa/Kigali'
                  AND bucket <
                    TIMESTAMPTZ '2026-09-29 00:00:00 Africa/Kigali'
                GROUP BY bucket, meter_id
                ORDER BY bucket
            """,
        },
    },

    "regional_completeness": {
        "keys": ["region"],
        "metrics": [
            "planned_readings",
            "reading_count",
            "completeness_pct",
        ],
        "energy_tolerance": 0.001,
        "queries": {
            "Raw": """
                WITH registered AS (
                    SELECT
                        meter_id,
                        COALESCE(region, 'Unknown') AS region
                    FROM meters
                ),
                regional AS (
                    SELECT
                        r.region,
                        COUNT(DISTINCT r.meter_id)
                            AS registered_meters,
                        COUNT(DISTINCT (e.meter_id, e.event_time))
                            FILTER (WHERE e.meter_id IS NOT NULL)
                            AS reading_count
                    FROM registered r
                    LEFT JOIN energy_readings e
                      ON e.meter_id = r.meter_id
                     AND e.event_time >=
                        TIMESTAMPTZ '2026-09-22 00:00:00 Africa/Kigali'
                     AND e.event_time <
                        TIMESTAMPTZ '2026-09-29 00:00:00 Africa/Kigali'
                    GROUP BY r.region
                )
                SELECT
                    region,
                    registered_meters * 7 * 96 AS planned_readings,
                    reading_count,
                    100.0 * reading_count /
                        NULLIF(registered_meters * 7 * 96, 0)
                        AS completeness_pct
                FROM regional
                ORDER BY region
            """,
            "Aggregate": """
                WITH registered AS (
                    SELECT
                        COALESCE(region, 'Unknown') AS region,
                        COUNT(*) AS registered_meters
                    FROM meters
                    GROUP BY COALESCE(region, 'Unknown')
                ),
                observed AS (
                    SELECT
                        COALESCE(region, 'Unknown') AS region,
                        SUM(reading_count) AS reading_count
                    FROM cagg_15m_region
                    WHERE bucket >=
                        TIMESTAMPTZ '2026-09-22 00:00:00 Africa/Kigali'
                      AND bucket <
                        TIMESTAMPTZ '2026-09-29 00:00:00 Africa/Kigali'
                    GROUP BY COALESCE(region, 'Unknown')
                )
                SELECT
                    r.region,
                    r.registered_meters * 7 * 96 AS planned_readings,
                    COALESCE(o.reading_count, 0) AS reading_count,
                    100.0 * COALESCE(o.reading_count, 0) /
                        NULLIF(r.registered_meters * 7 * 96, 0)
                        AS completeness_pct
                FROM registered r
                LEFT JOIN observed o ON o.region = r.region
                ORDER BY r.region
            """,
        },
    },
}


def norm_key(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value).strip()


def as_float(value):
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def run_query(conn, sql):
    start = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    elapsed_ms = (time.perf_counter() - start) * 1000
    return rows, elapsed_ms


def save_csv(path, rows):
    rows = list(rows)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    columns = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                k: (
                    v.isoformat() if isinstance(v, (datetime, date))
                    else v
                )
                for k, v in row.items()
            })


def key_map(rows, keys):
    result = {}
    duplicates = []
    for row in rows:
        key = tuple(norm_key(row[k]) for k in keys)
        if key in result:
            duplicates.append(key)
        else:
            result[key] = row
    return result, duplicates


def compare_panel(panel_name, spec, raw, agg):
    keys = spec["keys"]
    metrics = spec["metrics"]

    left, dup_left = key_map(raw, keys)
    right, dup_right = key_map(agg, keys)

    left_keys = set(left)
    right_keys = set(right)
    matched_keys = left_keys & right_keys

    raw_only = left_keys - right_keys
    agg_only = right_keys - left_keys

    details = []
    panel_pass = (
        not raw_only and not agg_only
        and not dup_left and not dup_right
    )

    for metric in metrics:
        diffs = []
        compared = 0
        exact = True

        for key in matched_keys:
            a = as_float(left[key].get(metric))
            b = as_float(right[key].get(metric))

            if a is None or b is None:
                exact = False
                continue

            diff = abs(a - b)
            diffs.append(diff)
            compared += 1

            if metric == "energy_kwh":
                tolerance = 0.001
            elif metric == "completeness_pct":
                tolerance = 0.000001
            else:
                tolerance = 0.0

            if diff > tolerance:
                exact = False

        max_diff = max(diffs) if diffs else None
        if not exact:
            panel_pass = False

        details.append({
            "panel": panel_name,
            "metric": metric,
            "compared_rows": compared,
            "raw_only_keys": len(raw_only),
            "aggregate_only_keys": len(agg_only),
            "raw_duplicate_keys": len(dup_left),
            "aggregate_duplicate_keys": len(dup_right),
            "max_absolute_difference": max_diff,
            "tolerance": (
                0.001 if metric == "energy_kwh"
                else 0.000001 if metric == "completeness_pct"
                else 0.0
            ),
            "result": "PASS" if exact else "FAIL",
        })

    summary = {
        "panel": panel_name,
        "raw_rows": len(raw),
        "aggregate_rows": len(agg),
        "matched_rows": len(matched_keys),
        "raw_only_keys": len(raw_only),
        "aggregate_only_keys": len(agg_only),
        "raw_duplicate_keys": len(dup_left),
        "aggregate_duplicate_keys": len(dup_right),
        "result": "PASS" if panel_pass else "FAIL",
    }
    return summary, details


def percentile_summary(values):
    return {
        "median_ms": round(statistics.median(values), 3),
        "min_ms": round(min(values), 3),
        "max_ms": round(max(values), 3),
    }


def get_storage(conn):
    sql = """
        SELECT
            c.view_name,
            c.materialization_hypertable_name,
            s.table_bytes,
            s.index_bytes,
            s.toast_bytes,
            s.total_bytes,
            ROUND((s.total_bytes / 1048576.0)::numeric, 3)
                AS total_mb
        FROM timescaledb_information.continuous_aggregates c
        CROSS JOIN LATERAL hypertable_detailed_size(
            format(
                '%I.%I',
                c.materialization_hypertable_schema,
                c.materialization_hypertable_name
            )::regclass
        ) s
        WHERE c.view_name IN (
            'cagg_15m_region',
            'cagg_hour_region',
            'cagg_day_meter'
        )
        ORDER BY c.view_name
    """
    rows, _ = run_query(conn, sql)
    save_csv(OUT / "aggregate_storage.csv", rows)
    return rows


def refresh_aggregates(conn):
    jobs = [
        ("cagg_15m_region", START, END),
        ("cagg_hour_region", START, END),
        ("cagg_day_meter", START, END),
    ]
    results = []

    for name, start_ts, end_ts in jobs:
        start = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute(
                """
                CALL refresh_continuous_aggregate(
                    %s, %s::timestamptz, %s::timestamptz
                )
                """,
                (name, start_ts, end_ts),
            )
        conn.commit()
        elapsed = (time.perf_counter() - start) * 1000
        results.append({
            "view_name": name,
            "start": start_ts,
            "end_exclusive": end_ts,
            "refresh_ms": round(elapsed, 3),
        })

    save_csv(OUT / "refresh_costs.csv", results)
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Manually refresh the three CAGGs for the test period first.",
    )
    parser.add_argument(
        "--allow-failed",
        action="store_true",
        help="Run timing tests even if validation fails.",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=10,
        help="Measured runs per mode (default 10).",
    )
    args = parser.parse_args()
    database_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://eric:kigali@10.32.87.163:5432/smart_energy_grid"
    )
    if not database_url:
        sys.exit(
            "ERROR: Set DATABASE_URL before running. "
            "Example: postgresql://eric:kigali@10.32.87.163:5432/smart_energy_grid"
        )

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT version() AS version")
            print("Connected to:", cur.fetchone()["version"])

        if args.refresh:
            print("\nRefreshing aggregates for 22–28 September...")
            for row in refresh_aggregates(conn):
                print(row)

        print("\nExporting data and validating five panels...")
        raw_results = {}
        agg_results = {}
        summaries = []
        details = []

        for name, spec in PANELS.items():
            raw, _ = run_query(conn, spec["queries"]["Raw"])
            agg, _ = run_query(conn, spec["queries"]["Aggregate"])

            raw_results[name] = raw
            agg_results[name] = agg

            save_csv(OUT / f"raw_{name}.csv", raw)
            save_csv(OUT / f"aggregate_{name}.csv", agg)

            summary, detail = compare_panel(name, spec, raw, agg)
            summaries.append(summary)
            details.extend(detail)
            print(
                f"{name}: {summary['result']} | "
                f"Raw={summary['raw_rows']} | "
                f"Aggregate={summary['aggregate_rows']} | "
                f"Matched={summary['matched_rows']}"
            )

        save_csv(OUT / "validation_summary.csv", summaries)
        save_csv(OUT / "validation_details.csv", details)

        failed = [s for s in summaries if s["result"] != "PASS"]
        if failed and not args.allow_failed:
            print(
                "\nVALIDATION FAILED. Timing has been skipped to comply "
                "with Section 7.3. Investigate the CSV reports first."
            )
            try:
                storage = get_storage(conn)
                print(f"Storage information exported: {len(storage)} views")
            except Exception as exc:
                print("Could not collect storage information:", exc)
            print("Results folder:", OUT.resolve())
            sys.exit(2)

        print("\nCollecting continuous-aggregate storage...")
        try:
            storage = get_storage(conn)
            for row in storage:
                print(row)
        except Exception as exc:
            print("WARNING: storage query failed:", exc)

        print(
            "\nTiming database queries. This measures query/fetch time "
            "and sequential five-panel batch time, not browser rendering."
        )
        timing_rows = []

        for mode in ("Raw", "Aggregate"):
            print(f"\nMode: {mode}")

            # One warm-up refresh of all five panel queries.
            for name, spec in PANELS.items():
                run_query(conn, spec["queries"][mode])
            print("Warm-up complete (excluded from statistics).")

            for run_number in range(1, args.runs + 1):
                batch_start = time.perf_counter()
                run_rows = []

                for name, spec in PANELS.items():
                    _, query_ms = run_query(conn, spec["queries"][mode])
                    run_rows.append({
                        "mode": mode,
                        "run": run_number,
                        "panel": name,
                        "query_ms": round(query_ms, 3),
                    })

                batch_ms = (time.perf_counter() - batch_start) * 1000

                for row in run_rows:
                    row["dashboard_batch_ms"] = round(batch_ms, 3)
                    timing_rows.append(row)

                print(
                    f"Run {run_number}/{args.runs}: "
                    f"five-query batch={batch_ms:.1f} ms"
                )

        save_csv(OUT / "timing_runs.csv", timing_rows)

        panel_groups = {}
        dashboard_groups = {}

        for row in timing_rows:
            panel_groups.setdefault(
                (row["mode"], row["panel"]), []
            ).append(row["query_ms"])

            dashboard_groups.setdefault(
                row["mode"], []
            ).append(row["dashboard_batch_ms"])

        timing_summary = []

        for (mode, panel), values in sorted(panel_groups.items()):
            timing_summary.append({
                "mode": mode,
                "measurement": "database query/fetch",
                "panel": panel,
                **percentile_summary(values),
            })

        for mode, values in sorted(dashboard_groups.items()):
            # Each batch time appears once per panel; de-duplicate by run.
            per_run = {}
            for row in timing_rows:
                if row["mode"] == mode:
                    per_run[row["run"]] = row["dashboard_batch_ms"]

            timing_summary.append({
                "mode": mode,
                "measurement": "five-query database batch",
                "panel": "ALL",
                **percentile_summary(list(per_run.values())),
            })

        save_csv(OUT / "timing_summary.csv", timing_summary)

        print("\nTiming summary:")
        for row in timing_summary:
            print(row)

        print("\nFinished. Results saved to:", OUT.resolve())
        print("Review validation_summary.csv before reporting any PASS.")
        print(
            "IMPORTANT: database timings are not browser render timings. "
            "Measure Grafana refresh-to-last-panel-render separately."
        )


if __name__ == "__main__":
    main()
