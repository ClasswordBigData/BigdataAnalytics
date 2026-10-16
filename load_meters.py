import os
import psycopg
from smart_meter_simulator import meter_metadata

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://eric:kigali@10.32.87.163:5432/smart_energy_grid"
)


def main():
    metadata = meter_metadata()

    if len(metadata) != 4000:
        raise ValueError(
            f"Expected 4000 meter records; received {len(metadata)}"
        )

    rows = [
        (
            int(m["meter_id"]),
            m["region"],
            m["customer_type"],
            float(m["base_power_kw"]),
            float(m["power_factor"])
        )
        for m in metadata
    ]

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS meters (
            meter_id BIGINT PRIMARY KEY,
            region TEXT,
            customer_type TEXT,
            base_power_kw DOUBLE PRECISION,
            power_factor DOUBLE PRECISION
        );
    """

    insert_sql = """
        INSERT INTO meters (
            meter_id,
            region,
            customer_type,
            base_power_kw,
            power_factor
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (meter_id) DO UPDATE SET
            region = EXCLUDED.region,
            customer_type = EXCLUDED.customer_type,
            base_power_kw = EXCLUDED.base_power_kw,
            power_factor = EXCLUDED.power_factor;
    """

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            cur.executemany(insert_sql, rows)

            cur.execute("SELECT COUNT(*) FROM meters;")
            count = cur.fetchone()[0]

            cur.execute("""
                SELECT meter_id, region, customer_type,
                       base_power_kw, power_factor
                FROM meters
                ORDER BY meter_id
                LIMIT 5;
            """)
            samples = cur.fetchall()

            print("Simulator records returned:", len(metadata))
            print("Rows in meters:", count)
            print("\nFirst five meter records:")
            for row in samples:
                print(row)

            if count != 4000:
                raise RuntimeError(
                    f"Expected 4000 rows; found {count}"
                )

    print("\nSUCCESS: Metadata import and verification completed.")


if __name__ == "__main__":
    main()