
import json
import os
import threading
import time

import psycopg
import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "10.32.87.163")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = "energy/meters/#"
MQTT_CLIENT_ID = "smart-energy-subscriber-pilot"

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://eric:kigali@10.32.87.163:5432/smart_energy_grid"
)

TARGET = int(os.getenv("TARGET_MESSAGES", "2000"))
PROGRESS_INTERVAL = 100
IDLE_WARNING_SECONDS = 30

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

INSERT_SQL = """
    INSERT INTO energy_live (
        meter_id,
        event_time,
        received_time,
        power_kw,
        voltage_v,
        current_a,
        frequency_hz,
        energy_kwh
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

received_count = 0
committed_count = 0
error_count = 0

count_lock = threading.Lock()
subscribed = threading.Event()
finished = threading.Event()

last_message_time = time.monotonic()


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print(f"MQTT connection failed: {reason_code}")
        return

    print(f"Connected to MQTT broker {MQTT_HOST}:{MQTT_PORT}")

    result, _ = client.subscribe(MQTT_TOPIC, qos=1)

    if result != mqtt.MQTT_ERR_SUCCESS:
        print(f"Subscribe request failed: return code {result}")


def on_subscribe(client, userdata, mid, reason_code_list, properties):
    if any(code.is_failure for code in reason_code_list):
        print(f"Subscription failed: {reason_code_list}")
        return

    print(f"SUBSCRIBER_READY: {MQTT_TOPIC}, QoS 1")
    subscribed.set()


def on_message(client, conn, message):
    global received_count, committed_count, error_count
    global last_message_time

    last_message_time = time.monotonic()

    with count_lock:
        received_count += 1
        current_received = received_count

    try:
        reading = json.loads(message.payload.decode("utf-8"))

        if not isinstance(reading, dict):
            raise ValueError("MQTT payload must be a JSON object")

        missing = set(FIELDS) - set(reading)
        if missing:
            raise ValueError(f"Missing fields: {sorted(missing)}")

        row = tuple(reading[field] for field in FIELDS)

        # Insert the reading and commit before counting it as successful.
        with conn.cursor() as cur:
            cur.execute(INSERT_SQL, row)

        conn.commit()

        with count_lock:
            committed_count += 1
            current_committed = committed_count

        if current_committed % PROGRESS_INTERVAL == 0:
            print(
                f"Received={current_received}, "
                f"database_committed={current_committed}, "
                f"errors={error_count}"
            )

        if current_committed >= TARGET:
            print(f"\nTARGET REACHED: {current_committed} database commits.")
            finished.set()

    except Exception as exc:
        try:
            conn.rollback()
        except Exception as rollback_exc:
            print(f"Rollback error: {rollback_exc}")

        with count_lock:
            error_count += 1
            current_errors = error_count

        print(
            f"INSERT ERROR #{current_errors}: "
            f"message_id={message.mid}, "
            f"topic={message.topic}, "
            f"error={type(exc).__name__}: {exc}"
        )


def main():
    global last_message_time

    client = None
    conn = None
    start_time = time.monotonic()
    last_idle_warning = start_time

    try:
        # Create the DB connection before starting the MQTT callback loop.
        conn = psycopg.connect(DATABASE_URL)
        conn.autocommit = False

        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.energy_live')")
            table_exists = cur.fetchone()[0]

        if table_exists is None:
            raise RuntimeError(
                "public.energy_live does not exist. Create it first."
            )

        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=MQTT_CLIENT_ID,
            protocol=mqtt.MQTTv311,
            clean_session=True,
            # Automatic acknowledgement is enabled by default.
        )

        client.user_data_set(conn)
        client.on_connect = on_connect
        client.on_subscribe = on_subscribe
        client.on_message = on_message

        username = os.getenv("MQTT_USERNAME")
        password = os.getenv("MQTT_PASSWORD")

        if username:
            client.username_pw_set(username, password)

        print(f"Target successful database commits: {TARGET}")
        print("Connecting to MQTT broker...")

        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        client.loop_start()

        if not subscribed.wait(timeout=30):
            raise TimeoutError(
                "No successful MQTT subscription confirmation within 30 seconds"
            )

        print("Subscriber ready; waiting for pilot messages.")

        while not finished.wait(timeout=1):
            now = time.monotonic()

            if not client.is_connected():
                print("WARNING: MQTT client is disconnected.")

            if (
                now - last_message_time >= IDLE_WARNING_SECONDS
                and now - last_idle_warning >= IDLE_WARNING_SECONDS
            ):
                with count_lock:
                    current_received = received_count
                    current_committed = committed_count

                print(
                    f"IDLE WARNING: no messages received for "
                    f"{IDLE_WARNING_SECONDS} seconds; "
                    f"received={current_received}, "
                    f"committed={current_committed}. "
                    "Waiting for more messages."
                )
                last_idle_warning = now

    except KeyboardInterrupt:
        print("\nSubscriber stopped by user.")

    except Exception as exc:
        print(f"\nSUBSCRIBER ERROR: {type(exc).__name__}: {exc}")

    finally:
        if client is not None:
            if client.is_connected():
                client.disconnect()
            client.loop_stop()

        if conn is not None:
            conn.close()

        elapsed = time.monotonic() - start_time

        with count_lock:
            print("\n========== SUBSCRIBER FINAL REPORT ==========")
            print(f"Target commits:       {TARGET}")
            print(f"Messages received:    {received_count}")
            print(f"Database commits:     {committed_count}")
            print(f"Processing errors:     {error_count}")
            print(f"Elapsed seconds:      {elapsed:.2f}")

            if committed_count >= TARGET:
                print("RESULT: Target reached.")
            else:
                print("RESULT: Target not reached; inspect logs above.")


if __name__ == "__main__":
    main()
