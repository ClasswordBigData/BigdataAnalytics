import json
import os
import threading
from itertools import islice

import paho.mqtt.client as mqtt
from smart_meter_simulator import iter_readings

MQTT_HOST = os.getenv("MQTT_HOST", "10.32.87.163")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
TARGET = 2000

connected = threading.Event()
connection_error = None


def on_connect(client, userdata, flags, reason_code, properties):
    global connection_error

    if reason_code.is_failure:
        connection_error = str(reason_code)
        print(f"MQTT connection failed: {reason_code}")
    else:
        print(f"Connected to broker {MQTT_HOST}:{MQTT_PORT}")
        connected.set()


def json_default(value):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def main():
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="smart-energy-publisher-pilot",
        protocol=mqtt.MQTTv311,
        clean_session=True,
    )

    client.on_connect = on_connect

    username = os.getenv("MQTT_USERNAME")
    password = os.getenv("MQTT_PASSWORD")
    if username:
        client.username_pw_set(username, password)

    dictionaries_taken = 0
    broker_acked = 0

    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        client.loop_start()

        if not connected.wait(timeout=30):
            raise TimeoutError(
                f"MQTT connection failed or timed out: {connection_error}"
            )

        for reading in islice(iter_readings(), TARGET):
            dictionaries_taken += 1

            meter_id = int(reading["meter_id"])
            topic = f"energy/meters/{meter_id}"
            payload = json.dumps(reading, default=json_default)

            info = client.publish(
                topic=topic,
                payload=payload,
                qos=1,
                retain=False,
            )

            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(
                    f"Publish failed for reading {dictionaries_taken}: "
                    f"return code {info.rc}"
                )

            info.wait_for_publish(timeout=30)

            if not info.is_published():
                raise TimeoutError(
                    f"No completed QoS 1 acknowledgement for reading "
                    f"{dictionaries_taken}"
                )

            broker_acked += 1

            if broker_acked % 100 == 0:
                print(
                    f"Dictionaries taken={dictionaries_taken}, "
                    f"broker acknowledgements={broker_acked}"
                )

        print("\nPUBLISHER FINAL COUNTS")
        print("Dictionaries taken:", dictionaries_taken)
        print("Broker acknowledgements completed:", broker_acked)

        if dictionaries_taken != TARGET or broker_acked != TARGET:
            print("WARNING: Publisher did not reach the 2,000 target.")

    finally:
        client.disconnect()
        client.loop_stop()


if __name__ == "__main__":
    main()