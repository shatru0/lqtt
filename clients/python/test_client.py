"""
MQTT broker test client (Python)

Tests QoS 0 and QoS 1 publish/subscribe against the lqtt broker.
Usage:
    python test_client.py [host] [port]
"""

import sys
import time
import threading
import paho.mqtt.client as mqtt

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 1883

results = {}
lock = threading.Lock()
errors = []


def set_result(key, value):
    with lock:
        results[key] = value


def get_result(key):
    with lock:
        return results.get(key)


received_qos0 = []
received_qos1 = []
pub_qos0_ok = threading.Event()
pub_qos1_ok = threading.Event()
sub_qos0_ready = threading.Event()
sub_qos1_ready = threading.Event()


def on_sub_qos0_msg(client, userdata, msg):
    received_qos0.append((msg.topic, msg.payload.decode(), msg.qos))
    set_result("sub_qos0_last", (msg.topic, msg.payload.decode(), msg.qos))


def on_sub_qos1_msg(client, userdata, msg):
    received_qos1.append((msg.topic, msg.payload.decode(), msg.qos))
    set_result("sub_qos1_last", (msg.topic, msg.payload.decode(), msg.qos))


def on_sub_qos0_connect(client, userdata, flags, rc):
    if rc != 0:
        errors.append(f"sub_qos0 connect failed: {rc}")
        return
    client.subscribe("test/qos0", qos=0)
    sub_qos0_ready.set()


def on_sub_qos1_connect(client, userdata, flags, rc):
    if rc != 0:
        errors.append(f"sub_qos1 connect failed: {rc}")
        return
    client.subscribe("test/qos1", qos=1)
    sub_qos1_ready.set()


def run_subscriber_qos0():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="py-sub-qos0", clean_session=True)
    client.on_connect = on_sub_qos0_connect
    client.on_message = on_sub_qos0_msg
    client.connect(HOST, PORT, 60)
    client.loop_forever()


def run_subscriber_qos1():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="py-sub-qos1", clean_session=True)
    client.on_connect = on_sub_qos1_connect
    client.on_message = on_sub_qos1_msg
    client.connect(HOST, PORT, 60)
    client.loop_forever()


def main():
    t0 = threading.Thread(target=run_subscriber_qos0, daemon=True)
    t1 = threading.Thread(target=run_subscriber_qos1, daemon=True)
    t0.start()
    t1.start()

    if not sub_qos0_ready.wait(timeout=3):
        print("FAIL: subscriber qos0 did not connect")
        sys.exit(1)
    if not sub_qos1_ready.wait(timeout=3):
        print("FAIL: subscriber qos1 did not connect")
        sys.exit(1)

    time.sleep(0.3)

    pub_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="py-pub", clean_session=True)

    def on_pub_connect(client, userdata, flags, rc):
        if rc != 0:
            errors.append(f"publisher connect failed: {rc}")

    pub_client.on_connect = on_pub_connect
    pub_client.connect(HOST, PORT, 60)
    pub_client.loop_start()
    time.sleep(0.3)

    print(f"Connected to {HOST}:{PORT}")

    # --- QoS 0 ---
    info = pub_client.publish("test/qos0", "hello-qos0", qos=0)
    print(f"  Published to test/qos0 (QoS 0): mid={info.mid}")
    time.sleep(0.5)

    # --- QoS 1 ---
    info = pub_client.publish("test/qos1", "hello-qos1", qos=1)
    print(f"  Published to test/qos1 (QoS 1): mid={info.mid}")
    time.sleep(0.5)

    # allow extra time for delivery
    time.sleep(1)

    pub_client.loop_stop()
    pub_client.disconnect()

    # --- verify ---
    passed = 0
    failed = 0

    # QoS 0
    if len(received_qos0) == 1:
        topic, payload, qos = received_qos0[0]
        if topic == "test/qos0" and payload == "hello-qos0" and qos == 0:
            print("  PASS: QoS 0 message received correctly")
            passed += 1
        else:
            print(f"  FAIL: QoS 0 mismatch (topic={topic}, payload={payload}, qos={qos})")
            failed += 1
    else:
        print(f"  FAIL: QoS 0 expected 1 message, got {len(received_qos0)}")
        failed += 1

    # QoS 1
    if len(received_qos1) == 1:
        topic, payload, qos = received_qos1[0]
        if topic == "test/qos1" and payload == "hello-qos1" and qos == 1:
            print("  PASS: QoS 1 message received correctly")
            passed += 1
        else:
            print(f"  FAIL: QoS 1 mismatch (topic={topic}, payload={payload}, qos={qos})")
            failed += 1
    else:
        print(f"  FAIL: QoS 1 expected 1 message, got {len(received_qos1)}")
        failed += 1

    if errors:
        for e in errors:
            print(f"  ERROR: {e}")
            failed += 1

    print(f"\n{'='*40}")
    print(f"Results: {passed} passed, {failed} failed")
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
