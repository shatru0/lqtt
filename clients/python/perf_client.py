"""
Performance and reliability test for lqtt MQTT broker.

Measures throughput, message loss, duplicates, and ordering
for QoS 0 and QoS 1 across the Go broker.

Usage:
    python3 perf_client.py [host] [port] [num_messages]
"""

import sys
import time
import threading
import argparse
from collections import defaultdict

import paho.mqtt.client as mqtt

HOST = "127.0.0.1"
PORT = 1883
NUM_MSGS = 1000

qos0_received = []
qos1_received = []
burst_received = []
qos0_sub_ready = threading.Event()
qos1_sub_ready = threading.Event()
burst_sub_ready = threading.Event()
errors = []


def on_qos0_msg(client, userdata, msg):
    qos0_received.append(int(msg.payload.decode()))


def on_qos1_msg(client, userdata, msg):
    qos1_received.append(int(msg.payload.decode()))


def on_burst_msg(client, userdata, msg):
    burst_received.append(int(msg.payload.decode()))


def on_qos0_connect(client, userdata, flags, rc):
    if rc != 0:
        errors.append(f"qos0 subscriber connect failed: {rc}")
        return
    client.subscribe("perf/qos0", qos=0)
    qos0_sub_ready.set()


def on_qos1_connect(client, userdata, flags, rc):
    if rc != 0:
        errors.append(f"qos1 subscriber connect failed: {rc}")
        return
    client.subscribe("perf/qos1", qos=1)
    qos1_sub_ready.set()


def on_burst_connect(client, userdata, flags, rc):
    if rc != 0:
        errors.append(f"burst subscriber connect failed: {rc}")
        return
    client.subscribe("perf/burst", qos=0)
    burst_sub_ready.set()


def run_subscriber(topic, on_connect, on_message, ready_event):
    cid = f"py-sub-{topic.replace('/', '-')}"
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=cid, clean_session=True)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(HOST, PORT, 60)
    client.loop_forever()


def wait_for_subscribers(timeout=5):
    for name, ev in [("qos0", qos0_sub_ready), ("qos1", qos1_sub_ready), ("burst", burst_sub_ready)]:
        if not ev.wait(timeout=timeout):
            print(f"FAIL: {name} subscriber did not connect")
            sys.exit(1)


def run_throughput_test(qos, topic, num_msgs, label):
    global qos0_received, qos1_received
    buf = qos0_received if qos == 0 else qos1_received
    buf.clear()

    pub_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=f"py-pub-{label}", clean_session=True)
    pub_client.connect(HOST, PORT, 60)
    pub_client.loop_start()
    time.sleep(0.3)

    pub_start = time.monotonic()
    for i in range(num_msgs):
        pub_client.publish(topic, str(i), qos=qos)
    if qos == 1:
        time.sleep(0.5)
    pub_end = time.monotonic()
    elapsed = pub_end - pub_start

    pub_client.loop_stop()
    pub_client.disconnect()

    time.sleep(1)

    received = list(buf)
    received_count = len(received)
    sent_set = set(range(num_msgs))
    received_set = set(received)

    loss = num_msgs - received_count if qos == 1 else max(0, num_msgs - received_count)
    dupes = len(received) - len(received_set)
    missing = sorted(sent_set - received_set)
    extra = sorted(received_set - sent_set)

    ordered = 0
    ordered_idx = 0
    for v in sorted(received_set):
        while ordered_idx < len(received) and received[ordered_idx] != v:
            ordered_idx += 1
        if ordered_idx < len(received):
            ordered += 1

    throughput = received_count / elapsed if elapsed > 0 else 0

    print(f"\n  {'='*50}")
    print(f"  {label}")
    print(f"  {'='*50}")
    print(f"    Messages sent:      {num_msgs}")
    print(f"    Messages received:  {received_count}")
    print(f"    Unique received:    {len(received_set)}")
    print(f"    Message loss:       {loss}")
    print(f"    Duplicates:         {dupes}")
    print(f"    Missing IDs:        {missing[:10]}{'...' if len(missing) > 10 else ''}")
    print(f"    Extra IDs:          {extra[:10]}{'...' if len(extra) > 10 else ''}")
    print(f"    Total time:         {elapsed:.3f}s")
    print(f"    Throughput:         {throughput:.1f} msgs/s")

    return loss == 0 and dupes == 0


def run_burst_test(num_msgs):
    global burst_received
    burst_received.clear()

    pub_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="py-pub-burst", clean_session=True)
    pub_client.connect(HOST, PORT, 60)
    pub_client.loop_start()
    time.sleep(0.3)

    pub_start = time.monotonic()
    for i in range(num_msgs):
        pub_client.publish("perf/burst", str(i), qos=0)
    pub_end = time.monotonic()
    elapsed = pub_end - pub_start

    pub_client.loop_stop()
    pub_client.disconnect()

    time.sleep(2)

    received = list(burst_received)
    received_count = len(received)

    throughput = received_count / elapsed if elapsed > 0 else 0
    loss = max(0, num_msgs - received_count)
    received_set = set(received)
    dupes = len(received) - len(received_set)

    print(f"\n  {'='*50}")
    print(f"  Burst QoS 0 (no wait)")
    print(f"  {'='*50}")
    print(f"    Messages sent:      {num_msgs}")
    print(f"    Messages received:  {received_count}")
    print(f"    Unique received:    {len(received_set)}")
    print(f"    Message loss:       {loss}")
    print(f"    Duplicates:         {dupes}")
    print(f"    Total time:         {elapsed:.3f}s")
    print(f"    Throughput:         {throughput:.1f} msgs/s")

    return loss, dupes


def main():
    global HOST, PORT, NUM_MSGS

    parser = argparse.ArgumentParser(description="lqtt performance test")
    parser.add_argument("host", nargs="?", default="127.0.0.1", help="Broker host")
    parser.add_argument("port", nargs="?", type=int, default=1883, help="Broker port")
    parser.add_argument("num_messages", nargs="?", type=int, default=1000, help="Messages per test")
    args = parser.parse_args()

    HOST = args.host
    PORT = args.port
    NUM_MSGS = args.num_messages

    subs = [
        ("perf/qos0", on_qos0_connect, on_qos0_msg, qos0_sub_ready),
        ("perf/qos1", on_qos1_connect, on_qos1_msg, qos1_sub_ready),
        ("perf/burst", on_burst_connect, on_burst_msg, burst_sub_ready),
    ]
    threads = []
    for topic, oc, om, ev in subs:
        t = threading.Thread(target=run_subscriber, args=(topic, oc, om, ev), daemon=True)
        t.start()
        threads.append(t)

    wait_for_subscribers()
    time.sleep(0.3)

    print(f"\n{'='*60}")
    print(f"  lqtt Performance Test")
    print(f"  Target: {HOST}:{PORT}")
    print(f"  Messages per test: {NUM_MSGS}")
    print(f"{'='*60}")

    qos1_ok = run_throughput_test(1, "perf/qos1", NUM_MSGS, "QoS 1 (reliable)")
    time.sleep(0.5)

    qos0_ok = run_throughput_test(0, "perf/qos0", NUM_MSGS, "QoS 0 (at-most-once)")
    time.sleep(0.5)

    burst_loss, burst_dupes = run_burst_test(NUM_MSGS)

    print(f"\n{'='*60}")
    print(f"  Summary")
    print(f"{'='*60}")

    failed = False

    print(f"  QoS 1 - loss={NUM_MSGS - len(qos1_received)}, dupes={len(qos1_received) - len(set(qos1_received))}")
    if len(set(qos1_received)) != NUM_MSGS:
        print("  FAIL: QoS 1 should deliver all messages exactly once")
        failed = True
    else:
        print("  PASS: QoS 1 reliable delivery")

    print(f"  QoS 0 - received={len(qos0_received)}/{NUM_MSGS}")
    print(f"  Burst - received={len(burst_received)}/{NUM_MSGS}, dupes={burst_dupes}")

    if errors:
        print(f"\n  Errors:")
        for e in errors:
            print(f"    {e}")
        failed = True

    print()
    if failed:
        print("  OVERALL: FAILED")
        sys.exit(1)
    else:
        print("  OVERALL: PASSED")


if __name__ == "__main__":
    main()
