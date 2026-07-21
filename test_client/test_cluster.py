#!/usr/bin/env python3
"""Test LQTT broker clustering with two nodes and two Python clients."""

import paho.mqtt.client as mqtt
import time
import sys
import threading

NODE1_PORT = 18833
NODE2_PORT = 18834

received = []

def on_message(client, userdata, msg):
    received.append((msg.topic, msg.payload.decode(), msg.qos))
    print(f"  [sub] received: topic={msg.topic} payload={msg.payload.decode()} qos={msg.qos}")

def on_subscribe(client, userdata, mid, reason_codes, properties):
    print(f"  [sub] subscribed (mid={mid}, reason_codes={reason_codes})")

def on_connect(client, userdata, flags, reason_code, properties):
    print(f"  [client] connected (rc={reason_code})")

# Subscribe on node1
sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "test-sub", protocol=mqtt.MQTTv5)
sub.on_connect = on_connect
sub.on_subscribe = on_subscribe
sub.on_message = on_message
sub.connect("127.0.0.1", NODE1_PORT, 60)
sub.subscribe("sensor/+/temp", qos=0)
sub.loop_start()

# Publish on node2
pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "test-pub", protocol=mqtt.MQTTv5)
pub.connect("127.0.0.1", NODE2_PORT, 60)
pub.loop_start()

time.sleep(0.5)

pub.publish("sensor/kitchen/temp", "25.5", qos=0)
time.sleep(0.5)

if received:
    print(f"SUCCESS: Received #{len(received)} messages across cluster")
    for t, p, q in received:
        print(f"  topic={t} payload={p} qos={q}")
else:
    print("FAIL: No message received")
    sys.exit(1)
