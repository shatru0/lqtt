#!/usr/bin/env python3
"""Test LQTT broker clustering with three nodes."""
import paho.mqtt.client as mqtt
import time
import sys

NODE1_PORT = 18833
NODE2_PORT = 18834
NODE3_PORT = 18835

received = []
errors = []

def on_message(client, userdata, msg):
    received.append((msg.topic, msg.payload.decode(), msg.qos))
    print(f"  [sub] received: topic={msg.topic} payload={msg.payload.decode()} qos={msg.qos}")

def on_subscribe(client, userdata, mid, reason_codes, properties):
    print(f"  [sub] subscribed (mid={mid}, reason_codes={reason_codes})")

def on_connect(client, userdata, flags, reason_code, properties):
    print(f"  [client] connected (rc={reason_code})")

# Subscribe on node1
print("=== Subscribe on node1, publish on node2 ===")
sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "test-sub", protocol=mqtt.MQTTv5)
sub.on_connect = on_connect
sub.on_subscribe = on_subscribe
sub.on_message = on_message
sub.connect("127.0.0.1", NODE1_PORT, 60)
sub.subscribe("sensor/+/temp", qos=0)
sub.loop_start()

pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "test-pub", protocol=mqtt.MQTTv5)
pub.connect("127.0.0.1", NODE2_PORT, 60)
pub.loop_start()

time.sleep(0.5)
pub.publish("sensor/kitchen/temp", "25.5", qos=0)
time.sleep(0.5)

if not received:
    print("FAIL: node1->node2 test failed")
    sys.exit(1)
print(f"PASS: received {len(received)} messages (node1<-node2)")

sub.loop_stop()
pub.loop_stop()

# Subscribe on node3, publish on node1
print("\n=== Subscribe on node3, publish on node1 ===")
received.clear()

sub2 = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "test-sub3", protocol=mqtt.MQTTv5)
sub2.on_connect = on_connect
sub2.on_subscribe = on_subscribe
sub2.on_message = on_message
sub2.connect("127.0.0.1", NODE3_PORT, 60)
sub2.subscribe("+/kitchen/temp", qos=0)
sub2.loop_start()

time.sleep(0.5)

pub2 = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "test-pub1", protocol=mqtt.MQTTv5)
pub2.connect("127.0.0.1", NODE1_PORT, 60)
pub2.loop_start()

pub2.publish("sensor/kitchen/temp", "30.0", qos=0)
time.sleep(0.5)

if not received:
    print("FAIL: node3->node1 test failed")
    sys.exit(1)
print(f"PASS: received {len(received)} messages (node3<-node1)")

sub2.loop_stop()
pub2.loop_stop()
received.clear()

# Subscribe on node2, publish on node3
print("\n=== Subscribe on node2, publish on node3 ===")
sub3 = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "test-sub2", protocol=mqtt.MQTTv5)
sub3.on_connect = on_connect
sub3.on_subscribe = on_subscribe
sub3.on_message = on_message
sub3.connect("127.0.0.1", NODE2_PORT, 60)
sub3.subscribe("sensor/#", qos=0)
sub3.loop_start()

time.sleep(0.5)

pub3 = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "test-pub3", protocol=mqtt.MQTTv5)
pub3.connect("127.0.0.1", NODE3_PORT, 60)
pub3.loop_start()

pub3.publish("sensor/bedroom/humidity", "60%", qos=0)
time.sleep(0.5)

if not received:
    print("FAIL: node2->node3 test failed")
    sys.exit(1)
print(f"PASS: received {len(received)} messages (node2<-node3)")

sub3.loop_stop()
pub3.loop_stop()

print("\n=== ALL TESTS PASSED ===")
