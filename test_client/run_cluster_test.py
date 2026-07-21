#!/usr/bin/env python3
"""Start two LQTT broker nodes and test clustering."""

import subprocess
import time
import os
import signal
import sys

WORKDIR = "/home/shatrugna/shatrugna_data/projects/lqtt"

# Start node1
print("Starting node1 on port 18833...")
node1 = subprocess.Popen(
    ["elixir", "--sname", "lqtt1", "--cookie", "lqtt", "-S", "mix", "run", "--no-halt"],
    cwd=WORKDIR,
    env={**os.environ, "PORT": "18833"},
    stdout=open("/tmp/lqtt_node1.log", "w"),
    stderr=subprocess.STDOUT,
    start_new_session=True,
)
print(f"Node1 PID: {node1.pid}")
time.sleep(4)

# Check if node1 started
with open("/tmp/lqtt_node1.log") as f:
    print(f.read())

# Start node2
node2 = subprocess.Popen(
    ["elixir", "--sname", "lqtt2", "--cookie", "lqtt", "-S", "mix", "run", "--no-halt"],
    cwd="/home/shatrugna/shatrugna_data/projects/lqtt",
    env={**os.environ, "PORT": "18834"},
    stdout=open("/tmp/lqtt_node2.log", "w"),
    stderr=subprocess.STDOUT,
    start_new_session=True,
)
print(f"Node2 PID: {node2.pid}")
time.sleep(4)
with open("/tmp/lqtt_node2.log") as f:
    print(f.read())