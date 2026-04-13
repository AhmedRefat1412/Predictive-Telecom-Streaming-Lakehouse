import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
import random

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "telecom.tower.network"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5
)

TOWERS = [
    {"id": "TOWER_01", "region": "Cairo"},
    {"id": "TOWER_02", "region": "Cairo"},
    {"id": "TOWER_03", "region": "Cairo"},
    {"id": "TOWER_04", "region": "Giza"},
    {"id": "TOWER_05", "region": "Giza"},
    {"id": "TOWER_06", "region": "Alexandria"},
    {"id": "TOWER_07", "region": "Alexandria"},
    {"id": "TOWER_08", "region": "Mansoura"},
    {"id": "TOWER_09", "region": "Aswan"},
    {"id": "TOWER_10", "region": "Luxor"},
]

tower_states = {}
for tower in TOWERS:
    tower_states[tower["id"]] = {
        "latency_ms":        random.randint(80, 150),
        "throughput_mbps":   random.randint(20, 60),
        "packet_loss":       round(random.uniform(0, 0.05), 3),
        "active_users":      random.randint(50, 200),
        "failure_mode":      False,
        "failure_countdown": 0
    }

def get_status(latency, packet_loss):
    if latency > 350 or packet_loss > 0.12:
        return "critical"
    elif latency > 250 or packet_loss > 0.08:
        return "degraded"
    else:
        return "online"

print("🌐 Network Metrics Producer Started (10 Towers)...")

while True:
    event_time = datetime.now(timezone.utc).isoformat()
    for tower in TOWERS:
        tid   = tower["id"]
        state = tower_states[tid]

        if not state["failure_mode"] and random.random() < 0.03:
            state["failure_mode"]      = True
            state["failure_countdown"] = random.randint(3, 8)

        if state["failure_mode"]:
            state["latency_ms"]      = min(500, state["latency_ms"]      + random.randint(30, 80))
            state["throughput_mbps"] = max(1,   state["throughput_mbps"] - random.randint(5, 15))
            state["packet_loss"]     = round(min(0.5, state["packet_loss"] + random.uniform(0.02, 0.08)), 3)
            state["active_users"]    = max(0,   state["active_users"]    - random.randint(10, 30))
            state["failure_countdown"] -= 1
            if state["failure_countdown"] <= 0:
                state["failure_mode"]    = False
                state["latency_ms"]      = random.randint(80, 150)
                state["throughput_mbps"] = random.randint(20, 60)
                state["packet_loss"]     = round(random.uniform(0, 0.05), 3)
                state["active_users"]    = random.randint(50, 200)
        else:
            state["latency_ms"]      = max(10,  state["latency_ms"]      + random.randint(-5, 5))
            state["throughput_mbps"] = max(1,   state["throughput_mbps"] + random.randint(-5, 5))
            state["packet_loss"]     = max(0,   round(state["packet_loss"] + random.uniform(-0.01, 0.01), 3))
            state["active_users"]    = max(0,   state["active_users"]    + random.randint(-5, 5))

        status  = get_status(state["latency_ms"], state["packet_loss"])
        message = {
            "tower_id":        tid,
            "region":          tower["region"],
            "event_time":      event_time,
            "latency_ms":      state["latency_ms"],
            "throughput_mbps": state["throughput_mbps"],
            "packet_loss":     state["packet_loss"],
            "active_users":    state["active_users"],
            "tower_status":    status
        }
        producer.send(topic=TOPIC, key=tid, value=message)
        print(f"[NET] {tid} | Latency: {state['latency_ms']}ms | Loss: {state['packet_loss']} | Status: {status}")

    time.sleep(10)