import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
import random

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "telecom.tower.system"

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
        "cpu_pct":           random.randint(20, 40),
        "memory_pct":        random.randint(30, 50),
        "power_kw":          round(random.uniform(2.5, 3.5), 2),
        "battery_level_pct": random.randint(70, 100),
        "failure_mode":      False,
        "failure_countdown": 0
    }

def get_status(cpu, memory, battery):
    if cpu > 90 or memory > 90 or battery < 15:
        return "critical"
    elif cpu > 75 or memory > 75 or battery < 30:
        return "degraded"
    else:
        return "online"

print("🖥️ System Metrics Producer Started (10 Towers)...")

while True:
    event_time = datetime.now(timezone.utc).isoformat()
    for tower in TOWERS:
        tid   = tower["id"]
        state = tower_states[tid]

        if not state["failure_mode"] and random.random() < 0.03:
            state["failure_mode"]      = True
            state["failure_countdown"] = random.randint(3, 8)

        if state["failure_mode"]:
            state["cpu_pct"]           = min(100, state["cpu_pct"]           + random.randint(5, 15))
            state["memory_pct"]        = min(100, state["memory_pct"]        + random.randint(3, 10))
            state["battery_level_pct"] = max(0,   state["battery_level_pct"] - random.uniform(2, 5))
            state["power_kw"]          = round(min(6.0, state["power_kw"]    + random.uniform(0.2, 0.5)), 2)
            state["failure_countdown"] -= 1
            if state["failure_countdown"] <= 0:
                state["failure_mode"]        = False
                state["cpu_pct"]             = random.randint(20, 40)
                state["memory_pct"]          = random.randint(30, 50)
                state["power_kw"]            = round(random.uniform(2.5, 3.5), 2)
                state["battery_level_pct"]   = random.randint(70, 90)
        else:
            state["cpu_pct"]           = min(100, max(0, state["cpu_pct"]           + random.randint(-5, 5)))
            state["memory_pct"]        = min(100, max(0, state["memory_pct"]        + random.randint(-3, 3)))
            state["power_kw"]          = round(max(0,    state["power_kw"]          + random.uniform(-0.2, 0.2)), 2)
            state["battery_level_pct"] = min(100, max(0, state["battery_level_pct"] - random.uniform(0, 0.5)))

        status  = get_status(state["cpu_pct"], state["memory_pct"], state["battery_level_pct"])
        message = {
            "tower_id":           tid,
            "region":             tower["region"],
            "event_time":         event_time,
            "cpu_pct":            state["cpu_pct"],
            "memory_pct":         state["memory_pct"],
            "power_kw":           state["power_kw"],
            "battery_level_pct":  round(state["battery_level_pct"], 2),
            "tower_status":       status
        }
        producer.send(topic=TOPIC, key=tid, value=message)
        print(f"[SYSTEM] {tid} | CPU: {state['cpu_pct']}% | MEM: {state['memory_pct']}% | Status: {status}")

    time.sleep(10)