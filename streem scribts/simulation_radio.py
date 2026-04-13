import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
import random

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "telecom.tower.radio"

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
        "signal_dbm":        random.randint(-110, -90),
        "cell_load_pct":     random.randint(30, 60),
        "handover_rate":     round(random.uniform(0.0, 2.0), 2),
        "drop_call_rate":    round(random.uniform(0.0, 1.0), 2),
        "failure_mode":      False,
        "failure_countdown": 0
    }

def get_status(signal, drop_call_rate):
    if signal < -118 or drop_call_rate > 4:
        return "critical"
    elif signal < -112 or drop_call_rate > 2.5:
        return "degraded"
    else:
        return "online"

print("📡 Radio Metrics Producer Started (10 Towers)...")

while True:
    event_time = datetime.now(timezone.utc).isoformat()
    for tower in TOWERS:
        tid   = tower["id"]
        state = tower_states[tid]

        if not state["failure_mode"] and random.random() < 0.03:
            state["failure_mode"]      = True
            state["failure_countdown"] = random.randint(3, 8)

        if state["failure_mode"]:
            state["signal_dbm"]     = max(-120, state["signal_dbm"]     - random.randint(2, 6))
            state["cell_load_pct"]  = min(100,  state["cell_load_pct"]  + random.randint(5, 15))
            state["handover_rate"]  = round(    state["handover_rate"]  + random.uniform(0.5, 1.5), 2)
            state["drop_call_rate"] = round(    state["drop_call_rate"] + random.uniform(0.3, 1.0), 2)
            state["failure_countdown"] -= 1
            if state["failure_countdown"] <= 0:
                state["failure_mode"]   = False
                state["signal_dbm"]     = random.randint(-110, -90)
                state["cell_load_pct"]  = random.randint(30, 60)
                state["handover_rate"]  = round(random.uniform(0.0, 2.0), 2)
                state["drop_call_rate"] = round(random.uniform(0.0, 1.0), 2)
        else:
            state["signal_dbm"]     = max(-120, min(-50, state["signal_dbm"]     + random.randint(-2, 2)))
            state["cell_load_pct"]  = max(0,    min(100, state["cell_load_pct"]  + random.randint(-5, 5)))
            state["handover_rate"]  = max(0,    round(   state["handover_rate"]  + random.uniform(-0.2, 0.2), 2))
            state["drop_call_rate"] = max(0,    round(   state["drop_call_rate"] + random.uniform(-0.1, 0.1), 2))

        status  = get_status(state["signal_dbm"], state["drop_call_rate"])
        message = {
            "tower_id":       tid,
            "region":         tower["region"],
            "event_time":     event_time,
            "signal_dbm":     state["signal_dbm"],
            "cell_load_pct":  state["cell_load_pct"],
            "handover_rate":  state["handover_rate"],
            "drop_call_rate": state["drop_call_rate"],
            "tower_status":   status
        }
        producer.send(topic=TOPIC, key=tid, value=message)
        print(f"[RADIO] {tid} | Signal: {state['signal_dbm']} dBm | Drop: {state['drop_call_rate']}% | Status: {status}")

    time.sleep(10)