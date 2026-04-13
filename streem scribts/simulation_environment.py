import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
import random

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "telecom.tower.environment"

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
        "temperature_c":     random.randint(25, 35),
        "humidity_pct":      random.randint(30, 60),
        "wind_speed_kmh":    random.randint(5, 20),
        "failure_mode":      False,
        "failure_countdown": 0
    }

def get_status(temperature, humidity):
    if temperature > 50 or humidity > 90:
        return "critical"
    elif temperature > 42 or humidity > 80:
        return "degraded"
    else:
        return "online"

print("🌡️ Environment Metrics Producer Started (10 Towers)...")

while True:
    event_time = datetime.now(timezone.utc).isoformat()
    for tower in TOWERS:
        tid   = tower["id"]
        state = tower_states[tid]

        if not state["failure_mode"] and random.random() < 0.03:
            state["failure_mode"]      = True
            state["failure_countdown"] = random.randint(3, 8)

        if state["failure_mode"]:
            state["temperature_c"]  = round(min(60,  state["temperature_c"]  + random.uniform(2, 5)), 1)
            state["humidity_pct"]   = round(min(100, state["humidity_pct"]   + random.uniform(3, 8)), 1)
            state["wind_speed_kmh"] = round(max(0,   state["wind_speed_kmh"] + random.uniform(2, 6)), 1)
            state["failure_countdown"] -= 1
            if state["failure_countdown"] <= 0:
                state["failure_mode"]    = False
                state["temperature_c"]   = random.randint(25, 35)
                state["humidity_pct"]    = random.randint(30, 60)
                state["wind_speed_kmh"]  = random.randint(5, 20)
        else:
            state["temperature_c"]  = round(state["temperature_c"]  + random.uniform(-0.5, 0.5), 1)
            state["humidity_pct"]   = round(max(0, min(100, state["humidity_pct"]   + random.uniform(-2, 2))), 1)
            state["wind_speed_kmh"] = round(max(0,          state["wind_speed_kmh"] + random.uniform(-1, 1)), 1)

        status  = get_status(state["temperature_c"], state["humidity_pct"])
        message = {
            "tower_id":       tid,
            "region":         tower["region"],
            "event_time":     event_time,
            "temperature_c":  state["temperature_c"],
            "humidity_pct":   state["humidity_pct"],
            "wind_speed_kmh": state["wind_speed_kmh"],
            "tower_status":   status
        }
        producer.send(topic=TOPIC, key=tid, value=message)
        print(f"[ENV] {tid} | Temp: {state['temperature_c']}°C | Humidity: {state['humidity_pct']}% | Status: {status}")

    time.sleep(10)