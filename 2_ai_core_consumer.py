import os
import sys
import json
import time
from datetime import datetime, timezone, timedelta
import pika
import requests

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
QUEUE_NAME = 'disaster_telemetry'
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')

DATA_DIR = os.path.join(os.getcwd(), 'data')
os.makedirs(DATA_DIR, exist_ok=True)

DATA_FILE = os.path.join(DATA_DIR, 'latest_prediction.json')
ALL_DATA_FILE = os.path.join(DATA_DIR, 'all_threats.json')
STATE_FILE = os.path.join(DATA_DIR, 'ai_state.json')

print("=== [BOOT] JSON TELEMETRY CONSUMER ONLINE ===", flush=True)

def purge_expired_items(items):
    """Filters out any event in the past or with <= 1 hour left."""
    valid_items = []
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=1)

    for it in items:
        ts_str = it.get("time_of_event")
        if not ts_str:
            continue
        try:
            event_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if event_dt > cutoff:
                valid_items.append(it)
            else:
                print(f"🧹 [PURGE]: Removed '{it.get('primary_threat')}' (ETA {ts_str} <= 1h threshold)", flush=True)
        except Exception:
            valid_items.append(it)
    return valid_items

def generate_initial_demo_data():
    now = datetime.now(timezone.utc)
    return [
        {
            "primary_threat": "[DEMO] Forecasted M6.2 Triggered Aftershock",
            "location": "Near Coast of Honshu, Japan",
            "time_of_event": (now + timedelta(hours=26)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "severity": "Severe",
            "probability": "68%",
            "lat": 37.8500,
            "lon": 142.1000,
            "is_demo": True,
            "cascading_effects": [
                "Regional transit deceleration advisory",
                "Coastal surge warning"
            ]
        },
        {
            "primary_threat": "[DEMO] Projected Wildfire Spread Perimeter",
            "location": "Pilbara Region, Western Australia",
            "time_of_event": (now + timedelta(hours=38)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "severity": "Extreme",
            "probability": "74%",
            "lat": -22.3500,
            "lon": 118.9000,
            "is_demo": True,
            "cascading_effects": [
                "Power transmission line emergency shutdown",
                "PM2.5 particulate advisory"
            ]
        }
    ]

def load_or_init_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
                state["items"] = purge_expired_items(state.get("items", []))
                if state["items"]:
                    save_state(state)
                    return state
        except Exception:
            pass

    state = {
        "real_ai_count": 0,
        "items": generate_initial_demo_data()
    }
    save_state(state)
    return state

def save_state(state):
    # Enforce cleanup before saving to disk
    state["items"] = purge_expired_items(state.get("items", []))

    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

    if state["items"]:
        with open(DATA_FILE, 'w') as f:
            json.dump(state["items"][0], f, indent=2)
        with open(ALL_DATA_FILE, 'w') as f:
            json.dump(state["items"], f, indent=2)
    print(f"[✓] Synced data/*.json files ({len(state['items'])} active threats, real count: {state['real_ai_count']})", flush=True)

# Seed files on fresh container boot
current_state = load_or_init_state()

def forecast_future_disasters(telemetry_batch):
    if not GROQ_API_KEY:
        print("[-] Skipping Groq call: GROQ_API_KEY missing.", flush=True)
        return None

    now = datetime.now(timezone.utc)
    current_time_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    min_future_time = (now + timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ")

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    sample_records = telemetry_batch[:45]

    payload = {
        "model": "openai/gpt-oss-120b",
        "messages": [
            {
                "role": "user",
                "content": (
                    f"CURRENT UTC TIME: {current_time_iso}\n"
                    f"MINIMUM VALID FUTURE ETA: {min_future_time}\n"
                    f"LIVE SCIENTIFIC TELEMETRY BATCH:\n{json.dumps(sample_records, indent=1)}\n\n"
                    "INSTRUCTIONS: PREDICT FUTURE CRISES ONLY (ETA strictly 12h to 72h Ahead)\n"
                    "1. DO NOT report past events. All events must occur in the future.\n"
                    "2. Earthquake readings: calculate secondary fault stress transfer / triggered aftershocks.\n"
                    "3. Satellite heat anomalies: calculate downwind perimeter containment breach window.\n"
                    "4. Lat/Lon must match incoming sensor data.\n\n"
                    "Return ONLY valid JSON matching this schema:\n"
                    "{\n"
                    '  "primary_threat": "Forecasted [Phenomenon] Risk / Window",\n'
                    '  "location": "Region, Country",\n'
                    '  "time_of_event": "FUTURE ISO 8601 timestamp",\n'
                    '  "severity": "Moderate | Severe | Extreme",\n'
                    '  "probability": "calibrated percentage string (e.g. 62%)",\n'
                    '  "lat": float_latitude,\n'
                    '  "lon": float_longitude,\n'
                    '  "cascading_effects": ["consequence 1", "consequence 2"]\n'
                    "}"
                )
            }
        ],
        "response_format": {"type": "json_object"},
        "reasoning_format": "parsed",
        "reasoning_effort": "medium"
    }

    try:
        print(f"[*] Dispatching {len(telemetry_batch)} events to AI model...", flush=True)
        res = requests.post(url, headers=headers, json=payload, timeout=35)
        if res.status_code == 200:
            choice = res.json()["choices"][0]["message"]
            
            thinking = choice.get("reasoning") or choice.get("reasoning_content")
            if thinking:
                print("\n" + "="*50, flush=True)
                print("🧠 [AI FUTURE REASONING LOG]:", flush=True)
                print(thinking.strip(), flush=True)
                print("="*50 + "\n", flush=True)

            ai_result = json.loads(choice["content"])
            ai_result["is_demo"] = False

            # Ensure timestamp is strictly in the future
            try:
                event_dt = datetime.fromisoformat(ai_result["time_of_event"].replace("Z", "+00:00"))
                if event_dt <= now + timedelta(hours=1):
                    ai_result["time_of_event"] = (now + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                ai_result["time_of_event"] = (now + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")

            return ai_result
        else:
            print(f"❌ Groq API Error {res.status_code}: {res.text}", flush=True)
    except Exception as e:
        print(f"❌ Groq Exception: {e}", flush=True)

    return None

def update_predictions_with_real_ai(new_ai_result):
    state = load_or_init_state()
    state["real_ai_count"] += 1
    count = state["real_ai_count"]

    if count < 2:
        demo_items = [it for it in state["items"] if it.get("is_demo", False)]
        real_items = [it for it in state["items"] if not it.get("is_demo", False)]
        if demo_items:
            demo_items.pop(0)
        state["items"] = [new_ai_result] + real_items + demo_items
        print(f"⚡ [REAL AI UPDATE #1]: Injected 1 real prediction, kept demo buffer.", flush=True)
    else:
        real_items = [it for it in state["items"] if not it.get("is_demo", False)]
        state["items"] = [new_ai_result] + real_items
        state["items"] = state["items"][:6]
        print(f"🚀 [MILESTONE REACHED: {count} REAL AI RESPONSES]: ALL DEMO DATA PERMANENTLY PURGED.", flush=True)

    save_state(state)

def callback(ch, method, properties, body):
    try:
        events = json.loads(body)
    except Exception:
        events = []

    print(f"\n📩 Message received from RabbitMQ ({len(events)} points).", flush=True)
    prediction = forecast_future_disasters(events)
    if prediction:
        update_predictions_with_real_ai(prediction)
        print(f"[✓] Saved threat: {prediction['primary_threat']} at {prediction['location']}", flush=True)

def start_consumer():
    # Immediate boot test so real prediction generates upon container launch
    print("[*] Running boot forecast cycle...", flush=True)
    boot_events = [
        {"title": "USGS M5.2 Subduction Activity", "place": "Off Coast of Oregon, USA", "lat": 43.6, "lon": -127.8, "magnitude": 5.2, "stream": "USGS_SEISMIC"},
        {"title": "NASA Satellite Radiative Hotspot", "lat": -14.2, "lon": 132.1, "fire_radiative_power_mw": 380.0, "stream": "NASA_FIRMS"}
    ]
    test_result = forecast_future_disasters(boot_events)
    if test_result:
        update_predictions_with_real_ai(test_result)

    print("🤖 Consumer Online. Listening on 'disaster_telemetry'...", flush=True)
    while True:
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            chan = conn.channel()
            chan.queue_declare(queue=QUEUE_NAME, durable=True)
            chan.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
            chan.start_consuming()
        except Exception as e:
            print(f"[-] RabbitMQ connection error: {e}. Retrying in 5s...", flush=True)
            time.sleep(5)

if __name__ == "__main__":
    start_consumer()
