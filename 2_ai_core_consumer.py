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

# Ensure matching relative/absolute directory
DATA_DIR = os.path.join(os.getcwd(), 'data')
os.makedirs(DATA_DIR, exist_ok=True)

DATA_FILE = os.path.join(DATA_DIR, 'latest_prediction.json')
ALL_DATA_FILE = os.path.join(DATA_DIR, 'all_threats.json')
STATE_FILE = os.path.join(DATA_DIR, 'ai_state.json')

print("=== [BOOT] AI CORE CONSUMER ONLINE ===", flush=True)

if not GROQ_API_KEY:
    print("❌ ERROR: GROQ_API_KEY is not set in environment variables!", flush=True)
else:
    print(f"✅ GROQ_API_KEY loaded (Length: {len(GROQ_API_KEY)})", flush=True)

def generate_initial_demo_data():
    now = datetime.now(timezone.utc)
    return [
        {
            "primary_threat": "[DEMO] Magnitude 6.2 Subduction Rupture",
            "location": "Near Coast of Honshu, Japan",
            "time_of_event": (now + timedelta(days=1)).strftime("%Y-%m-%dT14:00:00Z"),
            "severity": "Severe",
            "probability": "72%",
            "lat": 37.8500,
            "lon": 142.1000,
            "is_demo": True,
            "cascading_effects": [
                "Regional rail transit automatic stop",
                "Localized coastal surge alert"
            ]
        },
        {
            "primary_threat": "[DEMO] Tropical Cyclone Track Projection",
            "location": "Bay of Bengal, Indian Ocean",
            "time_of_event": (now + timedelta(days=2)).strftime("%Y-%m-%dT08:30:00Z"),
            "severity": "Severe",
            "probability": "65%",
            "lat": 14.5000,
            "lon": 88.2000,
            "is_demo": True,
            "cascading_effects": [
                "Maritime shipping diversion",
                "Coastal storm warning"
            ]
        }
    ]

def load_or_init_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            pass

    state = {
        "real_ai_count": 0,
        "items": generate_initial_demo_data()
    }
    save_state(state)
    return state

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

    # Write both endpoints that 3_web_server.py expects
    if state["items"]:
        with open(DATA_FILE, 'w') as f:
            json.dump(state["items"][0], f, indent=2)
        with open(ALL_DATA_FILE, 'w') as f:
            json.dump(state["items"], f, indent=2)
    print(f"[✓] Synced data files ({len(state['items'])} items, real AI count: {state['real_ai_count']})", flush=True)

# Seed on boot
current_state = load_or_init_state()

def synthesize_planetary_telemetry(telemetry_batch):
    if not GROQ_API_KEY:
        print("[-] Cannot call Groq: GROQ_API_KEY missing.", flush=True)
        return None

    current_time_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
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
                    f"Current UTC System Time: {current_time_iso}\n"
                    f"Live Telemetry Records: {json.dumps(sample_records, indent=1)}\n\n"
                    "INSTRUCTIONS:\n"
                    "1. Evaluate incoming live sensors (USGS Seismic, NASA MODIS Thermal, NOAA Plasma).\n"
                    "2. Identify the single most critical, active anomaly on Earth.\n"
                    "3. Extract verified latitude and longitude verbatim from the sensor data.\n"
                    "4. Base probability strictly on sensor verification: an active satellite thermal hotspot with FRP > 150MW or confirmed M5.0+ earthquake has 95%+ detection certainty; emerging tracks have lower proportional values. Do NOT invent arbitrary numbers.\n"
                    "5. Synthesize 2-3 realistic cascading physical/infrastructure consequences.\n\n"
                    "Return ONLY valid JSON matching this schema:\n"
                    "{\n"
                    '  "primary_threat": "Phenomenon name + sensor metric",\n'
                    '  "location": "Region, Country",\n'
                    '  "time_of_event": "Exact timestamp from sensor",\n'
                    '  "severity": "Low | Moderate | Severe | Extreme",\n'
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
        print(f"[*] Dispatching {len(telemetry_batch)} planetary events to Groq AI...", flush=True)
        res = requests.post(url, headers=headers, json=payload, timeout=35)
        if res.status_code == 200:
            choice = res.json()["choices"][0]["message"]
            
            # Print AI Thinking directly to Render runtime logs
            thinking = choice.get("reasoning") or choice.get("reasoning_content")
            if thinking:
                print("\n" + "="*50, flush=True)
                print("🧠 [LIVE REASONING TRACE]:", flush=True)
                print(thinking.strip(), flush=True)
                print("="*50 + "\n", flush=True)

            ai_result = json.loads(choice["content"])
            ai_result["is_demo"] = False
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
        # Step 1: Replace one demo item with this real AI response
        demo_items = [it for it in state["items"] if it.get("is_demo", False)]
        real_items = [it for it in state["items"] if not it.get("is_demo", False)]
        if demo_items:
            demo_items.pop(0)
        state["items"] = [new_ai_result] + real_items + demo_items
        print(f"⚡ [REAL AI UPDATE #1]: Injected 1 real response, retaining demo buffer.", flush=True)
    else:
        # Step 2: Purge ALL demo items permanently
        real_items = [it for it in state["items"] if not it.get("is_demo", False)]
        state["items"] = [new_ai_result] + real_items
        state["items"] = state["items"][:6]
        print(f"🚀 [MILESTONE REACHED: {count} REAL AI RESPONSES]: ALL DEMO DATA PURGED.", flush=True)

    save_state(state)

def callback(ch, method, properties, body):
    try:
        events = json.loads(body)
    except Exception:
        events = []

    print(f"\n📩 Message received from RabbitMQ ({len(events)} points).", flush=True)
    ai_threat = synthesize_planetary_telemetry(events)
    if ai_threat:
        update_predictions_with_real_ai(ai_threat)
        print(f"[✓] Active Threat Logged: {ai_threat.get('primary_threat')} at {ai_threat.get('location')}", flush=True)

def start_consumer():
    # Immediate boot test so you see real AI output right away
    print("[*] Running boot test cycle...", flush=True)
    boot_events = [
        {"title": "USGS M5.4 Earthquake", "place": "Off Coast of Honshu, Japan", "lat": 36.1, "lon": 141.2, "magnitude": 5.4, "stream": "USGS_SEISMIC"},
        {"title": "NASA Thermal Hotspot", "lat": -18.2, "lon": 133.4, "fire_radiative_power_mw": 320.0, "stream": "NASA_FIRMS"}
    ]
    test_result = synthesize_planetary_telemetry(boot_events)
    if test_result:
        update_predictions_with_real_ai(test_result)

    print("🤖 Consumer Online. Waiting for live RabbitMQ streams...", flush=True)
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
