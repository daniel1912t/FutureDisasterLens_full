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

print("=== [BOOT] FUTURE PREDICTIVE AI CORE ONLINE ===", flush=True)

def generate_initial_demo_data():
    now = datetime.now(timezone.utc)
    return [
        {
            "primary_threat": "[DEMO] Forecasted M6.0+ Triggered Aftershock Risk",
            "location": "Near Coast of Honshu, Japan",
            "time_of_event": (now + timedelta(hours=28)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "severity": "Severe",
            "probability": "64%",
            "lat": 37.8500,
            "lon": 142.1000,
            "is_demo": True,
            "cascading_effects": [
                "Predicted secondary subduction zone stress release",
                "High-speed rail early deceleration advisory"
            ]
        },
        {
            "primary_threat": "[DEMO] Projected Rapid Wildfire Spread Window",
            "location": "Pilbara Region, Western Australia",
            "time_of_event": (now + timedelta(hours=36)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "severity": "Extreme",
            "probability": "72%",
            "lat": -22.3500,
            "lon": 118.9000,
            "is_demo": True,
            "cascading_effects": [
                "Thermal updrafts threatening transmission corridors",
                "Downwind particulate air quality index degradation"
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

    if state["items"]:
        with open(DATA_FILE, 'w') as f:
            json.dump(state["items"][0], f, indent=2)
        with open(ALL_DATA_FILE, 'w') as f:
            json.dump(state["items"], f, indent=2)
    print(f"[✓] Synced future threat forecasts (Items: {len(state['items'])}, Real AI count: {state['real_ai_count']})", flush=True)

current_state = load_or_init_state()

def forecast_future_disasters(telemetry_batch):
    if not GROQ_API_KEY:
        print("[-] Skipping Groq call: GROQ_API_KEY is missing.", flush=True)
        return None

    now = datetime.now(timezone.utc)
    current_time_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    min_future_time = (now + timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ")

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    sample_records = telemetry_batch[:50]

    payload = {
        "model": "openai/gpt-oss-120b",
        "messages": [
            {
                "role": "user",
                "content": (
                    f"CURRENT UTC TIME: {current_time_iso}\n"
                    f"MINIMUM VALID FUTURE ETA: {min_future_time}\n"
                    f"INCOMING SCIENTIFIC TELEMETRY:\n{json.dumps(sample_records, indent=1)}\n\n"
                    "CRITICAL TASK: FUTURE HAZARD PREDICTION ONLY\n"
                    "You are a forward-looking predictive emergency AI. You MUST NOT report an event that already took place.\n"
                    "Instead, use the live measurements to forecast a FUTURE impending event within 12 to 72 hours:\n"
                    "1. For USGS Seismic/OEF: If a tremor is detected, compute the future aftershock risk window (+12h to +48h) or secondary fault stress transfer.\n"
                    "2. For NASA Satellite Hotspots: Project the forward firefront spread corridor or downwind containment breach over the next 18-36 hours.\n"
                    "3. For NOAA Atmospheric Lows / Cyclones: Project future landfall ETA (+24h to +72h).\n"
                    f"4. 'time_of_event' MUST BE IN THE FUTURE (Strictly after {current_time_iso}). Never output past timestamps.\n"
                    "5. Output calibrated statistical probability based on forecast models (e.g. 45% - 78%).\n\n"
                    "Return ONLY valid JSON matching this schema:\n"
                    "{\n"
                    '  "primary_threat": "Forecasted [Phenomenon] Risk / Impending Threat",\n'
                    '  "location": "Region, Country",\n'
                    '  "time_of_event": "FUTURE ISO 8601 timestamp (e.g. 2026-11-07T18:00:00Z)",\n'
                    '  "severity": "Moderate | Severe | Extreme",\n'
                    '  "probability": "calibrated percentage string (e.g. 62%)",\n'
                    '  "lat": float_latitude,\n'
                    '  "lon": float_longitude,\n'
                    '  "cascading_effects": ["future expected consequence 1", "future expected consequence 2"]\n'
                    "}"
                )
            }
        ],
        "response_format": {"type": "json_object"},
        "reasoning_format": "parsed",
        "reasoning_effort": "medium"
    }

    try:
        print(f"[*] Dispatching {len(telemetry_batch)} events to AI model to predict future risks...", flush=True)
        res = requests.post(url, headers=headers, json=payload, timeout=35)
        if res.status_code == 200:
            choice = res.json()["choices"][0]["message"]
            
            thinking = choice.get("reasoning") or choice.get("reasoning_content")
            if thinking:
                print("\n" + "="*50, flush=True)
                print("🧠 [AI FUTURE FORECAST REASONING]:", flush=True)
                print(thinking.strip(), flush=True)
                print("="*50 + "\n", flush=True)

            ai_result = json.loads(choice["content"])
            ai_result["is_demo"] = False
            
            # Enforce future timestamp
            try:
                event_dt = datetime.fromisoformat(ai_result["time_of_event"].replace("Z", "+00:00"))
                if event_dt <= now:
                    print(f"[!] Warning: Model returned past timestamp ({ai_result['time_of_event']}). Bumping to future forecast window.", flush=True)
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
        print(f"⚡ [REAL PREDICTION #1 SAVED]: Injected 1 real forecast. Retaining 1 demo buffer.", flush=True)
    else:
        real_items = [it for it in state["items"] if not it.get("is_demo", False)]
        state["items"] = [new_ai_result] + real_items
        state["items"] = state["items"][:6]
        print(f"🚀 [MILESTONE: {count} REAL PREDICTIONS]: ALL DEMO DATA PERMANENTLY PURGED.", flush=True)

    save_state(state)

def callback(ch, method, properties, body):
    try:
        events = json.loads(body)
    except Exception:
        events = []

    print(f"\n📩 Ingested {len(events)} telemetry events from broker.", flush=True)
    prediction = forecast_future_disasters(events)
    if prediction:
        update_predictions_with_real_ai(prediction)
        print(f"[✓] FUTURE THREAT LOGGED: {prediction.get('primary_threat')} targeting {prediction.get('location')} (ETA: {prediction.get('time_of_event')})", flush=True)

def start_consumer():
    # Immediate boot test so future forecasting runs on deploy
    print("[*] Running immediate boot forecast test...", flush=True)
    boot_events = [
        {"title": "USGS M5.1 Seismic Activity", "place": "Off Coast of Oregon, USA", "lat": 43.6, "lon": -127.8, "magnitude": 5.1, "stream": "USGS_SEISMIC"},
        {"title": "NASA Satellite Radiative Hotspot", "lat": -14.2, "lon": 132.1, "fire_radiative_power_mw": 410.0, "stream": "NASA_FIRMS"}
    ]
    test_result = forecast_future_disasters(boot_events)
    if test_result:
        update_predictions_with_real_ai(test_result)

    print("🤖 Consumer Online. Waiting for live RabbitMQ streams on 'disaster_telemetry'...", flush=True)
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
