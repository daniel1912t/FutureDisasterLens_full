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
DATA_FILE = '/app/data/latest_prediction.json'
STATE_FILE = '/app/data/ai_state.json'

print("=== [BOOT] AI CORE LIVE TELEMETRY CONSUMER ===", flush=True)

if not GROQ_API_KEY:
    print("❌ WARNING: GROQ_API_KEY is not set in environment variables!", flush=True)
else:
    print(f"✅ Detected GROQ_API_KEY (Length: {len(GROQ_API_KEY)})", flush=True)

def generate_initial_demo_data():
    """Generates 3 staggered demo entries marked explicitly as [DEMO]."""
    now = datetime.now(timezone.utc)
    t1 = (now + timedelta(days=1)).strftime("%Y-%m-%dT14:00:00Z")
    t2 = (now + timedelta(days=2)).strftime("%Y-%m-%dT08:30:00Z")
    t3 = (now + timedelta(days=3)).strftime("%Y-%m-%dT19:15:00Z")

    return [
        {
            "primary_threat": "[DEMO] Tropical Cyclone Track Projection",
            "location": "Bay of Bengal, Indian Ocean",
            "time_of_event": t1,
            "severity": "Severe",
            "probability": "68%",
            "lat": 14.5000,
            "lon": 88.2000,
            "is_demo": True,
            "cascading_effects": [
                "Coastal storm surge 2.5m",
                "Regional maritime shipping suspension"
            ]
        },
        {
            "primary_threat": "[DEMO] Magnitude 6.1 Subduction Zone Tremor",
            "location": "Near Coast of Honshu, Japan",
            "time_of_event": t2,
            "severity": "Moderate",
            "probability": "55%",
            "lat": 37.8500,
            "lon": 142.1000,
            "is_demo": True,
            "cascading_effects": [
                "High-speed rail automated transit pause",
                "Localized infrastructure shake alerts"
            ]
        },
        {
            "primary_threat": "[DEMO] High-Intensity Thermal Orbital Anomaly",
            "location": "Pilbara Region, Western Australia",
            "time_of_event": t3,
            "severity": "Extreme",
            "probability": "74%",
            "lat": -22.3500,
            "lon": 118.9000,
            "is_demo": True,
            "cascading_effects": [
                "Air quality particulate PM2.5 spike",
                "Localized inland transport corridor watch"
            ]
        }
    ]

def load_or_init_state():
    """Tracks how many real AI responses have been generated and preserves items."""
    os.makedirs('/app/data', exist_ok=True)
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
    """Saves state and immediately updates latest_prediction.json for the web UI."""
    os.makedirs('/app/data', exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)
    if state["items"]:
        with open(DATA_FILE, 'w') as f:
            json.dump(state["items"][0], f)

# Seed immediate startup data so the frontend is never blank on fresh boot
current_state = load_or_init_state()
print(f"[*] Boot State: {current_state['real_ai_count']} real AI responses registered.", flush=True)

def synthesize_planetary_telemetry(telemetry_batch):
    if not GROQ_API_KEY:
        print("[-] Skipping AI query: GROQ_API_KEY missing.", flush=True)
        return None

    current_time_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    sample_records = telemetry_batch[:60]

    payload = {
        "model": "openai/gpt-oss-120b",
        "messages": [
            {
                "role": "user",
                "content": (
                    f"Current UTC System Time: {current_time_iso}\n"
                    f"Total Incoming Planetary Telemetry Records: {len(telemetry_batch)}\n"
                    f"Raw Multi-Stream Sensor Batch:\n{json.dumps(sample_records, indent=1)}\n\n"
                    "SCIENTIFIC MISSION INSTRUCTIONS:\n"
                    "1. Correlate across ALL live sensors: USGS OEF seismic hypocenters, NASA MODIS orbital thermal radiative power, and NOAA deep-space plasma.\n"
                    "2. Select the single most severe, verified anomaly on Earth right now.\n"
                    "3. Extract verified latitude and longitude verbatim from the sensor data.\n"
                    "4. Base probability strictly on sensor verification: an active satellite thermal hotspot with FRP > 150MW or confirmed M5.0+ earthquake has 95-100% detection certainty; emerging tracks or lower readings have calibrated proportional values. Do NOT invent arbitrary 90% numbers.\n"
                    "5. Synthesize 2-3 realistic cascading physical/infrastructure consequences.\n\n"
                    "Return ONLY valid JSON matching this schema:\n"
                    "{\n"
                    '  "primary_threat": "Phenomenon name + sensor metric (e.g. M5.8 Subduction Rupture / 350MW Thermal Anomaly)",\n'
                    '  "location": "Region, Country",\n'
                    '  "time_of_event": "Exact timestamp from sensor",\n'
                    '  "severity": "Low | Moderate | Severe | Extreme",\n'
                    '  "probability": "calibrated percentage string (e.g. 58%)",\n'
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
        print(f"[*] Contacting Groq (openai/gpt-oss-120b) with {len(telemetry_batch)} planetary observations...", flush=True)
        res = requests.post(url, headers=headers, json=payload, timeout=35)
        
        if res.status_code == 200:
            choice = res.json()["choices"][0]["message"]
            
            # Print AI Thinking directly to Render runtime logs
            thinking = choice.get("reasoning") or choice.get("reasoning_content")
            if thinking:
                print("\n" + "="*50, flush=True)
                print("🧠 [100+ MULTI-STREAM PLANETARY REASONING TRACE]:", flush=True)
                print(thinking.strip(), flush=True)
                print("="*50 + "\n", flush=True)
            else:
                print("[*] (Model output generated directly)", flush=True)

            ai_result = json.loads(choice["content"])
            ai_result["is_demo"] = False
            return ai_result
        else:
            print(f"❌ Groq API error {res.status_code}: {res.text}", flush=True)
    except Exception as e:
        print(f"❌ Inference exception: {e}", flush=True)

    return None

def update_predictions_with_real_ai(new_ai_result):
    state = load_or_init_state()
    state["real_ai_count"] += 1
    count = state["real_ai_count"]

    if count < 2:
        # First real AI response: Replace one demo entry, keep the rest
        demo_items = [it for it in state["items"] if it.get("is_demo", False)]
        real_items = [it for it in state["items"] if not it.get("is_demo", False)]
        
        if demo_items:
            demo_items.pop(0)
            
        state["items"] = [new_ai_result] + real_items + demo_items
        print(f"⚡ [REAL AI RESPONSE #1]: Stored real prediction. Remaining demo buffer retained.", flush=True)
    else:
        # 2 or more real responses: Purge ALL demo items permanently
        real_items = [it for it in state["items"] if not it.get("is_demo", False)]
        state["items"] = [new_ai_result] + real_items
        state["items"] = state["items"][:5]  # Keep top 5 latest verified events
        print(f"🚀 [MILESTONE REACHED: {count} REAL AI RESPONSES]: ALL DEMO DATA PERMANENTLY REMOVED.", flush=True)

    save_state(state)

def callback(ch, method, properties, body):
    try:
        events = json.loads(body)
    except Exception:
        events = []

    print(f"\n📩 Message broker received {len(events)} telemetry data points.", flush=True)
    ai_threat = synthesize_planetary_telemetry(events)
    if ai_threat:
        update_predictions_with_real_ai(ai_threat)
        print(f"[✓] VERIFIED THREAT SAVED: {ai_threat.get('primary_threat')} at {ai_threat.get('location')} ({ai_threat.get('lat')}, {ai_threat.get('lon')})", flush=True)

def start_consumer():
    print("🤖 Multi-Stream Consumer Online. Listening on 'disaster_telemetry'...", flush=True)
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
