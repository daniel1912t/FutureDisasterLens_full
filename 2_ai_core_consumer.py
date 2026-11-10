import os
import sys
import json
import time
import threading
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

print("=== [BOOT] AUTONOMOUS FUTURE AI ENGINE ACTIVE ===", flush=True)

if not GROQ_API_KEY:
    print("❌ CRITICAL: GROQ_API_KEY is not set in Render Environment Variables!", flush=True)
else:
    print(f"✅ GROQ_API_KEY Loaded (Length: {len(GROQ_API_KEY)})", flush=True)

def purge_expired_items(items):
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
                print(f"🧹 [PURGE]: Dropped expired '{it.get('primary_threat')}' ({ts_str})", flush=True)
        except Exception:
            valid_items.append(it)
    return valid_items

def save_prediction_to_disk(new_ai_result):
    state = {"real_ai_count": 0, "items": []}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
        except Exception:
            pass

    state["real_ai_count"] = state.get("real_ai_count", 0) + 1
    count = state["real_ai_count"]

    if count < 2:
        demo_items = [it for it in state.get("items", []) if it.get("is_demo", False)]
        real_items = [it for it in state.get("items", []) if not it.get("is_demo", False)]
        if demo_items:
            demo_items.pop(0)
        state["items"] = [new_ai_result] + real_items + demo_items
    else:
        # 2+ real responses: wipe all demo artifacts permanently
        real_items = [it for it in state.get("items", []) if not it.get("is_demo", False)]
        state["items"] = [new_ai_result] + real_items
        state["items"] = state["items"][:6]
        print(f"🚀 [MILESTONE REACHED]: Real responses: {count}. All demo data permanently removed.", flush=True)

    state["items"] = purge_expired_items(state["items"])

    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

    if state["items"]:
        with open(DATA_FILE, 'w') as f:
            json.dump(state["items"][0], f, indent=2)
        with open(ALL_DATA_FILE, 'w') as f:
            json.dump(state["items"], f, indent=2)

    print(f"💾 [SAVED TO JSON]: '{new_ai_result.get('primary_threat')}' | ETA: {new_ai_result.get('time_of_event')}", flush=True)

def fetch_live_telemetry_direct():
    """Direct fallback to pull live global planetary events without waiting on brokers."""
    events = []
    try:
        # Live USGS Seismic M4.5+ Feed (Updated every 5 mins)
        r = requests.get('https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson', timeout=10)
        if r.status_code == 200:
            features = r.json().get('features', [])
            for f in features[:6]:
                props = f.get('properties', {})
                geom = f.get('geometry', {})
                coords = geom.get('coordinates', [0, 0])
                events.append({
                    "stream": "USGS_SEISMIC",
                    "title": props.get('title'),
                    "magnitude": props.get('mag'),
                    "place": props.get('place'),
                    "lon": coords[0],
                    "lat": coords[1],
                    "time": props.get('time')
                })
    except Exception as e:
        print(f"[-] Telemetry fetch notice: {e}", flush=True)

    if not events:
        events = [
            {"stream": "USGS_SEISMIC", "title": "M 5.3 Offshore Subduction Activity", "place": "Near Coast of Honshu, Japan", "lat": 38.2, "lon": 142.5, "magnitude": 5.3},
            {"stream": "NASA_FIRMS", "title": "Orbital Radiative Anomaly", "lat": -22.3, "lon": 119.1, "fire_radiative_power_mw": 395.0}
        ]
    return events

def run_ai_prediction_cycle(events_batch):
    if not GROQ_API_KEY:
        print("[-] Skipping cycle: GROQ_API_KEY is not defined.", flush=True)
        return

    now = datetime.now(timezone.utc)
    current_time_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    min_future_time = (now + timedelta(hours=14)).strftime("%Y-%m-%dT%H:%M:%SZ")

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": "openai/gpt-oss-120b",
        "messages": [
            {
                "role": "user",
                "content": (
                    f"CURRENT UTC TIME: {current_time_iso}\n"
                    f"MINIMUM ALLOWED FUTURE ETA: {min_future_time}\n"
                    f"LIVE PLANETARY TELEMETRY SENSORS:\n{json.dumps(events_batch[:35], indent=1)}\n\n"
                    "MISSION: FUTURE HAZARD PREDICTION\n"
                    "1. Formulate a future emergency forecast targeting 14 to 48 hours from right now.\n"
                    "2. If seismic: calculate secondary fault trigger window or aftershock sequence.\n"
                    "3. If thermal: calculate atmospheric dispersion / forward fire corridor.\n"
                    "4. 'time_of_event' MUST BE IN THE FUTURE (ISO 8601 strictly after the minimum allowed ETA).\n"
                    "5. Output calibrated realistic probability string (e.g. 58%, 72%).\n\n"
                    "Return ONLY valid JSON matching this schema:\n"
                    "{\n"
                    '  "primary_threat": "Forecasted [Phenomenon] Risk",\n'
                    '  "location": "Region, Country",\n'
                    '  "time_of_event": "FUTURE ISO 8601 timestamp",\n'
                    '  "severity": "Moderate | Severe | Extreme",\n'
                    '  "probability": "calibrated percentage string",\n'
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
        print(f"[*] Dispatching {len(events_batch)} telemetry records to Groq (openai/gpt-oss-120b)...", flush=True)
        res = requests.post(url, headers=headers, json=payload, timeout=35)
        if res.status_code == 200:
            choice = res.json()["choices"][0]["message"]
            
            # Print Reasoning Trace to Render runtime logs
            thinking = choice.get("reasoning") or choice.get("reasoning_content")
            if thinking:
                print("\n" + "="*50, flush=True)
                print("🧠 [GROQ REASONING LOG]:", flush=True)
                print(thinking.strip(), flush=True)
                print("="*50 + "\n", flush=True)

            ai_result = json.loads(choice["content"])
            ai_result["is_demo"] = False

            # Strict guardrail: Prevent premature purge
            try:
                event_dt = datetime.fromisoformat(ai_result["time_of_event"].replace("Z", "+00:00"))
                if event_dt <= now + timedelta(hours=2):
                    ai_result["time_of_event"] = (now + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                ai_result["time_of_event"] = (now + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")

            save_prediction_to_disk(ai_result)
        else:
            print(f"❌ Groq API error {res.status_code}: {res.text}", flush=True)
    except Exception as e:
        print(f"❌ Groq execution exception: {e}", flush=True)

# Continuous background thread so AI runs autonomously even if RabbitMQ has 0 messages
def autonomous_timer_loop():
    print("[*] Autonomous AI telemetry loop started.", flush=True)
    # Run immediately on boot
    telemetry = fetch_live_telemetry_direct()
    run_ai_prediction_cycle(telemetry)

    while True:
        # Re-evaluates global telemetry every 90 seconds
        time.sleep(90)
        try:
            telemetry = fetch_live_telemetry_direct()
            run_ai_prediction_cycle(telemetry)
        except Exception as e:
            print(f"[-] Autonomous loop cycle exception: {e}", flush=True)

def rabbitmq_consumer_loop():
    while True:
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            chan = conn.channel()
            chan.queue_declare(queue=QUEUE_NAME, durable=True)

            def callback(ch, method, properties, body):
                try:
                    events = json.loads(body)
                except Exception:
                    events = []
                print(f"📩 RabbitMQ received {len(events)} points. Triggering AI cycle...", flush=True)
                run_ai_prediction_cycle(events)

            chan.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
            chan.start_consuming()
        except Exception as e:
            # Expected if RabbitMQ isn't running locally or on Render
            time.sleep(15)

if __name__ == "__main__":
    # Start RabbitMQ consumer in a daemon thread
    t_rabbit = threading.Thread(target=rabbitmq_consumer_loop, daemon=True)
    t_rabbit.start()

    # Run the main autonomous telemetry loop
    autonomous_timer_loop()
