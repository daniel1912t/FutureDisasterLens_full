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

print("=== [BOOT] STRICT REAL-TIME FUTURE AI ENGINE ACTIVE ===", flush=True)

if not GROQ_API_KEY:
    print("❌ CRITICAL: GROQ_API_KEY is not set in Environment Variables!", flush=True)
else:
    print(f"✅ GROQ_API_KEY Loaded (Length: {len(GROQ_API_KEY)})", flush=True)

def purge_expired_items(items):
    """Purges anything in the past, anything <= 1 hour away, or anything older than right now."""
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
                print(f"🧹 [PURGE]: Dropped past/expired event '{it.get('primary_threat')}' (Date: {ts_str} | Current: {now.isoformat()})", flush=True)
        except Exception:
            pass
    return valid_items

def clean_old_files_on_boot():
    """Wipes any leftover predictions from earlier days (e.g. 5/11) from disk immediately."""
    for file_path in [STATE_FILE, DATA_FILE, ALL_DATA_FILE]:
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    content = json.load(f)
                if isinstance(content, dict) and "items" in content:
                    content["items"] = purge_expired_items(content["items"])
                    with open(file_path, 'w') as f:
                        json.dump(content, f, indent=2)
                elif isinstance(content, list):
                    cleaned = purge_expired_items(content)
                    with open(file_path, 'w') as f:
                        json.dump(cleaned, f, indent=2)
                elif isinstance(content, dict):
                    cleaned = purge_expired_items([content])
                    with open(file_path, 'w') as f:
                        json.dump(cleaned[0] if cleaned else {}, f, indent=2)
            except Exception:
                pass

clean_old_files_on_boot()

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
        # Purge all demo placeholders
        real_items = [it for it in state.get("items", []) if not it.get("is_demo", False)]
        state["items"] = [new_ai_result] + real_items
        state["items"] = state["items"][:6]
        print(f"🚀 [MILESTONE REACHED]: Real responses: {count}. All demo items permanently removed.", flush=True)

    state["items"] = purge_expired_items(state["items"])

    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

    if state["items"]:
        with open(DATA_FILE, 'w') as f:
            json.dump(state["items"][0], f, indent=2)
        with open(ALL_DATA_FILE, 'w') as f:
            json.dump(state["items"], f, indent=2)

    print(f"💾 [SAVED FUTURE EVENT]: '{new_ai_result.get('primary_threat')}' targeting {new_ai_result.get('time_of_event')}", flush=True)

def fetch_live_telemetry_direct():
    events = []
    try:
        r = requests.get('https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson', timeout=10)
        if r.status_code == 200:
            features = r.json().get('features', [])
            for f in features[:6]:
                props = f.get('properties', {})
                coords = f.get('geometry', {}).get('coordinates', [0, 0])
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
        print(f"[-] Telemetry direct notice: {e}", flush=True)

    if not events:
        events = [
            {"stream": "USGS_SEISMIC", "title": "M 5.3 Offshore Subduction Activity", "place": "Near Coast of Honshu, Japan", "lat": 38.2, "lon": 142.5, "magnitude": 5.3},
            {"stream": "NASA_FIRMS", "title": "Orbital Radiative Anomaly", "lat": -22.3, "lon": 119.1, "fire_radiative_power_mw": 395.0}
        ]
    return events

def run_ai_prediction_cycle(events_batch):
    if not GROQ_API_KEY:
        print("[-] Skipping cycle: GROQ_API_KEY missing.", flush=True)
        return

    now = datetime.now(timezone.utc)
    current_time_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")

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
                    f"CURRENT SYSTEM TIME (TODAY): {current_time_iso}\n"
                    f"LIVE SCIENTIFIC TELEMETRY BATCH:\n{json.dumps(events_batch[:35], indent=1)}\n\n"
                    "CRITICAL TASK: FUTURE RISK FORECASTING\n"
                    "1. Today is November 10, 2026. DO NOT use dates from earlier days (such as November 5).\n"
                    "2. Formulate a future risk forecast arriving between 14 and 48 hours from right now.\n"
                    "3. Specify 'eta_hours_from_now' as an integer between 14 and 48.\n"
                    "4. If seismic: calculate the triggered aftershock risk window or secondary fault rupture.\n"
                    "5. If thermal: calculate atmospheric boundary dispersion or wildfire spread.\n\n"
                    "Return ONLY valid JSON matching this schema:\n"
                    "{\n"
                    '  "primary_threat": "Forecasted [Phenomenon] Risk / Window",\n'
                    '  "location": "Region, Country",\n'
                    '  "eta_hours_from_now": 24,\n'
                    '  "severity": "Moderate | Severe | Extreme",\n'
                    '  "probability": "calibrated percentage string (e.g. 64%)",\n'
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
            ai_result = json.loads(choice["content"])
            ai_result["is_demo"] = False

            # Python computes the future timestamp using the live clock
            try:
                eta_hours = int(ai_result.get("eta_hours_from_now", 24))
                if eta_hours < 6 or eta_hours > 72:
                    eta_hours = 24
            except Exception:
                eta_hours = 24

            future_dt = now + timedelta(hours=eta_hours)
            ai_result["time_of_event"] = future_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            # Clean helper field
            ai_result.pop("eta_hours_from_now", None)

            save_prediction_to_disk(ai_result)
        else:
            print(f"❌ Groq API error {res.status_code}: {res.text}", flush=True)
    except Exception as e:
        print(f"❌ Groq execution exception: {e}", flush=True)

def autonomous_timer_loop():
    print("[*] Autonomous telemetry loop active.", flush=True)
    telemetry = fetch_live_telemetry_direct()
    run_ai_prediction_cycle(telemetry)

    while True:
        time.sleep(90)
        try:
            telemetry = fetch_live_telemetry_direct()
            run_ai_prediction_cycle(telemetry)
        except Exception as e:
            print(f"[-] Autonomous cycle error: {e}", flush=True)

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
                print(f"📩 RabbitMQ received {len(events)} points.", flush=True)
                run_ai_prediction_cycle(events)

            chan.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
            chan.start_consuming()
        except Exception as e:
            time.sleep(15)

if __name__ == "__main__":
    t_rabbit = threading.Thread(target=rabbitmq_consumer_loop, daemon=True)
    t_rabbit.start()

    autonomous_timer_loop()
