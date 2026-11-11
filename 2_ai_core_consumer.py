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

MODEL_ID = "openai/gpt-oss-120b"

print(f"=== [BOOT] STRICT JSON PURGE ENGINE: {MODEL_ID} ===", flush=True)

def purge_expired_items(items):
    """Strictly deletes anything in the past or with <= 1 hour left."""
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
                print(f"🧹 [PURGE DISK]: Wiped expired event '{it.get('primary_threat')}' ({ts_str})", flush=True)
        except Exception:
            pass
    return valid_items

def clean_all_disk_files():
    """Scours every single json file on disk and removes old entries completely."""
    # 1. State File
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                state = json.load(f)
            if isinstance(state, dict):
                state["items"] = purge_expired_items(state.get("items", []))
                with open(STATE_FILE, 'w', encoding='utf-8') as f:
                    json.dump(state, f, indent=2)
        except Exception as e:
            print(f"[-] State file scrub error: {e}", flush=True)

    # 2. All Threats File
    if os.path.exists(ALL_DATA_FILE):
        try:
            with open(ALL_DATA_FILE, 'r', encoding='utf-8') as f:
                all_threats = json.load(f)
            if isinstance(all_threats, list):
                cleaned = purge_expired_items(all_threats)
                with open(ALL_DATA_FILE, 'w', encoding='utf-8') as f:
                    json.dump(cleaned, f, indent=2)
        except Exception as e:
            print(f"[-] All threats scrub error: {e}", flush=True)

    # 3. Latest Prediction File
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                latest = json.load(f)
            if isinstance(latest, dict) and latest:
                cleaned = purge_expired_items([latest])
                if not cleaned:
                    # Overwrite with empty object if expired
                    with open(DATA_FILE, 'w', encoding='utf-8') as f:
                        json.dump({}, f, indent=2)
        except Exception as e:
            print(f"[-] Latest prediction scrub error: {e}", flush=True)

# Run strict cleanup immediately on launch
clean_all_disk_files()

def save_prediction_to_disk(new_ai_result):
    state = {"real_ai_count": 0, "items": []}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                state = json.load(f)
        except Exception:
            pass

    state["real_ai_count"] = state.get("real_ai_count", 0) + 1
    count = state["real_ai_count"]

    # Purge old items first
    clean_existing = purge_expired_items(state.get("items", []))

    if count < 2:
        demo_items = [it for it in clean_existing if it.get("is_demo", False)]
        real_items = [it for it in clean_existing if not it.get("is_demo", False)]
        if demo_items:
            demo_items.pop(0)
        state["items"] = [new_ai_result] + real_items + demo_items
    else:
        # All demo data purged permanently
        real_items = [it for it in clean_existing if not it.get("is_demo", False)]
        state["items"] = [new_ai_result] + real_items
        state["items"] = state["items"][:8]

    # Final filter
    state["items"] = purge_expired_items(state["items"])

    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)

    if state["items"]:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(state["items"][0], f, indent=2)
        with open(ALL_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(state["items"], f, indent=2)
    else:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump({}, f, indent=2)
        with open(ALL_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump([], f, indent=2)

    print(f"💾 [SAVED CLEAN JSON]: '{new_ai_result.get('primary_threat')}' targeting {new_ai_result.get('time_of_event')}", flush=True)

def fetch_live_telemetry_direct():
    events = []
    try:
        r = requests.get('https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson', timeout=10)
        if r.status_code == 200:
            features = r.json().get('features', [])
            for f in features[:8]:
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
        print(f"[-] USGS fetch notice: {e}", flush=True)

    if not events:
        events = [
            {"stream": "USGS_SEISMIC", "title": "M 5.6 Seismic Rupture", "place": "Near Coast of Honshu, Japan", "lat": 38.2, "lon": 142.5, "magnitude": 5.6},
            {"stream": "NASA_FIRMS", "title": "Satellite Thermal Anomaly", "lat": -22.3, "lon": 119.1, "fire_radiative_power_mw": 410.0}
        ]
    return events

def run_ai_prediction_cycle(events_batch):
    if not GROQ_API_KEY:
        print("[-] Skipping cycle: GROQ_API_KEY is missing.", flush=True)
        return

    now = datetime.now(timezone.utc)
    current_time_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    prompt_content = (
        f"CURRENT UTC TIME: {current_time_iso}\n"
        f"LIVE PLANETARY TELEMETRY:\n{json.dumps(events_batch[:30], indent=1)}\n\n"
        "TASK: PREDICTIVE EMERGENCY FORECAST (14h to 48h in advance)\n"
        "Analyze the incoming telemetry and project a realistic future hazard event.\n"
        "1. Set 'eta_hours_from_now' to an integer between 14 and 48.\n"
        "2. For seismic data: estimate aftershock sequence or secondary stress transfer.\n"
        "3. For thermal hotspots: estimate firefront perimeter breach or atmospheric dispersion.\n"
        "4. Coordinates must match the sensor event location.\n\n"
        "Return ONLY JSON matching this format:\n"
        "{\n"
        '  "primary_threat": "Forecasted [Phenomenon] Risk / Window",\n'
        '  "location": "Region, Country",\n'
        '  "eta_hours_from_now": 24,\n'
        '  "severity": "Moderate | Severe | Extreme",\n'
        '  "probability": "percentage string (e.g. 64%)",\n'
        '  "lat": float_latitude,\n'
        '  "lon": float_longitude,\n'
        '  "cascading_effects": ["consequence 1", "consequence 2"]\n'
        "}"
    )

    payload = {
        "model": MODEL_ID,
        "messages": [{"role": "user", "content": prompt_content}],
        "response_format": {"type": "json_object"}
    }

    try:
        print(f"[*] Dispatching to Groq ({MODEL_ID})...", flush=True)
        res = requests.post(url, headers=headers, json=payload, timeout=35)
        if res.status_code == 200:
            raw_choice = res.json()["choices"][0]["message"]["content"]
            ai_result = json.loads(raw_choice)
            ai_result["is_demo"] = False

            try:
                eta_hours = int(ai_result.get("eta_hours_from_now", 24))
                if eta_hours < 6 or eta_hours > 72:
                    eta_hours = 24
            except Exception:
                eta_hours = 24

            future_dt = now + timedelta(hours=eta_hours)
            ai_result["time_of_event"] = future_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            ai_result.pop("eta_hours_from_now", None)

            save_prediction_to_disk(ai_result)
        else:
            print(f"❌ Groq API error {res.status_code}: {res.text}", flush=True)
    except Exception as e:
        print(f"❌ Groq execution exception: {e}", flush=True)

def autonomous_timer_loop():
    print("[*] Autonomous loop online. Running first cycle...", flush=True)
    telemetry = fetch_live_telemetry_direct()
    run_ai_prediction_cycle(telemetry)

    while True:
        time.sleep(90)
        try:
            clean_all_disk_files()  # Periodically scrub files on disk
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
                print(f"📩 RabbitMQ received {len(events)} telemetry points.", flush=True)
                run_ai_prediction_cycle(events)

            chan.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
            chan.start_consuming()
        except Exception as e:
            time.sleep(15)

if __name__ == "__main__":
    t_rabbit = threading.Thread(target=rabbitmq_consumer_loop, daemon=True)
    t_rabbit.start()

    autonomous_timer_loop()
