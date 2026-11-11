import os
import sys
import json
import time
import threading
import traceback
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
HEALTH_FILE = os.path.join(DATA_DIR, 'ai_health.json')

MODEL_ID = "openai/gpt-oss-120b"

print("\n" + "="*60, flush=True)
print(f"🛰️  [DIAGNOSTICS BOOT] FUTURE HAZARD ENGINE: {MODEL_ID}", flush=True)
print("="*60, flush=True)

def update_health_status(status, message, details=None):
    """Writes live AI status directly to data/ai_health.json for tracking."""
    payload = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": status,
        "model": MODEL_ID,
        "message": message,
        "details": details or {}
    }
    try:
        with open(HEALTH_FILE, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        print(f"[-] Could not write health file: {e}", flush=True)

# 1. Check API Key Presence
if not GROQ_API_KEY:
    err_msg = "GROQ_API_KEY environment variable is NOT SET or empty in Render dashboard!"
    print(f"\n❌ [CRITICAL CONFIG ERROR]: {err_msg}\n", flush=True)
    update_health_status("ERROR", err_msg)
else:
    masked_key = f"{GROQ_API_KEY[:6]}...{GROQ_API_KEY[-4:]}" if len(GROQ_API_KEY) > 10 else "INVALID_LENGTH"
    print(f"🔑 GROQ_API_KEY Loaded: {masked_key} (Total Length: {len(GROQ_API_KEY)})", flush=True)

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
                print(f"🧹 [PURGE DISK]: Wiped expired event '{it.get('primary_threat')}' ({ts_str})", flush=True)
        except Exception:
            pass
    return valid_items

def clean_all_disk_files():
    for fpath in [STATE_FILE, ALL_DATA_FILE]:
        if os.path.exists(fpath):
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                if isinstance(content, dict) and "items" in content:
                    content["items"] = purge_expired_items(content["items"])
                    with open(fpath, 'w', encoding='utf-8') as f:
                        json.dump(content, f, indent=2)
                elif isinstance(content, list):
                    cleaned = purge_expired_items(content)
                    with open(fpath, 'w', encoding='utf-8') as f:
                        json.dump(cleaned, f, indent=2)
            except Exception as e:
                print(f"[-] Disk scrub error on {fpath}: {e}", flush=True)

    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                latest = json.load(f)
            if isinstance(latest, dict) and latest:
                cleaned = purge_expired_items([latest])
                if not cleaned:
                    with open(DATA_FILE, 'w', encoding='utf-8') as f:
                        json.dump({}, f, indent=2)
        except Exception as e:
            print(f"[-] Disk scrub error on {DATA_FILE}: {e}", flush=True)

clean_all_disk_files()

# 2. Pre-flight Network & Authorization Probe
def run_boot_probe():
    """Fires a lightweight request to test if Groq accepts the key and model."""
    if not GROQ_API_KEY:
        return False

    print("\n🔍 [PROBE]: Testing Groq endpoint connectivity...", flush=True)
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY.strip()}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": MODEL_ID,
        "messages": [{"role": "user", "content": "Respond with the word 'OK' only."}],
        "max_tokens": 5
    }

    try:
        res = requests.post(url, headers=headers, json=payload, timeout=15)
        if res.status_code == 200:
            print(f"✅ [PROBE PASSED]: Groq API is reachable and authorized with model '{MODEL_ID}'.\n", flush=True)
            update_health_status("ONLINE", "API authenticated successfully")
            return True
        else:
            print("\n" + "!"*60, flush=True)
            print(f"❌ [PROBE FAILED] Groq returned HTTP {res.status_code}:", flush=True)
            print(res.text, flush=True)
            print("!"*60 + "\n", flush=True)
            update_health_status("ERROR", f"HTTP {res.status_code}", {"response_text": res.text})
            return False
    except requests.exceptions.RequestException as e:
        print("\n" + "!"*60, flush=True)
        print(f"❌ [PROBE NETWORK ERROR]: Could not connect to api.groq.com: {e}", flush=True)
        print("!"*60 + "\n", flush=True)
        update_health_status("NETWORK_ERROR", str(e))
        return False

run_boot_probe()

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

    clean_existing = purge_expired_items(state.get("items", []))

    if count < 2:
        demo_items = [it for it in clean_existing if it.get("is_demo", False)]
        real_items = [it for it in clean_existing if not it.get("is_demo", False)]
        if demo_items:
            demo_items.pop(0)
        state["items"] = [new_ai_result] + real_items + demo_items
    else:
        real_items = [it for it in clean_existing if not it.get("is_demo", False)]
        state["items"] = [new_ai_result] + real_items
        state["items"] = state["items"][:8]

    state["items"] = purge_expired_items(state["items"])

    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)

    if state["items"]:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(state["items"][0], f, indent=2)
        with open(ALL_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(state["items"], f, indent=2)

    print(f"💾 [SAVED TO JSON]: '{new_ai_result.get('primary_threat')}' targeting {new_ai_result.get('time_of_event')}", flush=True)

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
        print(f"[-] USGS direct telemetry fetch notice: {e}", flush=True)

    if not events:
        events = [
            {"stream": "USGS_SEISMIC", "title": "M 5.6 Seismic Activity", "place": "Near Coast of Honshu, Japan", "lat": 38.2, "lon": 142.5, "magnitude": 5.6},
            {"stream": "NASA_FIRMS", "title": "Satellite Radiative Anomaly", "lat": -22.3, "lon": 119.1, "fire_radiative_power_mw": 410.0}
        ]
    return events

# 3. Main Prediction Cycle with Comprehensive Diagnostics
def run_ai_prediction_cycle(events_batch):
    if not GROQ_API_KEY:
        print("[-] Skipping cycle: GROQ_API_KEY is not configured.", flush=True)
        return

    now = datetime.now(timezone.utc)
    current_time_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY.strip()}",
        "Content-Type": "application/json"
    }

    prompt_content = (
        f"CURRENT UTC TIME: {current_time_iso}\n"
        f"LIVE PLANETARY TELEMETRY SENSORS:\n{json.dumps(events_batch[:30], indent=1)}\n\n"
        "TASK: PREDICTIVE EMERGENCY FORECAST (14h to 48h in advance)\n"
        "Analyze the incoming telemetry and project a realistic future hazard event.\n"
        "1. Set 'eta_hours_from_now' to an integer between 14 and 48.\n"
        "2. For seismic data: estimate aftershock sequence or secondary fault trigger.\n"
        "3. For thermal hotspots: estimate firefront breach or dispersion corridor.\n"
        "4. Coordinates must match the sensor event location.\n\n"
        "Return ONLY valid JSON matching this schema:\n"
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

    print(f"[*] Dispatching {len(events_batch)} telemetry records to Groq ({MODEL_ID})...", flush=True)
    start_time = time.time()

    try:
        res = requests.post(url, headers=headers, json=payload, timeout=35)
        duration = round(time.time() - start_time, 2)

        if res.status_code == 200:
            print(f"✅ [GROQ SUCCESS] Response received in {duration}s", flush=True)
            res_json = res.json()
            raw_choice = res_json["choices"][0]["message"]["content"]

            try:
                ai_result = json.loads(raw_choice)
            except json.JSONDecodeError as je:
                print("\n" + "!"*60, flush=True)
                print(f"❌ [JSON PARSE ERROR]: Groq response was not valid JSON: {je}", flush=True)
                print(f"Raw Output Received:\n{raw_choice}", flush=True)
                print("!"*60 + "\n", flush=True)
                update_health_status("PARSE_ERROR", str(je), {"raw_text": raw_choice})
                return

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
            update_health_status("ONLINE", f"Prediction generated successfully in {duration}s")

        else:
            print("\n" + "!"*60, flush=True)
            print(f"❌ [GROQ API ERROR] Status Code: {res.status_code}", flush=True)
            print(f"Endpoint: {url}", flush=True)
            print(f"Full Error Response:\n{res.text}", flush=True)
            print("!"*60 + "\n", flush=True)
            update_health_status("API_ERROR", f"HTTP {res.status_code}", {"response_text": res.text})

    except requests.exceptions.Timeout:
        print(f"❌ [TIMEOUT ERROR]: Groq API request timed out after 35 seconds.", flush=True)
        update_health_status("TIMEOUT", "Request timed out after 35s")
    except requests.exceptions.RequestException as re:
        print("\n" + "!"*60, flush=True)
        print(f"❌ [NETWORK REQUEST EXCEPTION]: {re}", flush=True)
        print("Traceback:")
        traceback.print_exc(file=sys.stdout)
        print("!"*60 + "\n", flush=True)
        update_health_status("REQUEST_EXCEPTION", str(re))
    except Exception as e:
        print("\n" + "!"*60, flush=True)
        print(f"❌ [UNEXPECTED SYSTEM EXCEPTION]: {e}", flush=True)
        print("Traceback:")
        traceback.print_exc(file=sys.stdout)
        print("!"*60 + "\n", flush=True)
        update_health_status("SYSTEM_EXCEPTION", str(e))

def autonomous_timer_loop():
    print("[*] Autonomous loop online. Executing initial prediction cycle...", flush=True)
    telemetry = fetch_live_telemetry_direct()
    run_ai_prediction_cycle(telemetry)

    while True:
        time.sleep(90)
        try:
            clean_all_disk_files()
            telemetry = fetch_live_telemetry_direct()
            run_ai_prediction_cycle(telemetry)
        except Exception as e:
            print(f"[-] Autonomous cycle uncaught exception: {e}", flush=True)
            traceback.print_exc(file=sys.stdout)

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
        except Exception:
            time.sleep(15)

if __name__ == "__main__":
    t_rabbit = threading.Thread(target=rabbitmq_consumer_loop, daemon=True)
    t_rabbit.start()

    autonomous_timer_loop()
