import os
import sys
import json
import re
import time
import base64
import threading
import traceback
from datetime import datetime, timezone, timedelta
import pika
import requests

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
QUEUE_NAME = 'disaster_telemetry'

# DUAL API KEY ROTATION SETUP
GROQ_KEYS = [k.strip() for k in [os.environ.get('GROQ_API_KEY'), os.environ.get('GROQ_API_KEY_2')] if k and k.strip()]
key_index = 0

def get_current_groq_key():
    global key_index
    if not GROQ_KEYS:
        return None
    return GROQ_KEYS[key_index % len(GROQ_KEYS)]

def rotate_groq_key():
    global key_index
    if len(GROQ_KEYS) > 1:
        key_index = (key_index + 1) % len(GROQ_KEYS)
        print(f"🔄 [KEY ROTATION]: Switched to Groq Key Slot #{key_index + 1}", flush=True)

# GITHUB PERSISTENCE
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')
GITHUB_REPO = os.environ.get('GITHUB_REPO')
GITHUB_BRANCH = os.environ.get('GITHUB_BRANCH', 'main')
GITHUB_FILE_PATH = "data/threats.json"

DATA_DIR = os.path.join(os.getcwd(), 'data')
os.makedirs(DATA_DIR, exist_ok=True)

DATA_FILE = os.path.join(DATA_DIR, 'latest_prediction.json')
ALL_DATA_FILE = os.path.join(DATA_DIR, 'all_threats.json')
STATE_FILE = os.path.join(DATA_DIR, 'ai_state.json')

MODEL_ID = "openai/gpt-oss-120b"

print(f"=== [BOOT] ROBUST JSON AI ENGINE: {MODEL_ID} ===", flush=True)

# ----------------- GITHUB REST API ENGINE -----------------
def get_github_file_sha():
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return None
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}?ref={GITHUB_BRANCH}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN.strip()}",
        "Accept": "application/vnd.github.v3+json"
    }
    try:
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code == 200:
            return res.json().get("sha")
    except Exception as e:
        print(f"[-] GitHub SHA probe error: {e}", flush=True)
    return None

def fetch_threats_from_github():
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return []
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}?ref={GITHUB_BRANCH}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN.strip()}",
        "Accept": "application/vnd.github.v3+json"
    }
    try:
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code == 200:
            content_b64 = res.json().get("content", "")
            raw_text = base64.b64decode(content_b64).decode("utf-8")
            data = json.loads(raw_text)
            print(f"📥 [GITHUB RESTORE]: Restored {len(data)} threats from GitHub repository.", flush=True)
            return data
    except Exception as e:
        print(f"[-] Error restoring threats from GitHub: {e}", flush=True)
    return []

def commit_threats_to_github(items):
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return

    sha = get_github_file_sha()
    json_text = json.dumps(items, indent=2)
    b64_content = base64.b64encode(json_text.encode("utf-8")).decode("utf-8")

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN.strip()}",
        "Accept": "application/vnd.github.v3+json"
    }
    payload = {
        "message": f"telemetry: sync {len(items)} planetary predictions [skip ci]",
        "content": b64_content,
        "branch": GITHUB_BRANCH
    }
    if sha:
        payload["sha"] = sha

    try:
        res = requests.put(url, headers=headers, json=payload, timeout=15)
        if res.status_code in [200, 201]:
            print(f"🚀 [GITHUB SYNC SUCCESS]: Committed {len(items)} records to {GITHUB_REPO}/{GITHUB_FILE_PATH}", flush=True)
        else:
            print(f"❌ [GITHUB SYNC ERROR] HTTP {res.status_code}: {res.text}", flush=True)
    except Exception as e:
        print(f"❌ [GITHUB REST EXCEPTION]: {e}", flush=True)

# ----------------- LOCAL PURGE & STATE -----------------
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
                print(f"🧹 [PURGE]: Dropped expired event '{it.get('primary_threat')}' ({ts_str})", flush=True)
        except Exception:
            pass
    return valid_items

boot_github_items = purge_expired_items(fetch_threats_from_github())
if boot_github_items:
    with open(ALL_DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(boot_github_items, f, indent=2)
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(boot_github_items[0], f, indent=2)

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

    print(f"💾 [SAVED LOCALLY]: '{new_ai_result.get('primary_threat')}' targeting {new_ai_result.get('time_of_event')}", flush=True)
    commit_threats_to_github(state["items"])

# ----------------- COMPACT TELEMETRY INGESTION -----------------
def fetch_live_telemetry_direct():
    compact_events = []
    
    # 1. USGS Seismic
    try:
        r = requests.get('https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson', timeout=10)
        if r.status_code == 200:
            for f in r.json().get('features', [])[:4]:
                p = f.get('properties', {})
                c = f.get('geometry', {}).get('coordinates', [0, 0])
                compact_events.append(f"SEISMIC: M{p.get('mag', 5.0)} at {p.get('place', 'Ocean')} (Lat: {round(c[1], 2)}, Lon: {round(c[0], 2)})")
    except Exception as e:
        print(f"[-] USGS fetch notice: {e}", flush=True)

    # 2. NASA EONET Severe Hazards
    try:
        r_nasa = requests.get('https://eonet.gsfc.nasa.gov/api/v3/events?status=open&limit=6', timeout=10)
        if r_nasa.status_code == 200:
            for ev in r_nasa.json().get('events', [])[:4]:
                cat = ev.get('categories', [{}])[0].get('title', 'Hazard')
                geoms = ev.get('geometry', [])
                if geoms:
                    c = geoms[-1].get('coordinates', [0, 0])
                    if isinstance(c, list) and len(c) >= 2 and isinstance(c[0], (int, float)):
                        compact_events.append(f"{cat.upper()}: {ev.get('title')} (Lat: {round(c[1], 2)}, Lon: {round(c[0], 2)})")
    except Exception as e:
        print(f"[-] NASA EONET fetch notice: {e}", flush=True)

    if not compact_events:
        compact_events = [
            "CYCLONE: Deep Vortex Warning (Lat: 16.4, Lon: 134.2)",
            "WILDFIRE: Severe Thermal Front (Lat: -18.4, Lon: 133.8)",
            "SEISMIC: M5.8 Subduction Rupture (Lat: 38.2, Lon: 142.5)"
        ]
    return compact_events

# ----------------- BULLETPROOF AI PREDICTION -----------------
def run_ai_prediction_cycle(events_list):
    current_key = get_current_groq_key()
    if not current_key:
        print("[-] Skipping cycle: No valid GROQ_API_KEY available.", flush=True)
        return

    now = datetime.now(timezone.utc)
    current_time_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    telemetry_summary = "\n".join(f"- {e}" for e in events_list[:5])

    # System instruction strictly isolates the schema definition
    system_prompt = (
        "You are an automated disaster prediction engine. "
        "You must respond ONLY with a raw, valid JSON object. "
        "Do NOT include any markdown code blocks, thoughts, explanations, or text outside the JSON object.\n\n"
        "Required JSON schema:\n"
        "{\n"
        '  "primary_threat": "Forecasted [Hazard Type] Window",\n'
        '  "location": "Region, Country",\n'
        '  "eta_hours_from_now": 24,\n'
        '  "severity": "Moderate | Severe | Extreme",\n'
        '  "probability": "74%",\n'
        '  "lat": 0.0,\n'
        '  "lon": 0.0,\n'
        '  "cascading_effects": ["impact 1", "impact 2"]\n'
        "}"
    )

    user_prompt = (
        f"CURRENT UTC TIME: {current_time_iso}\n"
        f"SENSORS BATCH:\n{telemetry_summary}\n\n"
        "INSTRUCTIONS:\n"
        "1. Predict an emerging planetary disaster event based on the telemetry.\n"
        "2. Dynamically estimate 'eta_hours_from_now' (integer, e.g. 6, 15, 28, 44) based on hazard speed.\n"
        "3. Coordinates 'lat' and 'lon' must match one of the sensors in the batch.\n"
        "4. Output valid JSON now."
    )

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {current_key}",
        "Content-Type": "application/json"
    }

    # Request without strict response_format to prevent 400 rejection; regex parses output
    payload = {
        "model": MODEL_ID,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.2,
        "max_tokens": 800
    }

    try:
        print(f"[*] Calling Groq ({MODEL_ID}) with Key Slot #{key_index + 1}...", flush=True)
        res = requests.post(url, headers=headers, json=payload, timeout=25)
        
        if res.status_code == 200:
            raw_text = res.json()["choices"][0]["message"]["content"].strip()
            
            # Extract JSON cleanly even if wrapped in markdown (```json ... ```)
            match = re.search(r'\{.*\}', raw_text, re.DOTALL)
            if not match:
                print(f"❌ [PARSE ERROR] Could not find JSON block in output: {raw_text}", flush=True)
                rotate_groq_key()
                return

            ai_result = json.loads(match.group(0))
            ai_result["is_demo"] = False

            # Dynamic AI timing logic
            eta_hours = None
            if "eta_hours_from_now" in ai_result:
                try:
                    eta_hours = float(ai_result["eta_hours_from_now"])
                except Exception:
                    pass

            if eta_hours is not None and eta_hours > 1:
                future_dt = now + timedelta(hours=eta_hours)
                ai_result["time_of_event"] = future_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            elif "time_of_event" in ai_result:
                try:
                    parsed_dt = datetime.fromisoformat(ai_result["time_of_event"].replace("Z", "+00:00"))
                    if parsed_dt <= now + timedelta(hours=1):
                        ai_result["time_of_event"] = (now + timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    ai_result["time_of_event"] = (now + timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                ai_result["time_of_event"] = (now + timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%SZ")

            ai_result.pop("eta_hours_from_now", None)

            save_prediction_to_disk(ai_result)
            rotate_groq_key()

        elif res.status_code in [413, 429]:
            print(f"⚠️ [RATE/TOKEN LIMIT HIT on Key #{key_index + 1}] HTTP {res.status_code}: {res.text}", flush=True)
            rotate_groq_key()
        else:
            print(f"❌ Groq API error {res.status_code}: {res.text}", flush=True)
            rotate_groq_key()

    except Exception as e:
        print(f"❌ Groq execution exception: {e}", flush=True)
        rotate_groq_key()

def autonomous_timer_loop():
    print("[*] Autonomous loop online.", flush=True)
    telemetry = fetch_live_telemetry_direct()
    run_ai_prediction_cycle(telemetry)

    while True:
        time.sleep(90)
        try:
            telemetry = fetch_live_telemetry_direct()
            run_ai_prediction_cycle(telemetry)
        except Exception as e:
            print(f"[-] Autonomous cycle exception: {e}", flush=True)

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
                print(f"📩 RabbitMQ delivered {len(events)} telemetry points.", flush=True)
                run_ai_prediction_cycle(events)

            chan.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
            chan.start_consuming()
        except Exception:
            time.sleep(15)

if __name__ == "__main__":
    t_rabbit = threading.Thread(target=rabbitmq_consumer_loop, daemon=True)
    t_rabbit.start()

    autonomous_timer_loop()
