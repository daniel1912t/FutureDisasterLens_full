import os
import sys
import json
import time
import base64
import threading
import traceback
from datetime import datetime, timezone, timedelta
import pika
import requests

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
QUEUE_NAME = 'disaster_telemetry'
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')

# GITHUB PERSISTENCE CONFIGURATION
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')
GITHUB_REPO = os.environ.get('GITHUB_REPO')  # format: 'username/repo-name'
GITHUB_BRANCH = os.environ.get('GITHUB_BRANCH', 'main')
GITHUB_FILE_PATH = "data/threats.json"

DATA_DIR = os.path.join(os.getcwd(), 'data')
os.makedirs(DATA_DIR, exist_ok=True)

DATA_FILE = os.path.join(DATA_DIR, 'latest_prediction.json')
ALL_DATA_FILE = os.path.join(DATA_DIR, 'all_threats.json')
STATE_FILE = os.path.join(DATA_DIR, 'ai_state.json')

MODEL_ID = "openai/gpt-oss-120b"

print(f"=== [BOOT] GITHUB-PERSISTENT HAZARD ENGINE: {MODEL_ID} ===", flush=True)

# ----------------- GITHUB REST API ENGINE -----------------
def get_github_file_sha():
    """Gets current SHA of the file on GitHub to allow updating it."""
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
    """Pulls existing active threats directly from GitHub on container boot."""
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
            print(f"📥 [GITHUB RESTORE]: Loaded {len(data)} threats from GitHub repository.", flush=True)
            return data
    except Exception as e:
        print(f"[-] Error restoring threats from GitHub: {e}", flush=True)
    return []

def commit_threats_to_github(items):
    """Commits and pushes the clean JSON array straight to GitHub."""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        print("[-] Skipping GitHub commit: GITHUB_TOKEN or GITHUB_REPO not configured.", flush=True)
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
            print(f"🚀 [GITHUB SYNC SUCCESS]: Committed {len(items)} active records to {GITHUB_REPO}/{GITHUB_FILE_PATH}", flush=True)
        else:
            print(f"❌ [GITHUB SYNC ERROR] HTTP {res.status_code}: {res.text}", flush=True)
    except Exception as e:
        print(f"❌ [GITHUB REST EXCEPTION]: {e}", flush=True)

# ----------------- LOCAL PURGE & RESTORE -----------------
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

# Restore previous data from GitHub on boot so memory is never empty
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

    # 1. Update local files for immediate Flask serving
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)

    if state["items"]:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(state["items"][0], f, indent=2)
        with open(ALL_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(state["items"], f, indent=2)

    print(f"💾 [SAVED LOCALLY]: '{new_ai_result.get('primary_threat')}' targeting {new_ai_result.get('time_of_event')}", flush=True)

    # 2. Commit permanently to GitHub repository
    commit_threats_to_github(state["items"])

# ----------------- LIVE TELEMETRY & AI INFERENCE -----------------
def fetch_live_telemetry_direct():
    events = []
    try:
        r = requests.get('https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson', timeout=10)
        if r.status_code == 200:
            for f in r.json().get('features', [])[:4]:
                props = f.get('properties', {})
                coords = f.get('geometry', {}).get('coordinates', [0, 0])
                events.append({
                    "stream": "USGS_SEISMIC",
                    "type": "Tectonic Fault Stress",
                    "title": props.get('title'),
                    "magnitude": props.get('mag'),
                    "place": props.get('place'),
                    "lon": coords[0],
                    "lat": coords[1]
                })
    except Exception as e:
        print(f"[-] USGS fetch notice: {e}", flush=True)

    try:
        r_nasa = requests.get('https://eonet.gsfc.nasa.gov/api/v3/events?status=open&limit=8', timeout=10)
        if r_nasa.status_code == 200:
            for ev in r_nasa.json().get('events', [])[:5]:
                cat = ev.get('categories', [{}])[0].get('title', 'Environmental Hazard')
                geoms = ev.get('geometry', [])
                if geoms:
                    coords = geoms[-1].get('coordinates', [0, 0])
                    if isinstance(coords, list) and len(coords) >= 2 and isinstance(coords[0], (int, float)):
                        events.append({
                            "stream": "NASA_EONET",
                            "type": cat,
                            "title": ev.get('title'),
                            "lon": coords[0],
                            "lat": coords[1]
                        })
    except Exception as e:
        print(f"[-] NASA EONET fetch notice: {e}", flush=True)

    if len(events) < 2:
        events = [
            {"stream": "NOAA_NHC", "type": "Tropical Cyclone", "title": "Rapid Pressure Drop & Vortex Deepening", "place": "Philippine Sea, Western Pacific", "lat": 16.4, "lon": 134.2},
            {"stream": "NASA_EONET", "type": "Severe Wildfire Front", "title": "Corridor Thermal Breakthrough", "place": "Northern Territory, Australia", "lat": -18.4, "lon": 133.8},
            {"stream": "USGS_SEISMIC", "type": "Tectonic Subduction", "title": "M 5.8 Fault Displacement", "place": "Near Coast of Honshu, Japan", "lat": 38.2, "lon": 142.5}
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
        "Authorization": f"Bearer {GROQ_API_KEY.strip()}",
        "Content-Type": "application/json"
    }

    prompt_content = (
        f"CURRENT UTC TIME: {current_time_iso}\n"
        f"GLOBAL TELEMETRY SENSOR MATRIX:\n{json.dumps(events_batch, indent=1)}\n\n"
        "OBJECTIVE: DIVERSE PLANETARY DISASTER FORECASTING (14h to 48h Window)\n"
        "Evaluate the sensor data across ALL available categories (Tropical Cyclones, Wildfire Perimeters, Volcanic Haze, Tsunami/Ocean Surges, Flash Floods, and Fault Slip Displacements).\n"
        "1. DO NOT default only to aftershocks. Pick the most acute emerging threat from the batch, rotating hazard classes across runs.\n"
        "2. Threat categories to forecast:\n"
        "   - 'Forecasted Category [X] Cyclone Landfall Window'\n"
        "   - 'Projected Wildfire Pyroconvective Boundary Breach'\n"
        "   - 'Forecasted Coastal Tsunami / Seiche Surge Risk'\n"
        "   - 'Forecasted Volcanic Ash Plume Dispersion Corridor'\n"
        "   - 'Projected Severe Flash Flood & Dam Spillway Threat'\n"
        "   - 'Forecasted Secondary Fault Stress Transfer Risk'\n"
        "3. Set 'eta_hours_from_now' to an integer between 14 and 48.\n"
        "4. Coordinates ('lat', 'lon') MUST strictly match one of the sensor coordinates in the telemetry batch.\n\n"
        "Return ONLY valid JSON matching this schema:\n"
        "{\n"
        '  "primary_threat": "Forecasted [Specific Disaster Type] Risk / Window",\n'
        '  "location": "Region, Country",\n'
        '  "eta_hours_from_now": 24,\n'
        '  "severity": "Moderate | Severe | Extreme",\n'
        '  "probability": "calibrated percentage string (e.g. 72%)",\n'
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
        print(f"[*] Dispatching multi-source telemetry to Groq ({MODEL_ID})...", flush=True)
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
    print("[*] Autonomous telemetry generator started.", flush=True)
    telemetry = fetch_live_telemetry_direct()
    run_ai_prediction_cycle(telemetry)

    while True:
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
