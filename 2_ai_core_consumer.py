import os
import sys
import json
import re
import math
import time
import random
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

print(f"=== [BOOT] DIVERSE MULTI-REGION AI ENGINE: {MODEL_ID} ===", flush=True)

# ----------------- GEOSPATIAL DEDUPLICATION -----------------
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def deduplicate_threats(items):
    """Prevents the same disaster in the same area from cloning into multiple entries."""
    unique = []
    for item in items:
        lat1 = float(item.get("lat", 0))
        lon1 = float(item.get("lon", 0))
        title1 = item.get("primary_threat", "").lower()

        is_duplicate = False
        for existing in unique:
            lat2 = float(existing.get("lat", 0))
            lon2 = float(existing.get("lon", 0))
            title2 = existing.get("primary_threat", "").lower()

            dist = haversine_km(lat1, lon1, lat2, lon2)
            # Same coordinates/area (< 300km) AND overlapping hazard keywords
            same_hazard = any(word in title2 for word in title1.split() if len(word) > 4)
            if dist < 300 and same_hazard:
                is_duplicate = True
                break
        if not is_duplicate:
            unique.append(item)
    return unique

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
            print(f"📥 [GITHUB RESTORE]: Restored {len(data)} threats from GitHub.", flush=True)
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
        "message": f"telemetry: sync {len(items)} diverse predictions [skip ci]",
        "content": b64_content,
        "branch": GITHUB_BRANCH
    }
    if sha:
        payload["sha"] = sha

    try:
        res = requests.put(url, headers=headers, json=payload, timeout=15)
        if res.status_code in [200, 201]:
            print(f"🚀 [GITHUB SYNC SUCCESS]: Committed {len(items)} unique records to {GITHUB_REPO}/{GITHUB_FILE_PATH}", flush=True)
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
    return deduplicate_threats(valid_items)

# Restore and immediately deduplicate existing GitHub records on boot
boot_github_items = purge_expired_items(fetch_threats_from_github())
if boot_github_items:
    with open(ALL_DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(boot_github_items, f, indent=2)
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(boot_github_items[0], f, indent=2)
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump({"real_ai_count": len(boot_github_items), "items": boot_github_items}, f, indent=2)

def save_prediction_to_disk(new_ai_result):
    state = {"real_ai_count": 0, "items": []}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                state = json.load(f)
        except Exception:
            pass

    state["real_ai_count"] = state.get("real_ai_count", 0) + 1
    clean_existing = purge_expired_items(state.get("items", []))
    real_items = [it for it in clean_existing if not it.get("is_demo", False)]

    # Prepend newest prediction, then deduplicate so matching area+hazard updates cleanly
    combined = deduplicate_threats([new_ai_result] + real_items)[:8]
    state["items"] = combined

    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)

    if state["items"]:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(state["items"][0], f, indent=2)
        with open(ALL_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(state["items"], f, indent=2)

    print(f"💾 [SAVED UNIQUE]: '{new_ai_result.get('primary_threat')}' at {new_ai_result.get('location')} | ETA: {new_ai_result.get('time_of_event')}", flush=True)
    commit_threats_to_github(state["items"])

# ----------------- DIVERSE TELEMETRY INGESTION -----------------
def fetch_live_telemetry_direct():
    compact_events = []

    # 1. USGS Seismic (global M4.5+)
    try:
        r = requests.get('https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson', timeout=10)
        if r.status_code == 200:
            for f in r.json().get('features', [])[:6]:
                p = f.get('properties', {})
                c = f.get('geometry', {}).get('coordinates', [0, 0])
                compact_events.append(f"SEISMIC: M{p.get('mag', 5.0)} at {p.get('place', 'Ocean')} (Lat: {round(c[1], 2)}, Lon: {round(c[0], 2)})")
    except Exception as e:
        print(f"[-] USGS fetch notice: {e}", flush=True)

    # 2. NASA EONET Severe Hazards (Wildfires, Volcanoes, Storms, Floods, Ice)
    try:
        r_nasa = requests.get('https://eonet.gsfc.nasa.gov/api/v3/events?status=open&limit=15', timeout=10)
        if r_nasa.status_code == 200:
            for ev in r_nasa.json().get('events', []):
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
            "VOLCANO: Ash Plume Advisory (Lat: 14.75, Lon: -91.55)",
            "SEISMIC: M5.8 Subduction Rupture (Lat: 38.2, Lon: 142.5)"
        ]

    # Shuffle telemetry order so the model doesn't fixate on index 0
    random.shuffle(compact_events)
    return compact_events

# ----------------- DIVERSE AI PREDICTION CYCLE -----------------
def run_ai_prediction_cycle(events_list):
    current_key = get_current_groq_key()
    if not current_key:
        print("[-] Skipping cycle: No valid GROQ_API_KEY available.", flush=True)
        return

    now = datetime.now(timezone.utc)
    current_time_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Read currently active threats so we force the AI to pick a DIFFERENT region, hazard, and ETA
    existing_threats = []
    if os.path.exists(ALL_DATA_FILE):
        try:
            with open(ALL_DATA_FILE, 'r', encoding='utf-8') as f:
                existing_threats = json.load(f)
        except Exception:
            pass

    already_tracked_str = "\n".join(
        f"- {t.get('primary_threat')} at {t.get('location')} ({t.get('lat')}, {t.get('lon')}) [ETA: {t.get('time_of_event')}]"
        for t in existing_threats
    ) or "None yet."

    telemetry_summary = "\n".join(f"- {e}" for e in events_list[:8])

    system_prompt = (
        "You are a global planetary hazard forecasting AI. "
        "Respond ONLY with a raw, valid JSON object. No markdown, no explanations.\n\n"
        "Required JSON schema:\n"
        "{\n"
        '  "primary_threat": "Forecasted [Specific Hazard] Window",\n'
        '  "location": "Region, Country",\n'
        '  "eta_hours_from_now": 14,\n'
        '  "severity": "Moderate | Severe | Extreme",\n'
        '  "probability": "74%",\n'
        '  "lat": 0.0,\n'
        '  "lon": 0.0,\n'
        '  "cascading_effects": ["impact 1", "impact 2"]\n'
        "}"
    )

    user_prompt = (
        f"CURRENT UTC TIME: {current_time_iso}\n\n"
        f"ALREADY TRACKED ON MAP (DO NOT REPEAT THESE REGIONS OR HAZARDS):\n{already_tracked_str}\n\n"
        f"NEW INCOMING SENSORS:\n{telemetry_summary}\n\n"
        "CRITICAL RULES:\n"
        "1. Pick a DIFFERENT sensor/region/disaster type than what is listed in ALREADY TRACKED ON MAP.\n"
        "2. Choose a realistic, varied 'eta_hours_from_now' between 4 and 68 hours (e.g., 5, 9, 17, 29, 46, 62) — NEVER repeat the same ETA window.\n"
        "3. Coordinates 'lat' and 'lon' must match the chosen sensor from NEW INCOMING SENSORS.\n"
        "4. Output valid JSON now."
    )

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {current_key}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": MODEL_ID,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.75,
        "max_tokens": 800
    }

    try:
        print(f"[*] Calling Groq ({MODEL_ID}) with Key Slot #{key_index + 1}...", flush=True)
        res = requests.post(url, headers=headers, json=payload, timeout=25)

        if res.status_code == 200:
            raw_text = res.json()["choices"][0]["message"]["content"].strip()

            match = re.search(r'\{.*\}', raw_text, re.DOTALL)
            if not match:
                print(f"❌ [PARSE ERROR] Could not find JSON block in output: {raw_text}", flush=True)
                rotate_groq_key()
                return

            ai_result = json.loads(match.group(0))
            ai_result["is_demo"] = False

            eta_hours = None
            if "eta_hours_from_now" in ai_result:
                try:
                    eta_hours = float(ai_result["eta_hours_from_now"])
                except Exception:
                    pass

            if eta_hours is not None and eta_hours > 1:
                future_dt = now + timedelta(hours=eta_hours)
                ai_result["time_of_event"] = future_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                # Varied fallback spread instead of static 12h
                future_dt = now + timedelta(hours=random.randint(6, 54))
                ai_result["time_of_event"] = future_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

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
    print("[*] Autonomous diverse loop online.", flush=True)
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
