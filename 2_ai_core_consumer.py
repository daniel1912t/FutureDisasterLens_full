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

# DIRECT GITHUB PERSISTENCE CONFIG
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN', '').strip()
GITHUB_REPO = "daniel1912t/FutureDisasterLens_full"
GITHUB_BRANCH = "main"
GITHUB_FILE_PATH = "data/threats.json"

DATA_DIR = os.path.join(os.getcwd(), 'data')
os.makedirs(DATA_DIR, exist_ok=True)

DATA_FILE = os.path.join(DATA_DIR, 'latest_prediction.json')
ALL_DATA_FILE = os.path.join(DATA_DIR, 'all_threats.json')
STATE_FILE = os.path.join(DATA_DIR, 'ai_state.json')
HEALTH_FILE = os.path.join(DATA_DIR, 'ai_health.json')

MODEL_ID = "openai/gpt-oss-120b"

def update_health_status(status, message, details=None):
    payload = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": status,
        "model": MODEL_ID,
        "active_key_slot": (key_index % len(GROQ_KEYS)) + 1 if GROQ_KEYS else 0,
        "total_keys_loaded": len(GROQ_KEYS),
        "github_configured": bool(GITHUB_TOKEN),
        "github_repo": GITHUB_REPO,
        "message": message,
        "details": details or {}
    }
    try:
        with open(HEALTH_FILE, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2)
    except Exception:
        pass

print(f"=== [BOOT] FIXED HAZARD ENGINE: {MODEL_ID} ===", flush=True)
print(f"🔑 Groq Keys Loaded: {len(GROQ_KEYS)} | GitHub Token Set: {bool(GITHUB_TOKEN)}", flush=True)
update_health_status("BOOTING", "Consumer initialized")

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
            same_hazard = any(word in title2 for word in title1.split() if len(word) > 4)
            if dist < 300 and same_hazard:
                is_duplicate = True
                break
        if not is_duplicate:
            unique.append(item)
    return unique

# ----------------- GITHUB AUTO-CREATE & SYNC ENGINE -----------------
def get_github_file_info():
    """Returns (sha, decoded_list, status_code)."""
    if not GITHUB_TOKEN:
        return None, [], 0

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}?ref={GITHUB_BRANCH}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    try:
        res = requests.get(url, headers=headers, timeout=12)
        if res.status_code == 200:
            payload = res.json()
            sha = payload.get("sha")
            content_b64 = payload.get("content", "")
            raw_text = base64.b64decode(content_b64).decode("utf-8").strip()
            data = json.loads(raw_text) if raw_text else []
            if not isinstance(data, list):
                data = [data] if data else []
            return sha, data, 200
        elif res.status_code == 404:
            return None, [], 404
        else:
            print(f"❌ [GITHUB GET ERROR] HTTP {res.status_code}: {res.text}", flush=True)
            return None, [], res.status_code
    except Exception as e:
        print(f"[-] GitHub read exception: {e}", flush=True)
    return None, [], -1

def commit_threats_to_github(items):
    """Creates data/threats.json if missing, or updates it with active threats."""
    if not GITHUB_TOKEN:
        print("❌ [GITHUB SKIP]: GITHUB_TOKEN is missing in Render Environment Variables.", flush=True)
        return

    sha, _, status = get_github_file_info()
    # Never overwrite an existing remote file with an empty array due to a temporary read error
    if not items and status != 404:
        return

    json_text = json.dumps(items, indent=2)
    b64_content = base64.b64encode(json_text.encode("utf-8")).decode("utf-8")

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    action = "update" if sha else "create"
    payload = {
        "message": f"telemetry: {action} {GITHUB_FILE_PATH} ({len(items)} active threats) [skip ci]",
        "content": b64_content,
        "branch": GITHUB_BRANCH
    }
    if sha:
        payload["sha"] = sha

    try:
        res = requests.put(url, headers=headers, json=payload, timeout=15)
        if res.status_code in [200, 201]:
            status_word = "CREATED NEW FILE" if res.status_code == 201 else "UPDATED FILE"
            print(f"🚀 [GITHUB {status_word}]: Saved {len(items)} threats to {GITHUB_REPO}/{GITHUB_FILE_PATH}", flush=True)
        else:
            print(f"❌ [GITHUB WRITE ERROR] HTTP {res.status_code}: {res.text}", flush=True)
    except Exception as e:
        print(f"❌ [GITHUB PUT EXCEPTION]: {e}", flush=True)

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

# Restore from GitHub on startup without wiping existing data
_, initial_gh_items, init_status = get_github_file_info()
boot_items = purge_expired_items(initial_gh_items)

if boot_items:
    print(f"📥 [GITHUB RESTORE]: Loaded {len(boot_items)} active threats from GitHub.", flush=True)
    with open(ALL_DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(boot_items, f, indent=2)
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(boot_items[0], f, indent=2)
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump({"real_ai_count": len(boot_items), "items": boot_items}, f, indent=2)
elif init_status == 404:
    commit_threats_to_github([])

def save_prediction_to_disk(new_ai_result):
    _, remote_items, _ = get_github_file_info()
    local_items = []
    if os.path.exists(ALL_DATA_FILE):
        try:
            with open(ALL_DATA_FILE, 'r', encoding='utf-8') as f:
                local_items = json.load(f)
        except Exception:
            pass

    merged = purge_expired_items(remote_items + local_items)
    real_items = [it for it in merged if not it.get("is_demo", False)]

    updated_list = deduplicate_threats([new_ai_result] + real_items)[:10]

    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump({"real_ai_count": len(updated_list), "items": updated_list}, f, indent=2)

    if updated_list:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(updated_list[0], f, indent=2)
        with open(ALL_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(updated_list, f, indent=2)

    print(f"💾 [SAVED]: '{new_ai_result.get('primary_threat')}' at {new_ai_result.get('location')} | ETA: {new_ai_result.get('time_of_event')}", flush=True)
    commit_threats_to_github(updated_list)

# ----------------- DIVERSE TELEMETRY INGESTION -----------------
def fetch_live_telemetry_direct():
    compact_events = []

    try:
        r = requests.get('https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson', timeout=10)
        if r.status_code == 200:
            for f in r.json().get('features', [])[:6]:
                p = f.get('properties', {})
                c = f.get('geometry', {}).get('coordinates', [0, 0])
                compact_events.append(f"SEISMIC: M{p.get('mag', 5.0)} at {p.get('place', 'Ocean')} (Lat: {round(c[1], 2)}, Lon: {round(c[0], 2)})")
    except Exception as e:
        print(f"[-] USGS fetch notice: {e}", flush=True)

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

    random.shuffle(compact_events)
    return compact_events

# ----------------- DIVERSE AI PREDICTION CYCLE WITH IMMEDIATE FAILOVER -----------------
def run_ai_prediction_cycle(events_list):
    if not GROQ_KEYS:
        err = "No GROQ_API_KEY or GROQ_API_KEY_2 configured in Render."
        print(f"❌ {err}", flush=True)
        update_health_status("CONFIG_ERROR", err)
        return

    now = datetime.now(timezone.utc)
    current_time_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")

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
        "Respond ONLY with a valid JSON object. No markdown fences, no commentary.\n\n"
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
        "2. Choose a realistic, varied 'eta_hours_from_now' between 4 and 68 hours (e.g., 5, 9, 17, 29, 46, 62).\n"
        "3. Coordinates 'lat' and 'lon' must match the chosen sensor from NEW INCOMING SENSORS.\n"
        "4. Output valid JSON now."
    )

    url = "https://api.groq.com/openai/v1/chat/completions"

    # Try each available key immediately before giving up on the cycle
    for attempt in range(len(GROQ_KEYS)):
        current_key = get_current_groq_key()
        slot_num = (key_index % len(GROQ_KEYS)) + 1

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
            "reasoning_effort": "low",
            "temperature": 0.75,
            "max_tokens": 2048
        }

        try:
            print(f"[*] Calling Groq ({MODEL_ID}) with Key Slot #{slot_num}...", flush=True)
            res = requests.post(url, headers=headers, json=payload, timeout=30)

            if res.status_code == 200:
                msg = res.json()["choices"][0]["message"]
                raw_text = (msg.get("content") or "").strip()

                match = re.search(r'\{.*\}', raw_text, re.DOTALL)
                if not match:
                    err = f"Key #{slot_num} returned non-JSON or empty content: {raw_text[:200]}"
                    print(f"❌ [PARSE ERROR] {err}", flush=True)
                    update_health_status("PARSE_ERROR", err)
                    rotate_groq_key()
                    continue

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
                    future_dt = now + timedelta(hours=random.randint(6, 54))
                    ai_result["time_of_event"] = future_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

                ai_result.pop("eta_hours_from_now", None)

                save_prediction_to_disk(ai_result)
                update_health_status("ONLINE", f"Generated '{ai_result.get('primary_threat')}' via Key #{slot_num}")
                rotate_groq_key()
                return

            else:
                err = f"HTTP {res.status_code} on Key #{slot_num}: {res.text}"
                print(f"❌ [GROQ ERROR] {err}", flush=True)
                update_health_status("API_ERROR", err)
                rotate_groq_key()

        except Exception as e:
            err = f"Exception on Key #{slot_num}: {e}"
            print(f"❌ [GROQ EXCEPTION] {err}", flush=True)
            update_health_status("EXCEPTION", err)
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
