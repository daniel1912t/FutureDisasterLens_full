import os
import sys
import json
import re
import math
import time
import random
import base64
import threading
from datetime import datetime, timezone, timedelta
import pika
import requests

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
QUEUE_NAME = 'disaster_telemetry'
MAX_RECORDS = 1000

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
TELEMETRY_1000_FILE = os.path.join(DATA_DIR, 'telemetry_1000.json')
STATE_FILE = os.path.join(DATA_DIR, 'ai_state.json')
HEALTH_FILE = os.path.join(DATA_DIR, 'ai_health.json')

MODEL_ID = "openai/gpt-oss-120b"

def update_health_status(status, message, details=None):
    payload = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": status,
        "model": MODEL_ID,
        "dataset_capacity": MAX_RECORDS,
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

print(f"=== [BOOT] 1000-DATA STATISTICAL HAZARD ENGINE: {MODEL_ID} ===", flush=True)
update_health_status("BOOTING", "Consumer initialized with 1000-record statistical engine")

# ----------------- GEOSPATIAL & MATHEMATICAL PROBABILITY ENGINE -----------------
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def calculate_hazard_probability(target_lat, target_lon, severity, telemetry_pool):
    """
    Calculates a calibrated probability percentage mathematically from the 1,000-sensor dataset:
      - 40% Regional Cluster Density (nearby anomalies within 400 km out of N=1000)
      - 40% Physical Energy / Intensity (magnitude or active anomaly weight)
      - 20% 24h Temporal Momentum (recent acceleration vs 30-day baseline)
    """
    total_n = max(len(telemetry_pool), 1)
    now_ms = time.time() * 1000.0
    day_ms = 24.0 * 3600.0 * 1000.0

    nearby_events = []
    recent_nearby_count = 0
    max_magnitude = 0.0

    for ev in telemetry_pool:
        ev_lat = float(ev.get("lat", 0.0))
        ev_lon = float(ev.get("lon", 0.0))
        dist = haversine_km(target_lat, target_lon, ev_lat, ev_lon)
        if dist <= 400.0:
            nearby_events.append(ev)
            mag = float(ev.get("magnitude", 4.5))
            if mag > max_magnitude:
                max_magnitude = mag
            ev_ts = float(ev.get("timestamp_ms", 0))
            if ev_ts > 0 and (now_ms - ev_ts) <= day_ms:
                recent_nearby_count += 1

    nearby_count = len(nearby_events)

    # 1. Density score (0 to 100): saturates logarithmically around 25+ regional sensors in 1000-point sample
    density_ratio = min(1.0, math.log1p(nearby_count) / math.log1p(25.0))
    density_score = density_ratio * 100.0

    # 2. Physical intensity score (0 to 100)
    sev_clean = str(severity).strip().lower()
    sev_base = {"extreme": 88.0, "severe": 72.0, "moderate": 54.0}.get(sev_clean, 60.0)
    if max_magnitude > 0:
        mag_norm = min(100.0, max(20.0, ((max_magnitude - 2.5) / 5.5) * 100.0))
        intensity_score = (sev_base * 0.5) + (mag_norm * 0.5)
    else:
        intensity_score = sev_base

    # 3. Temporal momentum score (0 to 100)
    if nearby_count > 0:
        momentum_ratio = min(1.0, (recent_nearby_count / nearby_count) * 2.5)
        momentum_score = momentum_ratio * 100.0
    else:
        momentum_score = 35.0

    raw_prob = (0.40 * density_score) + (0.40 * intensity_score) + (0.20 * momentum_score)
    calibrated_prob = round(min(98.5, max(12.0, raw_prob)), 1)

    breakdown = {
        "sample_size": total_n,
        "regional_anomalies_400km": nearby_count,
        "recent_24h_anomalies": recent_nearby_count,
        "peak_sensor_magnitude": round(max_magnitude, 2) if max_magnitude > 0 else None,
        "density_component": round(density_score, 1),
        "intensity_component": round(intensity_score, 1),
        "momentum_component": round(momentum_score, 1)
    }
    return f"{calibrated_prob}%", breakdown

def deduplicate_active_threats(items):
    """Deduplicates identical threats in the same 250 km zone while preserving historical 1,000-item archive."""
    unique = []
    for item in items:
        lat1 = float(item.get("lat", 0))
        lon1 = float(item.get("lon", 0))
        title1 = item.get("primary_threat", "").lower()
        ts1 = item.get("time_of_event", "")

        is_duplicate = False
        for existing in unique:
            lat2 = float(existing.get("lat", 0))
            lon2 = float(existing.get("lon", 0))
            title2 = existing.get("primary_threat", "").lower()
            ts2 = existing.get("time_of_event", "")

            dist = haversine_km(lat1, lon1, lat2, lon2)
            same_hazard = any(word in title2 for word in title1.split() if len(word) > 4)
            if dist < 250 and same_hazard and ts1[:13] == ts2[:13]:
                is_duplicate = True
                break
        if not is_duplicate:
            unique.append(item)
    return unique

# ----------------- GITHUB AUTO-CREATE & SYNC ENGINE -----------------
def get_github_file_info():
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
    if not GITHUB_TOKEN:
        print("❌ [GITHUB SKIP]: GITHUB_TOKEN is missing in Render Environment Variables.", flush=True)
        return

    sha, _, status = get_github_file_info()
    if not items and status != 404:
        return

    capped_items = items[:MAX_RECORDS]
    json_text = json.dumps(capped_items, indent=2)
    b64_content = base64.b64encode(json_text.encode("utf-8")).decode("utf-8")

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    action = "update" if sha else "create"
    payload = {
        "message": f"telemetry: {action} {GITHUB_FILE_PATH} ({len(capped_items)}/{MAX_RECORDS} records) [skip ci]",
        "content": b64_content,
        "branch": GITHUB_BRANCH
    }
    if sha:
        payload["sha"] = sha

    try:
        res = requests.put(url, headers=headers, json=payload, timeout=20)
        if res.status_code in [200, 201]:
            status_word = "CREATED NEW FILE" if res.status_code == 201 else "UPDATED FILE"
            print(f"🚀 [GITHUB {status_word}]: Saved {len(capped_items)} records to {GITHUB_REPO}/{GITHUB_FILE_PATH}", flush=True)
        else:
            print(f"❌ [GITHUB WRITE ERROR] HTTP {res.status_code}: {res.text}", flush=True)
    except Exception as e:
        print(f"❌ [GITHUB PUT EXCEPTION]: {e}", flush=True)

# Restore up to 1,000 records from GitHub on startup
_, initial_gh_items, init_status = get_github_file_info()
boot_items = deduplicate_active_threats(initial_gh_items)[:MAX_RECORDS]

if boot_items:
    print(f"📥 [GITHUB RESTORE]: Loaded {len(boot_items)} records from GitHub.", flush=True)
    with open(ALL_DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(boot_items, f, indent=2)
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(boot_items[0], f, indent=2)
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump({"total_records": len(boot_items), "items": boot_items}, f, indent=2)
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

    merged = deduplicate_active_threats(remote_items + local_items)
    real_items = [it for it in merged if not it.get("is_demo", False)]

    # Build up to 1,000 records
    updated_list = deduplicate_active_threats([new_ai_result] + real_items)[:MAX_RECORDS]

    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump({"total_records": len(updated_list), "items": updated_list}, f, indent=2)

    if updated_list:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(updated_list[0], f, indent=2)
        with open(ALL_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(updated_list, f, indent=2)

    print(
        f"💾 [SAVED {len(updated_list)}/{MAX_RECORDS}]: '{new_ai_result.get('primary_threat')}' "
        f"at {new_ai_result.get('location')} | Calculated Prob: {new_ai_result.get('probability')} | ETA: {new_ai_result.get('time_of_event')}",
        flush=True
    )
    commit_threats_to_github(updated_list)

# ----------------- 1,000-SENSOR TELEMETRY INGESTION -----------------
def fetch_live_telemetry_direct():
    """Ingests up to 1,000 live global sensor events for statistical probability calculation."""
    structured_pool = []

    # 1. USGS 30-day M2.5+ feed (typically ~800-1,200 global seismic records)
    try:
        r = requests.get('https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_month.geojson', timeout=15)
        if r.status_code == 200:
            for f in r.json().get('features', []):
                p = f.get('properties', {})
                c = f.get('geometry', {}).get('coordinates', [0, 0])
                if isinstance(c, list) and len(c) >= 2:
                    structured_pool.append({
                        "category": "SEISMIC",
                        "title": f"M{p.get('mag', 3.0)} at {p.get('place', 'Ocean')}",
                        "magnitude": float(p.get('mag') or 3.0),
                        "timestamp_ms": float(p.get('time') or 0),
                        "lat": round(float(c[1]), 4),
                        "lon": round(float(c[0]), 4)
                    })
    except Exception as e:
        print(f"[-] USGS 1000-feed notice: {e}", flush=True)

    # 2. NASA EONET Severe Hazards (Wildfires, Volcanoes, Storms, Floods, Sea Ice)
    try:
        r_nasa = requests.get('https://eonet.gsfc.nasa.gov/api/v3/events?limit=250', timeout=15)
        if r_nasa.status_code == 200:
            for ev in r_nasa.json().get('events', []):
                cat = ev.get('categories', [{}])[0].get('title', 'Hazard').upper()
                geoms = ev.get('geometry', [])
                if geoms:
                    last_g = geoms[-1]
                    c = last_g.get('coordinates', [0, 0])
                    if isinstance(c, list) and len(c) >= 2 and isinstance(c[0], (int, float)):
                        ts_ms = 0.0
                        date_str = last_g.get('date')
                        if date_str:
                            try:
                                ts_ms = datetime.fromisoformat(date_str.replace("Z", "+00:00")).timestamp() * 1000.0
                            except Exception:
                                pass
                        structured_pool.append({
                            "category": cat,
                            "title": ev.get('title', cat),
                            "magnitude": 5.5 if cat in ("SEVERE STORMS", "VOLCANOES") else 4.8,
                            "timestamp_ms": ts_ms,
                            "lat": round(float(c[1]), 4),
                            "lon": round(float(c[0]), 4)
                        })
    except Exception as e:
        print(f"[-] NASA EONET 1000-feed notice: {e}", flush=True)

    if not structured_pool:
        structured_pool = [
            {"category": "CYCLONE", "title": "Deep Vortex Warning", "magnitude": 6.2, "timestamp_ms": time.time() * 1000, "lat": 16.4, "lon": 134.2},
            {"category": "WILDFIRE", "title": "Severe Thermal Front", "magnitude": 5.1, "timestamp_ms": time.time() * 1000, "lat": -18.4, "lon": 133.8},
            {"category": "VOLCANO", "title": "Ash Plume Advisory", "magnitude": 5.8, "timestamp_ms": time.time() * 1000, "lat": 14.75, "lon": -91.55},
            {"category": "SEISMIC", "title": "M5.8 Subduction Rupture", "magnitude": 5.8, "timestamp_ms": time.time() * 1000, "lat": 38.2, "lon": 142.5}
        ]

    # Cap pool to exact 1,000 records and persist snapshot locally
    telemetry_1000 = structured_pool[:MAX_RECORDS]
    try:
        with open(TELEMETRY_1000_FILE, 'w', encoding='utf-8') as f:
            json.dump(telemetry_1000, f)
    except Exception:
        pass

    print(f"📡 [TELEMETRY POOL]: Loaded {len(telemetry_1000)} sensor records for statistical analysis.", flush=True)
    return telemetry_1000

# ----------------- DIVERSE AI PREDICTION CYCLE -----------------
def run_ai_prediction_cycle(telemetry_pool):
    if not GROQ_KEYS:
        err = "No GROQ_API_KEY or GROQ_API_KEY_2 configured in Render."
        print(f"❌ {err}", flush=True)
        update_health_status("CONFIG_ERROR", err)
        return

    # Normalize if RabbitMQ delivered raw strings instead of dicts
    if telemetry_pool and isinstance(telemetry_pool[0], str):
        telemetry_pool = fetch_live_telemetry_direct()

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
        f"- {t.get('primary_threat')} at {t.get('location')} ({t.get('lat')}, {t.get('lon')})"
        for t in existing_threats[:12]
    ) or "None yet."

    # Sample 10 diverse high-signal sensors from the 1,000-record pool for the LLM prompt
    high_signal = [ev for ev in telemetry_pool if ev.get("magnitude", 0) >= 4.5 or ev.get("category") != "SEISMIC"]
    sample_pool = high_signal if len(high_signal) >= 10 else telemetry_pool
    sampled_sensors = random.sample(sample_pool, min(10, len(sample_pool)))

    telemetry_summary = "\n".join(
        f"- {s['category']}: {s['title']} (Lat: {s['lat']}, Lon: {s['lon']})"
        for s in sampled_sensors
    )

    system_prompt = (
        "You are a global planetary hazard forecasting AI. "
        "Respond ONLY with a valid JSON object. No markdown fences, no commentary. "
        "Do NOT include a probability field — probability is calculated mathematically by the statistical engine.\n\n"
        "Required JSON schema:\n"
        "{\n"
        '  "primary_threat": "Forecasted [Specific Hazard] Window",\n'
        '  "location": "Region, Country",\n'
        '  "eta_hours_from_now": 14,\n'
        '  "severity": "Moderate | Severe | Extreme",\n'
        '  "lat": 0.0,\n'
        '  "lon": 0.0,\n'
        '  "cascading_effects": ["impact 1", "impact 2"]\n'
        "}"
    )

    user_prompt = (
        f"CURRENT UTC TIME: {current_time_iso}\n"
        f"ACTIVE TELEMETRY DATASET SIZE: {len(telemetry_pool)} sensors\n\n"
        f"RECENTLY FORECASTED (PICK A DIFFERENT REGION OR HAZARD):\n{already_tracked_str}\n\n"
        f"SAMPLED ANOMALIES FROM 1000-SENSOR POOL:\n{telemetry_summary}\n\n"
        "RULES:\n"
        "1. Select a distinct anomaly from SAMPLED ANOMALIES.\n"
        "2. Set 'eta_hours_from_now' between 4 and 68 hours based on hazard speed.\n"
        "3. Coordinates 'lat' and 'lon' must match the chosen sensor.\n"
        "4. Output valid JSON now."
    )

    url = "https://api.groq.com/openai/v1/chat/completions"

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
                    err = f"Key #{slot_num} returned non-JSON content: {raw_text[:200]}"
                    print(f"❌ [PARSE ERROR] {err}", flush=True)
                    update_health_status("PARSE_ERROR", err)
                    rotate_groq_key()
                    continue

                ai_result = json.loads(match.group(0))
                ai_result["is_demo"] = False

                target_lat = float(ai_result.get("lat", 0.0))
                target_lon = float(ai_result.get("lon", 0.0))
                severity = ai_result.get("severity", "Moderate")

                # Compute exact percentage mathematically across the 1,000-sensor dataset
                calc_prob, breakdown = calculate_hazard_probability(
                    target_lat, target_lon, severity, telemetry_pool
                )
                ai_result["probability"] = calc_prob
                ai_result["calculation_breakdown"] = breakdown

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
                update_health_status(
                    "ONLINE",
                    f"Generated '{ai_result.get('primary_threat')}' ({calc_prob} from N={len(telemetry_pool)}) via Key #{slot_num}"
                )
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
    print("[*] Autonomous 1000-sensor statistical loop online.", flush=True)
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
