import os
import sys
import json
import time
import sqlite3
from datetime import datetime, timezone, timedelta
import pika
import requests

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
QUEUE_NAME = 'disaster_telemetry'
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')

DATA_DIR = os.path.join(os.getcwd(), 'data')
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, 'threats.db')

print("=== [BOOT] SQLITE-BACKED FUTURE AI CONSUMER ONLINE ===", flush=True)

# ----------------- SQLITE DATABASE MANAGEMENT -----------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            primary_threat TEXT NOT NULL,
            location TEXT NOT NULL,
            time_of_event TEXT NOT NULL,
            severity TEXT NOT NULL,
            probability TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            cascading_effects TEXT NOT NULL,
            is_demo INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

def purge_expired_threats():
    """Removes records that have already occurred or have <= 1 hour left."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1 hour threshold into the future
    threshold = (datetime.now(timezone.utc) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    cursor.execute('DELETE FROM predictions WHERE time_of_event <= ?', (threshold,))
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    if deleted > 0:
        print(f"🧹 [PURGE]: Removed {deleted} threats that expired or had <= 1 hour remaining.", flush=True)

def insert_prediction(item):
    init_db()
    purge_expired_threats()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    effects_json = json.dumps(item.get("cascading_effects", []))
    is_demo_int = 1 if item.get("is_demo", False) else 0
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Check count of real predictions
    cursor.execute('SELECT COUNT(*) FROM predictions WHERE is_demo = 0')
    real_count = cursor.fetchone()[0]

    # If this is a real item and we have 2+ real items, purge all demo records
    if not item.get("is_demo", False) and (real_count + 1) >= 2:
        cursor.execute('DELETE FROM predictions WHERE is_demo = 1')
        print("🚀 [MILESTONE]: 2+ Real AI forecasts logged. All demo entries purged from DB.", flush=True)

    cursor.execute('''
        INSERT INTO predictions (primary_threat, location, time_of_event, severity, probability, lat, lon, cascading_effects, is_demo, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        item["primary_threat"],
        item["location"],
        item["time_of_event"],
        item["severity"],
        item["probability"],
        item["lat"],
        item["lon"],
        effects_json,
        is_demo_int,
        now_iso
    ))
    conn.commit()
    conn.close()

def seed_initial_demo_if_empty():
    init_db()
    purge_expired_threats()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM predictions')
    count = cursor.fetchone()[0]
    conn.close()

    if count == 0:
        print("[*] Database is empty. Seeding forward demo forecasts (+18h to +36h)...", flush=True)
        now = datetime.now(timezone.utc)
        demos = [
            {
                "primary_threat": "[DEMO] Forecasted M6.2 Triggered Aftershock",
                "location": "Near Coast of Honshu, Japan",
                "time_of_event": (now + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "severity": "Severe",
                "probability": "68%",
                "lat": 37.8500,
                "lon": 142.1000,
                "is_demo": True,
                "cascading_effects": [
                    "Regional transit deceleration advisory",
                    "Coastal surge warning"
                ]
            },
            {
                "primary_threat": "[DEMO] Projected Wildfire Containment Breach",
                "location": "Pilbara Region, Western Australia",
                "time_of_event": (now + timedelta(hours=34)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "severity": "Extreme",
                "probability": "74%",
                "lat": -22.3500,
                "lon": 118.9000,
                "is_demo": True,
                "cascading_effects": [
                    "Power transmission network shutdown",
                    "Air particulate spike"
                ]
            }
        ]
        for demo in demos:
            insert_prediction(demo)

seed_initial_demo_if_empty()

# ----------------- AI SYNTHESIS & INFERENCE -----------------
def forecast_future_disasters(telemetry_batch):
    if not GROQ_API_KEY:
        print("[-] Skipping Groq call: GROQ_API_KEY is missing.", flush=True)
        return None

    now = datetime.now(timezone.utc)
    current_time_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    min_future_time = (now + timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ")

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    sample_records = telemetry_batch[:45]

    payload = {
        "model": "openai/gpt-oss-120b",
        "messages": [
            {
                "role": "user",
                "content": (
                    f"CURRENT UTC TIME: {current_time_iso}\n"
                    f"MINIMUM VALID FUTURE ETA: {min_future_time}\n"
                    f"LIVE SCIENTIFIC TELEMETRY BATCH:\n{json.dumps(sample_records, indent=1)}\n\n"
                    "INSTRUCTIONS: FORECAST FUTURE CRISES (ETA 12h to 72h Ahead)\n"
                    "1. DO NOT output past events. All predictions must have an ETA strictly 12 to 72 hours from now.\n"
                    "2. Earthquake records: compute the probability and ETA of secondary fault stress transfer / triggered aftershocks.\n"
                    "3. Satellite heat anomalies: compute downwind perimeter breach window.\n"
                    "4. Lat/Lon must come verbatim from telemetry.\n\n"
                    "Return ONLY valid JSON matching this schema:\n"
                    "{\n"
                    '  "primary_threat": "Forecasted [Phenomenon] Risk / Window",\n'
                    '  "location": "Region, Country",\n'
                    '  "time_of_event": "FUTURE ISO 8601 timestamp",\n'
                    '  "severity": "Moderate | Severe | Extreme",\n'
                    '  "probability": "calibrated percentage string (e.g. 62%)",\n'
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
        print(f"[*] Dispatching {len(telemetry_batch)} events to AI model...", flush=True)
        res = requests.post(url, headers=headers, json=payload, timeout=35)
        if res.status_code == 200:
            choice = res.json()["choices"][0]["message"]
            
            thinking = choice.get("reasoning") or choice.get("reasoning_content")
            if thinking:
                print("\n" + "="*50, flush=True)
                print("🧠 [AI FUTURE REASONING LOG]:", flush=True)
                print(thinking.strip(), flush=True)
                print("="*50 + "\n", flush=True)

            ai_result = json.loads(choice["content"])
            ai_result["is_demo"] = False

            # Verify future timestamp
            try:
                event_dt = datetime.fromisoformat(ai_result["time_of_event"].replace("Z", "+00:00"))
                if event_dt <= now + timedelta(hours=1):
                    ai_result["time_of_event"] = (now + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                ai_result["time_of_event"] = (now + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")

            return ai_result
        else:
            print(f"❌ Groq API Error {res.status_code}: {res.text}", flush=True)
    except Exception as e:
        print(f"❌ Groq Exception: {e}", flush=True)

    return None

def callback(ch, method, properties, body):
    try:
        events = json.loads(body)
    except Exception:
        events = []

    print(f"\n📩 Message broker delivered {len(events)} telemetry readings.", flush=True)
    prediction = forecast_future_disasters(events)
    if prediction:
        insert_prediction(prediction)
        print(f"[✓] Saved to DB: {prediction['primary_threat']} at {prediction['location']} (ETA: {prediction['time_of_event']})", flush=True)

def start_consumer():
    init_db()
    # Immediate boot test so database is populated right away
    boot_events = [
        {"title": "USGS M5.2 Subduction Activity", "place": "Off Coast of Oregon, USA", "lat": 43.6, "lon": -127.8, "magnitude": 5.2, "stream": "USGS_SEISMIC"},
        {"title": "NASA Satellite Radiative Hotspot", "lat": -14.2, "lon": 132.1, "fire_radiative_power_mw": 380.0, "stream": "NASA_FIRMS"}
    ]
    test_result = forecast_future_disasters(boot_events)
    if test_result:
        insert_prediction(test_result)

    print("🤖 Consumer Online. Waiting for live RabbitMQ streams...", flush=True)
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
