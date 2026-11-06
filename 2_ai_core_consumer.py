import os
import sys
import json
import time
from datetime import datetime, timezone
import pika
import requests

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
QUEUE_NAME = 'disaster_telemetry'
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')
DATA_FILE = '/app/data/latest_prediction.json'

print("=== [BOOT] AI CORE (100+ MULTI-STREAM SYNTHESIZER) ===", flush=True)

if os.path.exists(DATA_FILE):
    try:
        os.remove(DATA_FILE)
    except Exception:
        pass

def synthesize_planetary_telemetry(telemetry_batch):
    if not GROQ_API_KEY:
        print("❌ GROQ_API_KEY missing from environment variables!", flush=True)
        return None

    current_time_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    # Pass summary metrics if payload is very large
    sample_records = telemetry_batch[:65]

    payload = {
        "model": "openai/gpt-oss-120b",
        "messages": [
            {
                "role": "user",
                "content": (
                    f"Current UTC System Time: {current_time_iso}\n"
                    f"Total Incoming Planetary Telemetry Records: {len(telemetry_batch)}\n"
                    f"Raw Multi-Stream Sensor Batch:\n{json.dumps(sample_records, indent=1)}\n\n"
                    "SCIENTIFIC MISSION INSTRUCTIONS:\n"
                    "1. Correlate across ALL streams: OEF Seismic Hypocenters, NASA MODIS Thermal Radiative Power, NOAA DSCOVR Space Plasma, and Severe Weather alerts.\n"
                    "2. Identify the single most dangerous anomaly on Earth right now.\n"
                    "3. Extract its verified latitude and longitude verbatim from the sensor that detected it.\n"
                    "4. Calibrate the probability strictly using the sensor data (e.g., M5.0+ quake or NASA FRP > 150MW = 95-100% detection; barometric low pressure drops = calculated forecast probability).\n"
                    "5. Output 3-4 realistic cascading regional impacts.\n\n"
                    "Return ONLY valid JSON with this exact format:\n"
                    "{\n"
                    '  "primary_threat": "Phenomenon name + sensor metric (e.g. M5.8 Subduction Rupture / 350MW Orbital Anomaly)",\n'
                    '  "location": "Region, Country",\n'
                    '  "time_of_event": "Exact timestamp from sensor",\n'
                    '  "severity": "Low | Moderate | Severe | Extreme",\n'
                    '  "probability": "calibrated percentage",\n'
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
        print(f"[*] Dispatching {len(telemetry_batch)} planetary data points to 120B AI...", flush=True)
        res = requests.post(url, headers=headers, json=payload, timeout=35)
        if res.status_code == 200:
            choice = res.json()["choices"][0]["message"]
            
            thinking = choice.get("reasoning") or choice.get("reasoning_content")
            if thinking:
                print("\n" + "="*50, flush=True)
                print("🧠 [100+ MULTI-STREAM PLANETARY REASONING TRACE]:", flush=True)
                print(thinking.strip(), flush=True)
                print("="*50 + "\n", flush=True)

            return json.loads(choice["content"])
        else:
            print(f"[-] Groq API error {res.status_code}: {res.text}", flush=True)
    except Exception as e:
        print(f"[-] Inference exception: {e}", flush=True)

    return None

def callback(ch, method, properties, body):
    try:
        events = json.loads(body)
    except Exception:
        events = []

    print(f"\n📩 Message Broker Received Batch: {len(events)} real sensor data points.", flush=True)
    threat = synthesize_planetary_telemetry(events)
    if threat:
        os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
        with open(DATA_FILE, 'w') as f:
            json.dump(threat, f)
        print(f"[✓] PRIMARY GLOBAL THREAT LOGGED: {threat.get('primary_threat')} at {threat.get('location')} ({threat.get('lat')}, {threat.get('lon')})", flush=True)

def start_consumer():
    print("🤖 Multi-Stream Consumer Online. Listening on 'disaster_telemetry'...", flush=True)
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
