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

print("=== [BOOT] AI CORE LIVE TELEMETRY CONSUMER ===", flush=True)

if os.path.exists(DATA_FILE):
    try:
        os.remove(DATA_FILE)
        print(f"[!] Removed stale prediction: {DATA_FILE}", flush=True)
    except Exception as e:
        print(f"[-] Could not delete: {e}", flush=True)

def analyze_real_events(events):
    if not GROQ_API_KEY:
        print("❌ GROQ_API_KEY missing from environment!", flush=True)
        return None

    current_time_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
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
                    f"Current UTC Timestamp: {current_time_iso}\n\n"
                    f"Real Global Sensor Feeds:\n{json.dumps(events, indent=2)}\n\n"
                    "ANALYSIS CRITERIA:\n"
                    "1. Evaluate this authentic telemetry feed. Identify the single most severe active global event.\n"
                    "2. Extract its exact verified latitude, longitude, and geographic location name directly from the data.\n"
                    "3. Assign a calibrated statistical probability based on actual sensor magnitude or category (e.g. magnitude 5.0+ earthquake has 100% detection certainty; a cyclone landfall track is typically 40% - 75%). DO NOT invent arbitrary 90% numbers.\n"
                    "4. Predict 2-4 tangible cascading physical/economic consequences for that region.\n\n"
                    "Output ONLY a raw, valid JSON object with this exact structure:\n"
                    "{\n"
                    '  "primary_threat": "Exact name and magnitude/type",\n'
                    '  "location": "Region, Country",\n'
                    '  "time_of_event": "Actual event ISO timestamp or forecast window",\n'
                    '  "severity": "Low | Moderate | Severe | Extreme",\n'
                    '  "probability": "percentage string",\n'
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
        print("[*] Requesting live analysis from Groq AI...", flush=True)
        res = requests.post(url, headers=headers, json=payload, timeout=30)
        if res.status_code == 200:
            choice = res.json()["choices"][0]["message"]
            
            thinking = choice.get("reasoning") or choice.get("reasoning_content")
            if thinking:
                print("\n" + "="*50, flush=True)
                print("🧠 [LIVE REASONING TRACE]:", flush=True)
                print(thinking.strip(), flush=True)
                print("="*50 + "\n", flush=True)

            return json.loads(choice["content"])
        else:
            print(f"[-] Groq Error {res.status_code}: {res.text}", flush=True)
    except Exception as e:
        print(f"[-] Processing exception: {e}", flush=True)

    return None

def callback(ch, method, properties, body):
    try:
        events = json.loads(body)
    except Exception:
        events = []
    
    print(f"\n📩 Ingested {len(events)} authentic events from message broker.", flush=True)
    result = analyze_real_events(events)
    if result:
        os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
        with open(DATA_FILE, 'w') as f:
            json.dump(result, f)
        print(f"[✓] VERIFIED THREAT SAVED: {result.get('primary_threat')} at {result.get('location')}", flush=True)

def start_consumer():
    print("🤖 Telemetry Consumer Online. Listening on 'disaster_telemetry'...", flush=True)
    while True:
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            chan = conn.channel()
            chan.queue_declare(queue=QUEUE_NAME, durable=True)
            chan.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
            chan.start_consuming()
        except Exception as e:
            print(f"[-] Waiting for RabbitMQ... ({e})", flush=True)
            time.sleep(5)

if __name__ == "__main__":
    start_consumer()
