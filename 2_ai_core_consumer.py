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

print("=== [BOOT] AI CORE CONSUMER STARTING ===", flush=True)

if not GROQ_API_KEY:
    print("❌ FATAL ERROR: GROQ_API_KEY is NOT set in environment variables!", flush=True)
else:
    print(f"✅ GROQ_API_KEY is detected (length: {len(GROQ_API_KEY)})", flush=True)

# Clean out old data file
if os.path.exists(DATA_FILE):
    try:
        os.remove(DATA_FILE)
        print(f"[!] Purged old data file: {DATA_FILE}", flush=True)
    except Exception as e:
        print(f"[-] Could not delete old file: {e}", flush=True)

def analyze_with_groq(all_events):
    if not GROQ_API_KEY:
        print("[-] Skipping AI: GROQ_API_KEY is missing.", flush=True)
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
                "role": "system",
                "content": (
                    f"You are a real-time disaster telemetry AI. The live UTC time is {current_time_iso}. "
                    "Analyze the feed and output ONLY a valid JSON object matching this schema:\n"
                    "{\n"
                    '  "primary_threat": "string",\n'
                    '  "location": "string",\n'
                    f'  "time_of_event": "{current_time_iso}",\n'
                    '  "severity": "High",\n'
                    '  "probability": "90%",\n'
                    '  "lat": 25.2048,\n'
                    '  "lon": 55.2708,\n'
                    '  "cascading_effects": ["effect 1", "effect 2"]\n'
                    "}"
                )
            },
            {
                "role": "user",
                "content": f"Telemetry events: {json.dumps(all_events)}"
            }
        ],
        "response_format": {"type": "json_object"},
        "reasoning_format": "parsed",
        "reasoning_effort": "medium"
    }

    try:
        print(f"[*] Calling Groq API (openai/gpt-oss-120b)...", flush=True)
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            res_data = response.json()
            choice = res_data["choices"][0]["message"]
            
            # Print AI Thinking to Render Logs
            thinking = choice.get("reasoning") or choice.get("reasoning_content")
            if thinking:
                print("\n" + "="*50, flush=True)
                print("🧠 [AI REASONING / THINKING TRACE]:", flush=True)
                print(thinking.strip(), flush=True)
                print("="*50 + "\n", flush=True)
            else:
                print("[*] Groq responded (Reasoning integrated directly into output)", flush=True)

            parsed_content = json.loads(choice["content"])
            return parsed_content
        else:
            print(f"❌ Groq API Error Status {response.status_code}: {response.text}", flush=True)
    except Exception as e:
        print(f"❌ Groq Exception: {e}", flush=True)

    return None

def process_and_save(events):
    print(f"[*] Triggering AI pipeline for {len(events)} events...", flush=True)
    result = analyze_with_groq(events)
    if result:
        os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
        with open(DATA_FILE, 'w') as f:
            json.dump(result, f)
        print(f"✅ SUCCESS: Saved prediction at {result.get('time_of_event')} to {DATA_FILE}", flush=True)
    else:
        print("[-] AI returned no result. File not written.", flush=True)

def callback(ch, method, properties, body):
    try:
        events = json.loads(body)
    except Exception:
        events = []
    print(f"\n📩 Message received from RabbitMQ ({len(events)} points)!", flush=True)
    process_and_save(events)

def start_ai_consumer():
    # TEST RUN: Immediately trigger one run on startup so logs show thinking right away
    print("[*] Running initial test cycle on startup...", flush=True)
    sample_data = [
        {"title": "Pacific Cyclone Alert", "lat": 14.5995, "lon": 120.9842, "severity": "High"},
        {"title": "Seismic Sensor Anomaly", "lat": 35.6762, "lon": 139.6503, "severity": "Medium"}
    ]
    process_and_save(sample_data)

    print("🤖 Consumer waiting for live RabbitMQ events on 'disaster_telemetry'...", flush=True)
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
    start_ai_consumer()
