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

print("=== [BOOT] AI CORE CONSUMER STARTED ===", flush=True)

if not GROQ_API_KEY:
    print("❌ FATAL: GROQ_API_KEY environment variable is missing!", flush=True)
else:
    print(f"✅ Detected GROQ_API_KEY (Length: {len(GROQ_API_KEY)})", flush=True)

# Delete stale prediction files
if os.path.exists(DATA_FILE):
    try:
        os.remove(DATA_FILE)
        print(f"[!] Purged stale prediction cache: {DATA_FILE}", flush=True)
    except Exception as e:
        print(f"[-] Could not delete stale cache: {e}", flush=True)

def analyze_with_groq(all_events):
    if not GROQ_API_KEY:
        print("[-] Skipping: No API key found.", flush=True)
        return None

    current_time_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    # Only reasoning_format="parsed" is sent (never include_reasoning alongside it)
    payload = {
        "model": "openai/gpt-oss-120b",
        "messages": [
            {
                "role": "user",
                "content": (
                    f"Current UTC time: {current_time_iso}.\n"
                    f"Telemetry data: {json.dumps(all_events)}\n\n"
                    "Think step-by-step and assess all disaster risks. "
                    "Output ONLY a valid JSON object matching this schema:\n"
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
            }
        ],
        "response_format": {"type": "json_object"},
        "reasoning_format": "parsed",
        "reasoning_effort": "medium"
    }

    try:
        print("[*] Contacting Groq API (openai/gpt-oss-120b)...", flush=True)
        response = requests.post(url, headers=headers, json=payload, timeout=35)
        
        if response.status_code == 200:
            res_data = response.json()
            choice = res_data["choices"][0]["message"]
            
            # Extract and print reasoning
            thinking = choice.get("reasoning") or choice.get("reasoning_content")
            if thinking:
                print("\n" + "="*50, flush=True)
                print("🧠 [AI REASONING / THINKING TRACE]:", flush=True)
                print(thinking.strip(), flush=True)
                print("="*50 + "\n", flush=True)
            else:
                print("[*] (Model generated output directly)", flush=True)

            return json.loads(choice["content"])
        else:
            print(f"❌ Groq API Error {response.status_code}: {response.text}", flush=True)
    except Exception as e:
        print(f"❌ Request Error: {e}", flush=True)

    return None

def process_and_save(events):
    print(f"[*] Processing {len(events)} telemetry events through AI...", flush=True)
    result = analyze_with_groq(events)
    if result:
        os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
        with open(DATA_FILE, 'w') as f:
            json.dump(result, f)
        print(f"✅ SUCCESS: Saved prediction at {result.get('time_of_event')} to {DATA_FILE}", flush=True)
    else:
        print("[-] Inference failed; file not created.", flush=True)

def callback(ch, method, properties, body):
    try:
        events = json.loads(body)
    except Exception:
        events = []
    print(f"\n📩 Message received from RabbitMQ ({len(events)} points)!", flush=True)
    process_and_save(events)

def start_ai_consumer():
    # Immediate boot test cycle so thinking prints without waiting for RabbitMQ
    print("[*] Executing boot test cycle...", flush=True)
    test_events = [
        {"title": "Pacific Cyclone Alert", "lat": 14.5995, "lon": 120.9842, "severity": "High"},
        {"title": "Tokyo Seismic Anomaly", "lat": 35.6762, "lon": 139.6503, "severity": "Medium"}
    ]
    process_and_save(test_events)

    print("🤖 Consumer waiting for RabbitMQ messages on 'disaster_telemetry'...", flush=True)
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
