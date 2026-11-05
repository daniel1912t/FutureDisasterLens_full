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

# Auto-purge stale prediction data on startup
if os.path.exists(DATA_FILE):
    try:
        os.remove(DATA_FILE)
        print(f"[!] Purged old data file: {DATA_FILE}", flush=True)
    except Exception as e:
        print(f"[-] Could not delete old data: {e}", flush=True)

def analyze_with_groq(all_events):
    if not GROQ_API_KEY:
        print("[-] GROQ_API_KEY is missing from environment variables!", flush=True)
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
                    f"You are an expert disaster telemetry AI. The live UTC time is {current_time_iso}. "
                    "Analyze the raw telemetry feed, evaluate cascading risks, and output ONLY a valid JSON object with keys: "
                    "primary_threat, location, time_of_event, severity, probability, lat, lon, cascading_effects (array)."
                )
            },
            {
                "role": "user",
                "content": f"Live telemetry feed: {json.dumps(all_events)}"
            }
        ],
        "response_format": {"type": "json_object"},
        # Tells Groq to return the chain-of-thought in message.reasoning
        "include_reasoning": True
    }

    try:
        print("[*] Contacting Groq AI Core (openai/gpt-oss-120b)...", flush=True)
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            result_json = response.json()
            message = result_json["choices"][0]["message"]
            
            # Extract and print AI internal chain-of-thought to Render logs
            reasoning = message.get("reasoning")
            if reasoning:
                print("\n" + "="*50, flush=True)
                print("🧠 [AI REASONING / THINKING TRACE]:", flush=True)
                print(reasoning.strip(), flush=True)
                print("="*50 + "\n", flush=True)
            else:
                print("[*] (Model reasoned directly into output)", flush=True)

            content = message["content"]
            return json.loads(content)
        else:
            print(f"[-] Groq API error ({response.status_code}): {response.text}", flush=True)
    except Exception as e:
        print(f"[-] Groq request failure: {e}", flush=True)
        
    return None

def callback(ch, method, properties, body):
    try:
        all_events = json.loads(body)
    except Exception:
        all_events = []

    print(f"\n[+] Telemetry received: {len(all_events)} data points. Triggering reasoning...", flush=True)
    ai_result = analyze_with_groq(all_events)

    if ai_result:
        os.makedirs('/app/data', exist_ok=True)
        with open(DATA_FILE, 'w') as f:
            json.dump(ai_result, f)
        print(f"[✓] Final JSON payload saved to {DATA_FILE}", flush=True)
    else:
        print("[-] Inference returned no result. State unchanged.", flush=True)

def start_ai_consumer():
    print("🤖 Multi-Agent AI Core Online. Listening on queue 'disaster_telemetry'...", flush=True)
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
            channel.start_consuming()
        except Exception as e:
            print(f"[-] Connection to RabbitMQ failed: {e}. Retrying in 5s...", flush=True)
            time.sleep(5)

if __name__ == "__main__":
    start_ai_consumer()
