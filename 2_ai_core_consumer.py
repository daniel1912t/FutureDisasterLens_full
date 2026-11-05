import os
import json
import time
from datetime import datetime, timezone
import pika
import requests

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
QUEUE_NAME = 'disaster_telemetry'
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')
DATA_FILE = '/app/data/latest_prediction.json'

# Auto-delete old prediction data on process start
if os.path.exists(DATA_FILE):
    try:
        os.remove(DATA_FILE)
        print(f"[!] Purged old data file: {DATA_FILE}")
    except Exception as e:
        print(f"[-] Could not delete old data: {e}")

def analyze_with_groq(all_events):
    if not GROQ_API_KEY:
        print("[-] GROQ_API_KEY is missing from environment variables!")
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
                    f"You are an expert disaster telemetry AI. The current UTC time is {current_time_iso}. "
                    "Analyze the incoming data points and generate a real-time risk assessment. "
                    "Output ONLY a raw, valid JSON object with keys: "
                    "primary_threat, location, time_of_event, severity, probability, lat, lon, cascading_effects (array)."
                )
            },
            {
                "role": "user",
                "content": f"Telemetry: {json.dumps(all_events)}"
            }
        ],
        "response_format": {"type": "json_object"}
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=25)
        if response.status_code == 200:
            content = response.json()["choices"][0]["message"]["content"]
            return json.loads(content)
        else:
            print(f"[-] Groq API error ({response.status_code}): {response.text}")
    except Exception as e:
        print(f"[-] Groq connection failure: {e}")
    return None

def callback(ch, method, properties, body):
    try:
        all_events = json.loads(body)
    except Exception:
        all_events = []

    print(f"[*] Received {len(all_events)} telemetry points. Requesting AI analysis...")
    ai_result = analyze_with_groq(all_events)

    if ai_result:
        os.makedirs('/app/data', exist_ok=True)
        with open(DATA_FILE, 'w') as f:
            json.dump(ai_result, f)
        print(f"[+] SUCCESS: Written fresh AI prediction at {ai_result.get('time_of_event')}")
    else:
        print("[-] AI analysis produced no output; skipping file write.")

def start_ai_consumer():
    print("🤖 AI Core Online. Waiting for RabbitMQ messages...")
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
            channel.start_consuming()
        except Exception as e:
            print(f"[-] RabbitMQ connection error: {e}. Retrying in 5s...")
            time.sleep(5)

if __name__ == "__main__":
    start_ai_consumer()
