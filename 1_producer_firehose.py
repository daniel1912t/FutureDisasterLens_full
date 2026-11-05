import os
import json
import time
import pika

try:
    import feedparser
except ImportError:
    feedparser = None

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
QUEUE_NAME = 'disaster_telemetry'

def main():
    print("[*] Starting Firehose Producer...", flush=True)
    while True:
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            chan = conn.channel()
            chan.queue_declare(queue=QUEUE_NAME, durable=True)
            
            data = [
                {"title": "NOAA Weather Alert: High Wind", "lat": 25.2048, "lon": 55.2708, "severity": "High", "source": "NOAA"},
                {"title": "Seismic Activity Detected", "lat": 35.6762, "lon": 139.6503, "severity": "Medium", "source": "USGS"}
            ]
            chan.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=json.dumps(data),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print(f"[+] Published {len(data)} telemetry records to AI Core.", flush=True)
            conn.close()
        except Exception as e:
            print(f"[*] Firehose waiting for RabbitMQ... ({e})", flush=True)
        time.sleep(60)

if __name__ == "__main__":
    main()
