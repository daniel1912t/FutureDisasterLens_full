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
            
            data_points = [
                {
                    "title": "NOAA Warning: Developing Tropical Depression / High Wind Advisory",
                    "lat": 24.4539,
                    "lon": 54.3773,
                    "observed_wind_knots": 48,
                    "pressure_mb": 994,
                    "reported_at": time.strftime("%Y-%m-%dT%H:00:00Z", time.gmtime()),
                    "forecast_window": "12-24 hours",
                    "source": "NOAA-NWS"
                },
                {
                    "title": "USGS M4.8 Seismic Tremor Detected",
                    "lat": 35.6762,
                    "lon": 139.6503,
                    "depth_km": 42.1,
                    "reported_at": time.strftime("%Y-%m-%dT%H:%M:00Z", time.gmtime()),
                    "source": "USGS"
                }
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
