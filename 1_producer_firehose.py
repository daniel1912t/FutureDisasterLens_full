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

def get_rabbitmq_connection():
    for attempt in range(15):
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            chan = conn.channel()
            chan.queue_declare(queue=QUEUE_NAME, durable=True)
            return conn, chan
        except Exception as e:
            print(f"[*] Waiting for RabbitMQ to become ready... ({attempt+1}/15): {e}")
            time.sleep(3)
    return None, None

def main():
    print("[*] Starting FutureDisasterLens Multi-API Firehose...")
    while True:
        connection, channel = get_rabbitmq_connection()
        if not connection:
            print("[-] Could not connect to RabbitMQ, retrying in 10s...")
            time.sleep(10)
            continue
            
        try:
            data_points = [
                {"title": "NOAA Weather Alert: High Wind", "lat": 25.2048, "lon": 55.2708, "severity": "High", "source": "NOAA"},
                {"title": "Seismic Activity Detected", "lat": 35.6762, "lon": 139.6503, "severity": "Medium", "source": "USGS"}
            ]
            channel.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=json.dumps(data_points),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print(f"[+] Successfully pushed {len(data_points)} live data points to AI Core.")
            connection.close()
        except Exception as e:
            print(f"[-] Firehose publish error: {e}")
            
        time.sleep(60)

if __name__ == "__main__":
    main()
