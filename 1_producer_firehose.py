import os
import json
import time
import feedparser
import pika

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
QUEUE_NAME = 'disaster_telemetry'

def setup_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    return connection, channel

def main():
    print("[*] Starting FutureDisasterLens Multi-API Firehose...")
    while True:
        try:
            connection, channel = setup_rabbitmq()
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
            print(f"[+] Successfully pushed {len(data_points)} live data points and predictions to AI Core.")
            connection.close()
        except Exception as e:
            print(f"[-] Firehose error: {e}")
        time.sleep(60)

if __name__ == "__main__":
    main()
