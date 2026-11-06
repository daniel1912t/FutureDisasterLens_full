import os
import json
import time
import requests
from datetime import datetime, timezone
import pika

try:
    import feedparser
except ImportError:
    feedparser = None

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
QUEUE_NAME = 'disaster_telemetry'

USGS_FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson"
GDACS_FEED_URL = "https://www.gdacs.org/xml/rss.xml"

def fetch_live_usgs_earthquakes():
    """Fetches real earthquakes magnitude 2.5+ from the past 24 hours."""
    records = []
    try:
        res = requests.get(USGS_FEED_URL, timeout=10)
        if res.status_code == 200:
            data = res.json()
            features = data.get("features", [])[:10]  # Take top 10 most recent
            for f in features:
                props = f.get("properties", {})
                geom = f.get("geometry", {})
                coords = geom.get("coordinates", [0, 0, 0])
                mag = props.get("mag")
                place = props.get("place")
                time_epoch = props.get("time") / 1000 if props.get("time") else time.time()
                iso_time = datetime.fromtimestamp(time_epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                records.append({
                    "title": f"USGS M{mag} Earthquake - {place}",
                    "lat": coords[1],
                    "lon": coords[0],
                    "depth_km": coords[2],
                    "magnitude": mag,
                    "reported_at": iso_time,
                    "source": "USGS Realtime",
                    "type": "Earthquake"
                })
    except Exception as e:
        print(f"[-] USGS fetch error: {e}", flush=True)
    return records

def fetch_live_gdacs_alerts():
    """Fetches active cyclones, floods, tsunamis, and volcanoes worldwide."""
    records = []
    if not feedparser:
        return records
    try:
        feed = feedparser.parse(GDACS_FEED_URL)
        for entry in feed.entries[:8]:
            title = entry.get("title", "Global Alert")
            lat = getattr(entry, "geo_lat", None)
            lon = getattr(entry, "geo_long", None)
            if lat and lon:
                records.append({
                    "title": title,
                    "lat": float(lat),
                    "lon": float(lon),
                    "published": entry.get("published", ""),
                    "source": "GDACS UN-OCHA",
                    "type": "Severe Alert"
                })
    except Exception as e:
        print(f"[-] GDACS fetch error: {e}", flush=True)
    return records

def get_rabbitmq_connection():
    for _ in range(10):
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            chan = conn.channel()
            chan.queue_declare(queue=QUEUE_NAME, durable=True)
            return conn, chan
        except Exception:
            time.sleep(3)
    return None, None

def main():
    print("[*] Starting Genuine Disaster Telemetry Firehose (USGS + GDACS)...", flush=True)
    while True:
        live_data = []
        live_data.extend(fetch_live_usgs_earthquakes())
        live_data.extend(fetch_live_gdacs_alerts())

        if not live_data:
            print("[!] No live data returned from APIs. Retrying in 15s...", flush=True)
            time.sleep(15)
            continue

        print(f"[+] Retrieved {len(live_data)} real active global disaster records.", flush=True)
        
        conn, chan = get_rabbitmq_connection()
        if conn and chan:
            try:
                chan.basic_publish(
                    exchange='',
                    routing_key=QUEUE_NAME,
                    body=json.dumps(live_data),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                print(f"[✓] Published {len(live_data)} verified events to RabbitMQ.", flush=True)
                conn.close()
            except Exception as e:
                print(f"[-] Publish error: {e}", flush=True)
        else:
            print("[-] RabbitMQ unavailable. Retrying in 10s...", flush=True)

        # Pull updates every 90 seconds
        time.sleep(90)

if __name__ == "__main__":
    main()
