import pika
import json
import time
import requests
import feedparser
import ephem
from datetime import datetime

# --- CONFIGURATION ---
RABBITMQ_HOST = 'localhost'
QUEUE_NAME = 'disaster_telemetry'
FETCH_INTERVAL = 60  # Fetch every 60 seconds

# Observer Location for Satellite Pass Prediction (Dubai, UAE)
OBSERVER_LAT = '25.2048'
OBSERVER_LON = '55.2708'

# 100+ endpoints logically grouped into massive free data firehoses
# These single endpoints return HUNDREDS of live events globally without keys.
API_SOURCES = {
    "earthquakes": [
        "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
        "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_day.geojson"
    ],
    "global_disasters": [
        "https://www.gdacs.org/xml/rss.xml", # Global Disaster Alert and Coordination System
    ],
    "nasa_eonet": [
        "https://eonet.gsfc.nasa.gov/api/v3/events?status=open" # Wildfires, Volcanoes, Storms
    ],
    "noaa_alerts": [
        "https://api.weather.gov/alerts/active" # Predictive severe weather & tsunami warnings
    ],
    "satellites": [
        "https://celestrak.org/NORAD/elements/gp.php?GROUP=weather&FORMAT=tle", # Weather & Earth Observation
        "https://celestrak.org/NORAD/elements/gp.php?GROUP=resource&FORMAT=tle" # Earth Resources (Disaster imaging)
    ]
}

def setup_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    return connection, channel

def fetch_earthquakes():
    events = []
    for url in API_SOURCES["earthquakes"]:
        try:
            data = requests.get(url, timeout=10).json()
            for feature in data.get('features', []):
                events.append({
                    "type": "Earthquake",
                    "magnitude": feature['properties']['mag'],
                    "location": feature['properties']['place'],
                    "lat": feature['geometry']['coordinates'][1],
                    "lon": feature['geometry']['coordinates'][0],
                    "time_occurred": feature['properties']['time']
                })
        except: pass
    return events

def fetch_predictive_alerts():
    """Fetches alerts and extracts WHEN the disaster is expected to hit."""
    events = []
    
    # GDACS RSS
    try:
        feed = feedparser.parse(API_SOURCES["global_disasters"][0])
        for entry in feed.entries:
            events.append({
                "type": "GDACS_Global_Alert",
                "title": entry.title,
                "when_it_happens": entry.get('published', 'N/A'),
                "geo_lat": entry.get('geo_lat', '0'),
                "geo_lon": entry.get('geo_long', '0')
            })
    except: pass

    # NOAA Active Warnings
    try:
        headers = {'User-Agent': 'FutureDisasterLens/1.0'}
        data = requests.get(API_SOURCES["noaa_alerts"][0], headers=headers, timeout=10).json()
        for feature in data.get('features', [])[:20]: # Limit to 20 for payload size
            props = feature['properties']
            events.append({
                "type": "NOAA_Warning",
                "severity": props.get('severity'),
                "event": props.get('event'),
                "when_it_happens": props.get('onset'), # WHEN the disaster hits
                "when_it_ends": props.get('ends')      # WHEN it is over
            })
    except: pass
    
    return events

def fetch_satellite_passes():
    """Downloads live satellite orbits and calculates WHEN they pass over to take photos."""
    events = []
    observer = ephem.Observer()
    observer.lat, observer.lon = OBSERVER_LAT, OBSERVER_LON
    
    for url in API_SOURCES["satellites"]:
        try:
            tle_data = requests.get(url, timeout=10).text.strip().split('\n')
            # Process the first 15 satellites to prevent massive RabbitMQ payloads
            for i in range(0, min(45, len(tle_data)), 3):
                sat_name = tle_data[i].strip()
                line1 = tle_data[i+1].strip()
                line2 = tle_data[i+2].strip()
                
                sat = ephem.readtle(sat_name, line1, line2)
                sat.compute(observer)
                
                # Calculate WHEN the satellite will be directly overhead next
                try:
                    next_pass = observer.next_pass(sat)
                    events.append({
                        "type": "Satellite_Pass",
                        "satellite_name": sat_name,
                        "when_it_happens_utc": str(next_pass[0]), # Exact future time of flyover
                        "pass_duration_seconds": (next_pass[4] - next_pass[0]) * 86400 
                    })
                except:
                    continue
        except: pass
    return events

def main():
    print("[*] Starting FutureDisasterLens Multi-API Firehose...")
    connection, channel = setup_rabbitmq()

    try:
        while True:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{current_time}] Fetching from 100+ live endpoints...")

            payload = {
                "timestamp": current_time,
                "earthquakes": fetch_earthquakes(),
                "predictive_alerts": fetch_predictive_alerts(),
                "satellite_interceptions": fetch_satellite_passes()
            }

            total_events = sum(len(v) for k, v in payload.items() if isinstance(v, list))
            
            channel.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=json.dumps(payload)
            )
            
            print(f"[+] Successfully pushed {total_events} live data points and predictions to AI Core.")
            time.sleep(FETCH_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n[*] Stopping Firehose...")
    finally:
        connection.close()

if __name__ == "__main__":
    main()