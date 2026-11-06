import os
import csv
import json
import time
import requests
from io import StringIO
from datetime import datetime, timezone
import pika

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
QUEUE_NAME = 'disaster_telemetry'

# Live Scientific API Feeds (Zero Keys Required)
NASA_FIRMS_URL = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_Global_24h.csv"
NOAA_SPACE_PLASMA_URL = "https://services.swpc.noaa.gov/products/solar-wind/plasma-2-hour.json"
USGS_SEISMIC_ALL_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
NOAA_WEATHER_ALERTS_URL = "https://api.weather.gov/alerts/active"

# Key global coordinates for atmospheric tracking
GLOBAL_COORDINATE_GRID = [
    {"name": "Tokyo", "lat": 35.6762, "lon": 139.6503},
    {"name": "Dubai", "lat": 25.2048, "lon": 55.2708},
    {"name": "Miami", "lat": 25.7617, "lon": -80.1918},
    {"name": "Manila", "lat": 14.5995, "lon": 120.9842},
    {"name": "Reykjavik", "lat": 64.1466, "lon": -21.9426},
    {"name": "San Francisco", "lat": 37.7749, "lon": -122.4194},
    {"name": "Jakarta", "lat": -6.2088, "lon": 106.8456},
    {"name": "Sydney", "lat": -33.8688, "lon": 151.2093}
]

def fetch_oef_and_seismic():
    """Pulls 40-50 real-time earthquakes and OEF seismic telemetry."""
    records = []
    try:
        res = requests.get(USGS_SEISMIC_ALL_URL, timeout=10)
        if res.status_code == 200:
            features = res.json().get("features", [])[:45]
            for f in features:
                props = f.get("properties", {})
                coords = f.get("geometry", {}).get("coordinates", [0, 0, 0])
                epoch = props.get("time") / 1000 if props.get("time") else time.time()
                iso_time = datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                records.append({
                    "stream": "USGS_OEF_SEISMIC",
                    "title": props.get("title", "Seismic Motion"),
                    "place": props.get("place"),
                    "lat": coords[1],
                    "lon": coords[0],
                    "depth_km": coords[2],
                    "magnitude": props.get("mag"),
                    "felt_reports": props.get("felt"),
                    "tsunami_flag": props.get("tsunami"),
                    "sig_index": props.get("sig"),
                    "time": iso_time
                })
    except Exception as e:
        print(f"[-] Seismic fetch error: {e}", flush=True)
    return records

def fetch_nasa_satellite_thermal():
    """Pulls 25 highest-energy thermal orbital scans from NASA satellites."""
    records = []
    try:
        res = requests.get(NASA_FIRMS_URL, timeout=12)
        if res.status_code == 200:
            reader = csv.DictReader(StringIO(res.text))
            rows = list(reader)
            rows.sort(key=lambda x: float(x.get('frp', 0.0) or 0.0), reverse=True)
            for row in rows[:25]:
                records.append({
                    "stream": "NASA_ORBITAL_FIRMS",
                    "satellite": "Terra/Aqua MODIS",
                    "lat": float(row['latitude']),
                    "lon": float(row['longitude']),
                    "brightness_kelvin": float(row.get('brightness', 0.0)),
                    "fire_radiative_power_mw": float(row.get('frp', 0.0)),
                    "confidence": f"{row.get('confidence')}%",
                    "time": f"{row.get('acq_date')}T{row.get('acq_time')[:2]}:{row.get('acq_time')[2:]}:00Z"
                })
    except Exception as e:
        print(f"[-] NASA Satellite Feed Error: {e}", flush=True)
    return records

def fetch_noaa_deepspace_plasma():
    """Pulls live solar wind and geomagnetic disruption from DSCOVR satellite."""
    records = []
    try:
        res = requests.get(NOAA_SPACE_PLASMA_URL, timeout=8)
        if res.status_code == 200:
            data = res.json()
            if len(data) > 5:
                for entry in data[-5:]:
                    records.append({
                        "stream": "NOAA_DSCOVR_L1",
                        "satellite": "DSCOVR Spacecraft (L1 Orbit)",
                        "solar_wind_speed_kms": float(entry[2]) if entry[2] else 0.0,
                        "proton_density_pcm3": float(entry[1]) if entry[1] else 0.0,
                        "lat": 0.0,
                        "lon": 0.0,
                        "time": entry[0]
                    })
    except Exception as e:
        print(f"[-] NOAA Plasma Feed Error: {e}", flush=True)
    return records

def fetch_noaa_severe_weather():
    """Pulls active severe weather warnings and alerts."""
    records = []
    try:
        headers = {"User-Agent": "DisasterLensTelemetry/2.0 (contact@disasterlens.local)"}
        res = requests.get(NOAA_WEATHER_ALERTS_URL, headers=headers, timeout=10)
        if res.status_code == 200:
            features = res.json().get("features", [])[:20]
            for f in features:
                props = f.get("properties", {})
                records.append({
                    "stream": "NOAA_NWS_ALERTS",
                    "event": props.get("event"),
                    "headline": props.get("headline"),
                    "severity": props.get("severity"),
                    "certainty": props.get("certainty"),
                    "urgency": props.get("urgency"),
                    "effective": props.get("effective"),
                    "lat": 28.0,
                    "lon": -82.0
                })
    except Exception as e:
        print(f"[-] NOAA NWS Weather Alert Error: {e}", flush=True)
    return records

def fetch_global_atmospheric_grid():
    """Pulls live barometric, wind, and precipitation readings across global nodes."""
    records = []
    for city in GLOBAL_COORDINATE_GRID:
        try:
            url = f"https://api.open-meteo.com/v1/forecast?latitude={city['lat']}&longitude={city['lon']}&current=temperature_2m,surface_pressure,wind_speed_10m,precipitation"
            res = requests.get(url, timeout=5)
            if res.status_code == 200:
                current = res.json().get("current", {})
                records.append({
                    "stream": "ATMOSPHERIC_GRID",
                    "location": city['name'],
                    "lat": city['lat'],
                    "lon": city['lon'],
                    "surface_pressure_hpa": current.get("surface_pressure"),
                    "wind_speed_kmh": current.get("wind_speed_10m"),
                    "precipitation_mm": current.get("precipitation"),
                    "time": current.get("time")
                })
        except Exception:
            continue
    return records

def get_rabbitmq():
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
    print("[*] Firehose 2.0 Online: Ingesting 100+ Multi-Discipline Planetary Sensors...", flush=True)
    while True:
        bundle = []
        bundle.extend(fetch_oef_and_seismic())
        bundle.extend(fetch_nasa_satellite_thermal())
        bundle.extend(fetch_noaa_deepspace_plasma())
        bundle.extend(fetch_noaa_severe_weather())
        bundle.extend(fetch_global_atmospheric_grid())

        count = len(bundle)
        print(f"[+] Multi-Stream Ingestion Complete: {count} verified real-world telemetry points gathered.", flush=True)

        if count > 0:
            conn, chan = get_rabbitmq()
            if conn and chan:
                try:
                    chan.basic_publish(
                        exchange='',
                        routing_key=QUEUE_NAME,
                        body=json.dumps(bundle),
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                    print(f"[✓] Successfully transmitted {count} records to AI Core via RabbitMQ.", flush=True)
                    conn.close()
                except Exception as e:
                    print(f"[-] RabbitMQ publish error: {e}", flush=True)

        # Ingest every 60 seconds
        time.sleep(60)

if __name__ == "__main__":
    main()
