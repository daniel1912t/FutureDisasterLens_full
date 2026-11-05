import os
import pika
import json
import random
import itertools
from datetime import datetime, timedelta
from groq import Groq

# 1. Load-Balancer: Rotate between multiple API keys
API_KEYS = [
    "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",
    "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"
]
key_iterator = itertools.cycle(API_KEYS)

MODEL_ID = "openai/gpt-oss-120b"
QUEUE_NAME = 'disaster_telemetry'
DATA_FILE = "data/latest_prediction.json"
ALL_DATA_FILE = "data/all_threats.json"
is_synthesizing = False 

def synthesize_disaster_prediction(payload):
    current_key = next(key_iterator)
    client = Groq(api_key=current_key)
    
    print(f"\n🧠 [COORDINATOR] Processing top 12 events (Using API Key: ...{current_key[-4:]})")
    
    now = datetime.now()
    prediction_hours = random.randint(12, 48)
    future_window = now + timedelta(hours=prediction_hours)
    
    current_time_str = now.strftime("%A, %d %b %Y %H:%M:%S")
    target_time_str = future_window.strftime("%A, %d %b %Y %H:%M:%S")
    
    print(f"🕒 Prediction Target: {prediction_hours} hours ahead ({target_time_str})")

    # 2. Token-Safe Slicing (Max 12 per category)
    raw_quakes = payload.get("earthquakes", [])
    geo_subset = raw_quakes[:12] 
    geo_data = json.dumps(geo_subset)
    
    try:
        geo_response = client.chat.completions.create(
            model=MODEL_ID,
            messages=[{"role": "user", "content": f"Analyze this raw seismic data and report anomalies: {geo_data}"}]
        )
        geo_analysis = geo_response.choices[0].message.content
    except Exception as e:
        geo_analysis = "Geological data inconclusive."
        print(f"Geo Agent Error: {e}")

    raw_alerts = payload.get("predictive_alerts", [])
    raw_sats = payload.get("satellite_interceptions", [])
    atmos_subset = {
        "alerts": raw_alerts[:12],
        "satellites": raw_sats[:12]
    }
    atmos_data = json.dumps(atmos_subset)
    
    try:
        atmos_response = client.chat.completions.create(
            model=MODEL_ID,
            messages=[{"role": "user", "content": f"Analyze this weather/alert data for anomalies: {atmos_data}"}]
        )
        atmos_analysis = atmos_response.choices[0].message.content
    except Exception as e:
        atmos_analysis = "Atmospheric data inconclusive."
        print(f"Atmos Agent Error: {e}")

    # 3. Coordinator Synthesis
    print("🧠 [COORDINATOR] Fusing agent reports with deep geographical reasoning...")
    prompt = f"""
    SYSTEM CLOCK - CURRENT TIME: {current_time_str}
    PREDICTION WINDOW: {target_time_str} (Exactly {prediction_hours} hours from now)
    
    CRITICAL COMMAND: You are an advanced forecasting engine. Do not merely parrot the raw data. You must MERGE the live telemetry patterns with your deep reasoning capabilities. 
    
    1. Cross-reference the live anomaly patterns with your internal knowledge of global geological fault lines, atmospheric physics, and historical disaster vulnerabilities.
    2. Use physical reasoning to calculate the exact probability percentage of this cascading event occurring near the PREDICTION WINDOW.
    3. Your predicted 'time_of_event' MUST occur in the future, near the PREDICTION WINDOW. Do not output past dates.
    
    Geo Report: {geo_analysis}
    Atmos Report: {atmos_analysis}
    
    Respond EXACTLY in this JSON format (no markdown tags, no backticks, just raw JSON):
    {{
        "primary_threat": "Disaster Type",
        "location": "City, Country",
        "lat": 35.68,
        "lon": 139.76,
        "time_of_event": "Day, DD Mon YYYY HH:MM:SS",
        "severity": "High",
        "probability": "85%",
        "cascading_effects": ["Event 1", "Event 2"]
    }}
    """
    
    try:
        response = client.chat.completions.create(
            model=MODEL_ID,
            messages=[{"role": "user", "content": prompt}]
        )
        
        clean_json = response.choices[0].message.content.replace('```json', '').replace('```', '').strip()
        prediction_data = json.loads(clean_json)
        
        os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
        
        # 1. Overwrite the latest prediction for the live UI
        with open(DATA_FILE, 'w') as f:
            json.dump(prediction_data, f, indent=4)
            
        # 2. Append to the master log of all threats
        all_threats = []
        if os.path.exists(ALL_DATA_FILE):
            with open(ALL_DATA_FILE, 'r') as f:
                try:
                    all_threats = json.load(f)
                except json.JSONDecodeError:
                    pass
                    
        all_threats.append(prediction_data)
        
        with open(ALL_DATA_FILE, 'w') as f:
            json.dump(all_threats, f, indent=4)
            
        print(f"🚨 [PREDICTION SAVED] Target: {prediction_data['location']} at {prediction_data['time_of_event']} ({prediction_data.get('probability', 'N/A')})")
    except Exception as e:
        print(f"Synthesis failed: {e}")

def callback(ch, method, properties, body):
    global is_synthesizing
    
    if is_synthesizing:
        return
        
    try:
        payload = json.loads(body)
        total_events = sum(len(v) for k, v in payload.items() if isinstance(v, list))
        print(f"Received payload with {total_events} live data points. Triggering AI...")
        
        is_synthesizing = True
        synthesize_disaster_prediction(payload)
        is_synthesizing = False
            
    except Exception as e:
        print(f"Callback error: {e}")

def start_ai_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    channel.queue_declare(queue=QUEUE_NAME)
    
    print(f"🤖 Multi-Agent AI Core Online (Model: {MODEL_ID}). Waiting for telemetry on '{QUEUE_NAME}'...")
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == '__main__':
    start_ai_consumer()