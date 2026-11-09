import os
import json
from datetime import datetime, timezone, timedelta
from flask import Flask, render_template, jsonify, make_response
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

DATA_DIR = os.path.join(os.getcwd(), 'data')
os.makedirs(DATA_DIR, exist_ok=True)

DATA_FILE = os.path.join(DATA_DIR, 'latest_prediction.json')
ALL_DATA_FILE = os.path.join(DATA_DIR, 'all_threats.json')
STATE_FILE = os.path.join(DATA_DIR, 'ai_state.json')

def purge_expired_records(items):
    """Filters out threats that are in the past or have <= 1 hour left."""
    valid_items = []
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=1)

    for it in items:
        ts_str = it.get("time_of_event")
        if not ts_str:
            continue
        try:
            event_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if event_dt > cutoff:
                valid_items.append(it)
        except Exception:
            valid_items.append(it)
    return valid_items

def get_threat_data():
    # 1. Read state file first
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
                clean_items = purge_expired_records(state.get("items", []))
                if clean_items:
                    return clean_items[0], clean_items
        except Exception:
            pass

    # 2. Read latest_prediction.json
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r') as f:
                data = json.load(f)
                items = data if isinstance(data, list) else [data]
                clean_items = purge_expired_records(items)
                if clean_items:
                    return clean_items[0], clean_items
        except Exception:
            pass

    # 3. Emergency fallback if container just woke up empty
    now = datetime.now(timezone.utc)
    fallback = {
        "primary_threat": "[DEMO] Forecasted M6.2 Triggered Aftershock",
        "location": "Near Coast of Honshu, Japan",
        "time_of_event": (now + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "severity": "Severe",
        "probability": "68%",
        "lat": 37.8500,
        "lon": 142.1000,
        "is_demo": True,
        "cascading_effects": [
            "Regional transit deceleration advisory",
            "Coastal surge warning"
        ]
    }
    return fallback, [fallback]

def nocache_response(payload, status=200):
    resp = make_response(jsonify(payload), status)
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    return resp

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/healthz')
def healthz():
    return jsonify({"status": "healthy"}), 200

@app.route('/favicon.ico')
def favicon():
    return '', 204

@app.route('/api/latest_threat', methods=['GET'])
@app.route('/api/threats/latest', methods=['GET'])
def get_latest_threat():
    single, _ = get_threat_data()
    return nocache_response(single)

@app.route('/api/threats', methods=['GET'])
@app.route('/api/later_threats', methods=['GET'])
@app.route('/api/historical_predictions', methods=['GET'])
def get_all_threats():
    _, all_items = get_threat_data()
    return nocache_response(all_items)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 7860))
    print(f"[*] Starting FutureDisasterLens Server on port {port}", flush=True)
    app.run(host='0.0.0.0', port=port)
