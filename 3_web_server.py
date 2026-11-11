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

def purge_expired_from_disk(items):
    """Filters items and removes anything <= 1 hour left or in the past."""
    valid_items = []
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=1)
    had_expired = False

    for item in items:
        ts = item.get("time_of_event")
        if not ts:
            continue
        try:
            event_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if event_dt > cutoff:
                valid_items.append(item)
            else:
                had_expired = True
        except Exception:
            pass

    # If any old item was detected, overwrite the JSON on disk immediately
    if had_expired:
        try:
            with open(ALL_DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(valid_items, f, indent=2)
            with open(DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(valid_items[0] if valid_items else {}, f, indent=2)
        except Exception:
            pass

    return valid_items

def load_json_data():
    items = []

    # 1. Read all_threats.json
    if os.path.exists(ALL_DATA_FILE):
        try:
            with open(ALL_DATA_FILE, 'r', encoding='utf-8') as f:
                content = json.load(f)
                if isinstance(content, list):
                    items = content
                elif isinstance(content, dict) and content:
                    items = [content]
        except Exception:
            pass

    # 2. Check latest_prediction.json
    if not items and os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                content = json.load(f)
                if isinstance(content, list):
                    items = content
                elif isinstance(content, dict) and content:
                    items = [content]
        except Exception:
            pass

    active_items = purge_expired_from_disk(items)

    # 3. Only if files are truly empty, provide a clean forward demo
    if not active_items:
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
                "Regional rail deceleration advisory",
                "Coastal surge warning"
            ]
        }
        return fallback, [fallback]

    return active_items[0], active_items

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
    single, _ = load_json_data()
    return nocache_response(single)

@app.route('/api/threats', methods=['GET'])
@app.route('/api/later_threats', methods=['GET'])
@app.route('/api/historical_predictions', methods=['GET'])
def get_all_threats():
    _, all_items = load_json_data()
    return nocache_response(all_items)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 7860))
    print(f"[*] Starting FutureDisasterLens Server on port {port}", flush=True)
    app.run(host='0.0.0.0', port=port)
