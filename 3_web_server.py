import os
import json
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS

app = Flask(__name__, static_folder='frontend', static_url_path='')
CORS(app)

DATA_FILE = '/app/data/latest_prediction.json'
STATE_FILE = '/app/data/ai_state.json'

DEFAULT_FALLBACK = {
    "primary_threat": "[DEMO] Magnitude 6.2 Subduction Rupture",
    "location": "Near Coast of Honshu, Japan",
    "time_of_event": "2026-11-07T12:00:00Z",
    "severity": "Severe",
    "probability": "72%",
    "lat": 37.8500,
    "lon": 142.1000,
    "cascading_effects": [
        "Regional bullet train automatic seismic shutdown",
        "Coastal surge alert"
    ]
}

def get_latest_data():
    # 1. Try reading state file
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
                items = state.get("items", [])
                if items:
                    return items[0], items
        except Exception:
            pass

    # 2. Try reading latest_prediction.json
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r') as f:
                data = json.load(f)
                if isinstance(data, list) and len(data) > 0:
                    return data[0], data
                elif isinstance(data, dict):
                    return data, [data]
        except Exception:
            pass

    return DEFAULT_FALLBACK, [DEFAULT_FALLBACK]

@app.route('/api/latest_threat', methods=['GET'])
@app.route('/api/threats/latest', methods=['GET'])
def latest_threat():
    single_item, _ = get_latest_data()
    return jsonify(single_item)

@app.route('/api/threats', methods=['GET'])
@app.route('/api/historical_predictions', methods=['GET'])
def all_threats():
    _, all_items = get_latest_data()
    return jsonify(all_items)

@app.route('/')
def index():
    return send_from_directory('frontend', 'index.html')

@app.route('/<path:path>')
def static_files(path):
    return send_from_directory('frontend', path)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
