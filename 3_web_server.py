import os
import json
from flask import Flask, render_template, jsonify, make_response
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

DATA_FILE = os.path.join('data', 'latest_prediction.json')
ALL_DATA_FILE = os.path.join('data', 'all_threats.json')
STATE_FILE = os.path.join('data', 'ai_state.json')

# Safe fallback threat payload to prevent UI blanking on cold start
INITIAL_FALLBACK = {
    "primary_threat": "[DEMO] Magnitude 6.2 Subduction Rupture",
    "location": "Near Coast of Honshu, Japan",
    "time_of_event": "2026-11-07T12:00:00Z",
    "severity": "Severe",
    "probability": "72%",
    "lat": 37.8500,
    "lon": 142.1000,
    "is_demo": True,
    "cascading_effects": [
        "Regional bullet train automated seismic shutdown",
        "Coastal surge alert"
    ]
}

def nocache_response(payload, status=200):
    """Wraps JSON response with headers to prevent browser-side caching."""
    resp = make_response(jsonify(payload), status)
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    return resp

def read_json_safely(filepath):
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"[-] Error reading {filepath}: {e}", flush=True)
    return None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/healthz')
def healthz():
    return jsonify({"status": "healthy", "service": "FutureDisasterLens"}), 200

@app.route('/api/latest_threat', methods=['GET'])
@app.route('/api/threats/latest', methods=['GET'])
def get_latest_threat():
    # 1. Try reading standard latest prediction
    data = read_json_safely(DATA_FILE)
    if data:
        return nocache_response(data)

    # 2. Try reading from state file items if available
    state = read_json_safely(STATE_FILE)
    if state and state.get("items"):
        return nocache_response(state["items"][0])

    # 3. Safe fallback so UI renders immediately without 404
    return nocache_response(INITIAL_FALLBACK)

@app.route('/api/later_threats', methods=['GET'])
@app.route('/api/threats', methods=['GET'])
@app.route('/api/historical_predictions', methods=['GET'])
def get_all_threats():
    # 1. Check for historical threat log
    all_data = read_json_safely(ALL_DATA_FILE)
    if all_data:
        return nocache_response(all_data)

    # 2. Check state file items
    state = read_json_safely(STATE_FILE)
    if state and state.get("items"):
        return nocache_response(state["items"])

    # 3. Return active fallback list
    return nocache_response([INITIAL_FALLBACK])

if __name__ == '__main__':
    # Dynamically match Render's PORT or fallback to standard 7860
    port = int(os.environ.get('PORT', 7860))
    print(f"[*] Starting FutureDisasterLens Server on port {port}", flush=True)
    app.run(host='0.0.0.0', port=port)
