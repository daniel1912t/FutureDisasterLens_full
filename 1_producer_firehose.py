import os
import json
import sqlite3
from datetime import datetime, timezone, timedelta
from flask import Flask, render_template, jsonify, make_response
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

DATA_DIR = os.path.join(os.getcwd(), 'data')
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, 'threats.db')

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def purge_expired():
    """Prunes past threats and threats with <= 1 hour remaining."""
    if not os.path.exists(DB_PATH):
        return
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        threshold = (datetime.now(timezone.utc) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        cursor.execute('DELETE FROM predictions WHERE time_of_event <= ?', (threshold,))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[-] DB Prune error: {e}", flush=True)

def fetch_active_threats():
    purge_expired()
    if not os.path.exists(DB_PATH):
        return []

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT primary_threat, location, time_of_event, severity, probability, lat, lon, cascading_effects, is_demo
        FROM predictions
        ORDER BY id DESC
        LIMIT 10
    ''')
    rows = cursor.fetchall()
    conn.close()

    results = []
    for r in rows:
        try:
            effects = json.loads(r["cascading_effects"])
        except Exception:
            effects = []

        results.append({
            "primary_threat": r["primary_threat"],
            "location": r["location"],
            "time_of_event": r["time_of_event"],
            "severity": r["severity"],
            "probability": r["probability"],
            "lat": r["lat"],
            "lon": r["lon"],
            "cascading_effects": effects,
            "is_demo": bool(r["is_demo"])
        })
    return results

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
    threats = fetch_active_threats()
    if threats:
        return nocache_response(threats[0])
    
    # Safe cold-boot fallback if DB just wiped on Render wakeup
    now = datetime.now(timezone.utc)
    fallback = {
        "primary_threat": "Synchronizing Live Telemetry Stream...",
        "location": "Global Observation Network",
        "time_of_event": (now + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "severity": "Moderate",
        "probability": "50%",
        "lat": 37.85,
        "lon": 142.10,
        "is_demo": True,
        "cascading_effects": ["Calibrating sensor feeds", "Ingesting satellite records"]
    }
    return nocache_response(fallback)

@app.route('/api/threats', methods=['GET'])
@app.route('/api/later_threats', methods=['GET'])
@app.route('/api/historical_predictions', methods=['GET'])
def get_all_threats():
    threats = fetch_active_threats()
    return nocache_response(threats)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 7860))
    print(f"[*] Starting FutureDisasterLens Server on port {port}", flush=True)
    app.run(host='0.0.0.0', port=port)
