import os
import json
import base64
from datetime import datetime, timezone, timedelta
import requests
from flask import Flask, render_template, jsonify, make_response
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

MAX_RECORDS = 1000

DATA_DIR = os.path.join(os.getcwd(), 'data')
os.makedirs(DATA_DIR, exist_ok=True)

DATA_FILE = os.path.join(DATA_DIR, 'latest_prediction.json')
ALL_DATA_FILE = os.path.join(DATA_DIR, 'all_threats.json')
TELEMETRY_1000_FILE = os.path.join(DATA_DIR, 'telemetry_1000.json')
HEALTH_FILE = os.path.join(DATA_DIR, 'ai_health.json')

# DIRECT GITHUB REPOSITORY TARGET
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN', '').strip()
GITHUB_REPO = "daniel1912t/FutureDisasterLens_full"
GITHUB_BRANCH = "main"
GITHUB_FILE_PATH = "data/threats.json"

def fetch_from_github_direct():
    if not GITHUB_TOKEN:
        return []
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}?ref={GITHUB_BRANCH}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    try:
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code == 200:
            raw = base64.b64decode(res.json().get("content", "")).decode("utf-8").strip()
            data = json.loads(raw) if raw else []
            if isinstance(data, list) and data:
                capped = data[:MAX_RECORDS]
                with open(ALL_DATA_FILE, 'w', encoding='utf-8') as f:
                    json.dump(capped, f, indent=2)
                with open(DATA_FILE, 'w', encoding='utf-8') as f:
                    json.dump(capped[0], f, indent=2)
                return capped
    except Exception as e:
        print(f"[-] Web server GitHub fetch notice: {e}", flush=True)
    return []

def load_all_records():
    items = []

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

    if not items:
        items = fetch_from_github_direct()

    return items[:MAX_RECORDS]

def get_active_and_all():
    all_items = load_all_records()
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=1)

    active_items = []
    for item in all_items:
        ts = item.get("time_of_event")
        if not ts:
            continue
        try:
            event_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if event_dt > cutoff:
                active_items.append(item)
        except Exception:
            pass

    if not active_items:
        if all_items:
            active_items = all_items[:15]
        else:
            fallback = {
                "primary_threat": "[DEMO] Forecasted M6.2 Triggered Aftershock",
                "location": "Near Coast of Honshu, Japan",
                "time_of_event": (now + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "severity": "Severe",
                "probability": "68.4%",
                "lat": 37.8500,
                "lon": 142.1000,
                "is_demo": True,
                "cascading_effects": [
                    "Regional rail deceleration advisory",
                    "Coastal surge warning"
                ]
            }
            return fallback, [fallback], [fallback]

    return active_items[0], active_items[:30], all_items

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

@app.route('/api/ai_health', methods=['GET'])
def get_ai_health():
    all_items = load_all_records()
    telemetry_count = 0
    if os.path.exists(TELEMETRY_1000_FILE):
        try:
            with open(TELEMETRY_1000_FILE, 'r', encoding='utf-8') as f:
                telemetry_count = len(json.load(f))
        except Exception:
            pass

    base_health = {}
    if os.path.exists(HEALTH_FILE):
        try:
            with open(HEALTH_FILE, 'r', encoding='utf-8') as f:
                base_health = json.load(f)
        except Exception:
            pass

    base_health.update({
        "stored_threats_count": len(all_items),
        "max_threats_capacity": MAX_RECORDS,
        "telemetry_pool_size": telemetry_count,
        "github_configured": bool(GITHUB_TOKEN),
        "github_repo": GITHUB_REPO,
        "github_branch": GITHUB_BRANCH
    })
    return nocache_response(base_health)

@app.route('/api/latest_threat', methods=['GET'])
@app.route('/api/threats/latest', methods=['GET'])
def get_latest_threat():
    single, _, _ = get_active_and_all()
    return nocache_response(single)

@app.route('/api/threats', methods=['GET'])
@app.route('/api/later_threats', methods=['GET'])
def get_active_threats():
    _, active_items, _ = get_active_and_all()
    return nocache_response(active_items)

@app.route('/api/historical_predictions', methods=['GET'])
@app.route('/api/all_threats', methods=['GET'])
def get_all_1000_threats():
    _, _, all_items = get_active_and_all()
    return nocache_response(all_items)

@app.route('/api/telemetry_1000', methods=['GET'])
def get_telemetry_1000():
    if os.path.exists(TELEMETRY_1000_FILE):
        try:
            with open(TELEMETRY_1000_FILE, 'r', encoding='utf-8') as f:
                return nocache_response(json.load(f))
        except Exception:
            pass
    return nocache_response([])

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 7860))
    print(f"[*] Starting FutureDisasterLens Server (1000-record mode) on port {port}", flush=True)
    app.run(host='0.0.0.0', port=port)
