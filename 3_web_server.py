from flask import Flask, render_template, jsonify
import json
import os

app = Flask(__name__)
DATA_FILE = 'data/latest_prediction.json'
ALL_DATA_FILE = 'data/all_threats.json'

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/latest_threat')
def get_latest_threat():
    # Serves the single most recent prediction for the UI
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f:
            try:
                data = json.load(f)
                return jsonify(data)
            except:
                return jsonify({"error": "Data reading error"}), 500
    else:
        return jsonify({"error": "No AI prediction available yet."}), 404

@app.route('/api/later_threats')
def get_all_threats():
    # Serves the full continuous log of every prediction the AI has made
    if os.path.exists(ALL_DATA_FILE):
        with open(ALL_DATA_FILE, 'r') as f:
            try:
                data = json.load(f)
                return jsonify(data)
            except:
                return jsonify({"error": "Archive reading error"}), 500
    else:
        return jsonify([]), 200

if __name__ == '__main__':
    print("[*] Starting Cloud Web Server on port 7860")
    # 0.0.0.0 tells Flask to accept traffic from the outside internet, not just your local laptop
    app.run(host='0.0.0.0', port=7860)