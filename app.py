from flask import Flask, jsonify, render_template, send_from_directory
import sync_service
import os
import threading
import schedule
import time
from datetime import datetime
import pytz

app = Flask(__name__)

def sync_job():
    print(f"Running scheduled sync at {datetime.now()}")
    result = sync_service.sync_data()
    print(result)

def run_scheduler():
    # Run sync on startup
    sync_job()
    
    # Schedule daily sync at 12:00 AM UAE time
    uae_tz = pytz.timezone('Asia/Dubai')
    schedule.every().day.at("00:00").do(sync_job)
    
    print("Scheduler started. Sync will run daily at 12:00 AM UAE time")
    
    while True:
        schedule.run_pending()
        time.sleep(60)

# Start scheduler in background thread
scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
scheduler_thread.start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/vehicles')
def get_vehicles():
    vehicles = sync_service.get_vehicles()
    return jsonify(vehicles)

@app.route('/api/sync')
def sync():
    result = sync_service.sync_data()
    return jsonify(result)

@app.route('/images/<serial_id>/<filename>')
def serve_image(serial_id, filename):
    return send_from_directory(os.path.join('Images', serial_id), filename)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
