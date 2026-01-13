from flask import Flask, jsonify, render_template
import sync_service
import threading
import os

app = Flask(__name__)

sync_in_progress = False

def startup_load():
    """Load cache from Drive on startup, then check for updates"""
    global sync_in_progress
    try:
        print('Loading data from Drive...')
        sync_service.load_cache_from_drive()
        print('Checking for updates...')
        sync_in_progress = True
        sync_service.sync_data()
        print('Startup complete!')
    except Exception as e:
        print(f'Startup error: {e}')
    finally:
        sync_in_progress = False

# Load cache and check for updates on startup
threading.Thread(target=startup_load, daemon=True).start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    """Proxy endpoint to serve data.csv content"""
    DATA_CSV_URL = 'https://drive.google.com/uc?export=download&id=1Ns8JC15uYHlRXBNtcMYTKtq9idUJGqFM'
    
    try:
        import requests
        response = requests.get(DATA_CSV_URL, timeout=10)
        return response.text, 200, {'Content-Type': 'text/csv'}
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/vehicles')
def api_vehicles():
    global sync_in_progress
    
    if not sync_in_progress:
        sync_in_progress = True
        def background_sync():
            global sync_in_progress
            try:
                sync_service.sync_data()
            finally:
                sync_in_progress = False
        
        threading.Thread(target=background_sync, daemon=True).start()
        return jsonify({
            'status': 'sync_started',
            'message': 'Sync running in background',
            'vehicles': sync_service.vehicles_cache
        })
    else:
        return jsonify({
            'status': 'sync_in_progress',
            'message': 'Sync already running',
            'vehicles': sync_service.vehicles_cache
        })

@app.route('/api/status')
def api_status():
    return jsonify({
        'sync_in_progress': sync_in_progress,
        'vehicles_count': len(sync_service.vehicles_cache)
    })

if __name__ == '__main__':
    # Sync already runs in background thread on startup
    app.run(host='0.0.0.0', port=5000)
