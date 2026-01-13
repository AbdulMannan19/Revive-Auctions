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
        print('[DEBUG] startup_load() started')
        print('Loading data from Drive...')
        sync_service.load_cache_from_drive()
        print(f'[DEBUG] After load_cache_from_drive, cache has {len(sync_service.vehicles_cache)} vehicles')
        print('Checking for updates...')
        sync_in_progress = True
        sync_service.sync_data()
        print('[DEBUG] After sync_data')
        print('Startup complete!')
    except Exception as e:
        print(f'Startup error: {e}')
        print('[DEBUG] Exception details:')
        import traceback
        traceback.print_exc()
    finally:
        sync_in_progress = False

# Load cache and check for updates on startup
threading.Thread(target=startup_load, daemon=True).start()

@app.route('/')
def index():
    print(f'[DEBUG] / route called, vehicles_cache length: {len(sync_service.vehicles_cache)}')
    if sync_service.vehicles_cache:
        print(f'[DEBUG] First vehicle in cache: {sync_service.vehicles_cache[0]}')
    else:
        print('[DEBUG] vehicles_cache is EMPTY!')
    return render_template('index.html', vehicles=sync_service.vehicles_cache)

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
