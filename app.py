from flask import Flask, jsonify, render_template
import sync_service
import threading

app = Flask(__name__)

sync_in_progress = False

@app.route('/')
def index():
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
    app.run(host='0.0.0.0', port=5000)
