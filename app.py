from flask import Flask, request, jsonify, render_template
import threading
import time

app = Flask(__name__)

# Global sync status
sync_status = {
    "running": False,
    "last_run": None,
    "last_result": None,
    "current_step": None,
    "error": None
}
sync_lock = threading.Lock()

def run_sync_background():
    """Run sync in background thread"""
    global sync_status
    from sync_handler import handle_sync_background
    
    with sync_lock:
        sync_status["running"] = True
        sync_status["current_step"] = "Starting sync..."
        sync_status["error"] = None
    
    try:
        result = handle_sync_background(sync_status)
        
        with sync_lock:
            sync_status["running"] = False
            sync_status["last_run"] = time.time()
            sync_status["last_result"] = result
            sync_status["current_step"] = "Completed"
    except Exception as e:
        with sync_lock:
            sync_status["running"] = False
            sync_status["last_run"] = time.time()
            sync_status["error"] = str(e)
            sync_status["current_step"] = "Failed"

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/data', methods=['GET'])
def get_data():
    from services.downloading_csv_service import find_folder_by_name, find_file_by_name, download_csv_as_dataframe
    from services.oauth_service import get_drive_service, AuthenticationError, ConfigurationError
    
    try:
        service = get_drive_service()
        
        # Find Revive Auctions folder
        root_folder_id = find_folder_by_name(service, 'Revive Auctions')
        if not root_folder_id:
            return jsonify({"error": "Revive Auctions folder not found"}), 404
        
        # Find data.csv
        data_csv_id = find_file_by_name(service, 'data.csv', root_folder_id)
        if not data_csv_id:
            return jsonify({"error": "data.csv not found"}), 404
        
        # Download and parse data.csv
        df = download_csv_as_dataframe(service, data_csv_id)
        if df is None:
            return jsonify({"error": "Failed to read data.csv"}), 500
        
        # Replace NaN with empty strings for JSON serialization
        df = df.fillna('')
        
        # Convert to list of dictionaries
        data = df.to_dict('records')
        
        return jsonify({"data": data}), 200
    
    except AuthenticationError as e:
        return jsonify({
            "error": "Authentication failed",
            "message": str(e),
            "error_code": "AUTH_ERROR"
        }), 401
    
    except ConfigurationError as e:
        return jsonify({
            "error": "Configuration error",
            "message": str(e),
            "error_code": "CONFIG_ERROR"
        }), 500
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/sync', methods=['POST'])
def sync():
    global sync_status
    
    with sync_lock:
        if sync_status["running"]:
            return jsonify({
                "success": False,
                "message": "Sync already in progress",
                "current_step": sync_status["current_step"]
            }), 409
    
    # Start sync in background thread
    thread = threading.Thread(target=run_sync_background, daemon=True)
    thread.start()
    
    return jsonify({
        "success": True,
        "message": "Sync started in background",
        "status_url": "/sync/status"
    }), 202

@app.route('/sync/status', methods=['GET'])
def sync_status_endpoint():
    with sync_lock:
        status_copy = sync_status.copy()
    
    return jsonify({
        "running": status_copy["running"],
        "current_step": status_copy["current_step"],
        "last_run": status_copy["last_run"],
        "last_result": status_copy["last_result"],
        "error": status_copy["error"]
    }), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
