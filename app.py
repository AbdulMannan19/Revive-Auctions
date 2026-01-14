from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/data', methods=['GET'])
def get_data():
    from services.downloading_csv_service import get_drive_service, find_folder_by_name, find_file_by_name, download_csv_as_dataframe
    
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
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/sync', methods=['POST'])
def sync():
    from sync_handler import handle_sync
    return handle_sync()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
