from flask import Flask, jsonify, render_template, send_from_directory
import sync_service
import os

app = Flask(__name__)

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
