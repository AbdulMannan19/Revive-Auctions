from flask import Flask, jsonify, render_template
import sync_service

app = Flask(__name__)

sync_service.sync_data()

@app.route('/')
def index():
    return render_template('index.html', vehicles=sync_service.vehicles_cache)

@app.route('/api/vehicles')
def api_vehicles():
    sync_service.sync_data()
    return jsonify(sync_service.vehicles_cache)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
