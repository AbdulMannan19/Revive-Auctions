from flask import Flask, jsonify, render_template_string
import requests
import csv
import os
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)

SHEET_URL = os.getenv('SHEET_URL')

def get_vehicles():
    doc_id = SHEET_URL.split('/d/')[1].split('/')[0]
    gid = SHEET_URL.split('gid=')[1].split('#')[0]
    export_url = f'https://docs.google.com/spreadsheets/d/{doc_id}/export?format=csv&gid={gid}'
    
    response = requests.get(export_url, timeout=10)
    reader = csv.reader(response.text.splitlines())
    rows = list(reader)
    
    vehicles = []
    for row in rows[3:]:
        if len(row) >= 5 and row[0].strip().isdigit():
            vehicles.append({
                'id': row[0].strip(),
                'details': row[1].strip(),
                'price': row[2].strip(),
                'location': row[3].strip(),
                'drive_link': row[4].strip()
            })
    return vehicles

@app.route('/')
def index():
    return render_template_string('''
<!DOCTYPE html>
<html>
<head>
    <title>Revive Auctions</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: Arial, sans-serif; background: #f5f5f5; }
        .header { background: #2c3e50; color: white; padding: 20px; text-align: center; }
        .container { max-width: 1200px; margin: 20px auto; padding: 0 20px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 20px; }
        .card { background: white; border-radius: 8px; padding: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .card:hover { box-shadow: 0 4px 8px rgba(0,0,0,0.2); }
        .id { font-size: 24px; font-weight: bold; color: #2c3e50; margin-bottom: 10px; }
        .details { color: #555; margin-bottom: 10px; font-size: 14px; }
        .price { color: #27ae60; font-size: 20px; font-weight: bold; margin-bottom: 10px; }
        .location { color: #7f8c8d; font-size: 12px; margin-bottom: 10px; }
        .link { display: inline-block; margin-top: 10px; padding: 8px 16px; background: #3498db; color: white; text-decoration: none; border-radius: 4px; font-size: 12px; }
        .link:hover { background: #2980b9; }
        .loading { text-align: center; padding: 40px; color: #7f8c8d; }
    </style>
</head>
<body>
    <div class="header"><h1>Revive Auctions</h1></div>
    <div class="container">
        <div id="grid" class="grid"><div class="loading">Loading...</div></div>
    </div>
    <script>
        fetch('/api/vehicles')
            .then(r => r.json())
            .then(vehicles => {
                document.getElementById('grid').innerHTML = vehicles.map(v => `
                    <div class="card">
                        <div class="id">#${v.id}</div>
                        <div class="details">${v.details}</div>
                        <div class="price">₹${v.price}</div>
                        <div class="location">${v.location}</div>
                        <a href="${v.drive_link}" target="_blank" class="link">View Images</a>
                    </div>
                `).join('');
            });
    </script>
</body>
</html>
    ''')

@app.route('/api/vehicles')
def api_vehicles():
    return jsonify(get_vehicles())

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
