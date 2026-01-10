import requests
import csv
import os
import re
import shutil
import filecmp
from dotenv import load_dotenv

load_dotenv()

SHEET_URL = os.getenv('SHEET_URL')
CSV_FILE = 'sheet_data.csv'
OLD_CSV_FILE = 'sheet_data_old.csv'

def get_folder_id(url):
    match = re.search(r'/folders/([a-zA-Z0-9_-]+)', url)
    return match.group(1) if match else None

def get_file_ids_from_folder(folder_id):
    try:
        url = f'https://drive.google.com/drive/folders/{folder_id}'
        response = requests.get(url, timeout=10)
        file_ids = re.findall(r'"([a-zA-Z0-9_-]{25,})"', response.text)
        return list(set([fid for fid in file_ids if len(fid) > 20]))
    except:
        return []

def download_image(file_id, save_path):
    try:
        url = f'https://drive.google.com/uc?export=download&id={file_id}'
        response = requests.get(url, stream=True, timeout=10)
        if response.status_code == 200:
            content_type = response.headers.get('Content-Type', '')
            if 'image' in content_type or len(response.content) > 1000:
                with open(save_path, 'wb') as f:
                    f.write(response.content)
                return True
    except:
        pass
    return False

def download_sheet_to_csv(sheet_url, output_file):
    try:
        doc_id = sheet_url.split('/d/')[1].split('/')[0]
        gid = sheet_url.split('gid=')[1].split('&')[0].split('#')[0] if 'gid=' in sheet_url else '0'
        export_url = f'https://docs.google.com/spreadsheets/d/{doc_id}/export?format=csv&gid={gid}'
        response = requests.get(export_url, timeout=10)
        with open(output_file, 'wb') as f:
            f.write(response.content)
        return True
    except:
        return False

def download_images_from_csv(csv_file):
    if os.path.exists('Images'):
        shutil.rmtree('Images')
    os.makedirs('Images', exist_ok=True)
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)
        
        for row in rows[3:]:
            if len(row) < 5 or not row[0].isdigit():
                continue
            
            serial_id = row[0]
            drive_link = row[4]
            
            folder_id = get_folder_id(drive_link)
            if not folder_id:
                continue
            
            serial_folder = os.path.join('Images', serial_id)
            os.makedirs(serial_folder, exist_ok=True)
            
            file_ids = get_file_ids_from_folder(folder_id)
            
            for idx, file_id in enumerate(file_ids):
                save_path = os.path.join(serial_folder, f'{idx+1}.jpg')
                download_image(file_id, save_path)

def csv_changed():
    if not os.path.exists(OLD_CSV_FILE):
        return True
    return not filecmp.cmp(CSV_FILE, OLD_CSV_FILE, shallow=False)

def sync_data():
    if not download_sheet_to_csv(SHEET_URL, CSV_FILE):
        return {'status': 'error', 'message': 'Failed to download CSV'}
    
    if csv_changed():
        download_images_from_csv(CSV_FILE)
        if os.path.exists(OLD_CSV_FILE):
            os.remove(OLD_CSV_FILE)
        shutil.copy(CSV_FILE, OLD_CSV_FILE)
        return {'status': 'success', 'message': 'CSV updated, images downloaded'}
    
    return {'status': 'success', 'message': 'No changes detected, skipped image download'}

def get_vehicles():
    if not os.path.exists(CSV_FILE):
        return []
    
    vehicles = []
    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)
        
        for row in rows[3:]:
            if len(row) < 5 or not row[0].isdigit():
                continue
            
            serial_id = row[0]
            details = row[1]
            price = row[2]
            location = row[3]
            
            images = []
            img_folder = os.path.join('Images', serial_id)
            if os.path.exists(img_folder):
                images = sorted([f for f in os.listdir(img_folder) if f.endswith('.jpg')])
            
            vehicles.append({
                'id': serial_id,
                'details': details,
                'price': price,
                'location': location,
                'images': images
            })
    
