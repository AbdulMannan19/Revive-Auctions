import requests
import csv
import os
import pandas as pd
import io
from dotenv import load_dotenv
import drive_service

load_dotenv()

SHEET_URL = os.getenv('SHEET_URL')
vehicles_cache = []
last_csv_data = None
drive_service_instance = None
root_folder_id = None
buffer_folder_id = None
images_folder_id = None

def init_drive_service():
    global drive_service_instance, root_folder_id, buffer_folder_id, images_folder_id
    if drive_service_instance is None:
        print('Initializing Google Drive service...')
        drive_service_instance = drive_service.get_drive_service()
        root_folder_id, buffer_folder_id, images_folder_id = drive_service.setup_folder_structure(drive_service_instance)
        print(f'✓ Folder structure: Revive Auctions → Buffer & Images')

def fetch_csv_from_sheet():
    doc_id = SHEET_URL.split('/d/')[1].split('/')[0]
    gid = SHEET_URL.split('gid=')[1].split('#')[0]
    export_url = f'https://docs.google.com/spreadsheets/d/{doc_id}/export?format=csv&gid={gid}'
    response = requests.get(export_url, timeout=10)
    return response.text

def clean_csv_data(csv_text):
    lines = csv_text.splitlines()
    reader = csv.reader(lines)
    rows = list(reader)
    
    clean_rows = []
    for row in rows[3:]:
        if len(row) >= 5 and row[0].strip().isdigit():
            clean_rows.append([
                row[0].strip(),
                row[1].strip(),
                row[2].strip(),
                row[3].strip(),
                row[4].strip()
            ])
    
    df = pd.DataFrame(clean_rows, columns=['ID', 'Vehicle Details', 'Price', 'Location', 'Drive Link'])
    return df

def df_to_clean_csv(df):
    import io
    output = io.StringIO()
    df.to_csv(output, index=False, lineterminator='\n')
    return output.getvalue()

def parse_vehicles(csv_text):
    df = clean_csv_data(csv_text)
    vehicles = []
    
    for _, row in df.iterrows():
        vehicles.append({
            'id': row['ID'],
            'details': row['Vehicle Details'],
            'price': row['Price'],
            'location': row['Location'],
            'drive_link': row['Drive Link']
        })
    
    return vehicles

def parse_clean_csv(csv_text):
    """Parse already-cleaned CSV (like data.csv from Drive)"""
    try:
        df = pd.read_csv(io.StringIO(csv_text))
        vehicles = []
        
        for _, row in df.iterrows():
            vehicles.append({
                'id': row['ID'],
                'details': row['Vehicle Details'],
                'price': row['Price'],
                'location': row['Location'],
                'drive_link': row['Drive Link']
            })
        
        return vehicles
    except Exception as e:
        print(f'Error parsing CSV: {e}')
        return []

def load_cache_from_drive():
    """Load vehicles_cache from data.csv on Drive"""
    global vehicles_cache
    
    init_drive_service()
    
    data_csv = drive_service.download_csv_from_drive(drive_service_instance, 'data.csv', root_folder_id)
    
    if data_csv:
        vehicles_cache = parse_clean_csv(data_csv)
        print(f'✓ Loaded {len(vehicles_cache)} vehicles from data.csv')
    else:
        print('⚠ No data.csv found on Drive')

def sync_data():
    global vehicles_cache, last_csv_data
    
    init_drive_service()
    new_csv = fetch_csv_from_sheet()
    
    # Clean the new data
    df_new = clean_csv_data(new_csv)
    
    # Load existing source.csv from Drive and convert to df
    existing_csv = drive_service.download_csv_from_drive(drive_service_instance, 'source.csv', root_folder_id)
    
    if existing_csv is None:
        data_changed = True
        print('No existing source.csv found - first sync')
    else:
        df_existing = pd.read_csv(io.StringIO(existing_csv))
        
        # Convert both to strings for comparison (ignore type differences)
        df_existing_str = df_existing.astype(str)
        df_new_str = df_new.astype(str)
        
        data_changed = not df_existing_str.equals(df_new_str)
        
        if data_changed:
            print('Data has changed - syncing')
        else:
            print('Data is identical - skipping sync')
    
    if data_changed:
        print('\n=== Starting Sync ===')
        
        clean_csv = df_to_clean_csv(df_new)
        print('Uploading buffer.csv to Drive...')
        drive_service.upload_csv_to_drive(drive_service_instance, clean_csv, 'buffer.csv', root_folder_id)
        
        print(f'Creating vehicle folders and copying images to Buffer/ (TEST: 3 vehicles, 5 images each)...')
        
        for idx, row in df_new.head(3).iterrows():
            vehicle_id = row['ID']
            drive_link = row['Drive Link']
            
            print(f'Vehicle {vehicle_id}:', end=' ')
            folder_id = drive_service.create_vehicle_folder(drive_service_instance, buffer_folder_id, vehicle_id)
            copied, new_link = drive_service.copy_images_for_vehicle(drive_service_instance, vehicle_id, drive_link, folder_id)
            print(f'✓ {copied} images')
        
        print('\nBuffer ready! Swapping...')
        drive_service.swap_buffer_to_images(drive_service_instance, buffer_folder_id, images_folder_id)
        drive_service.swap_csv_files(drive_service_instance, root_folder_id)
        
        # Create data.csv with managed Drive links
        drive_service.create_data_csv_with_managed_links(drive_service_instance, root_folder_id, images_folder_id)
        
        # Load the new data.csv for cache
        data_csv = drive_service.download_csv_from_drive(drive_service_instance, 'data.csv', root_folder_id)
        if data_csv:
            vehicles_cache = parse_clean_csv(data_csv)  # Use parse_clean_csv here too!
        
        last_csv_data = new_csv
        print(f'\n✓ Sync complete! {len(vehicles_cache)} vehicles ready')
    else:
        print('No changes detected - source.csv matches Excel sheet')
