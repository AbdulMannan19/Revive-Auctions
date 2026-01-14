import os
import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

def get_drive_service():
    """Initialize and return Google Drive service."""
    creds = Credentials.from_authorized_user_file('token.json')
    service = build('drive', 'v3', credentials=creds)
    return service

def find_folder_by_name(service, folder_name, parent_id=None):
    """Find a folder by name, optionally within a parent folder."""
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    
    results = service.files().list(q=query, fields='files(id, name)').execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

def find_file_by_name(service, file_name, parent_id=None):
    """Find a file by name, optionally within a parent folder."""
    query = f"name='{file_name}' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    
    results = service.files().list(q=query, fields='files(id, name)').execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

def create_folder(service, folder_name, parent_id=None):
    """Create a folder in Google Drive."""
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    if parent_id:
        file_metadata['parents'] = [parent_id]
    
    folder = service.files().create(body=file_metadata, fields='id').execute()
    return folder.get('id')

def extract_folder_id_from_url(url):
    """Extract folder ID from Google Drive URL."""
    if not url or pd.isna(url):
        return None
    
    # Pattern: /folders/FOLDER_ID
    match = re.search(r'/folders/([a-zA-Z0-9_-]+)', str(url))
    if match:
        return match.group(1)
    return None

def get_files_in_folder(service, folder_id):
    """Get all files in a Google Drive folder."""
    query = f"'{folder_id}' in parents and trashed=false"
    results = service.files().list(
        q=query,
        fields='files(id, name, mimeType)',
        pageSize=1000
    ).execute()
    return results.get('files', [])

def copy_file(service, file_id, new_name, destination_folder_id):
    """Copy a file to a destination folder."""
    file_metadata = {
        'name': new_name,
        'parents': [destination_folder_id]
    }
    
    copied_file = service.files().copy(
        fileId=file_id,
        body=file_metadata,
        fields='id, name'
    ).execute()
    
    return copied_file

def process_vehicle(vehicle_index, vehicle_row, buffer_folder_id, max_images=None):
    """Process a single vehicle: create folder and copy images."""
    # Create a new service instance for this thread
    service = get_drive_service()
    
    try:
        vehicle_num = vehicle_index + 1
        drive_link = vehicle_row.get('DRIVE LINK', '')
        
        # Extract folder ID from drive link
        source_folder_id = extract_folder_id_from_url(drive_link)
        
        if not source_folder_id:
            print(f"Vehicle {vehicle_num}: No valid drive link found")
            return {
                'vehicle_num': vehicle_num,
                'status': 'skipped',
                'reason': 'No valid drive link'
            }
        
        # Create vehicle folder in Buffer
        vehicle_folder_name = str(vehicle_num)
        vehicle_folder_id = create_folder(service, vehicle_folder_name, buffer_folder_id)
        
        # Get all files from source folder
        files = get_files_in_folder(service, source_folder_id)
        
        if not files:
            print(f"Vehicle {vehicle_num}: No files found in source folder")
            return {
                'vehicle_num': vehicle_num,
                'status': 'completed',
                'images_copied': 0,
                'folder_id': vehicle_folder_id
            }
        
        # Filter image files
        image_files = [f for f in files if f['mimeType'].startswith('image/')]
        
        # Limit images for testing
        if max_images:
            image_files = image_files[:max_images]
        
        # Copy each file
        copied_count = 0
        for file in image_files:
            copy_file(service, file['id'], file['name'], vehicle_folder_id)
            copied_count += 1
        
        return {
            'vehicle_num': vehicle_num,
            'status': 'completed',
            'images_copied': copied_count,
            'folder_id': vehicle_folder_id
        }
        
    except Exception as e:
        print(f"Vehicle {vehicle_num}: Error - {str(e)}")
        return {
            'vehicle_num': vehicle_num,
            'status': 'error',
            'error': str(e)
        }

def copy_images_from_buffer(parallel=True, max_workers=5, max_vehicles=None, max_images_per_vehicle=None):
    """
    Load buffer.csv and copy images for each vehicle.
    
    Args:
        parallel: Whether to process vehicles in parallel (default: True)
        max_workers: Number of parallel workers (default: 5)
        max_vehicles: Maximum number of vehicles to process (default: None for all)
        max_images_per_vehicle: Maximum images per vehicle (default: None for all)
    """
    service = get_drive_service()
    
    # Find Revive Auctions folder
    root_folder_id = find_folder_by_name(service, 'Revive Auctions')
    if not root_folder_id:
        raise ValueError("Revive Auctions folder not found")
    
    # Find Buffer folder
    buffer_folder_id = find_folder_by_name(service, 'Buffer', root_folder_id)
    if not buffer_folder_id:
        raise ValueError("Buffer folder not found")
    
    # Find buffer.csv
    buffer_csv_id = find_file_by_name(service, 'buffer.csv', root_folder_id)
    if not buffer_csv_id:
        raise ValueError("buffer.csv not found")
    
    # Download buffer.csv
    request = service.files().get_media(fileId=buffer_csv_id)
    file_buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(file_buffer, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
    
    file_buffer.seek(0)
    df = pd.read_csv(file_buffer)
    
    if max_vehicles:
        df = df.head(max_vehicles)
    
    results = []
    
    if parallel and len(df) > 1:
        # Process in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_vehicle, idx, row, buffer_folder_id, max_images_per_vehicle): idx
                for idx, row in df.iterrows()
            }
            
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
    else:
        # Process sequentially
        for idx, row in df.iterrows():
            result = process_vehicle(idx, row, buffer_folder_id, max_images_per_vehicle)
            results.append(result)
    
    # Summary
    completed = sum(1 for r in results if r['status'] == 'completed')
    skipped = sum(1 for r in results if r['status'] == 'skipped')
    errors = sum(1 for r in results if r['status'] == 'error')
    
    return results

if __name__ == '__main__':
    copy_images_from_buffer()
