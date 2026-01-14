import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
import io

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

def get_folders_in_folder(service, parent_folder_id):
    """Get all folders in a parent folder, sorted by name."""
    query = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    results = service.files().list(
        q=query,
        fields='files(id, name)',
        orderBy='name',
        pageSize=1000
    ).execute()
    return results.get('files', [])

def delete_all_files_in_folder(service, folder_id):
    """Delete all files and folders within a folder."""
    query = f"'{folder_id}' in parents and trashed=false"
    results = service.files().list(
        q=query,
        fields='files(id, name)',
        pageSize=1000
    ).execute()
    
    files = results.get('files', [])
    for file in files:
        service.files().delete(fileId=file['id']).execute()

def move_file(service, file_id, new_parent_id, old_parent_id):
    """Move a file to a new parent folder."""
    service.files().update(
        fileId=file_id,
        addParents=new_parent_id,
        removeParents=old_parent_id,
        fields='id, parents'
    ).execute()

def clear_csv_file(service, file_id):
    """Clear the contents of a CSV file."""
    csv_buffer = io.BytesIO(b'')
    media = MediaIoBaseUpload(csv_buffer, mimetype='text/csv', resumable=True)
    service.files().update(
        fileId=file_id,
        media_body=media
    ).execute()

def transfer_buffer_to_data():
    """
    Transfer buffer.csv to data.csv with new drive links.
    Clear buffer.csv and move all Buffer folder contents to Images folder.
    """
    service = get_drive_service()
    
    # Find Revive Auctions folder
    root_folder_id = find_folder_by_name(service, 'Revive Auctions')
    if not root_folder_id:
        raise ValueError("Revive Auctions folder not found")
    
    # Find Buffer and Images folders
    buffer_folder_id = find_folder_by_name(service, 'Buffer', root_folder_id)
    images_folder_id = find_folder_by_name(service, 'Images', root_folder_id)
    if not buffer_folder_id or not images_folder_id:
        raise ValueError("Buffer or Images folder not found")
    
    # Find buffer.csv and data.csv
    buffer_csv_id = find_file_by_name(service, 'buffer.csv', root_folder_id)
    data_csv_id = find_file_by_name(service, 'data.csv', root_folder_id)
    if not buffer_csv_id or not data_csv_id:
        raise ValueError("buffer.csv or data.csv not found")
    
    # Download buffer.csv
    request = service.files().get_media(fileId=buffer_csv_id)
    file_buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(file_buffer, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
    
    file_buffer.seek(0)
    df = pd.read_csv(file_buffer)
    
    # Remove unnamed columns
    df = df[[col for col in df.columns if not str(col).startswith('Unnamed')]]
    
    # Ensure ID column exists as first column
    if 'ID' not in df.columns:
        df.insert(0, 'ID', range(1, len(df) + 1))
    
    # Clear Images folder (delete all contents)
    print("Clearing Images folder...")
    delete_all_files_in_folder(service, images_folder_id)
    
    # Get all folders in Buffer (numbered folders)
    buffer_folders = get_folders_in_folder(service, buffer_folder_id)
    
    # Create a mapping of folder number to folder ID (before moving)
    folder_map = {}
    for folder in buffer_folders:
        try:
            folder_num = int(folder['name'])
            folder_map[folder_num] = folder['id']
        except ValueError:
            continue
    
    # Move all folders from Buffer to Images
    print("Moving folders from Buffer to Images...")
    for item in buffer_folders:
        move_file(service, item['id'], images_folder_id, buffer_folder_id)
    
    print(f"Moved {len(buffer_folders)} folders to Images")
    
    # Remove the DRIVE LINK column if it exists
    if 'DRIVE LINK' in df.columns:
        df = df.drop('DRIVE LINK', axis=1)
    
    # Create new DRIVE LINK column with Images folder links
    drive_links = []
    for i in range(len(df)):
        vehicle_id = int(df.iloc[i]['ID'])
        if vehicle_id in folder_map:
            folder_id = folder_map[vehicle_id]
            drive_link = f"https://drive.google.com/drive/folders/{folder_id}"
        else:
            drive_link = ""
        drive_links.append(drive_link)
    
    df['DRIVE LINK'] = drive_links
    
    # Upload to data.csv (override completely)
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8')
    csv_buffer.seek(0)
    
    media = MediaIoBaseUpload(csv_buffer, mimetype='text/csv', resumable=True)
    service.files().update(
        fileId=data_csv_id,
        media_body=media
    ).execute()
    
    print(f"Transferred {len(df)} records to data.csv")
    
    # Clear buffer.csv
    print("Clearing buffer.csv...")
    clear_csv_file(service, buffer_csv_id)
    
    print("Transfer complete - buffer cleared")

if __name__ == '__main__':
    transfer_buffer_to_data()
