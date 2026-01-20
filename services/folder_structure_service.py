from googleapiclient.http import MediaFileUpload
import io
from googleapiclient.http import MediaIoBaseUpload
from services.oauth_service import get_drive_service

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

def create_csv_file(service, file_name, parent_id=None):
    """Create an empty CSV file in Google Drive."""
    file_metadata = {
        'name': file_name,
        'mimeType': 'text/csv'
    }
    if parent_id:
        file_metadata['parents'] = [parent_id]
    
    # Create empty CSV content
    csv_content = io.BytesIO(b'')
    media = MediaIoBaseUpload(csv_content, mimetype='text/csv', resumable=True)
    
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return file.get('id')

def create_folders():
    """
    Create the folder structure in Google Drive:
    - Revive Auctions/
      - Buffer/
      - Images/
      - buffer.csv
      - data.csv
      - source.csv
    
    If the structure already exists, it won't recreate it.
    """
    service = get_drive_service()
    
    # Check if root folder exists
    root_folder_id = find_folder_by_name(service, 'Revive Auctions')
    
    if root_folder_id:
        # Check if subfolders exist
        buffer_folder_id = find_folder_by_name(service, 'Buffer', root_folder_id)
        images_folder_id = find_folder_by_name(service, 'Images', root_folder_id)
        buffer_csv_id = find_file_by_name(service, 'buffer.csv', root_folder_id)
        data_csv_id = find_file_by_name(service, 'data.csv', root_folder_id)
        
        # Create missing subfolders
        if not buffer_folder_id:
            buffer_folder_id = create_folder(service, 'Buffer', root_folder_id)
        
        if not images_folder_id:
            images_folder_id = create_folder(service, 'Images', root_folder_id)
        
        # Create missing CSV files
        if not buffer_csv_id:
            buffer_csv_id = create_csv_file(service, 'buffer.csv', root_folder_id)
        
        if not data_csv_id:
            data_csv_id = create_csv_file(service, 'data.csv', root_folder_id)
    else:
        # Create entire structure from scratch
        root_folder_id = create_folder(service, 'Revive Auctions')
        
        # Create subfolders
        buffer_folder_id = create_folder(service, 'Buffer', root_folder_id)
        images_folder_id = create_folder(service, 'Images', root_folder_id)
        
        # Create CSV files
        buffer_csv_id = create_csv_file(service, 'buffer.csv', root_folder_id)
        data_csv_id = create_csv_file(service, 'data.csv', root_folder_id)
    
    return {
        'root_folder_id': root_folder_id,
        'buffer_folder_id': buffer_folder_id or find_folder_by_name(service, 'Buffer', root_folder_id),
        'images_folder_id': images_folder_id or find_folder_by_name(service, 'Images', root_folder_id),
        'buffer_csv_id': buffer_csv_id or find_file_by_name(service, 'buffer.csv', root_folder_id),
        'data_csv_id': data_csv_id or find_file_by_name(service, 'data.csv', root_folder_id)
    }

if __name__ == '__main__':
    create_folders()
