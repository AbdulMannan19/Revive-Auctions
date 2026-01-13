import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import io

SCOPES = ['https://www.googleapis.com/auth/drive']
ROOT_FOLDER_NAME = 'Revive Auctions'
BUFFER_FOLDER_NAME = 'Buffer'
IMAGES_FOLDER_NAME = 'Images'
CSV_BUFFER_NAME = 'buffer.csv'
CSV_SOURCE_NAME = 'source.csv'
CSV_DATA_NAME = 'data.csv'

def get_drive_service():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                with open('token.json', 'w') as token:
                    token.write(creds.to_json())
            except Exception as e:
                print(f'Error refreshing token: {e}')
                raise Exception('Token refresh failed. Please re-authenticate locally.')
        else:
            # Only run browser auth if not on server
            if os.getenv('RENDER'):
                raise Exception('No valid credentials. Please authenticate locally first.')
            
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
            
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
    
    return build('drive', 'v3', credentials=creds)

def get_or_create_folder(service, folder_name, parent_id=None):
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    
    results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    folders = results.get('files', [])
    
    if folders:
        return folders[0]['id']
    
    folder_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    if parent_id:
        folder_metadata['parents'] = [parent_id]
    
    folder = service.files().create(body=folder_metadata, fields='id').execute()
    folder_id = folder['id']
    
    # Make folder publicly accessible
    make_public(service, folder_id)
    
    return folder_id

def make_public(service, file_id):
    try:
        permission = {
            'type': 'anyone',
            'role': 'reader'
        }
        service.permissions().create(
            fileId=file_id,
            body=permission,
            fields='id'
        ).execute()
    except Exception as e:
        print(f'Warning: Could not make file public: {e}')

def setup_folder_structure(service):
    root_id = get_or_create_folder(service, ROOT_FOLDER_NAME)
    buffer_id = get_or_create_folder(service, BUFFER_FOLDER_NAME, root_id)
    images_id = get_or_create_folder(service, IMAGES_FOLDER_NAME, root_id)
    return root_id, buffer_id, images_id

def create_vehicle_folder(service, parent_folder_id, vehicle_id):
    import time
    folder_name = str(vehicle_id)
    
    # Check if folder already exists
    query = f"name='{folder_name}' and '{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    
    for attempt in range(3):
        try:
            results = service.files().list(q=query, fields='files(id)').execute()
            folders = results.get('files', [])
            
            if folders:
                return folders[0]['id']
            
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_folder_id]
            }
            folder = service.files().create(body=folder_metadata, fields='id').execute()
            return folder['id']
        except Exception as e:
            if attempt < 2:
                print(f'  Retry {attempt+1}/3...', end=' ')
                time.sleep(2)
            else:
                print(f'  Error: {e}')
                raise

def extract_folder_id(url):
    import re
    match = re.search(r'/folders/([a-zA-Z0-9_-]+)', url)
    return match.group(1) if match else None

def get_images_from_folder(service, folder_id, limit=5):
    try:
        query = f"'{folder_id}' in parents and mimeType contains 'image/' and trashed=false"
        results = service.files().list(
            q=query,
            fields='files(id, name)',
            pageSize=limit if limit else 1000
        ).execute()
        files = results.get('files', [])
        return files[:limit] if limit else files
    except Exception as e:
        print(f'  Error accessing folder: {e}')
        return []

def copy_image_to_folder(service, source_file_id, dest_folder_id, new_name):
    import time
    for attempt in range(3):
        try:
            file_metadata = {
                'name': new_name,
                'parents': [dest_folder_id]
            }
            copied_file = service.files().copy(fileId=source_file_id, body=file_metadata, fields='id').execute()
            
            # Make the copied image public
            make_public(service, copied_file['id'])
            
            return True
        except Exception as e:
            if attempt < 2:
                time.sleep(1)
            else:
                print(f'    Error: {e}')
                return False

def copy_images_for_vehicle(service, vehicle_id, source_drive_link, dest_folder_id):
    source_folder_id = extract_folder_id(source_drive_link)
    if not source_folder_id:
        print(f'  ✗ Invalid Drive link')
        return 0, None
    
    images = get_images_from_folder(service, source_folder_id, limit=5)
    copied_count = 0
    
    for idx, img in enumerate(images):
        if copy_image_to_folder(service, img['id'], dest_folder_id, f'image_{idx+1}.jpg'):
            copied_count += 1
    
    # Get the public link to the destination folder
    folder_info = service.files().get(fileId=dest_folder_id, fields='webViewLink').execute()
    dest_folder_link = folder_info.get('webViewLink', source_drive_link)
    
    return copied_count, dest_folder_link

def get_file_in_folder(service, filename, folder_id):
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields='files(id)').execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

def upload_csv_to_drive(service, csv_content, filename, folder_id):
    with open('temp.csv', 'w', encoding='utf-8') as f:
        f.write(csv_content)
    
    file_metadata = {
        'name': filename,
        'parents': [folder_id]
    }
    media = MediaFileUpload('temp.csv', mimetype='text/csv')
    
    existing_file_id = get_file_in_folder(service, filename, folder_id)
    
    if existing_file_id:
        service.files().update(fileId=existing_file_id, media_body=media).execute()
        make_public(service, existing_file_id)
    else:
        uploaded_file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        make_public(service, uploaded_file['id'])
    
    try:
        os.remove('temp.csv')
    except:
        pass

def download_csv_from_drive(service, filename, folder_id):
    file_id = get_file_in_folder(service, filename, folder_id)
    if not file_id:
        return None
    
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
    
    return fh.getvalue().decode('utf-8')

def delete_file_in_folder(service, filename, folder_id):
    file_id = get_file_in_folder(service, filename, folder_id)
    if file_id:
        service.files().delete(fileId=file_id).execute()

def delete_folder_contents(service, folder_id):
    query = f"'{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields='files(id)').execute()
    files = results.get('files', [])
    
    for file in files:
        service.files().delete(fileId=file['id']).execute()

def move_folder_contents(service, source_folder_id, dest_folder_id):
    query = f"'{source_folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields='files(id)').execute()
    files = results.get('files', [])
    
    for file in files:
        service.files().update(
            fileId=file['id'],
            addParents=dest_folder_id,
            removeParents=source_folder_id,
            fields='id, parents'
        ).execute()

def swap_buffer_to_images(service, buffer_folder_id, images_folder_id):
    print('Swapping Buffer → Images...')
    delete_folder_contents(service, images_folder_id)
    move_folder_contents(service, buffer_folder_id, images_folder_id)
    print('✓ Swap complete! Buffer is now empty')

def swap_csv_files(service, root_folder_id):
    # Get buffer.csv content
    buffer_content = download_csv_from_drive(service, CSV_BUFFER_NAME, root_folder_id)
    
    if buffer_content:
        # Update source.csv with buffer content (keeps same file ID = permanent link!)
        upload_csv_to_drive(service, buffer_content, CSV_SOURCE_NAME, root_folder_id)
        
        # Delete buffer.csv
        delete_file_in_folder(service, CSV_BUFFER_NAME, root_folder_id)
        
        print('✓ CSV swapped: buffer.csv → source.csv (file ID preserved)')
    else:
        print('✗ buffer.csv not found, cannot swap')

def get_vehicle_folder_link(service, images_folder_id, vehicle_id):
    """Get the public webViewLink for a vehicle folder in Images/"""
    folder_name = str(vehicle_id)
    query = f"name='{folder_name}' and '{images_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    
    results = service.files().list(q=query, fields='files(id, webViewLink)').execute()
    folders = results.get('files', [])
    
    if folders:
        return folders[0].get('webViewLink', '')
    return ''

def create_data_csv_with_managed_links(service, root_folder_id, images_folder_id):
    """Read source.csv and create data.csv with managed Drive links"""
    import pandas as pd
    import io
    
    print('Creating data.csv with managed Drive links...')
    
    # Download source.csv
    source_csv = download_csv_from_drive(service, CSV_SOURCE_NAME, root_folder_id)
    if not source_csv:
        print('  ✗ source.csv not found')
        return
    
    # Parse into DataFrame
    df = pd.read_csv(io.StringIO(source_csv))
    
    # Update Drive links with managed folder links
    for idx, row in df.iterrows():
        vehicle_id = row['ID']
        managed_link = get_vehicle_folder_link(service, images_folder_id, vehicle_id)
        if managed_link:
            df.at[idx, 'Drive Link'] = managed_link
    
    # Convert back to CSV
    output = io.StringIO()
    df.to_csv(output, index=False, lineterminator='\n')
    clean_csv = output.getvalue()
    
    # Upload as data.csv
    upload_csv_to_drive(service, clean_csv, CSV_DATA_NAME, root_folder_id)
    print('✓ data.csv created with managed links')

