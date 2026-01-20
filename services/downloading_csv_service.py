import os
import pandas as pd
from dotenv import load_dotenv
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
import io
import warnings
from services.oauth_service import get_drive_service

# Suppress warnings
warnings.filterwarnings('ignore')

load_dotenv()

def find_file_by_name(service, file_name, parent_id=None):
    """Find a file by name, optionally within a parent folder."""
    query = f"name='{file_name}' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    
    results = service.files().list(q=query, fields='files(id, name)').execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

def find_folder_by_name(service, folder_name, parent_id=None):
    """Find a folder by name, optionally within a parent folder."""
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    
    results = service.files().list(q=query, fields='files(id, name)').execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

def download_csv_as_dataframe(service, file_id):
    """Download a CSV file from Google Drive and return as DataFrame."""
    request = service.files().get_media(fileId=file_id)
    file_buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(file_buffer, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
    
    file_buffer.seek(0)
    try:
        df = pd.read_csv(file_buffer)
        return df
    except:
        return None

def compare_buffer_and_data_csv():
    """Compare buffer.csv and data.csv (first 3 columns only) to check if they're the same."""
    service = get_drive_service()
    
    root_folder_id = find_folder_by_name(service, 'Revive Auctions')
    if not root_folder_id:
        return False
    
    buffer_csv_id = find_file_by_name(service, 'buffer.csv', root_folder_id)
    data_csv_id = find_file_by_name(service, 'data.csv', root_folder_id)
    
    if not buffer_csv_id or not data_csv_id:
        return False
    
    buffer_df = download_csv_as_dataframe(service, buffer_csv_id)
    data_df = download_csv_as_dataframe(service, data_csv_id)
    
    if buffer_df is None or data_df is None:
        return False
    
    # Compare only first 3 columns (excluding DRIVE LINK)
    buffer_subset = buffer_df.iloc[:, :3]
    data_subset = data_df.iloc[:, :3]
    
    # Compare DataFrames
    return buffer_subset.equals(data_subset)

def parse_and_load_vehicle_data():
    """
    Load and parse the Excel file from Google Sheets URL.
    Extracts only vehicle data, excluding the auction closing header.
    Uploads the data to buffer.csv in Google Drive.
    """
    sheet_url = os.getenv('SHEET_URL')
    
    if not sheet_url:
        raise ValueError("SHEET_URL not found in environment variables")
    
    # Convert Google Sheets URL to export format
    # Extract the document ID and gid if present
    if '/d/' in sheet_url:
        doc_id = sheet_url.split('/d/')[1].split('/')[0]
        gid = '0'  # Default sheet
        if 'gid=' in sheet_url:
            gid = sheet_url.split('gid=')[1].split('&')[0].split('#')[0]
        export_url = f'https://docs.google.com/spreadsheets/d/{doc_id}/export?format=xlsx&gid={gid}'
    else:
        raise ValueError("Invalid Google Sheets URL format")
    
    # Read the Excel file
    df = pd.read_excel(export_url)
    
    # Find the row that contains "ONLINE AUCTION CLOSING" and skip it
    # Look for the header row with vehicle details
    header_keywords = ['VEHICLE DETAILS', 'LOCATION', 'DRIVE LINK']
    
    # Find the actual header row
    header_row_idx = None
    for idx, row in df.iterrows():
        row_str = ' '.join([str(val).upper() for val in row if pd.notna(val)])
        if any(keyword in row_str for keyword in header_keywords):
            header_row_idx = idx
            break
    
    if header_row_idx is not None:
        # Set the correct header row
        df.columns = df.iloc[header_row_idx]
        # Keep only data after the header row
        df = df.iloc[header_row_idx + 1:].reset_index(drop=True)
    
    # Remove any rows that contain "ONLINE AUCTION CLOSING"
    df = df[~df.astype(str).apply(lambda row: row.str.contains('ONLINE AUCTION CLOSING', case=False, na=False).any(), axis=1)]
    
    # Remove completely empty rows
    df = df.dropna(how='all')
    
    # Clean up column names - convert to string and strip
    df.columns = [str(col).strip() if pd.notna(col) else f'Unnamed_{i}' for i, col in enumerate(df.columns)]
    
    # Remove any unnamed columns from source
    df = df[[col for col in df.columns if not col.startswith('Unnamed')]]
    
    # Add ID column as the first column
    df.insert(0, 'ID', range(1, len(df) + 1))
    
    # Upload to buffer.csv in Google Drive
    service = get_drive_service()
    
    # Find the Revive Auctions folder
    root_folder_id = find_folder_by_name(service, 'Revive Auctions')
    if not root_folder_id:
        raise ValueError("Revive Auctions folder not found in Google Drive")
    
    # Find buffer.csv
    buffer_csv_id = find_file_by_name(service, 'buffer.csv', root_folder_id)
    if not buffer_csv_id:
        raise ValueError("buffer.csv not found in Revive Auctions folder")
    
    # Convert DataFrame to CSV
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8')
    csv_buffer.seek(0)
    
    # Upload to Google Drive
    media = MediaIoBaseUpload(csv_buffer, mimetype='text/csv', resumable=True)
    service.files().update(
        fileId=buffer_csv_id,
        media_body=media
    ).execute()

if __name__ == '__main__':
    parse_and_load_vehicle_data()
