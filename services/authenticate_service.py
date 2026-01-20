import os
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials

SCOPES = ['https://www.googleapis.com/auth/drive']

def authenticate():
    creds = None
    
    if os.path.exists('token.json'):
        print("token.json already exists.")
        response = input("Do you want to re-authenticate? (y/n): ")
        if response.lower() != 'y':
            print("Authentication cancelled.")
            return
        
        os.rename('token.json', 'token.json.old')
        print("Existing token backed up to token.json.old")
    
    if not os.path.exists('credentials.json'):
        print("\nERROR: credentials.json not found!")
        print("\nTo get credentials.json:")
        print("1. Go to https://console.cloud.google.com/apis/credentials")
        print("2. Create OAuth 2.0 Client ID (Desktop app)")
        print("3. Download the JSON file and save it as 'credentials.json'")
        return
    
    print("\nStarting authentication flow...")
    print("A browser window will open. Please sign in and authorize the application.")
    
    try:
        flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
        
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
        
        os.chmod('token.json', 0o600)
        
        print("\n✓ Authentication successful!")
        print("✓ token.json has been created")
        print("\nYou can now run your Flask application.")
        
    except Exception as e:
        print(f"\n✗ Authentication failed: {str(e)}")
        
        if os.path.exists('token.json.old'):
            os.rename('token.json.old', 'token.json')
            print("Restored previous token.json")

if __name__ == '__main__':
    print("=" * 60)
    print("Google Drive API Authentication")
    print("=" * 60)
    authenticate()
