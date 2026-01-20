import os
import json
import threading
import logging
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_refresh_lock = threading.RLock()
SCOPES = ['https://www.googleapis.com/auth/drive']

class AuthenticationError(Exception):
    pass

class ConfigurationError(Exception):
    pass

def _load_credentials():
    if not os.path.exists('credentials.json'):
        raise ConfigurationError(
            "credentials.json not found. "
            "Please download it from Google Cloud Console: "
            "https://console.cloud.google.com/apis/credentials"
        )
    
    if not os.path.exists('token.json'):
        raise AuthenticationError(
            "token.json not found. Initial authentication required. "
            "Please run the authentication flow to generate token.json"
        )
    
    try:
        with open('token.json', 'r') as token_file:
            token_data = json.load(token_file)
    except json.JSONDecodeError as e:
        raise ConfigurationError(
            f"token.json is corrupted (invalid JSON). "
            f"Please delete it and re-authenticate. Error: {str(e)}"
        )
    except PermissionError:
        raise ConfigurationError(
            "Cannot read token.json - permission denied. "
            "Please check file permissions."
        )
    
    required_fields = ['client_id', 'client_secret', 'refresh_token']
    missing_fields = [field for field in required_fields if field not in token_data]
    if missing_fields:
        raise ConfigurationError(
            f"token.json is missing required fields: {', '.join(missing_fields)}. "
            f"Please delete it and re-authenticate."
        )
    
    try:
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        return creds
    except Exception as e:
        raise ConfigurationError(
            f"Failed to load credentials from token.json: {str(e)}"
        )

def _save_credentials(creds):
    try:
        if os.path.exists('token.json'):
            backup_path = 'token.json.backup'
            with open('token.json', 'r') as src:
                with open(backup_path, 'w') as dst:
                    dst.write(src.read())
        
        token_data = {
            'token': creds.token,
            'refresh_token': creds.refresh_token,
            'token_uri': creds.token_uri,
            'client_id': creds.client_id,
            'client_secret': creds.client_secret,
            'scopes': creds.scopes,
            'expiry': creds.expiry.isoformat() if creds.expiry else None
        }
        
        temp_path = 'token.json.tmp'
        with open(temp_path, 'w') as token_file:
            json.dump(token_data, token_file, indent=2)
        
        os.replace(temp_path, 'token.json')
        os.chmod('token.json', 0o600)
        
        logger.info(f"Token saved successfully. Expires at: {creds.expiry}")
        
    except Exception as e:
        logger.error(f"Failed to save token.json: {str(e)}")

def _needs_refresh(creds, threshold_minutes=5):
    if not creds.valid:
        return True
    
    if not creds.expiry:
        return False
    
    time_until_expiry = creds.expiry - datetime.utcnow()
    return time_until_expiry < timedelta(minutes=threshold_minutes)

def _refresh_credentials(creds):
    max_retries = 3
    retry_delays = [1, 2, 4]
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Refreshing token (attempt {attempt + 1}/{max_retries})...")
            creds.refresh(Request())
            logger.info(f"Token refreshed successfully. New expiry: {creds.expiry}")
            return creds
            
        except RefreshError as e:
            error_msg = str(e)
            
            if 'invalid_grant' in error_msg.lower():
                raise AuthenticationError(
                    "Refresh token has expired or been revoked. "
                    "Re-authentication required. Please run the authentication flow again. "
                    f"Error: {error_msg}"
                )
            
            if attempt < max_retries - 1:
                delay = retry_delays[attempt]
                logger.warning(
                    f"Token refresh failed (attempt {attempt + 1}): {error_msg}. "
                    f"Retrying in {delay} seconds..."
                )
                import time
                time.sleep(delay)
            else:
                raise AuthenticationError(
                    f"Token refresh failed after {max_retries} attempts. "
                    f"Last error: {error_msg}"
                )
                
        except Exception as e:
            if attempt < max_retries - 1:
                delay = retry_delays[attempt]
                logger.warning(
                    f"Token refresh error (attempt {attempt + 1}): {str(e)}. "
                    f"Retrying in {delay} seconds..."
                )
                import time
                time.sleep(delay)
            else:
                raise AuthenticationError(
                    f"Token refresh failed after {max_retries} attempts. "
                    f"Error: {str(e)}"
                )

def get_drive_service():
    with _refresh_lock:
        creds = _load_credentials()
        
        if _needs_refresh(creds):
            logger.info("Token expired or expiring soon, refreshing...")
            creds = _refresh_credentials(creds)
            _save_credentials(creds)
        else:
            logger.debug("Token is valid, no refresh needed")
        
        service = build('drive', 'v3', credentials=creds)
        return service
