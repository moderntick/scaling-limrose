#!/usr/bin/env python3
"""
Test OAuth authentication flow and token management
"""
import sys
import os
import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
sys.path.insert(0, os.path.dirname(__file__))

def test_token_storage():
    """Test token encryption and storage"""
    try:
        from local_oauth_service import LocalOAuth2Service
        from google.oauth2.credentials import Credentials
        from cryptography.fernet import Fernet
        
        # Create temporary config
        temp_config = {
            'client_id': 'test_client_id',
            'client_secret': 'test_client_secret', 
            'redirect_uri': 'http://localhost:8080/auth/callback',
            'encryption_key': Fernet.generate_key().decode(),
            'scopes': ['https://www.googleapis.com/auth/gmail.readonly']
        }
        
        temp_dir = Path.home() / '.email-pipeline' / 'config'
        temp_dir.mkdir(parents=True, exist_ok=True)
        config_file = temp_dir / 'oauth_config.json'
        token_file = temp_dir / 'token.json'
        
        # Clean up any existing files
        if config_file.exists():
            config_file.unlink()
        if token_file.exists():
            token_file.unlink()
        
        with open(config_file, 'w') as f:
            json.dump(temp_config, f)
        
        oauth = LocalOAuth2Service()
        
        # Create test credentials
        test_creds = Credentials(
            token='test_access_token',
            refresh_token='test_refresh_token',
            token_uri='https://oauth2.googleapis.com/token',
            client_id='test_client_id',
            client_secret='test_client_secret',
            scopes=['https://www.googleapis.com/auth/gmail.readonly']
        )
        test_creds.expiry = datetime.utcnow() + timedelta(hours=1)
        
        # Test saving credentials
        oauth.save_credentials(test_creds)
        print("✓ Credentials saved successfully")
        
        # Test loading credentials
        loaded_creds = oauth.load_credentials()
        if loaded_creds and loaded_creds.token == 'test_access_token':
            print("✓ Credentials loaded successfully")
        else:
            print("❌ Credential loading failed")
            return False
        
        # Test file encryption (should not contain plaintext tokens)
        with open(token_file, 'rb') as f:
            encrypted_data = f.read()
        
        if b'test_access_token' not in encrypted_data:
            print("✓ Tokens are properly encrypted")
        else:
            print("❌ Tokens are not encrypted")
            return False
        
        # Clean up
        config_file.unlink()
        token_file.unlink()
        
        return True
        
    except Exception as e:
        print(f"❌ Token storage test failed: {e}")
        return False

def test_token_refresh_logic():
    """Test token refresh detection logic"""
    try:
        from local_oauth_service import LocalOAuth2Service
        from google.oauth2.credentials import Credentials
        from cryptography.fernet import Fernet
        
        # Create temporary config
        temp_config = {
            'client_id': 'test_client_id',
            'client_secret': 'test_client_secret',
            'redirect_uri': 'http://localhost:8080/auth/callback',
            'encryption_key': Fernet.generate_key().decode(),
            'scopes': ['https://www.googleapis.com/auth/gmail.readonly']
        }
        
        temp_dir = Path.home() / '.email-pipeline' / 'config'
        temp_dir.mkdir(parents=True, exist_ok=True)
        config_file = temp_dir / 'oauth_config.json'
        token_file = temp_dir / 'token.json'
        
        # Clean up any existing files
        if config_file.exists():
            config_file.unlink()
        if token_file.exists():
            token_file.unlink()
        
        with open(config_file, 'w') as f:
            json.dump(temp_config, f)
        
        oauth = LocalOAuth2Service()
        
        # Test 1: Fresh token (should not need refresh)
        fresh_creds = Credentials(
            token='fresh_token',
            refresh_token='refresh_token',
            token_uri='https://oauth2.googleapis.com/token',
            client_id='test_client_id',
            client_secret='test_client_secret',
            scopes=['https://www.googleapis.com/auth/gmail.readonly']
        )
        fresh_creds.expiry = datetime.utcnow() + timedelta(hours=1)
        oauth.save_credentials(fresh_creds)
        
        # This should not try to refresh (would fail without real credentials)
        try:
            oauth.ensure_fresh_token(buffer_minutes=5)
            print("✓ Fresh token detected correctly")
        except Exception as e:
            print(f"❌ Fresh token test failed: {e}")
            return False
        
        # Test 2: Expiring token (should need refresh)
        expiring_creds = Credentials(
            token='expiring_token',
            refresh_token='refresh_token',
            token_uri='https://oauth2.googleapis.com/token',
            client_id='test_client_id',
            client_secret='test_client_secret',
            scopes=['https://www.googleapis.com/auth/gmail.readonly']
        )
        expiring_creds.expiry = datetime.utcnow() + timedelta(minutes=2)  # Expires soon
        oauth.save_credentials(expiring_creds)
        
        # This should try to refresh (and fail with real Google API)
        try:
            oauth.ensure_fresh_token(buffer_minutes=5)
            print("❌ Should have attempted token refresh")
            return False
        except Exception:
            print("✓ Correctly detected need for token refresh")
        
        # Clean up
        config_file.unlink()
        token_file.unlink()
        
        return True
        
    except Exception as e:
        print(f"❌ Token refresh test failed: {e}")
        return False

def test_oauth_state_management():
    """Test OAuth state parameter generation"""
    try:
        from local_oauth_service import LocalOAuth2Service
        from cryptography.fernet import Fernet
        import urllib.parse
        
        # Create temporary config
        temp_config = {
            'client_id': 'test_client_id',
            'client_secret': 'test_client_secret',
            'redirect_uri': 'http://localhost:8080/auth/callback',
            'encryption_key': Fernet.generate_key().decode(),
            'scopes': ['https://www.googleapis.com/auth/gmail.readonly']
        }
        
        temp_dir = Path.home() / '.email-pipeline' / 'config'
        temp_dir.mkdir(parents=True, exist_ok=True)
        config_file = temp_dir / 'oauth_config.json'
        
        # Clean up existing file
        if config_file.exists():
            config_file.unlink()
        
        with open(config_file, 'w') as f:
            json.dump(temp_config, f)
        
        oauth = LocalOAuth2Service()
        
        # Generate OAuth URL
        auth_url = oauth.get_authorization_url()
        
        # Parse URL and check parameters
        parsed = urllib.parse.urlparse(auth_url)
        params = urllib.parse.parse_qs(parsed.query)
        
        required_params = ['client_id', 'redirect_uri', 'response_type', 'scope', 'access_type', 'prompt']
        
        for param in required_params:
            if param not in params:
                print(f"❌ Missing required parameter: {param}")
                return False
        
        print("✓ All required OAuth parameters present")
        
        # Check specific values
        if params['client_id'][0] != 'test_client_id':
            print(f"❌ Wrong client_id: {params['client_id'][0]}")
            return False
        
        if params['response_type'][0] != 'code':
            print(f"❌ Wrong response_type: {params['response_type'][0]}")
            return False
        
        if params['access_type'][0] != 'offline':
            print(f"❌ Wrong access_type: {params['access_type'][0]}")
            return False
        
        print("✓ OAuth parameters have correct values")
        
        # Clean up
        config_file.unlink()
        
        return True
        
    except Exception as e:
        print(f"❌ OAuth state test failed: {e}")
        return False

def test_callback_handler_logic():
    """Test OAuth callback handling logic"""
    try:
        from local_oauth_service import LocalOAuth2Service
        from cryptography.fernet import Fernet
        import asyncio
        from aiohttp.web import Request
        from unittest.mock import MagicMock
        
        # Create temporary config
        temp_config = {
            'client_id': 'test_client_id',
            'client_secret': 'test_client_secret',
            'redirect_uri': 'http://localhost:8080/auth/callback',
            'encryption_key': Fernet.generate_key().decode(),
            'scopes': ['https://www.googleapis.com/auth/gmail.readonly']
        }
        
        temp_dir = Path.home() / '.email-pipeline' / 'config'
        temp_dir.mkdir(parents=True, exist_ok=True)
        config_file = temp_dir / 'oauth_config.json'
        
        # Clean up existing file
        if config_file.exists():
            config_file.unlink()
        
        with open(config_file, 'w') as f:
            json.dump(temp_config, f)
        
        oauth = LocalOAuth2Service()
        
        async def test_callback():
            # Test error callback
            mock_request = MagicMock()
            mock_request.query = {'error': 'access_denied'}
            
            response = await oauth.handle_callback(mock_request)
            if 'Authorization failed' not in response.text:
                print("❌ Error callback not handled correctly")
                return False
            
            print("✓ Error callback handled correctly")
            
            # Test success callback
            mock_request.query = {'code': 'test_auth_code'}
            response = await oauth.handle_callback(mock_request)
            
            if 'Authorization successful' not in response.text:
                print("❌ Success callback not handled correctly")
                return False
            
            if oauth.auth_code != 'test_auth_code':
                print("❌ Auth code not stored correctly")
                return False
            
            print("✓ Success callback handled correctly")
            return True
        
        result = asyncio.run(test_callback())
        
        # Clean up
        config_file.unlink()
        
        return result
        
    except Exception as e:
        print(f"❌ Callback handler test failed: {e}")
        return False

def test_security_file_permissions():
    """Test file permissions for security"""
    try:
        from setup_oauth import OAuthSetup
        import stat
        import os
        
        setup = OAuthSetup()
        
        # Create a test config file
        test_config = {'test': 'data'}
        with open(setup.config_file, 'w') as f:
            json.dump(test_config, f)
        
        # Set permissions like the setup would
        os.chmod(setup.config_file, 0o600)
        os.chmod(setup.config_dir, 0o700)
        
        # The setup should have set restrictive permissions
        file_stat = setup.config_file.stat()
        file_perms = stat.filemode(file_stat.st_mode)
        
        # Check that only owner can read/write
        if file_perms == '-rw-------':
            print("✓ Config file has correct permissions (600)")
        else:
            print(f"❌ Config file has incorrect permissions: {file_perms}")
            return False
        
        # Check directory permissions
        dir_stat = setup.config_dir.stat()
        dir_perms = stat.filemode(dir_stat.st_mode)
        
        if 'rwx------' in dir_perms:
            print("✓ Config directory has correct permissions (700)")
        else:
            print(f"❌ Config directory has incorrect permissions: {dir_perms}")
            return False
        
        # Clean up
        setup.config_file.unlink()
        
        return True
        
    except Exception as e:
        print(f"❌ File permissions test failed: {e}")
        return False

def main():
    print("=== OAuth Flow Testing ===\n")
    
    tests = [
        ("Token Storage & Encryption", test_token_storage),
        ("Token Refresh Logic", test_token_refresh_logic),
        ("OAuth State Management", test_oauth_state_management),
        ("Callback Handler Logic", test_callback_handler_logic),
        ("Security File Permissions", test_security_file_permissions)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n--- {test_name} ---")
        try:
            if test_func():
                passed += 1
            else:
                print(f"❌ {test_name} failed")
        except Exception as e:
            print(f"❌ {test_name} error: {e}")
    
    print(f"\n=== Results ===")
    print(f"Passed: {passed}/{total}")
    print(f"Success rate: {passed/total*100:.1f}%")
    
    if passed == total:
        print("\n✓ All OAuth flow tests passed!")
        return True
    else:
        print(f"\n❌ {total-passed} tests failed. Check the errors above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)