#!/usr/bin/env python3
"""
Test OAuth setup functionality without user interaction
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

def test_imports():
    """Test that all required modules can be imported"""
    try:
        import cryptography
        print("✓ cryptography imported successfully")
        
        import aiohttp
        print("✓ aiohttp imported successfully")
        
        from google.oauth2.credentials import Credentials
        print("✓ google.oauth2.credentials imported successfully")
        
        from googleapiclient.discovery import build
        print("✓ googleapiclient.discovery imported successfully")
        
        from local_oauth_service import LocalOAuth2Service
        print("✓ LocalOAuth2Service imported successfully")
        
        from setup_oauth import OAuthSetup
        print("✓ OAuthSetup imported successfully")
        
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

def test_oauth_service_init():
    """Test OAuth service initialization without config file"""
    try:
        from local_oauth_service import LocalOAuth2Service
        
        # This should fail because no config exists
        try:
            oauth = LocalOAuth2Service()
            print("❌ Expected config error but service initialized")
            return False
        except FileNotFoundError as e:
            print(f"✓ Correctly detected missing config: {str(e)[:50]}...")
            return True
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def test_config_directory_creation():
    """Test that setup can create config directory"""
    try:
        from setup_oauth import OAuthSetup
        setup = OAuthSetup()
        
        # Check if config directory was created
        if setup.config_dir.exists():
            print("✓ Config directory created successfully")
            return True
        else:
            print("❌ Config directory not created")
            return False
    except Exception as e:
        print(f"❌ Error creating config directory: {e}")
        return False

def test_encryption_key_generation():
    """Test Fernet key generation"""
    try:
        from cryptography.fernet import Fernet
        
        # Generate a test key
        key = Fernet.generate_key()
        print(f"✓ Encryption key generated: {key[:10]}...")
        
        # Test encryption/decryption
        f = Fernet(key)
        test_data = b"test oauth token"
        encrypted = f.encrypt(test_data)
        decrypted = f.decrypt(encrypted)
        
        if decrypted == test_data:
            print("✓ Encryption/decryption working correctly")
            return True
        else:
            print("❌ Encryption/decryption failed")
            return False
    except Exception as e:
        print(f"❌ Encryption test failed: {e}")
        return False

def test_port_detection():
    """Test port detection functionality"""
    try:
        import socket
        import asyncio
        
        async def test_port_func():
            # Try to bind to a port
            for port in range(8080, 8090):
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.bind(('localhost', port))
                    sock.close()
                    print(f"✓ Port {port} available")
                    return True
                except OSError:
                    continue
            print("❌ No ports available in range 8080-8089")
            return False
        
        return asyncio.run(test_port_func())
    except Exception as e:
        print(f"❌ Port detection test failed: {e}")
        return False

def test_oauth_url_generation():
    """Test OAuth URL generation with dummy config"""
    try:
        # Create a temporary config for testing
        import json
        import tempfile
        from pathlib import Path
        from cryptography.fernet import Fernet
        
        # Create temporary config
        temp_config = {
            'client_id': 'test_client_id',
            'client_secret': 'test_client_secret',
            'redirect_uri': 'http://localhost:8080/auth/callback',
            'encryption_key': Fernet.generate_key().decode(),
            'scopes': [
                'https://www.googleapis.com/auth/gmail.readonly',
                'https://www.googleapis.com/auth/gmail.modify',
                'https://www.googleapis.com/auth/userinfo.email'
            ]
        }
        
        # Create temp config file
        temp_dir = Path.home() / '.email-pipeline' / 'config'
        temp_dir.mkdir(parents=True, exist_ok=True)
        config_file = temp_dir / 'oauth_config.json'
        
        with open(config_file, 'w') as f:
            json.dump(temp_config, f)
        
        # Test OAuth service
        from local_oauth_service import LocalOAuth2Service
        oauth = LocalOAuth2Service()
        auth_url = oauth.get_authorization_url()
        
        # Clean up
        config_file.unlink()
        
        if 'accounts.google.com' in auth_url and 'test_client_id' in auth_url:
            print("✓ OAuth URL generated correctly")
            return True
        else:
            print(f"❌ Invalid OAuth URL: {auth_url[:50]}...")
            return False
            
    except Exception as e:
        print(f"❌ OAuth URL generation failed: {e}")
        return False

def main():
    print("=== OAuth Setup Testing ===\n")
    
    tests = [
        ("Import Tests", test_imports),
        ("OAuth Service Init", test_oauth_service_init),
        ("Config Directory", test_config_directory_creation),
        ("Encryption", test_encryption_key_generation),
        ("Port Detection", test_port_detection),
        ("OAuth URL Generation", test_oauth_url_generation)
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
        print("\n✓ All tests passed! OAuth setup is working correctly.")
        return True
    else:
        print(f"\n❌ {total-passed} tests failed. Check the errors above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)