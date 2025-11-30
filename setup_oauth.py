#!/usr/bin/env python3
"""
Interactive setup script for Gmail OAuth configuration
"""
import os
import json
import secrets
from pathlib import Path
import webbrowser
from cryptography.fernet import Fernet
import sys

class OAuthSetup:
    def __init__(self):
        self.config_dir = Path.home() / '.email-pipeline' / 'config'
        self.config_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.config_file = self.config_dir / 'oauth_config.json'
        self.token_file = self.config_dir / 'token.json'
        
    def run(self):
        print("=== Email Pipeline OAuth Setup ===\n")
        print("This wizard will help you set up Gmail authentication.")
        print("You'll need to create a Google Cloud project and OAuth credentials.\n")
        
        if self.config_file.exists():
            overwrite = input("OAuth configuration already exists. Overwrite? (y/n): ")
            if overwrite.lower() != 'y':
                print("Setup cancelled.")
                return
        
        # Step 1: Guide user through Google Cloud setup
        self.guide_google_cloud_setup()
        
        # Step 2: Collect OAuth credentials
        print("\n=== Enter OAuth Credentials ===")
        print("Enter the credentials from your Google Cloud Console:\n")
        
        client_id = input("OAuth Client ID: ").strip()
        if not client_id:
            print("Error: Client ID is required")
            sys.exit(1)
            
        client_secret = input("OAuth Client Secret: ").strip()
        if not client_secret:
            print("Error: Client Secret is required")
            sys.exit(1)
        
        # Step 3: Generate encryption key for token storage
        encryption_key = Fernet.generate_key()
        
        # Step 4: Save configuration
        config = {
            'client_id': client_id,
            'client_secret': client_secret,
            'redirect_uri': 'http://localhost:8080/auth/callback',
            'encryption_key': encryption_key.decode(),
            'scopes': [
                'https://www.googleapis.com/auth/gmail.readonly',
                'https://www.googleapis.com/auth/gmail.modify',
                'https://www.googleapis.com/auth/userinfo.email'
            ]
        }
        
        with open(self.config_file, 'w') as f:
            json.dump(config, f, indent=2)
        
        # Set restrictive permissions on config file
        os.chmod(self.config_file, 0o600)
        
        # Set restrictive permissions on config directory  
        os.chmod(self.config_dir, 0o700)
        
        print(f"\n✓ Configuration saved to {self.config_file}")
        print("✓ OAuth setup complete!")
        
        # Step 5: Test authentication
        print("\n" + "="*50)
        test_auth = input("\nWould you like to test the authentication now? (y/n): ")
        if test_auth.lower() == 'y':
            self.test_authentication()
    
    def guide_google_cloud_setup(self):
        print("\n=== Google Cloud Setup Instructions ===")
        print("\n1. Create a Google Cloud Project:")
        print("   - Go to https://console.cloud.google.com")
        print("   - Click 'Create Project' (or select existing project)")
        print("   - Choose a project name (e.g., 'Email Pipeline')")
        print("   - Note the project ID")
        
        input("\nPress Enter when you've created/selected a project...")
        
        print("\n2. Enable Gmail API:")
        print("   - In the Google Cloud Console, go to 'APIs & Services' > 'Library'")
        print("   - Search for 'Gmail API'")
        print("   - Click on it and press 'Enable'")
        
        input("\nPress Enter when you've enabled the Gmail API...")
        
        print("\n3. Configure OAuth Consent Screen:")
        print("   - Go to 'APIs & Services' > 'OAuth consent screen'")
        print("   - Choose 'External' user type (unless using Google Workspace)")
        print("   - Fill in the required fields:")
        print("     • App name: Email Pipeline")
        print("     • User support email: Your email")
        print("     • Developer contact: Your email")
        print("   - Click 'Save and Continue'")
        print("   - On Scopes page, click 'Save and Continue'")
        print("   - On Test users page, add your email and click 'Save and Continue'")
        
        input("\nPress Enter when you've configured the consent screen...")
        
        print("\n4. Create OAuth 2.0 Credentials:")
        print("   - Go to 'APIs & Services' > 'Credentials'")
        print("   - Click 'Create Credentials' > 'OAuth client ID'")
        print("   - Choose 'Web application' as the application type")
        print("   - Name: 'Email Pipeline OAuth'")
        print("   - Add these authorized redirect URIs:")
        print("     • http://localhost:8080/auth/callback")
        print("     • http://localhost:3000/auth/callback")
        print("   - Click 'Create'")
        print("   - IMPORTANT: Copy the Client ID and Client Secret")
        
        open_browser = input("\nOpen Google Cloud Console in browser? (y/n): ")
        if open_browser.lower() == 'y':
            webbrowser.open("https://console.cloud.google.com/apis/credentials")
    
    def test_authentication(self):
        """Test the OAuth authentication"""
        print("\nTesting OAuth authentication...")
        print("This will open a browser window for authentication.")
        
        try:
            # Import and run the OAuth service
            from local_oauth_service import LocalOAuth2Service
            import asyncio
            
            async def test():
                oauth = LocalOAuth2Service()
                await oauth.authenticate()
                print("\n✓ Authentication test successful!")
                print("You're ready to use the Email Pipeline with OAuth.")
            
            asyncio.run(test())
        except Exception as e:
            print(f"\n❌ Authentication test failed: {e}")
            print("\nPlease check your credentials and try again.")

if __name__ == "__main__":
    setup = OAuthSetup()
    setup.run()