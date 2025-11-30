"""
Local OAuth2 Service for Gmail API
Handles authentication flow for self-hosted deployments
"""
import os
import json
import asyncio
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict
import aiohttp
from aiohttp import web
import webbrowser
from cryptography.fernet import Fernet
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import urllib.parse
import logging
import socket

logger = logging.getLogger(__name__)

class LocalOAuth2Service:
    def __init__(self):
        self.config_path = Path.home() / '.email-pipeline' / 'config' / 'oauth_config.json'
        self.token_path = Path.home() / '.email-pipeline' / 'config' / 'token.json'
        self.load_config()
        self.callback_received = asyncio.Event()
        self.auth_code = None
        self.auth_error = None
        
    def load_config(self):
        """Load OAuth configuration from local config file"""
        if not self.config_path.exists():
            print("❌ OAuth configuration not found")
            print("💡 Solution: Run 'python setup_oauth.py' to configure OAuth credentials")
            raise FileNotFoundError(
                "OAuth configuration not found. Please run 'python setup_oauth.py' first."
            )
        
        with open(self.config_path, 'r') as f:
            config = json.load(f)
        
        self.client_id = config['client_id']
        self.client_secret = config['client_secret']
        self.redirect_uri = config['redirect_uri']
        self.scopes = config['scopes']
        self.fernet = Fernet(config['encryption_key'].encode())
    
    async def find_available_port(self, start_port=8080, max_attempts=10):
        """Find an available port for the OAuth callback server"""
        for port in range(start_port, start_port + max_attempts):
            try:
                # Test if port is available
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.bind(('localhost', port))
                sock.close()
                return port
            except OSError:
                continue
        print("❌ No available ports found for OAuth callback server")
        print("💡 Solution: Close other applications using ports 8080-8089, or restart your computer")
        raise Exception(f"No available ports found between {start_port} and {start_port + max_attempts}")
    
    async def authenticate(self):
        """Start OAuth flow and get user authorization"""
        # Check if we already have valid tokens
        if self.token_path.exists():
            creds = self.load_credentials()
            if creds and creds.valid:
                print("✓ Already authenticated!")
                return creds
            elif creds and creds.expired and creds.refresh_token:
                print("Refreshing expired token...")
                try:
                    creds.refresh(Request())
                    self.save_credentials(creds)
                    print("✓ Token refreshed successfully!")
                    return creds
                except Exception as e:
                    print(f"Token refresh failed: {e}")
                    print("Starting new authentication flow...")
        
        # Find available port
        port = await self.find_available_port()
        self.redirect_uri = f'http://localhost:{port}/auth/callback'
        
        # Start local server for OAuth callback
        app = web.Application()
        app.router.add_get('/auth/callback', self.handle_callback)
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', port)
        
        try:
            await site.start()
            
            print(f"\n=== Gmail Authentication Required ===")
            print(f"OAuth callback server listening on port {port}")
            print("A browser window will open for you to authorize Gmail access.")
            
            # Generate OAuth URL
            auth_url = self.get_authorization_url()
            
            # Open browser
            print(f"\nOpening browser...")
            try:
                webbrowser.open(auth_url)
                print("Browser opened successfully.")
            except:
                print(f"\n❌ Couldn't open browser automatically.")
                print(f"Please visit this URL manually:\n\n{auth_url}\n")
            
            print("\nWaiting for authorization...")
            
            # Wait for callback with timeout
            try:
                await asyncio.wait_for(self.callback_received.wait(), timeout=300)  # 5 minute timeout
            except asyncio.TimeoutError:
                raise Exception("Authentication timed out after 5 minutes")
            
            # Check if we got an error
            if self.auth_error:
                raise Exception(f"Authorization failed: {self.auth_error}")
            
            if not self.auth_code:
                raise Exception("Authorization failed - no code received")
            
            # Exchange code for tokens
            print("Exchanging authorization code for tokens...")
            creds = await self.exchange_code_for_tokens(self.auth_code)
            
            # Save encrypted tokens
            self.save_credentials(creds)
            
            print("✓ Authentication successful!")
            return creds
            
        finally:
            # Clean up server
            await runner.cleanup()
    
    def get_authorization_url(self):
        """Generate the OAuth authorization URL"""
        params = {
            'client_id': self.client_id,
            'redirect_uri': self.redirect_uri,
            'response_type': 'code',
            'scope': ' '.join(self.scopes),
            'access_type': 'offline',
            'prompt': 'consent'  # Force consent to ensure refresh token
        }
        return f"https://accounts.google.com/o/oauth2/v2/auth?{urllib.parse.urlencode(params)}"
    
    async def handle_callback(self, request):
        """Handle OAuth callback"""
        code = request.query.get('code')
        error = request.query.get('error')
        
        if error:
            self.auth_error = error
            self.auth_code = None
            self.callback_received.set()
            html = f"""
            <html>
            <head>
                <title>Authorization Failed</title>
                <style>
                    body {{ font-family: Arial, sans-serif; text-align: center; padding: 50px; }}
                    .error {{ color: red; font-size: 24px; }}
                </style>
            </head>
            <body>
                <div class="error">❌ Authorization failed</div>
                <p>Error: {error}</p>
                <p>Please close this window and try again.</p>
            </body>
            </html>
            """
            return web.Response(text=html, content_type='text/html')
        
        self.auth_code = code
        self.auth_error = None
        self.callback_received.set()
        
        # Return success page
        html = """
        <html>
        <head>
            <title>Authorization Successful</title>
            <style>
                body { font-family: Arial, sans-serif; text-align: center; padding: 50px; }
                .success { color: green; font-size: 24px; }
            </style>
        </head>
        <body>
            <div class="success">✓ Authorization successful!</div>
            <p>You can close this window and return to the application.</p>
        </body>
        </html>
        """
        return web.Response(text=html, content_type='text/html')
    
    async def exchange_code_for_tokens(self, code):
        """Exchange authorization code for access and refresh tokens"""
        token_url = 'https://oauth2.googleapis.com/token'
        
        data = {
            'code': code,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'redirect_uri': self.redirect_uri,
            'grant_type': 'authorization_code'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(token_url, data=data) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"Token exchange failed: {error_text}")
                
                token_data = await response.json()
        
        # Create credentials object
        creds = Credentials(
            token=token_data['access_token'],
            refresh_token=token_data.get('refresh_token'),
            token_uri='https://oauth2.googleapis.com/token',
            client_id=self.client_id,
            client_secret=self.client_secret,
            scopes=self.scopes
        )
        
        # Set expiry if provided
        if 'expires_in' in token_data:
            creds.expiry = datetime.now(timezone.utc) + timedelta(seconds=token_data['expires_in'])
        
        return creds
    
    def save_credentials(self, creds):
        """Save encrypted credentials to local file"""
        token_data = {
            'token': creds.token,
            'refresh_token': creds.refresh_token,
            'token_uri': creds.token_uri,
            'client_id': creds.client_id,
            'client_secret': creds.client_secret,
            'scopes': creds.scopes,
            'expiry': creds.expiry.isoformat() if creds.expiry else None
        }
        
        # Encrypt sensitive data
        encrypted_data = self.fernet.encrypt(json.dumps(token_data).encode())
        
        # Ensure directory exists
        self.token_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        
        with open(self.token_path, 'wb') as f:
            f.write(encrypted_data)
        
        # Set restrictive permissions
        os.chmod(self.token_path, 0o600)
    
    def load_credentials(self):
        """Load and decrypt credentials from local file"""
        if not self.token_path.exists():
            return None
        
        try:
            with open(self.token_path, 'rb') as f:
                encrypted_data = f.read()
            
            # Decrypt
            token_data = json.loads(self.fernet.decrypt(encrypted_data))
            
            # Create credentials object
            creds = Credentials(
                token=token_data['token'],
                refresh_token=token_data['refresh_token'],
                token_uri=token_data['token_uri'],
                client_id=token_data['client_id'],
                client_secret=token_data['client_secret'],
                scopes=token_data['scopes']
            )
            
            if token_data['expiry']:
                creds.expiry = datetime.fromisoformat(token_data['expiry'])
            
            return creds
        except Exception as e:
            logger.error(f"Failed to load credentials: {e}")
            return None
    
    def ensure_fresh_token(self, buffer_minutes=5):
        """Ensure token is fresh, refreshing if needed"""
        creds = self.load_credentials()
        if not creds:
            raise Exception("No credentials found. Please authenticate first.")
        
        if creds.expired or (creds.expiry and creds.expiry <= datetime.now(timezone.utc) + timedelta(minutes=buffer_minutes)):
            logger.info("Token expired or expiring soon, refreshing...")
            creds.refresh(Request())
            self.save_credentials(creds)
            
        return creds
    
    def get_gmail_service(self):
        """Get authenticated Gmail service"""
        creds = self.ensure_fresh_token()
        return build('gmail', 'v1', credentials=creds)
    
    def revoke_credentials(self):
        """Revoke stored credentials"""
        if self.token_path.exists():
            os.remove(self.token_path)
            print("✓ Credentials revoked successfully")