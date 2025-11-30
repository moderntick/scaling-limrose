#!/usr/bin/env python3
"""
OAuth Troubleshooting Tool
Diagnoses common OAuth setup and runtime issues
"""
import sys
import os
import json
from pathlib import Path
import subprocess
import socket
import requests
from urllib.parse import urlparse

def check_system_requirements():
    """Check basic system requirements"""
    print("=== System Requirements Check ===")
    
    issues = []
    
    # Python version
    python_version = sys.version_info
    if python_version < (3, 7):
        issues.append(f"Python {python_version.major}.{python_version.minor} detected. Python 3.7+ required.")
    else:
        print(f"✓ Python {python_version.major}.{python_version.minor}.{python_version.micro} (compatible)")
    
    # Required modules
    required_modules = [
        'cryptography', 'aiohttp', 'google.oauth2', 'googleapiclient'
    ]
    
    for module in required_modules:
        try:
            __import__(module)
            print(f"✓ {module} module available")
        except ImportError:
            issues.append(f"Missing required module: {module}")
    
    # Internet connectivity
    try:
        requests.get('https://www.google.com', timeout=5)
        print("✓ Internet connectivity working")
    except:
        issues.append("No internet connectivity or Google is blocked")
    
    return issues

def check_oauth_config():
    """Check OAuth configuration"""
    print("\n=== OAuth Configuration Check ===")
    
    issues = []
    config_path = Path.home() / '.email-pipeline' / 'config' / 'oauth_config.json'
    
    # Check if config exists
    if not config_path.exists():
        issues.append("OAuth configuration not found. Run 'python setup_oauth.py'")
        return issues
    
    print(f"✓ Config file found: {config_path}")
    
    # Check config file permissions
    import stat
    file_stat = config_path.stat()
    file_perms = oct(file_stat.st_mode)[-3:]
    
    if file_perms != '600':
        issues.append(f"Config file has insecure permissions: {file_perms} (should be 600)")
    else:
        print(f"✓ Config file permissions secure: {file_perms}")
    
    # Check config file content
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        required_keys = ['client_id', 'client_secret', 'redirect_uri', 'encryption_key', 'scopes']
        
        for key in required_keys:
            if key not in config:
                issues.append(f"Missing config key: {key}")
            else:
                print(f"✓ Config has {key}")
        
        # Check client ID format
        if 'client_id' in config:
            client_id = config['client_id']
            if not client_id.endswith('.apps.googleusercontent.com'):
                issues.append("Client ID format looks incorrect (should end with .apps.googleusercontent.com)")
            else:
                print("✓ Client ID format looks correct")
        
        # Check redirect URI
        if 'redirect_uri' in config:
            redirect_uri = config['redirect_uri']
            parsed = urlparse(redirect_uri)
            if parsed.hostname != 'localhost':
                issues.append(f"Redirect URI should use localhost, got: {parsed.hostname}")
            else:
                print(f"✓ Redirect URI uses localhost: {redirect_uri}")
        
        # Check scopes
        if 'scopes' in config:
            expected_scopes = [
                'https://www.googleapis.com/auth/gmail.readonly',
                'https://www.googleapis.com/auth/gmail.modify',
                'https://www.googleapis.com/auth/userinfo.email'
            ]
            missing_scopes = set(expected_scopes) - set(config['scopes'])
            if missing_scopes:
                issues.append(f"Missing OAuth scopes: {missing_scopes}")
            else:
                print(f"✓ All required scopes present")
        
    except json.JSONDecodeError:
        issues.append("Config file contains invalid JSON")
    except Exception as e:
        issues.append(f"Error reading config file: {e}")
    
    return issues

def check_google_cloud_setup():
    """Check Google Cloud project setup"""
    print("\n=== Google Cloud Setup Check ===")
    
    issues = []
    config_path = Path.home() / '.email-pipeline' / 'config' / 'oauth_config.json'
    
    if not config_path.exists():
        issues.append("Cannot check Google Cloud setup - no config file")
        return issues
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        client_id = config.get('client_id', '')
        
        # Test OAuth endpoints
        print("Testing Google OAuth endpoint accessibility...")
        
        try:
            auth_url = 'https://accounts.google.com/o/oauth2/v2/auth'
            response = requests.get(auth_url, timeout=10)
            if response.status_code == 200:
                print("✓ Google OAuth endpoints accessible")
            else:
                issues.append(f"Google OAuth endpoint returned status {response.status_code}")
        except requests.RequestException as e:
            issues.append(f"Cannot reach Google OAuth endpoints: {e}")
        
        # Test token endpoint
        try:
            token_url = 'https://oauth2.googleapis.com/token'
            # Just test connectivity, don't actually call
            response = requests.head(token_url, timeout=10)
            print("✓ Google token endpoint accessible")
        except requests.RequestException as e:
            issues.append(f"Cannot reach Google token endpoint: {e}")
        
    except Exception as e:
        issues.append(f"Error checking Google Cloud setup: {e}")
    
    return issues

def check_network_connectivity():
    """Check network and firewall issues"""
    print("\n=== Network Connectivity Check ===")
    
    issues = []
    
    # Test port availability
    test_ports = [8080, 8081, 8082, 8083, 8084]
    available_ports = []
    
    for port in test_ports:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(('localhost', port))
            sock.close()
            available_ports.append(port)
        except OSError:
            pass
    
    if available_ports:
        print(f"✓ Available ports for OAuth callback: {available_ports}")
    else:
        issues.append("No available ports found in range 8080-8084")
    
    # Test local server capability
    try:
        import http.server
        import socketserver
        
        # Try to start a simple server briefly
        port = available_ports[0] if available_ports else 9999
        handler = http.server.SimpleHTTPRequestHandler
        
        with socketserver.TCPServer(("localhost", port), handler) as httpd:
            print(f"✓ Can start local HTTP server on port {port}")
    except Exception as e:
        issues.append(f"Cannot start local HTTP server: {e}")
    
    return issues

def check_existing_tokens():
    """Check existing OAuth tokens"""
    print("\n=== Existing Token Check ===")
    
    issues = []
    token_path = Path.home() / '.email-pipeline' / 'config' / 'token.json'
    
    if not token_path.exists():
        print("ℹ️  No existing tokens found (normal for first setup)")
        return issues
    
    print(f"✓ Token file found: {token_path}")
    
    # Check token file permissions
    import stat
    file_stat = token_path.stat()
    file_perms = oct(file_stat.st_mode)[-3:]
    
    if file_perms != '600':
        issues.append(f"Token file has insecure permissions: {file_perms} (should be 600)")
    else:
        print(f"✓ Token file permissions secure: {file_perms}")
    
    # Try to decrypt tokens
    try:
        config_path = Path.home() / '.email-pipeline' / 'config' / 'oauth_config.json'
        if config_path.exists():
            from local_oauth_service import LocalOAuth2Service
            oauth = LocalOAuth2Service()
            creds = oauth.load_credentials()
            
            if creds:
                print("✓ Tokens can be decrypted successfully")
                
                # Check token expiry
                if hasattr(creds, 'expiry') and creds.expiry:
                    from datetime import datetime, timezone
                    if creds.expiry < datetime.now(timezone.utc):
                        print("⚠️  Access token is expired (will be refreshed automatically)")
                    else:
                        print("✓ Access token is valid")
                
                if hasattr(creds, 'refresh_token') and creds.refresh_token:
                    print("✓ Refresh token is available")
                else:
                    issues.append("No refresh token available - may need to re-authenticate")
            else:
                issues.append("Could not load credentials from token file")
    except Exception as e:
        issues.append(f"Error checking existing tokens: {e}")
    
    return issues

def check_gmail_api_access():
    """Check Gmail API access"""
    print("\n=== Gmail API Access Check ===")
    
    issues = []
    
    try:
        from local_oauth_service import LocalOAuth2Service
        
        oauth = LocalOAuth2Service()
        
        # Check if we have valid credentials
        if not oauth.token_path.exists():
            print("ℹ️  No tokens found - cannot test Gmail API access")
            print("   Run OAuth authentication first to test API access")
            return issues
        
        # Try to get Gmail service
        gmail_service = oauth.get_gmail_service()
        print("✓ Gmail service object created successfully")
        
        # Try a simple API call
        try:
            profile = gmail_service.users().getProfile(userId='me').execute()
            if 'emailAddress' in profile:
                email = profile['emailAddress']
                print(f"✓ Gmail API access working - authenticated as: {email}")
            else:
                issues.append("Gmail API call succeeded but unexpected response format")
        except Exception as e:
            error_str = str(e).lower()
            if 'quota' in error_str:
                issues.append("Gmail API quota exceeded")
            elif 'permission' in error_str or 'forbidden' in error_str:
                issues.append("Insufficient permissions for Gmail API")
            elif 'disabled' in error_str:
                issues.append("Gmail API is disabled in Google Cloud Console")
            else:
                issues.append(f"Gmail API call failed: {e}")
        
    except FileNotFoundError:
        issues.append("OAuth not configured - run 'python setup_oauth.py' first")
    except Exception as e:
        issues.append(f"Error testing Gmail API access: {e}")
    
    return issues

def check_browser_capability():
    """Check browser availability for OAuth flow"""
    print("\n=== Browser Capability Check ===")
    
    issues = []
    
    # Try to detect available browsers
    import webbrowser
    
    # Test if webbrowser can open URLs
    try:
        # This doesn't actually open a browser, just tests the capability
        browsers = []
        
        # Common browser commands
        browser_commands = [
            'google-chrome', 'chrome', 'chromium',
            'firefox', 'safari', 'edge'
        ]
        
        for browser in browser_commands:
            try:
                result = subprocess.run(
                    ['which', browser], 
                    capture_output=True, 
                    text=True, 
                    timeout=2
                )
                if result.returncode == 0:
                    browsers.append(browser)
            except:
                pass
        
        if browsers:
            print(f"✓ Available browsers: {browsers}")
        else:
            issues.append("No browsers detected - OAuth flow may require manual URL opening")
        
        print("✓ webbrowser module available")
        
    except Exception as e:
        issues.append(f"Error checking browser capability: {e}")
    
    return issues

def run_diagnostics():
    """Run all diagnostic checks"""
    print("🔍 OAuth Troubleshooting Tool")
    print("=" * 50)
    
    all_issues = []
    
    # Run all checks
    checks = [
        ("System Requirements", check_system_requirements),
        ("OAuth Configuration", check_oauth_config),
        ("Google Cloud Setup", check_google_cloud_setup),
        ("Network Connectivity", check_network_connectivity),
        ("Existing Tokens", check_existing_tokens),
        ("Gmail API Access", check_gmail_api_access),
        ("Browser Capability", check_browser_capability)
    ]
    
    for check_name, check_func in checks:
        try:
            issues = check_func()
            all_issues.extend(issues)
        except Exception as e:
            all_issues.append(f"Error during {check_name}: {e}")
    
    # Summary
    print(f"\n{'=' * 50}")
    print("🎯 DIAGNOSIS SUMMARY")
    print(f"{'=' * 50}")
    
    if all_issues:
        print(f"\n❌ Found {len(all_issues)} issue(s):")
        for i, issue in enumerate(all_issues, 1):
            print(f"  {i}. {issue}")
        
        print(f"\n💡 RECOMMENDED ACTIONS:")
        
        # Provide specific recommendations
        if any('OAuth configuration not found' in issue for issue in all_issues):
            print("  1. Run: python setup_oauth.py")
        
        if any('Gmail API' in issue for issue in all_issues):
            print("  2. Check Gmail API is enabled in Google Cloud Console")
        
        if any('permissions' in issue.lower() for issue in all_issues):
            print("  3. Fix file permissions: chmod 600 ~/.email-pipeline/config/*")
        
        if any('port' in issue.lower() for issue in all_issues):
            print("  4. Close applications using ports 8080-8084")
        
        if any('quota' in issue.lower() for issue in all_issues):
            print("  5. Wait for API quota reset or request increase")
        
        print(f"\n📚 For detailed help: docs/OAUTH_SETUP.md")
        
    else:
        print("\n✅ No issues found! OAuth setup appears to be working correctly.")
        print("\nIf you're still experiencing problems:")
        print("  1. Try: python gmail_oauth_extractor.py --test")
        print("  2. Check the application logs for detailed error messages")
        print("  3. Consider revoking and re-authenticating: python gmail_oauth_extractor.py --revoke")
    
    return len(all_issues) == 0

def main():
    """Main function"""
    if len(sys.argv) > 1 and sys.argv[1] == '--help':
        print("OAuth Troubleshooting Tool")
        print("\nUsage:")
        print("  python oauth_troubleshoot.py       # Run all diagnostic checks")
        print("  python oauth_troubleshoot.py --help # Show this help")
        return
    
    success = run_diagnostics()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()