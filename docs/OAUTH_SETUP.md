# Gmail OAuth Setup Guide

This guide will help you set up Gmail authentication for the Email Pipeline project.

## Prerequisites

- Python 3.7 or higher
- A Google account
- Internet connection
- PostgreSQL database (already set up for the email pipeline)

## Quick Start

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the setup script:
   ```bash
   python setup_oauth.py
   ```

3. Follow the interactive prompts to configure OAuth

## Detailed Setup Steps

### 1. Run the Setup Script

The setup script will guide you through the entire process:

```bash
python setup_oauth.py
```

This interactive script will:
- Guide you through creating a Google Cloud project
- Help you enable the Gmail API
- Assist with creating OAuth credentials
- Save your configuration securely
- Optionally test the authentication

### 2. Google Cloud Configuration

When prompted by the setup script, you'll need to:

#### Step 1: Create a Google Cloud Project
1. Visit [Google Cloud Console](https://console.cloud.google.com)
2. Click "Select a project" → "New Project"
3. Enter a project name (e.g., "Email Pipeline")
4. Click "Create"
5. Wait for the project to be created

#### Step 2: Enable Gmail API
1. In your project, go to "APIs & Services" → "Library"
2. Search for "Gmail API"
3. Click on "Gmail API"
4. Click "Enable"

#### Step 3: Configure OAuth Consent Screen
1. Go to "APIs & Services" → "OAuth consent screen"
2. Choose "External" user type (unless you have Google Workspace)
3. Fill in the required fields:
   - **App name**: Email Pipeline
   - **User support email**: Your email address
   - **Developer contact**: Your email address
4. Click "Save and Continue"
5. On the Scopes page, click "Save and Continue" (we'll add scopes via the OAuth flow)
6. On the Test users page:
   - Click "Add Users"
   - Add your email address
   - Click "Save and Continue"
7. Review and click "Back to Dashboard"

#### Step 4: Create OAuth 2.0 Credentials
1. Go to "APIs & Services" → "Credentials"
2. Click "Create Credentials" → "OAuth client ID"
3. Application type: **Web application**
4. Name: "Email Pipeline OAuth"
5. Under "Authorized redirect URIs", add:
   - `http://localhost:8080/auth/callback`
   - `http://localhost:3000/auth/callback`
6. Click "Create"
7. **Important**: Copy the Client ID and Client Secret when shown

### 3. Complete Setup

1. Enter your Client ID when prompted by the setup script
2. Enter your Client Secret when prompted
3. The script will save your configuration to `~/.email-pipeline/config/oauth_config.json`
4. Choose to test authentication when prompted

## First Run

When you first run the email extraction:

```bash
python gmail_oauth_extractor.py
```

1. A browser window will automatically open
2. Log in with your Google account
3. Review the requested permissions:
   - Read your email messages and settings
   - Modify email labels
   - View your email address
4. Click "Allow"
5. You'll see a success message in the browser
6. Return to the terminal to see extraction progress

Your authentication tokens are saved locally and encrypted for future use.

## Usage Options

### Extract Unread Emails (Default)
```bash
python gmail_oauth_extractor.py
```
Extracts unread emails from the last 7 days and saves them to the database.

### Test Mode
```bash
python gmail_oauth_extractor.py --test
```
Fetches 5 unread emails and displays them without saving to the database.

### Revoke Authentication
```bash
python gmail_oauth_extractor.py --revoke
```
Removes stored authentication tokens. You'll need to re-authenticate next time.

## Troubleshooting

### "OAuth configuration not found"
**Solution**: Run `python setup_oauth.py` to create the configuration.

### "Authorization failed"
**Possible causes**:
- Redirect URIs don't match exactly what you configured in Google Cloud
- Gmail API is not enabled
- OAuth consent screen is not properly configured

**Solutions**:
1. Double-check redirect URIs in Google Cloud Console
2. Ensure Gmail API is enabled
3. Verify OAuth consent screen is configured
4. Try revoking and re-authenticating: `python gmail_oauth_extractor.py --revoke`

### "No available ports found"
**Solution**: The OAuth callback server couldn't find an available port. Try:
- Close other applications using ports 8080-8089
- Manually specify a different port range in the code

### Token Expired
The application automatically refreshes expired tokens. If issues persist:
```bash
rm ~/.email-pipeline/config/token.json
python gmail_oauth_extractor.py
```

### Permission Denied Errors
Ensure your user has write permissions to the home directory:
```bash
chmod 700 ~/.email-pipeline
chmod 600 ~/.email-pipeline/config/*
```

## Security Notes

### Local Storage
- OAuth credentials are stored in `~/.email-pipeline/config/`
- Tokens are encrypted using Fernet (symmetric encryption)
- Config files have restrictive permissions (600)

### Best Practices
1. **Never commit credentials**: The `.gitignore` file excludes OAuth config files
2. **Keep credentials secure**: Don't share your `oauth_config.json` or `token.json`
3. **Use test mode first**: Always test with a small number of emails first
4. **Revoke when needed**: Use `--revoke` if you suspect credentials are compromised

### Required Scopes
The application requests these Gmail API scopes:
- `gmail.readonly`: Read email messages and labels
- `gmail.modify`: Modify email labels (mark as read)
- `userinfo.email`: Get your email address

## Integration with Email Pipeline

The OAuth extractor integrates seamlessly with the existing email pipeline:

1. **Database Compatibility**: Uses the same `raw_emails` table structure
2. **Deduplication**: Checks for existing emails before inserting
3. **Normalization**: Email addresses are normalized (dots removed, lowercase)
4. **Thread Support**: Maintains Gmail thread IDs for conversation tracking

## Advanced Configuration

### Custom Port Range
Edit `local_oauth_service.py` to change the port range:
```python
port = await self.find_available_port(start_port=9000, max_attempts=10)
```

### Token Refresh Settings
Tokens are automatically refreshed when they expire. To adjust the refresh buffer:
```python
def ensure_fresh_token(self, buffer_minutes=10):  # Default 5 minutes
```

### Logging
Enable debug logging by setting the environment variable:
```bash
export EMAIL_PIPELINE_DEBUG=1
python gmail_oauth_extractor.py
```

## FAQ

**Q: Can I use this with multiple Gmail accounts?**
A: Yes, but you'll need to modify the code to support multiple token files or run separate instances.

**Q: How long do tokens last?**
A: Access tokens expire after 1 hour. Refresh tokens don't expire unless revoked or unused for 6 months.

**Q: Can I use this in production?**
A: Yes, but consider:
- Moving OAuth config to environment variables
- Using a secrets management service
- Implementing proper error monitoring
- Setting up automated token refresh

**Q: Is this compatible with Google Workspace?**
A: Yes, it works with both personal Gmail and Google Workspace accounts.

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review the Google Cloud Console for API errors
3. Check application logs for detailed error messages
4. Open an issue on the project repository