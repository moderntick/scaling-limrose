"""
OAuth Error Handler for Gmail API Integration
Provides comprehensive error handling and user-friendly error messages
"""
import logging
import json
from typing import Dict, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class OAuthError:
    """OAuth error data structure"""
    code: str
    message: str
    solution: str
    is_recoverable: bool
    retry_delay: int = 0

class OAuthErrorHandler:
    """Centralized OAuth error handling"""
    
    # Common OAuth error codes and their handling
    ERROR_MAPPINGS = {
        'invalid_client': OAuthError(
            code='invalid_client',
            message='Invalid OAuth client credentials',
            solution='Check your client ID and secret in Google Cloud Console. Make sure they match exactly.',
            is_recoverable=True
        ),
        'invalid_grant': OAuthError(
            code='invalid_grant',
            message='Invalid authorization grant',
            solution='Authorization code expired or was already used. Please restart the authentication flow.',
            is_recoverable=True
        ),
        'invalid_request': OAuthError(
            code='invalid_request',
            message='Invalid OAuth request',
            solution='Check that redirect URIs match exactly between your config and Google Cloud Console.',
            is_recoverable=True
        ),
        'unauthorized_client': OAuthError(
            code='unauthorized_client',
            message='Client not authorized for this grant type',
            solution='Enable "Web application" OAuth client type in Google Cloud Console.',
            is_recoverable=True
        ),
        'access_denied': OAuthError(
            code='access_denied',
            message='User denied authorization',
            solution='Please grant access to the Gmail API when prompted. All permissions are required for the email pipeline to work.',
            is_recoverable=True
        ),
        'invalid_scope': OAuthError(
            code='invalid_scope',
            message='Invalid OAuth scope requested',
            solution='Check that Gmail API is enabled in Google Cloud Console.',
            is_recoverable=True
        ),
        'rate_limit_exceeded': OAuthError(
            code='rate_limit_exceeded',
            message='Too many authentication requests',
            solution='Wait 60 seconds before trying again. Consider using fewer concurrent requests.',
            is_recoverable=True,
            retry_delay=60
        ),
        'network_error': OAuthError(
            code='network_error',
            message='Network connection failed',
            solution='Check your internet connection and try again.',
            is_recoverable=True,
            retry_delay=10
        ),
        'timeout': OAuthError(
            code='timeout',
            message='Authentication timed out',
            solution='Complete the browser authorization within 5 minutes.',
            is_recoverable=True
        ),
        'port_unavailable': OAuthError(
            code='port_unavailable',
            message='Cannot start OAuth callback server',
            solution='Close other applications using ports 8080-8089, or restart your computer.',
            is_recoverable=True
        ),
        'config_not_found': OAuthError(
            code='config_not_found',
            message='OAuth configuration not found',
            solution='Run "python setup_oauth.py" to configure OAuth credentials.',
            is_recoverable=True
        ),
        'token_expired': OAuthError(
            code='token_expired',
            message='Access token expired and refresh failed',
            solution='Re-authenticate by running "python gmail_oauth_extractor.py --revoke" then re-running the extractor.',
            is_recoverable=True
        ),
        'api_disabled': OAuthError(
            code='api_disabled',
            message='Gmail API is disabled',
            solution='Enable Gmail API in Google Cloud Console under "APIs & Services" > "Library".',
            is_recoverable=True
        ),
        'quota_exceeded': OAuthError(
            code='quota_exceeded',
            message='API quota exceeded',
            solution='Wait for quota to reset (usually 24 hours) or request quota increase in Google Cloud Console.',
            is_recoverable=True,
            retry_delay=3600
        ),
        'insufficient_permissions': OAuthError(
            code='insufficient_permissions',
            message='Insufficient Gmail permissions',
            solution='Re-authenticate and grant all requested permissions. All scopes are required.',
            is_recoverable=True
        ),
        'browser_error': OAuthError(
            code='browser_error',
            message='Cannot open web browser',
            solution='Manually visit the OAuth URL shown in the terminal.',
            is_recoverable=True
        )
    }
    
    def __init__(self):
        self.error_count = {}
    
    def handle_exception(self, exception: Exception, context: str = "") -> OAuthError:
        """
        Handle an exception and return appropriate OAuthError
        
        Args:
            exception: The exception that occurred
            context: Additional context about where the error occurred
            
        Returns:
            OAuthError with appropriate message and solution
        """
        error_str = str(exception).lower()
        
        # Check for specific error patterns
        if 'invalid_client' in error_str:
            return self.ERROR_MAPPINGS['invalid_client']
        elif 'invalid_grant' in error_str:
            return self.ERROR_MAPPINGS['invalid_grant']
        elif 'access_denied' in error_str:
            return self.ERROR_MAPPINGS['access_denied']
        elif 'invalid_request' in error_str:
            return self.ERROR_MAPPINGS['invalid_request']
        elif 'unauthorized_client' in error_str:
            return self.ERROR_MAPPINGS['unauthorized_client']
        elif 'invalid_scope' in error_str:
            return self.ERROR_MAPPINGS['invalid_scope']
        elif 'rate' in error_str and 'limit' in error_str:
            return self.ERROR_MAPPINGS['rate_limit_exceeded']
        elif 'network' in error_str or 'connection' in error_str:
            return self.ERROR_MAPPINGS['network_error']
        elif 'timeout' in error_str:
            return self.ERROR_MAPPINGS['timeout']
        elif 'port' in error_str or 'address already in use' in error_str:
            return self.ERROR_MAPPINGS['port_unavailable']
        elif 'oauth configuration not found' in error_str:
            return self.ERROR_MAPPINGS['config_not_found']
        elif 'token' in error_str and 'expired' in error_str:
            return self.ERROR_MAPPINGS['token_expired']
        elif 'api' in error_str and 'disabled' in error_str:
            return self.ERROR_MAPPINGS['api_disabled']
        elif 'quota' in error_str and 'exceeded' in error_str:
            return self.ERROR_MAPPINGS['quota_exceeded']
        elif 'permission' in error_str or 'forbidden' in error_str:
            return self.ERROR_MAPPINGS['insufficient_permissions']
        elif 'browser' in error_str:
            return self.ERROR_MAPPINGS['browser_error']
        else:
            # Generic error
            return OAuthError(
                code='unknown_error',
                message=f'An unexpected error occurred: {str(exception)}',
                solution='Check the detailed error message above. If the problem persists, check the troubleshooting guide.',
                is_recoverable=False
            )
    
    def handle_http_error(self, status_code: int, response_text: str) -> OAuthError:
        """
        Handle HTTP errors from OAuth requests
        
        Args:
            status_code: HTTP status code
            response_text: Response body text
            
        Returns:
            OAuthError with appropriate handling
        """
        if status_code == 400:
            if 'invalid_grant' in response_text:
                return self.ERROR_MAPPINGS['invalid_grant']
            elif 'invalid_client' in response_text:
                return self.ERROR_MAPPINGS['invalid_client']
            else:
                return self.ERROR_MAPPINGS['invalid_request']
        elif status_code == 401:
            return self.ERROR_MAPPINGS['unauthorized_client']
        elif status_code == 403:
            if 'quota' in response_text.lower():
                return self.ERROR_MAPPINGS['quota_exceeded']
            else:
                return self.ERROR_MAPPINGS['insufficient_permissions']
        elif status_code == 404:
            return OAuthError(
                code='endpoint_not_found',
                message='OAuth endpoint not found',
                solution='Check that you\'re using the correct Google OAuth URLs.',
                is_recoverable=True
            )
        elif status_code == 429:
            return self.ERROR_MAPPINGS['rate_limit_exceeded']
        elif status_code >= 500:
            return OAuthError(
                code='server_error',
                message='Google OAuth server error',
                solution='Google\'s servers are experiencing issues. Try again in a few minutes.',
                is_recoverable=True,
                retry_delay=300
            )
        else:
            return OAuthError(
                code=f'http_{status_code}',
                message=f'HTTP {status_code} error during OAuth',
                solution=f'Unexpected HTTP error. Response: {response_text[:100]}...',
                is_recoverable=False
            )
    
    def log_error(self, oauth_error: OAuthError, context: str = ""):
        """Log an OAuth error with appropriate level"""
        error_key = oauth_error.code
        self.error_count[error_key] = self.error_count.get(error_key, 0) + 1
        
        log_message = f"OAuth Error [{oauth_error.code}]: {oauth_error.message}"
        if context:
            log_message += f" (Context: {context})"
        
        if oauth_error.is_recoverable:
            logger.warning(log_message)
        else:
            logger.error(log_message)
    
    def print_user_friendly_error(self, oauth_error: OAuthError, show_retry_info: bool = True):
        """Print a user-friendly error message"""
        print(f"\n❌ {oauth_error.message}")
        print(f"\n💡 Solution: {oauth_error.solution}")
        
        if oauth_error.is_recoverable:
            print("\n✨ This error can be fixed. Please try the solution above.")
            if oauth_error.retry_delay > 0 and show_retry_info:
                print(f"⏰ Recommended wait time: {oauth_error.retry_delay} seconds")
        else:
            print("\n⚠️  This is a critical error that requires manual intervention.")
        
        if oauth_error.code in ['config_not_found', 'invalid_client']:
            print("\n📚 For detailed setup instructions, see: docs/OAUTH_SETUP.md")
    
    def should_retry(self, oauth_error: OAuthError) -> Tuple[bool, int]:
        """
        Determine if an error should be retried and after what delay
        
        Returns:
            (should_retry, delay_seconds)
        """
        if not oauth_error.is_recoverable:
            return False, 0
        
        # Don't retry certain errors automatically
        non_retry_errors = [
            'config_not_found', 'invalid_client', 'access_denied',
            'unauthorized_client', 'api_disabled'
        ]
        
        if oauth_error.code in non_retry_errors:
            return False, 0
        
        # Limit retries for specific errors
        error_key = oauth_error.code
        retry_count = self.error_count.get(error_key, 0)
        
        if oauth_error.code == 'rate_limit_exceeded' and retry_count < 3:
            return True, oauth_error.retry_delay
        elif oauth_error.code == 'network_error' and retry_count < 5:
            return True, min(oauth_error.retry_delay * retry_count, 60)
        elif oauth_error.code == 'timeout' and retry_count < 2:
            return True, 0
        
        return False, 0
    
    def get_troubleshooting_info(self) -> Dict:
        """Get troubleshooting information for common issues"""
        return {
            'error_counts': self.error_count.copy(),
            'common_solutions': {
                'setup_issues': [
                    "Run 'python setup_oauth.py' to configure OAuth",
                    "Ensure Gmail API is enabled in Google Cloud Console",
                    "Check that redirect URIs match exactly",
                    "Verify OAuth consent screen is configured"
                ],
                'runtime_issues': [
                    "Check internet connection",
                    "Verify credentials haven't expired",
                    "Ensure no firewall blocking OAuth callback",
                    "Try revoking and re-authenticating"
                ],
                'permission_issues': [
                    "Grant all requested permissions during OAuth flow",
                    "Check that user has access to Gmail account",
                    "Verify OAuth scopes are correct",
                    "Ensure account isn't suspended or restricted"
                ]
            },
            'useful_commands': {
                'setup': 'python setup_oauth.py',
                'test': 'python gmail_oauth_extractor.py --test',
                'revoke': 'python gmail_oauth_extractor.py --revoke',
                'troubleshoot': 'python oauth_troubleshoot.py'
            }
        }

# Global error handler instance
oauth_error_handler = OAuthErrorHandler()