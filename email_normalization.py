import re
import hashlib
from typing import Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class ContentNormalizer:
    """
    Normalize email content for consistent hashing.
    Replaces URLs with [URL] and emails with [EMAIL] for proper deduplication.
    """
    
    def __init__(self):
        # URL pattern - matches http/https URLs
        self.url_pattern = re.compile(
            r'https?://(?:www\.)?[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*'
            r'(?:/[^?\s]*)?(?:\?[^#\s]*)?(?:#[^\s]*)?'
        )
        
        # Email pattern
        self.email_pattern = re.compile(
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        )
        
        # Whitespace normalization
        self.whitespace_pattern = re.compile(r'\s+')
        
        # Common tracking pixels and marketing parameters (for removal before URL replacement)
        self.tracking_patterns = [
            r'\?utm_[^&\s]+=[^&\s]+',
            r'&utm_[^&\s]+=[^&\s]+',
            r'/track/click/[a-zA-Z0-9]+',
            r'/pixel\.gif\?[^"\s]+',
            r'mailtrack\.io/trace/[a-zA-Z0-9]+',
        ]
    
    def normalize(self, text: str, preserve_structure: bool = False) -> str:
        """
        Normalize text for consistent hashing.
        
        Args:
            text: Text to normalize
            preserve_structure: If True, preserve newlines and structure
            
        Returns:
            Normalized text with URLs and emails replaced by placeholders
        """
        if not text:
            return ""
        
        # Convert to lowercase for case-insensitive matching
        normalized = text.lower()
        
        # Remove tracking URLs and parameters first
        for pattern in self.tracking_patterns:
            normalized = re.sub(pattern, '', normalized)
        
        # CRITICAL: Replace ALL URLs with [URL] placeholder
        # This ensures emails with different URLs but same content are detected as duplicates
        normalized = self.url_pattern.sub('[URL]', normalized)
        
        # CRITICAL: Replace ALL email addresses with [EMAIL] placeholder
        # This ensures emails mentioning different addresses are detected as duplicates
        normalized = self.email_pattern.sub('[EMAIL]', normalized)
        
        # Remove zero-width characters that can be used to manipulate hashes
        zero_width_chars = [
            '\u200b',  # Zero-width space
            '\u200c',  # Zero-width non-joiner
            '\u200d',  # Zero-width joiner
            '\ufeff',  # Zero-width no-break space
        ]
        for char in zero_width_chars:
            normalized = normalized.replace(char, '')
        
        # Normalize whitespace
        if preserve_structure:
            # Preserve newlines but normalize spaces
            lines = normalized.split('\n')
            lines = [self.whitespace_pattern.sub(' ', line.strip()) for line in lines]
            normalized = '\n'.join(lines)
        else:
            # Replace all whitespace with single spaces
            normalized = self.whitespace_pattern.sub(' ', normalized)
        
        # Normalize common Unicode variations
        variations = {
            ''': "'",  # Smart single quotes
            ''': "'",
            '"': '"',  # Smart double quotes
            '"': '"',
            '–': '-',  # En dash
            '—': '-',  # Em dash
            '…': '...',  # Ellipsis
        }
        for old, new in variations.items():
            normalized = normalized.replace(old, new)
        
        return normalized.strip()


class EmailNormalizer:
    """
    Email content normalization and fingerprinting for deduplication.
    Uses proper normalization that replaces URLs and emails with placeholders.
    """
    
    def __init__(self):
        """Initialize the email normalizer with proper content normalizer."""
        self.content_normalizer = ContentNormalizer()
    
    def normalize_email_content(self, body_text: str) -> str:
        """
        Apply proper normalization pipeline to email body text.
        
        Args:
            body_text: Raw email body text
            
        Returns:
            Normalized body text with URLs → [URL] and emails → [EMAIL]
        """
        if not body_text:
            return ""
        
        # Use the proper normalizer that replaces URLs and emails
        return self.content_normalizer.normalize(body_text, preserve_structure=True)
    
    def generate_fingerprint(self, body_text: str, body_html: str = None) -> str:
        """
        Generate SHA-256 fingerprint of properly normalized email content.
        
        Args:
            body_text: Raw email body text
            body_html: Raw email body HTML (optional fallback)
            
        Returns:
            SHA-256 hash of normalized content
        """
        # Use body_html if body_text is empty or just a placeholder
        content_to_use = body_text
        
        # Check if body_text is effectively empty or just a template message
        if (not body_text or 
            body_text.strip() == "" or 
            body_text.strip().lower() in ["view this newsletter in html", "view in browser", "[view in browser]"] or
            len(body_text.strip()) < 50):
            # Use HTML content if available
            if body_html:
                # For HTML content, we should extract text first
                # For now, use the raw HTML as it contains the actual content
                content_to_use = body_html
        
        # Normalize the content properly
        canonical_body = self.normalize_email_content(content_to_use)
        
        # IMPORTANT: Never return empty string hash
        # If normalization results in empty content, use the original
        if not canonical_body or len(canonical_body.strip()) == 0:
            logger.warning(f"Normalization resulted in empty content, using original")
            canonical_body = content_to_use
        
        # Generate SHA-256 hash
        return hashlib.sha256(canonical_body.encode('utf-8')).hexdigest()
    
    def get_normalized_content_and_fingerprint(self, body_text: str) -> Tuple[str, str]:
        """
        Get both normalized content and its fingerprint.
        
        Args:
            body_text: Raw email body text
            
        Returns:
            Tuple of (normalized_content, fingerprint)
        """
        canonical_body = self.normalize_email_content(body_text)
        
        # Never allow empty normalization
        if not canonical_body or len(canonical_body.strip()) == 0:
            canonical_body = body_text
            
        fingerprint = hashlib.sha256(canonical_body.encode('utf-8')).hexdigest()
        
        return canonical_body, fingerprint


# Singleton instance for easy import
email_normalizer = EmailNormalizer()


# Convenience functions for backward compatibility
def normalize_email_content(body_text: str) -> str:
    """Normalize email body text with proper URL and email replacement."""
    return email_normalizer.normalize_email_content(body_text)


def generate_email_fingerprint(body_text: str, body_html: str = None) -> str:
    """Generate fingerprint for email body text with proper normalization."""
    return email_normalizer.generate_fingerprint(body_text, body_html)