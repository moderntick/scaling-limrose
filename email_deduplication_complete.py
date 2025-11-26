#!/usr/bin/env python3
"""
Complete email deduplication system with ALL critical components.
Includes alias resolution, HTML extraction, and full content parsing.
"""

import re
import hashlib
import json
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from html.parser import HTMLParser
from html import unescape
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)


# ============================================================================
# HTML TO TEXT EXTRACTION
# ============================================================================

class HTMLTextExtractor(HTMLParser):
    """Extract text from HTML while preserving structure"""
    
    def __init__(self):
        super().__init__()
        self.text_parts = []
        self.current_tag = None
        self.skip_tags = {'script', 'style', 'meta', 'link'}
        self.block_tags = {'p', 'div', 'br', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'tr'}
        
    def handle_starttag(self, tag, attrs):
        self.current_tag = tag
        if tag in self.block_tags:
            if self.text_parts and self.text_parts[-1] != '\n':
                self.text_parts.append('\n')
                
    def handle_endtag(self, tag):
        if tag in self.block_tags:
            if self.text_parts and self.text_parts[-1] != '\n':
                self.text_parts.append('\n')
        self.current_tag = None
        
    def handle_data(self, data):
        if self.current_tag not in self.skip_tags:
            text = data.strip()
            if text:
                self.text_parts.append(text)
                self.text_parts.append(' ')
                
    def get_text(self):
        text = ''.join(self.text_parts)
        text = re.sub(r'\n\s*\n+', '\n\n', text)
        text = re.sub(r' +', ' ', text)
        return text.strip()


def html_to_text(html_content: str) -> str:
    """Convert HTML to plain text for fingerprinting"""
    if not html_content:
        return ""
        
    try:
        html_content = unescape(html_content)
        html_content = re.sub(r'<!--.*?-->', '', html_content, flags=re.DOTALL)
        
        parser = HTMLTextExtractor()
        parser.feed(html_content)
        return parser.get_text()
        
    except Exception as e:
        logger.warning(f"Error extracting text from HTML: {e}")
        text = re.sub(r'<[^>]+>', ' ', html_content)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()


# ============================================================================
# EMAIL ALIAS RESOLUTION
# ============================================================================

class EmailAliasResolver:
    """Resolves email addresses to their canonical form"""
    
    def __init__(self):
        self.plus_pattern = re.compile(r'\+[^@]+')
        self.dots_pattern = re.compile(r'\.')
        
        # Domain-specific rules
        self.domain_rules = {
            'gmail.com': {'ignore_dots': True, 'remove_plus': True},
            'googlemail.com': {'ignore_dots': True, 'remove_plus': True},
            'yahoo.com': {'remove_dash_suffix': True, 'remove_plus': True},
            'outlook.com': {'remove_plus': True},
            'hotmail.com': {'remove_plus': True},
            'protonmail.com': {'ignore_dots': True, 'preserve_plus': True},
        }
        
        # Common alias patterns
        self.alias_patterns = [
            (r'^(support|help|contact|info|hello|admin)@', 'primary@'),
            (r'^(noreply|no-reply|donotreply)@', 'automated@'),
            (r'^(notifications?|alerts?|updates?)@', 'automated@'),
        ]
    
    @lru_cache(maxsize=10000)
    def resolve(self, email: str) -> str:
        """Resolve email to canonical form"""
        if not email or '@' not in email:
            return email
            
        email = email.lower().strip()
        local, domain = email.rsplit('@', 1)
        
        # Apply domain-specific rules
        rules = self.domain_rules.get(domain, {})
        
        # Remove plus addressing unless preserved
        if not rules.get('preserve_plus', False):
            local = self.plus_pattern.sub('', local)
        
        # Gmail: ignore dots
        if rules.get('ignore_dots', False):
            local = self.dots_pattern.sub('', local)
        
        # Yahoo: remove dash suffixes
        if rules.get('remove_dash_suffix', False):
            local = re.sub(r'-[^@]+$', '', local)
        
        # Check alias patterns
        reconstructed = f"{local}@{domain}"
        for pattern, replacement in self.alias_patterns:
            if re.match(pattern, reconstructed):
                reconstructed = re.sub(pattern, replacement, reconstructed)
                break
        
        return reconstructed


# ============================================================================
# ADVANCED CONTENT PARSER
# ============================================================================

@dataclass
class ParsedEmail:
    """Structured representation of parsed email"""
    type: str  # 'original', 'forward', 'reply'
    new_content: Optional[str]
    quoted_content: Optional[str]
    new_content_meaningful: bool
    new_content_intent: Optional[str]
    parsing_confidence: float


class AdvancedContentParser:
    """Full-featured content parser with multi-language support"""
    
    def __init__(self):
        # Forward patterns for different email clients and languages
        self.forward_patterns = [
            # English
            (r'[-─━═]{3,}\s*Forwarded\s+[Mm]essage\s*[-─━═]{3,}', 'standard'),
            (r'Begin\s+forwarded\s+message:', 'apple'),
            (r'------\s*Original\s+Message\s*------', 'outlook'),
            (r'_{10,}\s*From:.*?Sent:.*?To:.*?Subject:', 'outlook_verbose'),
            
            # French
            (r'[-─━═]{3,}\s*Message\s+transféré\s*[-─━═]{3,}', 'french'),
            # Spanish  
            (r'[-─━═]{3,}\s*Mensaje\s+reenviado\s*[-─━═]{3,}', 'spanish'),
            # German
            (r'[-─━═]{3,}\s*Weitergeleitete\s+Nachricht\s*[-─━═]{3,}', 'german'),
        ]
        
        # Reply patterns
        self.reply_patterns = [
            (r'On\s+.+?,\s+.+?\s+wrote:', 'gmail'),
            (r'Le\s+.+?\s+à\s+.+?,\s+.+?\s+a\s+écrit\s*:', 'french'),
            (r'Am\s+.+?\s+um\s+.+?\s+schrieb\s+.+?:', 'german'),
        ]
        
        # Meaningful intent patterns
        self.intent_patterns = {
            # Information sharing
            r'\bfyi\b': 'information_sharing',
            r'\bfor your information\b': 'information_sharing',
            r'\bheads up\b': 'information_sharing',
            
            # Seeking input
            r'\bthoughts\?': 'seeking_input',
            r'\bwhat do you think': 'seeking_opinion',
            r'\byour opinion': 'seeking_opinion',
            r'\bplease advise': 'seeking_advice',
            
            # Action required
            r'\bplease review\b': 'action_required',
            r'\baction required\b': 'action_required',
            r'\bcan you\b': 'request',
            
            # Urgency
            r'\burgent\b': 'urgent_forward',
            r'\basap\b': 'urgent_forward',
        }
        
        # Signature patterns
        self.signature_patterns = [
            r'--\s*\n',
            r'Best,?\s*\n',
            r'Regards,?\s*\n',
            r'Sent from my iPhone',
            r'Sent from my Android',
        ]
    
    def parse_email_structure(self, content: str) -> ParsedEmail:
        """Parse email into structural components"""
        if not content:
            return ParsedEmail(
                type='empty',
                new_content=None,
                quoted_content=None,
                new_content_meaningful=False,
                new_content_intent=None,
                parsing_confidence=1.0
            )
        
        # Try forward parsing
        for pattern, name in self.forward_patterns:
            match = re.search(pattern, content, re.MULTILINE | re.IGNORECASE | re.DOTALL)
            if match:
                new_content = content[:match.start()].strip()
                quoted_content = content[match.end():].strip()
                
                # Remove signatures from new content
                new_content = self._remove_signatures(new_content)
                
                # Analyze new content
                intent = self._detect_intent(new_content)
                is_meaningful = self._is_meaningful(new_content)
                
                return ParsedEmail(
                    type='forward',
                    new_content=new_content if new_content else None,
                    quoted_content=quoted_content,
                    new_content_meaningful=is_meaningful,
                    new_content_intent=intent,
                    parsing_confidence=0.9
                )
        
        # Try reply parsing
        lines = content.split('\n')
        for i, line in enumerate(lines):
            for pattern, name in self.reply_patterns:
                if re.match(pattern, line, re.IGNORECASE):
                    new_content = '\n'.join(lines[:i]).strip()
                    quoted_content = '\n'.join(lines[i:]).strip()
                    
                    new_content = self._remove_signatures(new_content)
                    intent = self._detect_intent(new_content)
                    is_meaningful = self._is_meaningful(new_content)
                    
                    return ParsedEmail(
                        type='reply',
                        new_content=new_content if new_content else None,
                        quoted_content=quoted_content,
                        new_content_meaningful=is_meaningful,
                        new_content_intent=intent,
                        parsing_confidence=0.85
                    )
        
        # Original email
        clean_content = self._remove_signatures(content)
        return ParsedEmail(
            type='original',
            new_content=clean_content,
            quoted_content=None,
            new_content_meaningful=True,
            new_content_intent='original_message',
            parsing_confidence=0.95
        )
    
    def _remove_signatures(self, content: str) -> str:
        """Remove email signatures"""
        if not content:
            return content
            
        for pattern in self.signature_patterns:
            match = re.search(pattern, content, re.MULTILINE | re.IGNORECASE)
            if match:
                return content[:match.start()].strip()
        
        return content
    
    def _detect_intent(self, content: str) -> Optional[str]:
        """Detect intent of new content"""
        if not content:
            return None
            
        normalized = content.lower()
        
        for pattern, intent in self.intent_patterns.items():
            if re.search(pattern, normalized):
                return intent
        
        # Check word count
        word_count = len(normalized.split())
        if word_count >= 2:
            return 'comment'
        elif word_count == 1:
            return 'brief_comment'
        
        return None
    
    def _is_meaningful(self, content: str) -> bool:
        """Determine if content is meaningful"""
        if not content or len(content.strip()) == 0:
            return False
        
        # Even single words can be meaningful
        return len(content.strip()) > 0


# ============================================================================
# ENHANCED FINGERPRINTING WITH ALL COMPONENTS
# ============================================================================

@dataclass
class CompleteEmailFingerprint:
    """Complete fingerprint with all components"""
    # Content hashes
    new_content_hash: Optional[str]
    quoted_content_hash: Optional[str]
    full_content_hash: str
    
    # Structural hashes
    structure_hash: str
    thread_hash: str
    recipient_set_hash: str
    
    # Metadata
    has_meaningful_new_content: bool
    new_content_intent: Optional[str]
    email_type: str
    parsing_confidence: float
    
    # Composite
    composite_hash: str
    
    # Normalized content
    normalized_content: str  # The normalized text content
    content_source: str  # 'text' or 'html' to indicate source
    
    # Version (must come last because it has a default)
    fingerprint_version: int = 4


class CompleteEmailFingerprinter:
    """Complete fingerprinting with all components"""
    
    def __init__(self):
        self.parser = AdvancedContentParser()
        self.alias_resolver = EmailAliasResolver()
        
        # Content normalizer from earlier implementation
        from email_normalization import ContentNormalizer
        self.normalizer = ContentNormalizer()
    
    def generate_fingerprints(self, email_data: Dict) -> CompleteEmailFingerprint:
        """Generate complete fingerprints for email"""
        
        # Extract content (handle HTML properly)
        content = self._extract_content(email_data)
        
        # Track the source - matches _extract_content logic
        content_source = 'text' if email_data.get('body_text', '').strip() else 'html'
        
        # Parse email structure
        parsed = self.parser.parse_email_structure(content)
        
        # Generate content hashes
        new_content_hash = None
        if parsed.new_content:
            normalized_new = self.normalizer.normalize(parsed.new_content)
            if normalized_new:
                new_content_hash = self._hash(normalized_new)
        
        quoted_content_hash = None
        if parsed.quoted_content:
            normalized_quoted = self.normalizer.normalize(parsed.quoted_content)
            if normalized_quoted:
                quoted_content_hash = self._hash(normalized_quoted)
        
        # Full content hash
        normalized_full = self.normalizer.normalize(content, preserve_structure=True)
        full_content_hash = self._hash(normalized_full)
        
        # Generate structural hashes
        structure_hash = self._generate_structure_hash(email_data, parsed)
        thread_hash = self._generate_thread_hash(email_data)
        recipient_set_hash = self._generate_recipient_hash(email_data)
        
        # Composite hash
        composite_hash = self._generate_composite_hash(
            new_content_hash,
            quoted_content_hash,
            full_content_hash,
            structure_hash,
            thread_hash,
            recipient_set_hash
        )
        
        return CompleteEmailFingerprint(
            new_content_hash=new_content_hash,
            quoted_content_hash=quoted_content_hash,
            full_content_hash=full_content_hash,
            structure_hash=structure_hash,
            thread_hash=thread_hash,
            recipient_set_hash=recipient_set_hash,
            has_meaningful_new_content=parsed.new_content_meaningful,
            new_content_intent=parsed.new_content_intent,
            email_type=parsed.type,
            parsing_confidence=parsed.parsing_confidence,
            composite_hash=composite_hash,
            normalized_content=normalized_full,
            content_source=content_source,
            fingerprint_version=4
        )
    
    def _extract_content(self, email_data: Dict) -> str:
        """Extract content from email, handling HTML properly"""
        body_text = email_data.get('body_text', '').strip()
        if body_text:
            return body_text
            
        body_html = email_data.get('body_html', '').strip()
        if body_html:
            return html_to_text(body_html)
            
        return ""
    
    def _hash(self, content: str) -> str:
        """Generate SHA-256 hash"""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    def _generate_structure_hash(self, email_data: Dict, parsed: ParsedEmail) -> str:
        """Generate hash of email structure"""
        
        # Extract and normalize subject
        subject = self._extract_thread_subject(email_data.get('subject', ''))
        
        structure = {
            'type': parsed.type,
            'subject_base': subject,
            'sender_domain': self._extract_domain(email_data.get('sender_email', '')),
            'has_attachments': email_data.get('has_attachments', False),
            'attachment_count': email_data.get('attachment_count', 0),
            'has_new_content': parsed.new_content_meaningful,
        }
        
        return self._hash(json.dumps(structure, sort_keys=True))
    
    def _generate_thread_hash(self, email_data: Dict) -> str:
        """Generate hash for thread identification"""
        
        thread_elements = []
        
        # Gmail thread ID
        if email_data.get('thread_id'):
            thread_elements.append(f"gmail_thread:{email_data['thread_id']}")
        
        # Standard email headers
        if email_data.get('message_id'):
            thread_elements.append(f"message_id:{email_data['message_id']}")
        
        if email_data.get('in_reply_to'):
            thread_elements.append(f"in_reply_to:{email_data['in_reply_to']}")
        
        if email_data.get('references'):
            refs = email_data['references']
            if isinstance(refs, list) and refs:
                thread_elements.append(f"ref_first:{refs[0]}")
                if len(refs) > 1:
                    thread_elements.append(f"ref_last:{refs[-1]}")
            elif isinstance(refs, str) and refs:
                thread_elements.append(f"references:{refs}")
        
        # Fallback: subject + sender
        if not thread_elements:
            subject = self._extract_thread_subject(email_data.get('subject', ''))
            sender_domain = self._extract_domain(email_data.get('sender_email', ''))
            if subject and sender_domain:
                thread_elements.append(f"fallback:{subject}:{sender_domain}")
        
        return self._hash('|'.join(sorted(thread_elements)))
    
    def _generate_recipient_hash(self, email_data: Dict) -> str:
        """Generate hash of normalized recipients"""
        
        all_recipients = set()
        
        # Resolve all recipients to canonical form
        for field in ['recipient_emails', 'cc_emails', 'bcc_emails']:
            recipients = email_data.get(field, [])
            if recipients:
                if isinstance(recipients, str):
                    recipients = [recipients]
                for recipient in recipients:
                    if recipient:
                        canonical = self.alias_resolver.resolve(recipient)
                        all_recipients.add(canonical)
        
        # Add sender
        sender = email_data.get('sender_email', '')
        if sender:
            canonical_sender = self.alias_resolver.resolve(sender)
            all_recipients.add(f"sender:{canonical_sender}")
        
        return self._hash('|'.join(sorted(all_recipients)))
    
    def _generate_composite_hash(self, *hashes) -> str:
        """Generate composite hash"""
        valid_hashes = [h for h in hashes if h]
        return self._hash('|'.join(valid_hashes))
    
    def _extract_thread_subject(self, subject: str) -> str:
        """Extract core subject for threading"""
        if not subject:
            return ''
        
        # Remove all Re:/Fwd: prefixes (multiple levels)
        thread_subject = re.sub(
            r'^(re:\s*|fwd?:\s*|fw:\s*)+', 
            '', 
            subject, 
            flags=re.IGNORECASE
        )
        
        # Remove tags like [URGENT], [EXTERNAL]
        thread_subject = re.sub(r'\[.*?\]', '', thread_subject)
        thread_subject = re.sub(r'\(.*?\)', '', thread_subject)
        
        # Normalize
        thread_subject = re.sub(r'\s+', ' ', thread_subject).strip().lower()
        
        return thread_subject
    
    def _extract_domain(self, email: str) -> str:
        """Extract domain from email"""
        if not email or '@' not in email:
            return ''
        return email.lower().split('@')[1].strip()


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def generate_complete_fingerprints(email_data: Dict) -> CompleteEmailFingerprint:
    """Generate complete fingerprints with all components"""
    fingerprinter = CompleteEmailFingerprinter()
    return fingerprinter.generate_fingerprints(email_data)


def extract_email_content(email_data: Dict) -> str:
    """Extract text content from email, handling HTML"""
    body_text = email_data.get('body_text', '').strip()
    if body_text:
        return body_text
        
    body_html = email_data.get('body_html', '').strip()
    if body_html:
        return html_to_text(body_html)
        
    return ""