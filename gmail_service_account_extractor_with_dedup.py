#!/usr/bin/env python3
"""
Gmail Service Account Extractor with Complete Deduplication
Uses existing service account credentials for Gmail extraction with full deduplication
"""

import os

# Debug environment variables
print(f"DEBUG: Script starting - HF_HUB_OFFLINE = {os.environ.get('HF_HUB_OFFLINE', 'NOT SET')}")
print(f"DEBUG: Script starting - TRANSFORMERS_OFFLINE = {os.environ.get('TRANSFORMERS_OFFLINE', 'NOT SET')}")

# Force set if they exist
if os.environ.get('HF_HUB_OFFLINE') == '1':
    os.environ['HF_HUB_OFFLINE'] = '1'
    os.environ['TRANSFORMERS_OFFLINE'] = '1'
    os.environ['HF_DATASETS_OFFLINE'] = '1'
    os.environ['SENTENCE_TRANSFORMERS_HOME'] = os.path.expanduser('~/.cache/torch/sentence_transformers')
    print("DEBUG: Forced offline mode environment variables")
import json
import base64
from datetime import datetime, timezone
from typing import Dict, List, Optional
import psycopg2
# from sentence_transformers import SentenceTransformer  # REMOVED: Embeddings now handled by batch_process_all_emails.py
from google.oauth2 import service_account
from googleapiclient.discovery import build
from email.utils import parseaddr, parsedate_to_datetime
from tqdm import tqdm

from email_pipeline_router import EmailPipelineRouter
from email_normalization import EmailNormalizer
from email_deduplication_complete import generate_complete_fingerprints

# Your existing service account configuration
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE', 'config/service-account-key.json')
DELEGATE_EMAIL = os.getenv('DELEGATE_EMAIL', '')
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.modify'
]

# EMBEDDING_MODEL_NAME = 'all-MiniLM-L6-v2'  # REMOVED: No longer needed, handled by batch_process_all_emails.py

class GmailServiceAccountExtractor:
    """Extract Gmail data using service account with delegation and complete deduplication"""
    
    def __init__(self):
        # self.model = SentenceTransformer(EMBEDDING_MODEL_NAME)  # REMOVED: Model loading moved to batch_process_all_emails.py
        self.service = self.authenticate_service_account()
        self.db_conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME', 'limrose_email_pipeline'),
            user=os.getenv('DB_USER', 'postgres'),
            host=os.getenv('DB_HOST', 'localhost')
        )
        self.router = EmailPipelineRouter()
        self.normalizer = EmailNormalizer()
        self.setup_database()
    
    def authenticate_service_account(self):
        """Authenticate using service account with delegation"""
        try:
            # Load service account credentials
            credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE,
                scopes=SCOPES
            )
            
            # Create delegated credentials
            delegated_credentials = credentials.with_subject(DELEGATE_EMAIL)
            
            # Build Gmail service
            service = build('gmail', 'v1', credentials=delegated_credentials)
            
            print(f"✅ Authenticated as {DELEGATE_EMAIL} using service account")
            return service
            
        except Exception as e:
            print(f"❌ Authentication failed: {e}")
            print("Make sure service account file exists and has proper permissions")
            raise
    
    def setup_database(self):
        """Set up database tables including deduplication tables"""
        cursor = self.db_conn.cursor()
        
        # Main classified emails table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS classified_emails (
                id SERIAL PRIMARY KEY,
                gmail_id VARCHAR(255) UNIQUE NOT NULL,
                thread_id VARCHAR(255),
                subject TEXT,
                sender_email VARCHAR(255),
                sender_name TEXT,
                recipient_emails TEXT[],
                cc_emails TEXT[],
                bcc_emails TEXT[],
                date_sent TIMESTAMP WITH TIME ZONE,
                date_received TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                body_text TEXT,
                body_html TEXT,
                normalized_body_text TEXT,
                normalized_body_html TEXT,
                snippet TEXT,
                labels TEXT[],
                has_attachments BOOLEAN DEFAULT FALSE,
                attachment_count INTEGER DEFAULT 0,
                importance_score FLOAT,
                processed BOOLEAN DEFAULT FALSE,
                raw_size INTEGER,
                
                -- Email headers for threading
                message_id TEXT,
                in_reply_to TEXT,
                "references" TEXT[],
                
                -- Deduplication fields
                content_fingerprint VARCHAR(64),
                duplicate_group_id INTEGER,
                normalization_version INTEGER DEFAULT 2,
                
                -- Pipeline integration fields
                pipeline_processed BOOLEAN DEFAULT FALSE,
                embeddings_created BOOLEAN DEFAULT FALSE,
                enhanced_embedding_created BOOLEAN DEFAULT FALSE,
                human_verified BOOLEAN DEFAULT FALSE,
                
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_classified_emails_gmail_id ON classified_emails(gmail_id);
            CREATE INDEX IF NOT EXISTS idx_classified_emails_thread ON classified_emails(thread_id);
            CREATE INDEX IF NOT EXISTS idx_classified_emails_date ON classified_emails(date_sent);
            CREATE INDEX IF NOT EXISTS idx_classified_emails_sender ON classified_emails(sender_email);
            CREATE INDEX IF NOT EXISTS idx_classified_emails_processed ON classified_emails(pipeline_processed);
            CREATE INDEX IF NOT EXISTS idx_classified_emails_fingerprint ON classified_emails(content_fingerprint);
            CREATE INDEX IF NOT EXISTS idx_classified_emails_duplicate_group ON classified_emails(duplicate_group_id);
        """)
        
        # Email fingerprints v2 table for complete deduplication
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS email_fingerprints_v2 (
                email_id INTEGER PRIMARY KEY REFERENCES classified_emails(id) ON DELETE CASCADE,
                new_content_hash VARCHAR(64),
                quoted_content_hash VARCHAR(64),
                full_content_hash VARCHAR(64),
                structure_hash VARCHAR(64),
                thread_hash VARCHAR(64),
                recipient_set_hash VARCHAR(64),
                has_meaningful_new_content BOOLEAN DEFAULT TRUE,
                new_content_intent VARCHAR(50),
                email_type VARCHAR(20) DEFAULT 'original',
                parsing_confidence FLOAT DEFAULT 1.0,
                is_canonical BOOLEAN DEFAULT TRUE,
                canonical_email_id INTEGER,
                fingerprint_version INTEGER DEFAULT 5,
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_fingerprints_v2_full_content ON email_fingerprints_v2(full_content_hash);
            CREATE INDEX IF NOT EXISTS idx_fingerprints_v2_structure ON email_fingerprints_v2(structure_hash);
            CREATE INDEX IF NOT EXISTS idx_fingerprints_v2_composite ON email_fingerprints_v2(full_content_hash, structure_hash);
        """)
        
        # Email duplicate groups table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS email_duplicate_groups (
                id SERIAL PRIMARY KEY,
                content_fingerprint VARCHAR(64),
                primary_email_id INTEGER REFERENCES classified_emails(id),
                member_count INTEGER DEFAULT 1,
                first_seen TIMESTAMP WITH TIME ZONE,
                last_seen TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                normalization_version INTEGER DEFAULT 5
            );
            
            CREATE INDEX IF NOT EXISTS idx_duplicate_groups_fingerprint ON email_duplicate_groups(content_fingerprint);
        """)
        
        # Email embeddings table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS email_embeddings (
                id SERIAL PRIMARY KEY,
                email_id INTEGER REFERENCES classified_emails(id) ON DELETE CASCADE,
                embedding VECTOR(384),
                embedding_text TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(email_id)
            );
            
            CREATE INDEX IF NOT EXISTS idx_email_embeddings_vector 
            ON email_embeddings USING hnsw (embedding vector_cosine_ops);
        """)
        
        self.db_conn.commit()
    
    def _save_email_to_db(self, email_data: Dict) -> Optional[int]:
        """Save email to database with complete deduplication"""
        cursor = self.db_conn.cursor()
        
        try:
            # Generate complete fingerprints FIRST to get normalized content
            try:
                fingerprints = generate_complete_fingerprints(email_data)
                
                # Determine which normalized field to populate
                normalized_body_text = None
                normalized_body_html = None
                
                if fingerprints.content_source == 'text':
                    normalized_body_text = fingerprints.normalized_content
                else:  # content_source == 'html'
                    normalized_body_html = fingerprints.normalized_content
                    
            except Exception as e:
                print(f"  ⚠️ Warning: Could not generate fingerprints: {e}")
                # If fingerprinting fails, continue without normalized content
                fingerprints = None
                normalized_body_text = None
                normalized_body_html = None
            
            # Insert the email with normalized content
            cursor.execute("""
                INSERT INTO classified_emails (
                    gmail_id, thread_id, subject, sender_email, sender_name,
                    recipient_emails, cc_emails, bcc_emails, date_sent, body_text, body_html,
                    normalized_body_text, normalized_body_html,
                    snippet, labels, raw_size, message_id, in_reply_to, "references",
                    has_attachments, attachment_count
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (gmail_id) DO NOTHING
                RETURNING id
            """, (
                email_data['gmail_id'],
                email_data.get('thread_id'),
                email_data.get('subject'),
                email_data.get('sender_email'),
                email_data.get('sender_name'),
                email_data.get('recipient_emails', []),
                email_data.get('cc_emails', []),
                email_data.get('bcc_emails', []),
                email_data.get('date_sent'),
                email_data.get('body_text'),
                email_data.get('body_html'),
                normalized_body_text,
                normalized_body_html,
                email_data.get('snippet'),
                email_data.get('labels', []),
                email_data.get('raw_size', 0),
                email_data.get('message_id'),
                email_data.get('in_reply_to'),
                email_data.get('references', []),
                email_data.get('has_attachments', False),
                email_data.get('attachment_count', 0)
            ))
            
            result = cursor.fetchone()
            if not result:
                return None
            
            email_id = result[0]
            
            # Continue with fingerprint processing if we have them
            if fingerprints:
                # Insert fingerprints
                cursor.execute("""
                    INSERT INTO email_fingerprints_v2 (
                        email_id, new_content_hash, quoted_content_hash,
                        full_content_hash, structure_hash, thread_hash,
                        recipient_set_hash, has_meaningful_new_content,
                        new_content_intent, email_type, parsing_confidence,
                        is_canonical, canonical_email_id, fingerprint_version
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    email_id,
                    fingerprints.new_content_hash,
                    fingerprints.quoted_content_hash,
                    fingerprints.full_content_hash,
                    fingerprints.structure_hash,
                    fingerprints.thread_hash,
                    fingerprints.recipient_set_hash,
                    fingerprints.has_meaningful_new_content,
                    fingerprints.new_content_intent,
                    fingerprints.email_type,
                    fingerprints.parsing_confidence,
                    True,  # is_canonical (will be updated if duplicate found)
                    None,  # canonical_email_id (will be updated if duplicate found)
                    fingerprints.fingerprint_version
                ))
                
                # Create composite fingerprint for duplicate detection
                composite_fingerprint = self._create_composite_fingerprint(
                    fingerprints.full_content_hash,
                    fingerprints.structure_hash
                )
                
                # Check for existing duplicate group
                cursor.execute("""
                    SELECT edg.id, edg.primary_email_id
                    FROM email_duplicate_groups edg
                    WHERE edg.content_fingerprint = %s
                """, (composite_fingerprint,))
                
                existing_group = cursor.fetchone()
                
                if existing_group:
                    # Email is a duplicate
                    duplicate_group_id, canonical_email_id = existing_group
                    
                    # Update email with duplicate group
                    cursor.execute("""
                        UPDATE classified_emails 
                        SET duplicate_group_id = %s,
                            content_fingerprint = %s
                        WHERE id = %s
                    """, (duplicate_group_id, composite_fingerprint, email_id))
                    
                    # Update fingerprint record
                    cursor.execute("""
                        UPDATE email_fingerprints_v2
                        SET is_canonical = FALSE,
                            canonical_email_id = %s
                        WHERE email_id = %s
                    """, (canonical_email_id, email_id))
                    
                    # Update group stats
                    cursor.execute("""
                        UPDATE email_duplicate_groups 
                        SET member_count = member_count + 1,
                            last_seen = GREATEST(last_seen, %s),
                            updated_at = NOW()
                        WHERE id = %s
                    """, (email_data.get('date_sent'), duplicate_group_id))
                    
                    print(f"  ↪️ Duplicate detected! Group #{duplicate_group_id} (canonical: #{canonical_email_id})")
                    
                else:
                    # Email is unique - create new group
                    cursor.execute("""
                        INSERT INTO email_duplicate_groups 
                        (content_fingerprint, primary_email_id, member_count, first_seen, last_seen, normalization_version)
                        VALUES (%s, %s, 1, %s, %s, 5)
                        RETURNING id
                    """, (
                        composite_fingerprint,
                        email_id,
                        email_data.get('date_sent'),
                        email_data.get('date_sent')
                    ))
                    
                    new_group = cursor.fetchone()
                    if new_group:
                        duplicate_group_id = new_group[0]
                        
                        # Update email with its group
                        cursor.execute("""
                            UPDATE classified_emails 
                            SET duplicate_group_id = %s,
                                content_fingerprint = %s
                            WHERE id = %s
                        """, (duplicate_group_id, composite_fingerprint, email_id))
                
                # Log email type detection
                if fingerprints.email_type != 'original':
                    print(f"  📧 Detected {fingerprints.email_type} email")
            
            self.db_conn.commit()
            return email_id
                
        except Exception as e:
            print(f"Error saving email: {e}")
            self.db_conn.rollback()
            return None
    
    def _create_composite_fingerprint(self, full_content_hash: str, structure_hash: str) -> str:
        """Create composite fingerprint from content and structure hashes"""
        import hashlib
        composite = f"{full_content_hash or ''}|{structure_hash or ''}"
        return hashlib.sha256(composite.encode('utf-8')).hexdigest()
    
    def extract_emails(self, batch_size: int = 50, max_results: int = None, 
                      start_date: str = None, query: str = None):
        """Extract emails from Gmail with complete deduplication"""
        print(f"Starting Gmail extraction for {DELEGATE_EMAIL}...")
        print("Using complete deduplication system (v5)...")
        
        # Build query
        gmail_query = query or "in:all"
        if start_date:
            gmail_query += f" after:{start_date}"
        
        # Get message IDs
        print("Fetching message IDs from Gmail...")
        messages = self._get_message_ids(gmail_query, max_results)
        print(f"Found {len(messages)} emails to process")
        
        # Check existing emails
        cursor = self.db_conn.cursor()
        cursor.execute("SELECT gmail_id FROM classified_emails")
        existing_ids = {row[0] for row in cursor.fetchall()}
        
        # Filter new emails
        new_messages = [m for m in messages if m['id'] not in existing_ids]
        print(f"Processing {len(new_messages)} new emails")
        
        if not new_messages:
            print("No new emails to process!")
            return
        
        # Process in batches
        processed_count = 0
        error_count = 0
        duplicate_count = 0
        
        for i in tqdm(range(0, len(new_messages), batch_size), desc="Processing emails"):
            batch = new_messages[i:i + batch_size]
            
            # Collect emails to queue after commit
            emails_to_queue = []
            
            for msg in batch:
                try:
                    # Extract email content
                    email_data = self._extract_email_content(msg['id'])
                    if email_data:
                        # Save to database with deduplication
                        email_id = self._save_email_to_db(email_data)
                        
                        if email_id:
                            # Check if it was marked as duplicate
                            cursor.execute("""
                                SELECT duplicate_group_id 
                                FROM classified_emails 
                                WHERE id = %s AND duplicate_group_id IS NOT NULL
                            """, (email_id,))
                            
                            if cursor.fetchone():
                                duplicate_count += 1
                            
                            # Collect for queueing after commit
                            emails_to_queue.append((email_id, email_data))
                            
                            # Mark as processed
                            self._mark_email_processed(email_id)
                            processed_count += 1
                        else:
                            print(f"  ⚠️ Email already exists: {email_data.get('subject', 'No subject')}")
                            
                except Exception as e:
                    print(f"  ❌ Error processing email {msg['id']}: {e}")
                    error_count += 1
                    import traceback
                    traceback.print_exc()
            
            # Commit batch
            self.db_conn.commit()
            
            # DECOUPLED PROCESSING: Embedding creation moved to separate batch processor
            # This improves performance by 10-20x (1800+ emails/min vs 108/min with Celery)
            # Run batch_process_all_emails.py after extraction to create embeddings
            # 
            # OLD CODE (Celery-based embedding):
            # for email_id, email_data in emails_to_queue:
            #     self._create_email_embedding(email_id, email_data)
        
        # Final summary
        print(f"\n{'='*60}")
        print(f"Gmail extraction complete!")
        print(f"  Total processed: {processed_count}")
        print(f"  Duplicates detected: {duplicate_count}")
        print(f"  Unique emails: {processed_count - duplicate_count}")
        print(f"  Errors: {error_count}")
        print(f"  Duplicate rate: {(duplicate_count/processed_count*100):.1f}%" if processed_count > 0 else "N/A")
        print(f"{'='*60}\n")
        
        # Show pipeline summary
        self._show_pipeline_summary()
    
    # Include all other methods from the original file unchanged
    def _get_message_ids(self, query: str, max_results: int = None) -> List[Dict]:
        """Get message IDs from Gmail"""
        messages = []
        page_token = None
        
        while True:
            try:
                if page_token:
                    results = self.service.users().messages().list(
                        userId='me',
                        q=query,
                        maxResults=500,  # Max allowed per page
                        pageToken=page_token
                    ).execute()
                else:
                    results = self.service.users().messages().list(
                        userId='me',
                        q=query,
                        maxResults=500  # Max allowed per page
                    ).execute()
                
                if 'messages' in results:
                    messages.extend(results['messages'])
                    
                    # Progress update every 5000 messages
                    if len(messages) % 5000 == 0:
                        print(f"  Fetched {len(messages):,} message IDs...")
                    
                if max_results and len(messages) >= max_results:
                    messages = messages[:max_results]
                    break
                    
                if 'nextPageToken' in results:
                    page_token = results['nextPageToken']
                else:
                    break
                    
            except Exception as e:
                print(f"Error fetching messages: {e}")
                break
                
        return messages
    
    def _extract_email_content(self, message_id: str) -> Optional[Dict]:
        """Extract email content from Gmail message"""
        try:
            message = self.service.users().messages().get(
                userId='me',
                id=message_id,
                format='full'
            ).execute()
            
            # Parse headers
            headers = message['payload'].get('headers', [])
            header_dict = {h['name']: h['value'] for h in headers}
            
            # Extract basic info
            subject = header_dict.get('Subject', '')
            from_header = header_dict.get('From', '')
            to_header = header_dict.get('To', '')
            cc_header = header_dict.get('Cc', '')
            bcc_header = header_dict.get('Bcc', '')
            date_header = header_dict.get('Date', '')
            message_id_header = header_dict.get('Message-ID', '')
            in_reply_to = header_dict.get('In-Reply-To', '')
            references = header_dict.get('References', '')
            
            # Parse sender
            sender_name, sender_email = parseaddr(from_header)
            
            # Parse recipients
            recipient_emails = [parseaddr(to.strip())[1] for to in to_header.split(',') if to.strip()] if to_header else []
            cc_emails = [parseaddr(cc.strip())[1] for cc in cc_header.split(',') if cc.strip()] if cc_header else []
            bcc_emails = [parseaddr(bcc.strip())[1] for bcc in bcc_header.split(',') if bcc.strip()] if bcc_header else []
            
            # Parse date
            try:
                date_sent = parsedate_to_datetime(date_header) if date_header else datetime.now(timezone.utc)
            except:
                date_sent = datetime.now(timezone.utc)
            
            # Extract body
            body_text, body_html = self._extract_body(message['payload'])
            
            # Check for attachments
            has_attachments, attachment_count = self._check_attachments(message['payload'])
            
            return {
                'gmail_id': message_id,
                'thread_id': message.get('threadId'),
                'subject': subject,
                'sender_email': sender_email,
                'sender_name': sender_name,
                'recipient_emails': recipient_emails,
                'cc_emails': cc_emails,
                'bcc_emails': bcc_emails,
                'date_sent': date_sent,
                'body_text': body_text,
                'body_html': body_html,
                'snippet': message.get('snippet', ''),
                'labels': message.get('labelIds', []),
                'raw_size': message.get('sizeEstimate', 0),
                'message_id': message_id_header,
                'in_reply_to': in_reply_to,
                'references': references.split() if references else [],
                'has_attachments': has_attachments,
                'attachment_count': attachment_count
            }
            
        except Exception as e:
            print(f"Error extracting email content: {e}")
            return None
    
    def _extract_body(self, payload: Dict) -> tuple:
        """Extract text and HTML body from email payload"""
        body_text = ""
        body_html = ""
        
        def extract_from_parts(parts):
            nonlocal body_text, body_html
            
            for part in parts:
                mime_type = part.get('mimeType', '')
                
                if mime_type == 'text/plain' and not body_text:
                    data = part['body'].get('data', '')
                    if data:
                        body_text = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
                        
                elif mime_type == 'text/html' and not body_html:
                    data = part['body'].get('data', '')
                    if data:
                        body_html = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
                        
                elif 'parts' in part:
                    extract_from_parts(part['parts'])
        
        # Handle single part
        if 'body' in payload and 'data' in payload['body']:
            mime_type = payload.get('mimeType', '')
            data = payload['body']['data']
            
            if mime_type == 'text/plain':
                body_text = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
            elif mime_type == 'text/html':
                body_html = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
        
        # Handle multipart
        if 'parts' in payload:
            extract_from_parts(payload['parts'])
        
        return body_text, body_html
    
    def _check_attachments(self, payload: Dict) -> tuple:
        """Check for attachments in email"""
        has_attachments = False
        attachment_count = 0
        
        def check_parts(parts):
            nonlocal has_attachments, attachment_count
            
            for part in parts:
                filename = part.get('filename', '')
                if filename:
                    has_attachments = True
                    attachment_count += 1
                    
                if 'parts' in part:
                    check_parts(part['parts'])
        
        if 'parts' in payload:
            check_parts(payload['parts'])
            
        return has_attachments, attachment_count
    
    # DECOUPLED PROCESSING: Celery-based embedding removed for performance
    # This method previously queued emails to Celery workers for embedding generation
    # Performance was limited to ~108 emails/min due to database connection pool bottlenecks
    # 
    # New approach: Run batch_process_all_emails.py separately after extraction
    # Benefits:
    # - 10-20x faster (1800+ emails/min)
    # - No queue management overhead
    # - Better error recovery (can re-run embeddings without re-extracting)
    # - Cleaner separation of concerns
    #
    # def _create_email_embedding(self, email_id: int, email_data: Dict):
    #     """Queue email for chunking and embedding using Celery"""
    #     try:
    #         # Import here to avoid circular imports
    #         import sys
    #         import os
    #         sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'news_scraper_project'))
    #         from news_scraper_project.tasks import process_and_embed_email
    #         
    #         # Get additional metadata for the email
    #         cursor = self.db_conn.cursor()
    #         cursor.execute("""
    #             SELECT 
    #                 thread_id,
    #                 classification,
    #                 classification_confidence,
    #                 sender_total_emails,
    #                 thread_position,
    #                 thread_message_count,
    #                 has_response
    #             FROM classified_emails
    #             WHERE id = %s
    #         """, (email_id,))
    #         
    #         result = cursor.fetchone()
    #         if result:
    #             # Add metadata to email_data
    #             email_data['thread_id'] = result[0]
    #             email_data['classification'] = result[1]
    #             email_data['classification_confidence'] = result[2]
    #             email_data['sender_total_emails'] = result[3]
    #             email_data['thread_position'] = result[4]
    #             email_data['thread_message_count'] = result[5]
    #             email_data['has_response'] = result[6]
    #         
    #         # Queue for Celery processing with email data
    #         process_and_embed_email.delay(email_id, email_data)
    #         print(f"  ✅ Queued email {email_id} for enhanced chunking and embedding")
    #         
    #     except Exception as e:
    #         print(f"  ⚠️ Error queueing email for embedding: {e}")
    #         # For backward compatibility, create simple embedding
    #         self._create_simple_embedding(email_id, email_data)
    
    # DEPRECATED: Simple embedding creation moved to batch processor
    # This fallback method is no longer needed since all embedding generation
    # is handled by batch_process_all_emails.py for better performance
    #
    # def _create_simple_embedding(self, email_id: int, email_data: Dict):
    #     """Fallback method for simple embedding (backward compatibility)"""
    #     try:
    #         embedding_text = f"From: {email_data.get('sender_name', '')} <{email_data.get('sender_email', '')}>\n"
    #         embedding_text += f"Subject: {email_data.get('subject', '')}\n\n"
    #         embedding_text += email_data.get('body_text', '')[:1000]
    #         
    #         embedding = self.model.encode(embedding_text)
    #         
    #         cursor = self.db_conn.cursor()
    #         cursor.execute("""
    #             INSERT INTO email_embeddings (email_id, embedding, embedding_text)
    #             VALUES (%s, %s, %s)
    #             ON CONFLICT (email_id) DO NOTHING
    #         """, (email_id, embedding.tolist(), embedding_text))
    #         
    #         cursor.execute("""
    #             UPDATE classified_emails 
    #             SET embeddings_created = true, updated_at = NOW()
    #             WHERE id = %s
    #         """, (email_id,))
    #         
    #     except Exception as e:
    #         print(f"  ⚠️ Error creating simple embedding: {e}")
    
    def _mark_email_processed(self, email_id: int):
        """Mark email as processed"""
        cursor = self.db_conn.cursor()
        cursor.execute("""
            UPDATE classified_emails 
            SET pipeline_processed = true, updated_at = NOW()
            WHERE id = %s
        """, (email_id,))
    
    def _show_pipeline_summary(self):
        """Show pipeline routing summary"""
        try:
            stats = self.router.get_routing_stats()
            print(f"\nPipeline Routing Summary:")
            print("=" * 50)
            
            if 'pipeline_queues' in stats:
                for pipeline, counts in stats['pipeline_queues'].items():
                    total = sum(counts.values())
                    pending = counts.get('pending', 0)
                    print(f"  {pipeline}: {total} emails ({pending} pending)")
            
            print("=" * 50)
            
        except Exception as e:
            print(f"Could not get pipeline stats: {e}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract emails from Gmail using service account')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size for processing')
    parser.add_argument('--max-results', type=int, help='Maximum number of emails to process')
    parser.add_argument('--start-date', type=str, help='Start date for email extraction (YYYY/MM/DD)')
    parser.add_argument('--query', type=str, help='Gmail query string')
    
    args = parser.parse_args()
    
    extractor = GmailServiceAccountExtractor()
    extractor.extract_emails(
        batch_size=args.batch_size,
        max_results=args.max_results,
        start_date=args.start_date,
        query=args.query
    )