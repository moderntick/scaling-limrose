#!/usr/bin/env python3
"""
Enhanced Email Embeddings System
Implements full conversation context, sender history, and pipeline classification
"""

import os
import json
import base64
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
import psycopg2
import psycopg2.extras
from sentence_transformers import SentenceTransformer
from google.oauth2 import service_account
from googleapiclient.discovery import build
from email.utils import parseaddr, parsedate_to_datetime
import hashlib

# Import only DateTimeJSONEncoder for JSON serialization
from email_pipeline_router import DateTimeJSONEncoder

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Service account configuration
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE', 'config/service-account-key.json')
DELEGATE_EMAIL = os.getenv('DELEGATE_EMAIL', '')
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/gmail.send'
]

EMBEDDING_MODEL_NAME = 'sentence-transformers/all-MiniLM-L6-v2'

class EnhancedEmailEmbeddings:
    """Enhanced email embedding system with full context and history"""
    
    def __init__(self):
        logger.info("[INIT] Starting EnhancedEmailEmbeddings initialization...")
        logger.info(f"[INIT] Loading SentenceTransformer model: {EMBEDDING_MODEL_NAME}")
        
        # Check if we're in offline mode
        if os.environ.get('HF_HUB_OFFLINE') == '1':
            snapshot_path = os.path.expanduser("~/.cache/huggingface/hub/models--sentence-transformers--all-MiniLM-L6-v2/snapshots/c9745ed1d9f207416be6d2e6f8de32d1f16199bf")
            if os.path.exists(snapshot_path):
                self.model = SentenceTransformer(snapshot_path, device='cpu')
            else:
                self.model = SentenceTransformer(EMBEDDING_MODEL_NAME, device='cpu', local_files_only=True)
        else:
            self.model = SentenceTransformer(EMBEDDING_MODEL_NAME)
        logger.info("[INIT] Model loaded successfully")
        
        logger.info("[INIT] Authenticating service account...")
        self.service = self.authenticate_service_account()
        logger.info("[INIT] Service account authenticated")
        
        logger.info("[INIT] Connecting to PostgreSQL database...")
        self.db_conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME', 'email_pipeline'),
            user=os.getenv('DB_USER', 'postgres'),
            host=os.getenv('DB_HOST', 'localhost')
        )
        logger.info("[INIT] Database connection established")
        
        logger.info("[INIT] Setting up enhanced database schema...")
        self.setup_enhanced_database()
        logger.info("[INIT] Database schema setup complete")
        
        # PERFORMANCE: Default to skip expensive operations for bulk processing
        self.skip_article_search = True
        self.skip_thread_context = True
        logger.info("[INIT] Performance mode enabled: article search and thread context disabled by default")
    
    def authenticate_service_account(self):
        """Authenticate using service account"""
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        delegated_credentials = credentials.with_subject(DELEGATE_EMAIL)
        return build('gmail', 'v1', credentials=delegated_credentials)
    
    def setup_enhanced_database(self):
        """Set up enhanced database schema for rich embeddings"""
        cursor = self.db_conn.cursor()
        
        # Enhanced email embeddings table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS enhanced_email_embeddings (
                id SERIAL PRIMARY KEY,
                email_id INTEGER REFERENCES classified_emails(id) ON DELETE CASCADE,
                gmail_id VARCHAR(255),
                embedding_type VARCHAR(50) NOT NULL, -- 'message', 'response', 'thread', 'context', 'comprehensive'
                embedding VECTOR(384),
                embedding_text TEXT,
                
                -- Context metadata
                thread_id VARCHAR(255),
                sender_email VARCHAR(255),
                pipeline_classification VARCHAR(50),
                sender_interaction_count INTEGER,
                thread_message_count INTEGER,
                
                -- Content metadata
                includes_response BOOLEAN DEFAULT FALSE,
                includes_thread_context BOOLEAN DEFAULT FALSE,
                includes_sender_history BOOLEAN DEFAULT FALSE,
                includes_pipeline_context BOOLEAN DEFAULT FALSE,
                related_article_count INTEGER DEFAULT 0,
                
                -- Search optimization
                search_keywords TEXT[],
                business_context TEXT,
                context_summary JSONB,
                
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                
                UNIQUE(email_id, embedding_type)
            );
            
            CREATE INDEX IF NOT EXISTS idx_enhanced_embeddings_email ON enhanced_email_embeddings(email_id);
            CREATE INDEX IF NOT EXISTS idx_enhanced_embeddings_type ON enhanced_email_embeddings(embedding_type);
            CREATE INDEX IF NOT EXISTS idx_enhanced_embeddings_sender ON enhanced_email_embeddings(sender_email);
            CREATE INDEX IF NOT EXISTS idx_enhanced_embeddings_pipeline ON enhanced_email_embeddings(pipeline_classification);
            CREATE INDEX IF NOT EXISTS idx_enhanced_embeddings_vector ON enhanced_email_embeddings 
                USING hnsw (embedding vector_cosine_ops);
        """)
        
        # Sender interaction history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sender_interaction_history (
                id SERIAL PRIMARY KEY,
                sender_email VARCHAR(255),
                sender_name TEXT,
                
                -- Interaction stats
                total_emails_sent INTEGER DEFAULT 0,
                total_emails_responded INTEGER DEFAULT 0,
                total_emails_received INTEGER DEFAULT 0,
                first_contact_date TIMESTAMP WITH TIME ZONE,
                last_contact_date TIMESTAMP WITH TIME ZONE,
                
                -- Relationship context
                relationship_type VARCHAR(50), -- 'source', 'advertiser', 'reader', 'colleague'
                interaction_quality VARCHAR(50), -- 'positive', 'neutral', 'negative'
                response_rate FLOAT,
                avg_response_time_hours FLOAT,
                
                -- Business outcomes
                total_revenue_generated DECIMAL(10,2) DEFAULT 0,
                articles_published INTEGER DEFAULT 0,
                meetings_held INTEGER DEFAULT 0,
                
                -- Context
                common_topics TEXT[],
                pipeline_history TEXT[],
                notes TEXT,
                
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                
                UNIQUE(sender_email)
            );
            
            CREATE INDEX IF NOT EXISTS idx_sender_history_email ON sender_interaction_history(sender_email);
            CREATE INDEX IF NOT EXISTS idx_sender_history_type ON sender_interaction_history(relationship_type);
        """)
        
        # Thread context table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS thread_context (
                id SERIAL PRIMARY KEY,
                gmail_thread_id VARCHAR(255) UNIQUE,
                
                -- Thread metadata
                participant_emails TEXT[],
                participant_names TEXT[],
                thread_message_count INTEGER DEFAULT 0,
                message_count INTEGER DEFAULT 0,
                our_message_count INTEGER DEFAULT 0,
                
                -- Thread analysis
                thread_type VARCHAR(50), -- 'inquiry', 'negotiation', 'follow_up', 'complaint'
                thread_status VARCHAR(50), -- 'active', 'closed', 'waiting', 'resolved'
                primary_pipeline VARCHAR(50),
                
                -- Timeline
                thread_start_date TIMESTAMP WITH TIME ZONE,
                started_date TIMESTAMP WITH TIME ZONE,
                last_activity_date TIMESTAMP WITH TIME ZONE,
                
                -- Context
                thread_summary TEXT,
                key_topics TEXT[],
                business_outcome TEXT,
                
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_thread_context_gmail_id ON thread_context(gmail_thread_id);
            CREATE INDEX IF NOT EXISTS idx_thread_context_pipeline ON thread_context(primary_pipeline);
        """)
        
        # Pipeline context enrichment
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_context_enrichment (
                id SERIAL PRIMARY KEY,
                email_id INTEGER REFERENCES classified_emails(id),
                pipeline_type VARCHAR(50),
                
                -- Related articles (for story context)
                related_articles JSONB,
                
                -- Business intelligence
                sender_business_profile JSONB,
                competitive_analysis JSONB,
                
                -- Response suggestions
                suggested_responses JSONB,
                response_templates JSONB,
                
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_pipeline_enrichment_email ON pipeline_context_enrichment(email_id);
            CREATE INDEX IF NOT EXISTS idx_pipeline_enrichment_pipeline ON pipeline_context_enrichment(pipeline_type);
        """)
        
        self.db_conn.commit()
    
    def create_embedding_for_classified_email(self, email_id: int, classifications: List[str]):
        """
        New entry point for the batch classification workflow.
        Uses the provided classifications as the definitive source.
        """
        logger.info(f"[EMBEDDING] Starting enhanced embedding for classified email_id: {email_id} with classifications: {classifications}")
        cursor = self.db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        try:
            # 1. Get email data
            logger.info(f"[EMBEDDING] Step 1: Fetching email data for ID {email_id}...")
            cursor.execute("SELECT * FROM classified_emails WHERE id = %s", (email_id,))
            email_data = cursor.fetchone()
            if not email_data:
                error_msg = f"Could not find email with id {email_id}"
                logger.error(error_msg)
                raise ValueError(error_msg)

            email_data = dict(email_data)

            # 2. Get sender and thread context (these functions are designed to be safe if context doesn't exist)
            logger.info(f"[EMBEDDING] Step 2: Getting sender history for {email_data['sender_email']}...")
            sender_history = self._get_or_create_sender_history(email_data['sender_email'], email_data['sender_name'])
            logger.info(f"[EMBEDDING] Sender history retrieved")
            
            logger.info(f"[EMBEDDING] Step 3: Getting thread context for thread_id {email_data.get('thread_id')}...")
            thread_context = self._get_or_create_thread_context(email_data['thread_id'], email_data)
            logger.info(f"[EMBEDDING] Thread context retrieved")

            # 3. Get related articles (optional but good for context)
            logger.info(f"[EMBEDDING] Step 4: Getting related articles...")
            related_articles = self._get_related_articles(email_data, classifications)
            logger.info(f"[EMBEDDING] Found {len(related_articles)} related articles")

            # 4. Create the comprehensive embedding with the provided classifications
            # Pass classifications directly instead of a classification object
            logger.info(f"[EMBEDDING] Step 5: Creating comprehensive embedding...")
            embedding_result = self._create_comprehensive_embedding(
                email_data, sender_history, thread_context, classifications, related_articles
            )
            logger.info(f"[EMBEDDING] Comprehensive embedding created")

            # 5. Store pipeline context enrichment
            logger.info(f"[EMBEDDING] Step 6: Storing pipeline context enrichment...")
            self._store_pipeline_enrichment(email_id, classifications, related_articles, sender_history)
            logger.info(f"[EMBEDDING] Pipeline context enrichment stored")

            # 6. Update interaction history
            logger.info(f"[EMBEDDING] Step 7: Updating sender interaction history...")
            self._update_sender_interaction_history(email_data['sender_email'], email_data, classifications)
            logger.info(f"[EMBEDDING] Sender interaction history updated")

            # 7. Mark email as enriched
            logger.info(f"[EMBEDDING] Step 8: Marking email as enriched...")
            self._mark_email_as_enriched(email_id)
            logger.info(f"[EMBEDDING] Email marked as enriched")
            
            logger.info(f"Successfully created enhanced embedding for email {email_id}")
            return embedding_result

        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error creating embedding for email {email_id}: {e}", exc_info=True)
            raise
        finally:
            cursor.close()
    
    def process_email_with_full_context(self, email_data: Dict) -> Dict:
        """Process email with full conversation context and history"""
        email_id = email_data.get('id')
        gmail_id = email_data.get('gmail_id')
        thread_id = email_data.get('thread_id')
        sender_email = email_data.get('sender_email')
        
        try:
            # 1. Get or create sender interaction history
            sender_history = self._get_or_create_sender_history(sender_email, email_data.get('sender_name'))
            
            # 2. Get or create thread context
            thread_context = self._get_or_create_thread_context(thread_id, email_data)
            
            # 3. Use existing classification if available
            if 'classification' in email_data and email_data['classification']:
                classifications = [email_data['classification']]
            else:
                # No classification provided - this shouldn't happen in normal flow
                logger.warning("[CONTEXT] No classification provided for email, using 'unclassified'")
                classifications = ['unclassified']
            
            # 4. Get related articles for context
            related_articles = self._get_related_articles(email_data, classifications)
            
            # 5. Create comprehensive embedding
            embedding_result = self._create_comprehensive_embedding(
                email_data, sender_history, thread_context, classifications, related_articles
            )
            
            # 6. Store pipeline context enrichment
            self._store_pipeline_enrichment(email_id, classifications, related_articles, sender_history)
            
            # 7. Update interaction history
            self._update_sender_interaction_history(sender_email, email_data, classifications)
            
            # 8. Mark the original email as fully processed
            self._mark_email_as_enriched(email_id)

            return embedding_result
            
        except Exception as e:
            # Rollback on any error to keep connection usable
            self.db_conn.rollback()
            error_msg = f"Error processing email {gmail_id}: {e}"
            logger.error(error_msg, exc_info=True)
            raise RuntimeError(error_msg) from e
    
    def _get_or_create_sender_history(self, sender_email: str, sender_name: str) -> Dict:
        """Get or create sender interaction history"""
        cursor = self.db_conn.cursor()
        
        try:
            # Try to get existing history
            cursor.execute("""
                SELECT * FROM sender_interaction_history 
                WHERE sender_email = %s
            """, (sender_email,))
            
            history = cursor.fetchone()
            
            if history:
                # Convert to dict
                columns = [desc[0] for desc in cursor.description]
                history_dict = dict(zip(columns, history))
            else:
                # Create new history
                cursor.execute("""
                    INSERT INTO sender_interaction_history (
                        sender_email, sender_name, first_contact_date, 
                        last_contact_date, total_emails_sent
                    ) VALUES (%s, %s, %s, %s, %s)
                    RETURNING *
                """, (sender_email, sender_name, datetime.now(timezone.utc), 
                      datetime.now(timezone.utc), 1))
                
                history = cursor.fetchone()
                columns = [desc[0] for desc in cursor.description]
                history_dict = dict(zip(columns, history))
                
                self.db_conn.commit()
            
            return history_dict
            
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error getting/creating sender history for {sender_email}: {e}")
            raise
        finally:
            cursor.close()
    
    def _get_or_create_thread_context(self, thread_id: str, email_data: Dict) -> Dict:
        """Get or create thread context"""
        # Check if thread context is disabled for performance
        if hasattr(self, 'skip_thread_context') and self.skip_thread_context:
            logger.info("[THREAD] Thread context skipped for performance")
            return {}
            
        if not thread_id:
            logger.info("[THREAD] No thread_id provided, returning empty context")
            return {}
            
        cursor = self.db_conn.cursor()
        
        try:
            # Try to get existing thread context
            logger.info(f"[THREAD] Checking for existing thread context for {thread_id}...")
            cursor.execute("""
                SELECT * FROM thread_context 
                WHERE gmail_thread_id = %s
            """, (thread_id,))
            
            context = cursor.fetchone()
            
            if context:
                columns = [desc[0] for desc in cursor.description]
                context_dict = dict(zip(columns, context))
            else:
                # Get full thread from Gmail
                logger.info(f"[THREAD] No existing context found, fetching full thread from Gmail...")
                thread_messages = self._get_full_thread_messages(thread_id)
                logger.info(f"[THREAD] Retrieved {len(thread_messages)} messages from thread")
                
                # Analyze thread
                logger.info(f"[THREAD] Analyzing thread...")
                thread_analysis = self._analyze_thread(thread_messages)
                
                # Create new context
                cursor.execute("""
                    INSERT INTO thread_context (
                        gmail_thread_id, subject, participant_emails, participant_names,
                        message_count, our_message_count, thread_type, thread_status,
                        started_date, last_activity_date, 
                        thread_summary, key_topics
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                """, (
                    thread_id,
                    email_data.get('subject', ''),  # Adding subject from email data
                    thread_analysis['participants'],
                    thread_analysis['participant_names'],
                    thread_analysis['message_count'],
                    thread_analysis['our_message_count'],
                    thread_analysis['thread_type'],
                    thread_analysis['thread_status'],
                    thread_analysis['started_date'],
                    thread_analysis['last_activity_date'],
                    thread_analysis['summary'],
                    thread_analysis['key_topics']
                ))
                
                context = cursor.fetchone()
                columns = [desc[0] for desc in cursor.description]
                context_dict = dict(zip(columns, context))
                
                self.db_conn.commit()
            
            return context_dict
            
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error getting/creating thread context for {thread_id}: {e}")
            # Return empty dict on error to allow processing to continue
            return {}
        finally:
            cursor.close()
    
    def _get_full_thread_messages(self, thread_id: str) -> List[Dict]:
        """Get all messages in a thread"""
        try:
            logger.info(f"[GMAIL] Fetching thread {thread_id} from Gmail API...")
            thread = self.service.users().threads().get(
                userId='me',
                id=thread_id
            ).execute()
            logger.info(f"[GMAIL] Thread fetched successfully")
            
            messages = []
            for message in thread.get('messages', []):
                msg_data = self._extract_message_data(message)
                if msg_data:
                    messages.append(msg_data)
            
            return sorted(messages, key=lambda x: x.get('date_sent', datetime.min.replace(tzinfo=timezone.utc)))
            
        except Exception as e:
            logger.error(f"Error getting thread messages: {e}")
            return []
    
    def _extract_message_data(self, message: Dict) -> Optional[Dict]:
        """Extract data from Gmail message"""
        try:
            headers = {h['name']: h['value'] for h in message['payload'].get('headers', [])}
            
            # Parse sender
            sender_name, sender_email = parseaddr(headers.get('From', ''))
            
            # Get body
            body_text = self._extract_body_text(message['payload'])
            
            # Parse date
            date_str = headers.get('Date', '')
            try:
                date_sent = parsedate_to_datetime(date_str)
            except:
                date_sent = datetime.now(timezone.utc)
            
            return {
                'id': message['id'],
                'sender_name': sender_name,
                'sender_email': sender_email,
                'subject': headers.get('Subject', ''),
                'date_sent': date_sent,
                'body_text': body_text,
                'is_from_us': sender_email.lower() == DELEGATE_EMAIL.lower()
            }
            
        except Exception as e:
            logger.error(f"Error extracting message data: {e}")
            return None
    
    def _extract_body_text(self, payload) -> str:
        """Extract body text from message payload"""
        body_text = ""
        
        def extract_from_part(part):
            nonlocal body_text
            
            mime_type = part.get('mimeType', '')
            
            if mime_type == 'text/plain':
                data = part['body'].get('data', '')
                if data:
                    body_text += base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
            elif 'parts' in part:
                for subpart in part['parts']:
                    extract_from_part(subpart)
        
        if 'parts' in payload:
            for part in payload['parts']:
                extract_from_part(part)
        else:
            extract_from_part(payload)
        
        return body_text
    
    def _analyze_thread(self, thread_messages: List[Dict]) -> Dict:
        """Analyze thread for context"""
        if not thread_messages:
            return {
                'participants': [],
                'participant_names': [],
                'message_count': 0,
                'our_message_count': 0,
                'thread_type': 'unknown',
                'thread_status': 'unknown',
                'started_date': datetime.now(timezone.utc),
                'last_activity_date': datetime.now(timezone.utc),
                'summary': '',
                'key_topics': []
            }
        
        # Get participants
        participants = set()
        participant_names = set()
        our_message_count = 0
        
        for msg in thread_messages:
            participants.add(msg['sender_email'])
            participant_names.add(msg['sender_name'])
            if msg['is_from_us']:
                our_message_count += 1
        
        # Remove us from participants
        participants.discard(DELEGATE_EMAIL)
        
        # Determine thread type
        thread_type = self._determine_thread_type(thread_messages)
        
        # Determine thread status
        thread_status = self._determine_thread_status(thread_messages)
        
        # Create summary
        summary = self._create_thread_summary(thread_messages)
        
        # Extract key topics
        key_topics = self._extract_key_topics(thread_messages)
        
        return {
            'participants': list(participants),
            'participant_names': list(participant_names),
            'message_count': len(thread_messages),
            'our_message_count': our_message_count,
            'thread_type': thread_type,
            'thread_status': thread_status,
            'started_date': thread_messages[0]['date_sent'],
            'last_activity_date': thread_messages[-1]['date_sent'],
            'summary': summary,
            'key_topics': key_topics
        }
    
    def _determine_thread_type(self, thread_messages: List[Dict]) -> str:
        """Determine thread type based on content"""
        all_text = ' '.join([msg['body_text'] for msg in thread_messages]).lower()
        
        if any(word in all_text for word in ['advertising', 'sponsor', 'partnership', 'rates']):
            return 'sales_inquiry'
        elif any(word in all_text for word in ['story', 'article', 'interview', 'tip']):
            return 'story_discussion'
        elif any(word in all_text for word in ['meeting', 'coffee', 'call', 'chat']):
            return 'meeting_request'
        elif any(word in all_text for word in ['complaint', 'error', 'correction', 'issue']):
            return 'complaint'
        elif len(thread_messages) > 3:
            return 'ongoing_conversation'
        else:
            return 'inquiry'
    
    def _determine_thread_status(self, thread_messages: List[Dict]) -> str:
        """Determine current thread status"""
        if not thread_messages:
            return 'empty'
        
        last_message = thread_messages[-1]
        
        # If last message is from us, probably waiting for response
        if last_message['is_from_us']:
            return 'waiting_for_response'
        
        # If last message is recent and not from us, needs response
        now = datetime.now(timezone.utc)
        last_msg_time = last_message['date_sent']
        if last_msg_time.tzinfo is None:
            last_msg_time = last_msg_time.replace(tzinfo=timezone.utc)
            
        hours_since_last = (now - last_msg_time).total_seconds() / 3600
        
        if hours_since_last < 24:
            return 'needs_response'
        elif hours_since_last < 168:  # 1 week
            return 'pending'
        else:
            return 'dormant'
    
    def _create_thread_summary(self, thread_messages: List[Dict]) -> str:
        """Create a summary of the thread"""
        if not thread_messages:
            return ""
        
        # Get first and last messages
        first_msg = thread_messages[0]
        last_msg = thread_messages[-1]
        
        summary = f"Thread started by {first_msg['sender_name']} "
        summary += f"about '{first_msg['subject']}'. "
        summary += f"{len(thread_messages)} messages exchanged. "
        
        if last_msg['is_from_us']:
            summary += "We sent the last message."
        else:
            summary += f"Waiting for response from {last_msg['sender_name']}."
        
        return summary
    
    def _extract_key_topics(self, thread_messages: List[Dict]) -> List[str]:
        """Extract key topics from thread"""
        all_text = ' '.join([msg['body_text'] for msg in thread_messages]).lower()
        
        # Simple keyword extraction
        keywords = []
        
        # Business topics
        business_terms = ['advertising', 'partnership', 'rates', 'pricing', 'contract', 'sponsor']
        for term in business_terms:
            if term in all_text:
                keywords.append(term)
        
        # News topics
        news_terms = ['story', 'article', 'interview', 'exclusive', 'breaking', 'tip']
        for term in news_terms:
            if term in all_text:
                keywords.append(term)
        
        # Location topics
        location_terms = ['brooklyn', 'bushwick', 'williamsburg', 'queens', 'nyc']
        for term in location_terms:
            if term in all_text:
                keywords.append(term)
        
        return keywords[:10]  # Return top 10
    
    def _get_related_articles(self, email_data: Dict, classifications: List[str]) -> List[Dict]:
        """Get related articles for context"""
        # Check if article search is disabled for performance
        if hasattr(self, 'skip_article_search') and self.skip_article_search:
            logger.info("[ARTICLES] Article search skipped for performance")
            return []
            
        try:
            logger.info("[ARTICLES] Starting related articles search...")
            # Extract key terms from email
            email_text = f"{email_data.get('subject', '')} {email_data.get('body_text', '')}"
            
            # Add classification-based search terms
            search_terms = []
            
            # Add subject words
            if email_data.get('subject'):
                subject_words = [w for w in email_data['subject'].split() if len(w) > 3]
                search_terms.extend(subject_words[:5])
            
            # Add classification-based keywords
            for classification in classifications:
                if 'story' in classification.lower():
                    search_terms.extend(['story', 'article', 'news'])
                elif 'sales' in classification.lower():
                    search_terms.extend(['advertising', 'sponsor', 'partnership'])
                elif 'meeting' in classification.lower():
                    search_terms.extend(['meeting', 'event', 'community'])
            
            # Search for related articles using vector similarity
            logger.info("[ARTICLES] Creating embedding for article search...")
            query_embedding = self.model.encode(email_text)
            logger.info("[ARTICLES] Embedding created, executing vector similarity search...")
            
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT 
                    ac.id,
                    a.headline,
                    o.name as outlet_name,
                    a.publish_date as publish_date,
                    ac.text,
                    1 - (ac.embedding <=> %s::vector) as similarity
                FROM article_chunks ac
                JOIN articles a ON a.id = ac.article_id
                LEFT JOIN outlets o ON a.outlet_id = o.id
                WHERE 1 - (ac.embedding <=> %s::vector) > 0.7
                ORDER BY ac.embedding <=> %s::vector
                LIMIT 5
            """, (query_embedding.tolist(), query_embedding.tolist(), query_embedding.tolist()))
            
            related_articles = []
            for row in cursor.fetchall():
                related_articles.append({
                    'id': row[0],
                    'headline': row[1],
                    'outlet_name': row[2],
                    'publish_date': row[3].isoformat() if row[3] else None,
                    'text': row[4][:500],  # Limit text length
                    'similarity': float(row[5])
                })
            
            cursor.close()
            logger.info(f"[ARTICLES] Search complete, found {len(related_articles)} articles")
            return related_articles
            
        except Exception as e:
            logger.error(f"[ARTICLES] Error getting related articles: {e}")
            return []
    
    def _create_comprehensive_embedding(self, email_data: Dict, sender_history: Dict, 
                                       thread_context: Dict, classifications: List[str], 
                                       related_articles: List[Dict]) -> Dict:
        """Create comprehensive embedding with all context"""
        
        # Build comprehensive text for embedding
        embedding_text = ""
        
        # 1. Email content
        embedding_text += f"Email from: {email_data.get('sender_name')} <{email_data.get('sender_email')}>\n"
        embedding_text += f"Subject: {email_data.get('subject', '')}\n"
        embedding_text += f"Content: {email_data.get('body_text', '')}\n\n"
        
        # 2. Sender history context
        embedding_text += f"Sender History:\n"
        embedding_text += f"- Previous emails: {sender_history.get('total_emails_sent', 0)}\n"
        embedding_text += f"- Response rate: {sender_history.get('response_rate', 0) or 0:.2f}\n"
        embedding_text += f"- Relationship: {sender_history.get('relationship_type', 'unknown')}\n"
        embedding_text += f"- Common topics: {', '.join(sender_history.get('common_topics', []) or [])}\n\n"
        
        # 3. Thread context
        if thread_context:
            embedding_text += f"Thread Context:\n"
            embedding_text += f"- Thread type: {thread_context.get('thread_type', 'unknown')}\n"
            embedding_text += f"- Messages in thread: {thread_context.get('message_count', 0)}\n"
            embedding_text += f"- Our messages: {thread_context.get('our_message_count', 0)}\n"
            embedding_text += f"- Thread status: {thread_context.get('thread_status', 'unknown')}\n"
            embedding_text += f"- Summary: {thread_context.get('thread_summary', '')}\n\n"
        
        # 4. Pipeline classification (using provided classifications)
        embedding_text += f"Pipeline Classification:\n"
        embedding_text += f"- Primary pipeline: {classifications[0] if classifications else 'unknown'}\n"
        embedding_text += f"- All pipelines: {', '.join(classifications)}\n\n"
        
        # 5. Related articles context
        if related_articles:
            embedding_text += f"Related Articles Context:\n"
            for article in related_articles[:3]:  # Limit to top 3
                embedding_text += f"- {article['headline']} ({article.get('outlet_name', 'Unknown')}): {article['text'][:200]}...\n"
        
        # Create embedding
        logger.info(f"[COMPREHENSIVE] Creating vector embedding for text of length {len(embedding_text)}...")
        embedding = self.model.encode(embedding_text)
        logger.info(f"[COMPREHENSIVE] Embedding created with dimension {len(embedding)}")
        
        # Store enhanced embedding
        cursor = self.db_conn.cursor()
        try:
            # First check if an embedding already exists
            cursor.execute("""
                SELECT id FROM enhanced_email_embeddings 
                WHERE email_id = %s AND embedding_type = %s
            """, (email_data.get('id'), 'comprehensive'))
            
            existing = cursor.fetchone()
            
            if existing:
                # Update existing
                cursor.execute("""
                    UPDATE enhanced_email_embeddings SET
                        embedding = %s,
                        embedding_text = %s,
                        thread_id = %s,
                        sender_email = %s,
                        pipeline_classification = %s,
                        sender_interaction_count = %s,
                        thread_message_count = %s,
                        includes_response = %s,
                        includes_thread_context = %s,
                        includes_sender_history = %s,
                        includes_pipeline_context = %s,
                        search_keywords = %s,
                        business_context = %s,
                        updated_at = NOW()
                    WHERE email_id = %s AND embedding_type = %s
                    RETURNING id
                """, (
                    embedding.tolist(),
                    embedding_text,
                    email_data.get('thread_id'),
                    email_data.get('sender_email'),
                    classifications[0] if classifications else 'unknown',
                    sender_history.get('total_emails_sent', 0),
                    thread_context.get('message_count', 0) if thread_context else 0,
                    False,  # includes_response
                    bool(thread_context),
                    True,  # includes_sender_history
                    True,  # includes_pipeline_context
                    thread_context.get('key_topics', []) if thread_context else [],
                    f"Pipeline: {classifications[0] if classifications else 'unknown'}",
                    email_data.get('id'),
                    'comprehensive'
                ))
            else:
                # Insert new
                cursor.execute("""
                    INSERT INTO enhanced_email_embeddings (
                        email_id, embedding_type, embedding, embedding_text,
                        thread_id, sender_email, pipeline_classification,
                        sender_interaction_count, thread_message_count,
                        includes_response, includes_thread_context,
                        includes_sender_history, includes_pipeline_context,
                        search_keywords, business_context
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                email_data.get('id'),
                'comprehensive',
                embedding.tolist(),
                embedding_text,
                email_data.get('thread_id'),
                email_data.get('sender_email'),
                classifications[0] if classifications else 'unknown',
                sender_history.get('total_emails_sent', 0),
                thread_context.get('message_count', 0) if thread_context else 0,
                False,  # includes_response (not yet)
                bool(thread_context),   # includes_thread_context
                True,   # includes_sender_history
                True,   # includes_pipeline_context
                thread_context.get('key_topics', []) if thread_context else [],
                f"Pipeline: {classifications[0] if classifications else 'unknown'}"
            ))
            
            embedding_id = cursor.fetchone()[0]
            self.db_conn.commit()
            
            return {
                'embedding_id': embedding_id,
                'embedding': embedding,
                'embedding_text': embedding_text,
                'context_included': {
                    'sender_history': True,
                    'thread_context': bool(thread_context),
                    'pipeline_classification': True,
                    'related_articles': len(related_articles) > 0
                }
            }
            
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error creating comprehensive embedding: {e}")
            raise
        finally:
            cursor.close()
    
    def _store_pipeline_enrichment(self, email_id: int, classifications: List[str], 
                                  related_articles: List[Dict], sender_history: Dict):
        """Store pipeline context enrichment"""
        cursor = self.db_conn.cursor()
        
        try:
            # Create business profile
            sender_business_profile = {
                'email': sender_history.get('sender_email'),
                'name': sender_history.get('sender_name'),
                'relationship_type': sender_history.get('relationship_type'),
                'total_revenue': float(sender_history.get('total_revenue_generated', 0) or 0),
                'articles_published': sender_history.get('articles_published', 0),
                'response_rate': float(sender_history.get('response_rate', 0) or 0)
            }
            
            # Create suggested responses based on pipeline
            suggested_responses = self._generate_suggested_responses(classifications, sender_history)
            
            # Store enrichment for primary classification
            primary_pipeline = classifications[0] if classifications else 'unknown'
            
            cursor.execute("""
                INSERT INTO pipeline_context_enrichment (
                    email_id, pipeline_type, related_articles,
                    sender_business_profile, suggested_responses
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (email_id) DO UPDATE SET
                    pipeline_type = EXCLUDED.pipeline_type,
                    related_articles = EXCLUDED.related_articles,
                    sender_business_profile = EXCLUDED.sender_business_profile,
                    suggested_responses = EXCLUDED.suggested_responses
            """, (
                email_id,
                primary_pipeline,
                json.dumps(related_articles, cls=DateTimeJSONEncoder),
                json.dumps(sender_business_profile, cls=DateTimeJSONEncoder),
                json.dumps(suggested_responses, cls=DateTimeJSONEncoder)
            ))
            
            self.db_conn.commit()
            
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error storing pipeline enrichment: {e}")
            raise
        finally:
            cursor.close()
    
    def _generate_suggested_responses(self, classifications: List[str], sender_history: Dict) -> List[Dict]:
        """Generate context-aware suggested responses"""
        pipeline = classifications[0] if classifications else 'unknown'
        
        suggestions = []
        
        # Base responses by pipeline
        if 'sales' in pipeline.lower():
            suggestions.extend([
                {
                    "text": "Thank you for your interest in advertising with Bushwick Daily. I'll connect you with our sales team who can provide current rates and availability.",
                    "context": "Standard sales inquiry response"
                },
                {
                    "text": "Thanks for reaching out about partnership opportunities. Let me send you our media kit with current advertising options and pricing.",
                    "context": "Partnership inquiry with media kit"
                }
            ])
            
            # Add personalized response if repeat advertiser
            if sender_history.get('total_revenue_generated', 0) > 0:
                suggestions.insert(0, {
                    "text": f"Great to hear from you again! Given our successful past campaigns, I'd love to discuss new opportunities. When would be a good time to connect?",
                    "context": f"Returning advertiser (${sender_history.get('total_revenue_generated', 0):.2f} previous revenue)"
                })
        
        elif 'story' in pipeline.lower():
            suggestions.extend([
                {
                    "text": "Thank you for the story tip. I'll share this with our editorial team for consideration.",
                    "context": "Standard story tip response"
                },
                {
                    "text": "Thanks for your pitch. This looks interesting and aligns with our coverage. Let me discuss with the team and get back to you.",
                    "context": "Positive story pitch response"
                }
            ])
            
            # Add personalized response if productive source
            if sender_history.get('articles_published', 0) > 0:
                suggestions.insert(0, {
                    "text": f"Thanks for another great tip! Given our successful collaboration on past stories, I'll prioritize this with our editorial team.",
                    "context": f"Productive source ({sender_history.get('articles_published', 0)} articles published)"
                })
        
        elif 'meeting' in pipeline.lower():
            suggestions.extend([
                {
                    "text": "I'd be happy to meet. My schedule is pretty busy this week, but I have some availability next week. What days work best for you?",
                    "context": "Standard meeting request response"
                },
                {
                    "text": "Thanks for reaching out. Could you tell me a bit more about what you'd like to discuss? That will help me allocate the right amount of time.",
                    "context": "Meeting request needing clarification"
                }
            ])
        
        else:
            # Generic responses
            suggestions.extend([
                {
                    "text": "Thank you for your email. I'll review this and get back to you shortly.",
                    "context": "Generic acknowledgment"
                },
                {
                    "text": "Thanks for reaching out. Let me look into this and follow up with you.",
                    "context": "Generic follow-up needed"
                }
            ])
        
        return suggestions
    
    def _update_sender_interaction_history(self, sender_email: str, email_data: Dict, classifications: List[str]):
        """Update sender interaction history with new email"""
        cursor = self.db_conn.cursor()
        
        try:
            # Update interaction count and last contact date
            cursor.execute("""
                UPDATE sender_interaction_history
                SET total_emails_sent = total_emails_sent + 1,
                    last_contact_date = %s,
                    updated_at = NOW()
                WHERE sender_email = %s
            """, (email_data.get('date_sent', datetime.now(timezone.utc)), sender_email))
            
            # Update pipeline history if we have classifications
            if classifications:
                cursor.execute("""
                    UPDATE sender_interaction_history
                    SET pipeline_history = array_append(
                        COALESCE(pipeline_history, ARRAY[]::TEXT[]), 
                        %s
                    )
                    WHERE sender_email = %s
                    AND NOT (%s = ANY(COALESCE(pipeline_history, ARRAY[]::TEXT[])))
                """, (classifications[0], sender_email, classifications[0]))
            
            self.db_conn.commit()
            
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error updating sender interaction history: {e}")
            # Don't raise - this is not critical
        finally:
            cursor.close()
    
    def _mark_email_as_enriched(self, email_id: int):
        """Mark email as having been enriched with enhanced embeddings"""
        cursor = self.db_conn.cursor()
        
        try:
            # Update the email to mark it as pipeline processed
            cursor.execute("""
                UPDATE classified_emails 
                SET pipeline_processed = TRUE,
                    updated_at = NOW()
                WHERE id = %s
            """, (email_id,))
            
            self.db_conn.commit()
            
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error marking email as enriched: {e}")
            # Don't raise - this is not critical
        finally:
            cursor.close()
    
    def search_enhanced_emails(self, query: str, limit: int = 20, 
                              include_responses: bool = True,
                              pipeline_filter: Optional[str] = None) -> List[Dict]:
        """Search emails with enhanced context"""
        cursor = self.db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        try:
            sql = """
                SELECT DISTINCT
                    e.id, e.subject, e.sender_name, e.sender_email,
                    e.date_sent, e.snippet, e.body_text,
                    eee.pipeline_classification,
                    eee.sender_interaction_count,
                    eee.thread_message_count,
                    eee.context_summary
                FROM classified_emails e
                JOIN enhanced_email_embeddings eee ON e.id = eee.email_id
                WHERE eee.embedding_type = 'comprehensive'
            """
            
            params = []
            
            # Add search condition
            if query:
                sql += """
                    AND (
                        e.subject ILIKE %s OR
                        e.body_text ILIKE %s OR
                        e.sender_name ILIKE %s OR
                        e.sender_email ILIKE %s
                    )
                """
                search_pattern = f"%{query}%"
                params.extend([search_pattern] * 4)
            
            # Add pipeline filter
            if pipeline_filter:
                sql += " AND eee.pipeline_classification = %s"
                params.append(pipeline_filter)
            
            sql += " ORDER BY e.date_sent DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(sql, params)
            
            results = []
            for row in cursor.fetchall():
                result = dict(row)
                # Parse JSON context summary if it's a string
                if isinstance(result.get('context_summary'), str):
                    try:
                        result['context_summary'] = json.loads(result['context_summary'])
                    except json.JSONDecodeError:
                        pass
                results.append(result)
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching enhanced emails: {e}")
            raise
        finally:
            cursor.close()
    
    def __del__(self):
        """Clean up database connection"""
        if hasattr(self, 'db_conn') and self.db_conn:
            self.db_conn.close()


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Enhanced Email Embedding Processor")
    parser.add_argument("--email-id", type=int, required=True, help="The ID of the email to process.")
    parser.add_argument("--classifications", nargs='+', required=True, help="A list of classification labels for the email.")
    
    args = parser.parse_args()
    
    logger.info(f"[MAIN] Script started with args: email_id={args.email_id}, classifications={args.classifications}")

    try:
        logger.info("[MAIN] Creating EnhancedEmailEmbeddings instance...")
        embedding_system = EnhancedEmailEmbeddings()
        logger.info("[MAIN] Instance created, starting embedding process...")
        
        embedding_system.create_embedding_for_classified_email(
            email_id=args.email_id,
            classifications=args.classifications
        )
        logger.info(f"[MAIN] Processing complete for email {args.email_id}")
        print(f"Processing complete for email {args.email_id}")
    except Exception as e:
        logger.error(f"[MAIN] Failed to process email {args.email_id}: {e}", exc_info=True)
        sys.exit(1)