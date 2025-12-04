#!/usr/bin/env python3
"""
Gmail Email Extractor with OAuth2 Authentication
For local/self-hosted deployment
"""
import asyncio
import sys
import os
from pathlib import Path
from local_oauth_service import LocalOAuth2Service
from datetime import datetime, timezone, timedelta
import base64
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Optional
import logging
from email.utils import parsedate_to_datetime
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GmailOAuthExtractor:
    def __init__(self):
        self.oauth_service = LocalOAuth2Service()
        self.gmail_service = None
        self.user_email = None
        
        # Database configuration
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'limrose_email_pipeline'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', '')
        }
        
    async def setup(self):
        """Setup OAuth authentication"""
        try:
            print("Initializing Gmail OAuth authentication...")
            await self.oauth_service.authenticate()
            self.gmail_service = self.oauth_service.get_gmail_service()
            
            # Get authenticated user's email
            profile = self.gmail_service.users().getProfile(userId='me').execute()
            self.user_email = profile['emailAddress']
            print(f"✓ Authenticated as: {self.user_email}")
            
        except Exception as e:
            print(f"❌ Authentication failed: {e}")
            print("\nPlease run 'python setup_oauth.py' to configure OAuth credentials.")
            sys.exit(1)
    
    def get_db_connection(self):
        """Create database connection"""
        return psycopg2.connect(**self.db_config, cursor_factory=RealDictCursor)
    
    def validate_database_schema(self):
        """Validate that required database tables exist"""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # Check if classified_emails table exists
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_schema = 'public'
                            AND table_name = 'classified_emails'
                        );
                    """)

                    table_exists = cursor.fetchone()[0]

                    if not table_exists:
                        print("❌ Database table 'classified_emails' not found")
                        print("💡 Please create the database schema first:")
                        print("   - Run: ./update_emails_v2.sh --setup")
                        print("   - Or run: python scripts/setup_all_tables.py")
                        return False

                    print("✓ Database schema validation passed")
                    return True
                    
        except psycopg2.Error as e:
            print(f"❌ Database connection failed: {e}")
            print("💡 Check your database configuration in .env file")
            return False
        except Exception as e:
            print(f"❌ Database validation error: {e}")
            return False
    
    def normalize_email_address(self, email: str) -> str:
        """Normalize email address by removing dots and anything after +"""
        if '@' not in email:
            return email.lower().strip()
        
        local, domain = email.lower().strip().split('@', 1)
        
        # Remove anything after + in local part
        local = local.split('+')[0]
        
        # For Gmail addresses, remove dots
        if domain in ['gmail.com', 'googlemail.com']:
            local = local.replace('.', '')
        
        return f"{local}@{domain}"
    
    def extract_email_address(self, header_value: str) -> str:
        """Extract email address from header value like 'Name <email@domain.com>'"""
        # Match email in angle brackets
        match = re.search(r'<([^>]+)>', header_value)
        if match:
            return match.group(1)
        # If no angle brackets, assume the whole thing is an email
        return header_value.strip()
    
    def extract_emails(self, query: str = 'is:unread', max_results: int = 100) -> List[Dict]:
        """Extract emails using OAuth authentication"""
        try:
            all_messages = []
            page_token = None
            
            while True:
                # Build request with pagination
                if page_token:
                    results = self.gmail_service.users().messages().list(
                        userId='me',
                        q=query,
                        maxResults=min(max_results - len(all_messages), 500),
                        pageToken=page_token
                    ).execute()
                else:
                    results = self.gmail_service.users().messages().list(
                        userId='me',
                        q=query,
                        maxResults=min(max_results, 500)
                    ).execute()
                
                messages = results.get('messages', [])
                all_messages.extend(messages)
                
                # Check if we've reached our limit
                if len(all_messages) >= max_results:
                    all_messages = all_messages[:max_results]
                    break
                
                # Check for next page
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            
            print(f"Found {len(all_messages)} emails matching query: {query}")
            
            emails = []
            for idx, msg in enumerate(all_messages):
                if idx % 10 == 0:
                    print(f"Processing email {idx + 1}/{len(all_messages)}...")
                
                email_data = self.get_email_details(msg['id'])
                if email_data:
                    emails.append(email_data)
            
            return emails
            
        except Exception as e:
            print(f"Error extracting emails: {e}")
            return []
    
    def get_email_details(self, msg_id: str) -> Optional[Dict]:
        """Get detailed email information"""
        try:
            message = self.gmail_service.users().messages().get(
                userId='me',
                id=msg_id
            ).execute()
            
            # Extract headers
            headers = message['payload'].get('headers', [])
            
            # Extract header values
            subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
            sender_raw = next((h['value'] for h in headers if h['name'] == 'From'), '')
            to_raw = next((h['value'] for h in headers if h['name'] == 'To'), '')
            cc_raw = next((h['value'] for h in headers if h['name'] == 'Cc'), '')
            date_str = next((h['value'] for h in headers if h['name'] == 'Date'), '')
            message_id = next((h['value'] for h in headers if h['name'] == 'Message-ID'), '')
            in_reply_to = next((h['value'] for h in headers if h['name'] == 'In-Reply-To'), None)
            
            # Extract email addresses
            sender_email = self.extract_email_address(sender_raw) if sender_raw else ''
            to_email = self.extract_email_address(to_raw) if to_raw else ''
            
            # Normalize email addresses
            sender_normalized = self.normalize_email_address(sender_email)
            to_normalized = self.normalize_email_address(to_email)
            
            # Parse date
            received_date = datetime.now(timezone.utc)
            if date_str:
                try:
                    received_date = parsedate_to_datetime(date_str)
                    # Ensure timezone awareness
                    if received_date.tzinfo is None:
                        received_date = received_date.replace(tzinfo=timezone.utc)
                except:
                    logger.warning(f"Failed to parse date: {date_str}")
            
            # Extract body
            body = self.extract_body(message['payload'])
            
            # Get thread ID
            thread_id = message.get('threadId', '')
            
            # Get labels
            labels = message.get('labelIds', [])
            
            return {
                'gmail_id': msg_id,
                'thread_id': thread_id,
                'message_id': message_id,
                'in_reply_to': in_reply_to,
                'subject': subject,
                'sender': sender_raw,
                'sender_email': sender_email,
                'sender_normalized': sender_normalized,
                'recipient': to_raw,
                'recipient_email': to_email,
                'recipient_normalized': to_normalized,
                'cc': cc_raw,
                'date': date_str,
                'received_date': received_date,
                'body': body,
                'labels': labels,
                'is_sent': 'SENT' in labels,
                'raw_data': message
            }
            
        except Exception as e:
            logger.error(f"Error getting email details for {msg_id}: {e}")
            return None
    
    def extract_body(self, payload: Dict) -> str:
        """Extract email body from payload"""
        body = ""
        
        # Check if multipart
        if 'parts' in payload:
            for part in payload['parts']:
                if part['mimeType'] == 'text/plain':
                    if 'data' in part['body']:
                        body += base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
                elif 'parts' in part:
                    # Nested multipart
                    body += self.extract_body(part)
        elif payload['body'].get('data'):
            # Single part message
            body = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')
        
        return body.strip()
    
    def save_to_database(self, emails: List[Dict]) -> int:
        """Save emails to database with deduplication"""
        if not emails:
            return 0
        
        saved_count = 0
        
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    for email in emails:
                        try:
                            # Check if email already exists
                            cursor.execute(
                                "SELECT id FROM classified_emails WHERE gmail_id = %s",
                                (email['gmail_id'],)
                            )

                            if cursor.fetchone():
                                logger.info(f"Email {email['gmail_id']} already exists, skipping...")
                                continue

                            # Parse CC emails into array
                            cc_emails = []
                            if email.get('cc'):
                                # Split by comma and extract email addresses
                                cc_parts = email['cc'].split(',')
                                for cc_part in cc_parts:
                                    cc_email = self.extract_email_address(cc_part.strip())
                                    if cc_email:
                                        cc_emails.append(cc_email)

                            # Insert email into classified_emails table
                            cursor.execute("""
                                INSERT INTO classified_emails (
                                    gmail_id, thread_id, message_id, in_reply_to,
                                    subject, sender_name, sender_email,
                                    recipient_emails, cc_emails,
                                    date_sent, body_text, labels, processed
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                )
                                ON CONFLICT (gmail_id) DO NOTHING
                            """, (
                                email['gmail_id'],
                                email['thread_id'],
                                email['message_id'],
                                email.get('in_reply_to'),
                                email['subject'],
                                email['sender'],  # Full "Name <email>" format
                                email['sender_email'],  # Just email address
                                [email['recipient_email']] if email.get('recipient_email') else [],  # Array
                                cc_emails,  # Array of CC emails
                                email['received_date'],
                                email['body'],
                                email['labels'],
                                False  # Initially not processed
                            ))
                            
                            saved_count += 1
                            
                        except Exception as e:
                            logger.error(f"Error saving email {email['gmail_id']}: {e}")
                            conn.rollback()
                            continue
                    
                    conn.commit()
                    
        except Exception as e:
            logger.error(f"Database error: {e}")
        
        return saved_count
    
    def mark_as_processed(self, gmail_ids: List[str]):
        """Mark emails as processed by removing unread label"""
        try:
            for gmail_id in gmail_ids:
                self.gmail_service.users().messages().modify(
                    userId='me',
                    id=gmail_id,
                    body={'removeLabelIds': ['UNREAD']}
                ).execute()
            
            logger.info(f"Marked {len(gmail_ids)} emails as read")
            
        except Exception as e:
            logger.error(f"Error marking emails as read: {e}")

async def main():
    """Main function with example usage"""
    extractor = GmailOAuthExtractor()
    
    # Setup OAuth authentication
    await extractor.setup()
    
    # Validate database schema
    if not extractor.validate_database_schema():
        print("\n❌ Database validation failed. Cannot proceed.")
        return
    
    # Command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == '--revoke':
            extractor.oauth_service.revoke_credentials()
            print("✓ OAuth credentials revoked")
            return
        elif sys.argv[1] == '--test':
            # Test mode - just fetch a few emails
            emails = extractor.extract_emails(query='is:unread', max_results=5)
            for email in emails:
                print(f"\n--- Email ---")
                print(f"Subject: {email['subject']}")
                print(f"From: {email['sender']}")
                print(f"Date: {email['date']}")
                print(f"Preview: {email['body'][:100]}...")
            return
    
    # Default behavior - extract and save unread emails
    print("\n=== Extracting Unread Emails ===")
    
    # Get date range for the last 7 days
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y/%m/%d')
    query = f'is:unread after:{seven_days_ago}'
    
    # Extract emails
    emails = extractor.extract_emails(query=query, max_results=1000)
    
    if emails:
        # Save to database
        saved_count = extractor.save_to_database(emails)
        print(f"\n✓ Saved {saved_count} new emails to database")
        
        # Mark as processed
        if saved_count > 0:
            gmail_ids = [e['gmail_id'] for e in emails[:saved_count]]
            extractor.mark_as_processed(gmail_ids)
    else:
        print("No unread emails found")

if __name__ == "__main__":
    asyncio.run(main())