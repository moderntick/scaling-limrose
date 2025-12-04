#!/usr/bin/env python3
"""
Process ALL remaining emails - both short and regular
"""

import os
import psycopg2
import psycopg2.extras
from sentence_transformers import SentenceTransformer
import sys
import time
import re
import traceback
from datetime import datetime

# Configuration
DB_NAME = os.getenv("DB_NAME", "limrose_email_pipeline")
DB_USER = os.getenv("DB_USER", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
BATCH_SIZE = 200
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

class CompleteProcessor:
    def __init__(self):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Initializing complete processor...")
        
        # Check if we're in offline mode
        if os.environ.get('HF_HUB_OFFLINE') == '1':
            snapshot_path = os.path.expanduser("~/.cache/huggingface/hub/models--sentence-transformers--all-MiniLM-L6-v2/snapshots/c9745ed1d9f207416be6d2e6f8de32d1f16199bf")
            if os.path.exists(snapshot_path):
                self.model = SentenceTransformer(snapshot_path, device='cpu')
            else:
                self.model = SentenceTransformer(EMBEDDING_MODEL, device='cpu', local_files_only=True)
        else:
            self.model = SentenceTransformer(EMBEDDING_MODEL, device='cpu')
        self.conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST)
        self.conn.autocommit = False
        print(f"Database connected successfully")
        
    def process_short_emails(self):
        """Process emails with body text < 50 chars"""
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT ce.id, ce.subject, ce.snippet, ce.sender_email, ce.body_text
                FROM classified_emails ce
                WHERE NOT EXISTS (SELECT 1 FROM email_chunks WHERE email_id = ce.id)
                AND LENGTH(COALESCE(ce.body_text, '')) < 50
                AND (ce.subject IS NOT NULL OR ce.snippet IS NOT NULL)
                ORDER BY ce.id DESC
                LIMIT %s
            """, (BATCH_SIZE * 2,))  # Larger batch for short emails
            
            emails = cur.fetchall()
            if not emails:
                return 0
                
            texts = []
            for email in emails:
                text = f"Subject: {email.get('subject', 'No Subject')}\n"
                if email.get('snippet'):
                    text += f"Preview: {email['snippet'][:500]}"
                elif email.get('body_text') and len(email['body_text'].strip()) > 0:
                    text += f"Body: {email['body_text'].strip()}"
                texts.append(text)
            
            embeddings = self.model.encode(texts, batch_size=32, show_progress_bar=False)
            
            insert_data = []
            for email, embedding, text in zip(emails, embeddings, texts):
                metadata = {
                    'email_id': email['id'],
                    'type': 'short_email',
                    'subject': email.get('subject', '')
                }
                insert_data.append((
                    email['id'], 0, text[:500],
                    embedding.tolist(),
                    psycopg2.extras.Json(metadata)
                ))
            
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO email_chunks 
                (email_id, chunk_index, text, embedding, metadata)
                VALUES %s
                ON CONFLICT DO NOTHING
                """,
                insert_data,
                template="(%s, %s, %s, %s::vector, %s)"
            )
            
            # COMMIT STRATEGY: Commit after each batch for consistency
            # This ensures that if the process fails, we don't lose progress
            # Both short and regular email methods use the same approach
            self.conn.commit()
            return len(emails)
    
    def process_regular_emails(self):
        """Process all remaining emails"""
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT ce.id, ce.subject, ce.body_text, ce.body_html, ce.snippet
                FROM classified_emails ce
                WHERE NOT EXISTS (SELECT 1 FROM email_chunks WHERE email_id = ce.id)
                ORDER BY LENGTH(COALESCE(ce.body_text, '')) ASC  -- Start with smaller ones
                LIMIT %s
            """, (BATCH_SIZE,))
            
            emails = cur.fetchall()
            if not emails:
                return 0
            
            processed_count = 0
            for email in emails:
                try:
                    text = email.get('body_text', '')
                    if not text and email.get('body_html'):
                        text = re.sub('<[^<]+?>', '', email['body_html'])
                    
                    # For very short emails, include subject and snippet
                    if len(text.strip()) < 50:
                        combined_text = f"Subject: {email.get('subject', 'No Subject')}\n"
                        if text.strip():
                            combined_text += f"Body: {text.strip()}\n"
                        if email.get('snippet'):
                            combined_text += f"Preview: {email.get('snippet', '')[:200]}"
                        
                        # Create single chunk for short email
                        embedding = self.model.encode([combined_text], show_progress_bar=False)[0]
                        cur.execute("""
                            INSERT INTO email_chunks 
                            (email_id, chunk_index, text, embedding, metadata)
                            VALUES (%s, %s, %s, %s::vector, %s)
                            ON CONFLICT DO NOTHING
                        """, (
                            email['id'], 0, combined_text[:500],
                            embedding.tolist(),
                            psycopg2.extras.Json({'type': 'short_email'})
                        ))
                        processed_count += 1
                        continue
                    
                    # Truncate very long emails
                    if len(text) > 50000:
                        text = text[:50000] + "... [TRUNCATED]"
                    
                    # Clean text
                    text = re.sub(r'https?://[^\s]+', ' [URL] ', text)
                    text = re.sub(r'\s+', ' ', text)
                    
                    # Simple word-based chunking for longer emails
                    words = text.split()
                    chunks = []
                    current = []
                    size = 0
                    
                    for word in words:
                        if size + len(word) + 1 > 500 and current:
                            chunk_text = ' '.join(current)
                            if len(chunk_text) > 50:
                                chunks.append(chunk_text)
                            current = [word]
                            size = len(word)
                        else:
                            current.append(word)
                            size += len(word) + 1
                    
                    if current:
                        chunk_text = ' '.join(current)
                        if len(chunk_text) > 50:
                            chunks.append(chunk_text)
                    
                    # Limit chunks per email
                    chunks = chunks[:30]
                    
                    if chunks:
                        embeddings = self.model.encode(chunks, batch_size=32, show_progress_bar=False)
                        
                        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
                            metadata = {
                                'email_id': email['id'],
                                'chunk_index': i,
                                'total_chunks': len(chunks)
                            }
                            cur.execute("""
                                INSERT INTO email_chunks 
                                (email_id, chunk_index, text, embedding, metadata)
                                VALUES (%s, %s, %s, %s::vector, %s)
                                ON CONFLICT DO NOTHING
                            """, (
                                email['id'], i, chunk[:500],  # Truncate chunk text
                                embedding.tolist(),
                                psycopg2.extras.Json(metadata)
                            ))
                        
                        processed_count += 1
                    else:
                        # No valid chunks created - mark as processed anyway
                        cur.execute("""
                            INSERT INTO email_chunks 
                            (email_id, chunk_index, text, embedding, metadata)
                            VALUES (%s, 0, %s, %s::vector, %s)
                            ON CONFLICT DO NOTHING
                        """, (
                            email['id'], 
                            'SKIPPED: No valid chunks after processing',
                            [0.0] * 384,
                            psycopg2.extras.Json({'reason': 'no_valid_chunks'})
                        ))
                    
                except Exception as e:
                    # Mark as processed with error
                    try:
                        cur.execute("""
                            INSERT INTO email_chunks 
                            (email_id, chunk_index, text, embedding, metadata)
                            VALUES (%s, 0, %s, %s::vector, %s)
                            ON CONFLICT DO NOTHING
                        """, (
                            email['id'], 
                            f"ERROR: {str(e)[:100]}",
                            [0.0] * 384,
                            psycopg2.extras.Json({'error': str(e)[:200]})
                        ))
                    except Exception as inner_e:
                        # BEST PRACTICE: Never silently swallow exceptions
                        # Log the error with full context for debugging
                        # This error is non-critical - we can continue processing other emails
                        print(f"WARNING: Failed to mark email {email['id']} as error: {inner_e}")
                        print(f"  Original error was: {str(e)[:200]}")
                        # In production, this would go to a proper logging system
                        # For now, print the stack trace for debugging
                        traceback.print_exc()
            
            # COMMIT STRATEGY: Commit after each batch for data integrity
            # This matches the approach in process_short_emails()
            # If an error occurs, we keep the successfully processed emails
            self.conn.commit()
            return processed_count
    
    def run(self):
        """Process all emails"""
        start_time = time.time()
        total_short = 0
        total_regular = 0
        batch = 0
        
        while True:
            batch += 1
            batch_start = time.time()
            
            # Process short emails
            short_count = self.process_short_emails()
            total_short += short_count
            
            # Process regular emails
            regular_count = self.process_regular_emails()
            total_regular += regular_count
            
            if short_count == 0 and regular_count == 0:
                # All emails have been processed successfully
                # Both methods returned 0, meaning no more emails need processing
                break
            
            # Stats
            elapsed = time.time() - start_time
            rate = (total_short + total_regular) / (elapsed / 60)
            
            print(f"[Batch {batch}] Short: {short_count}, Regular: {regular_count} | Total: {total_short + total_regular} | Rate: {rate:.0f}/min")
            
            # Check progress
            with self.conn.cursor() as cur:
                cur.execute("SELECT COUNT(DISTINCT email_id) FROM email_chunks")
                chunked = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM classified_emails")
                total = cur.fetchone()[0]
                print(f"  Progress: {chunked:,}/{total:,} ({chunked/total*100:.1f}%)")
        
        print(f"\nCompleted in {(time.time() - start_time)/60:.1f} minutes")
        print(f"Processed: {total_short} short + {total_regular} regular = {total_short + total_regular} total")
    
    def close(self):
        self.conn.close()

if __name__ == "__main__":
    processor = CompleteProcessor()
    try:
        processor.run()
    except KeyboardInterrupt:
        print("\nStopped by user")
    finally:
        processor.close()