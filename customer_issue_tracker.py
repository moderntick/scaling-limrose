#!/usr/bin/env python3
"""
Customer Issue Tracker
Analyzes emails classified as customer issues, extracts complaint details,
tracks resolutions, and generates fix documentation.
"""

import os
import json
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import logging
from dataclasses import dataclass
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
DB_NAME = os.getenv("DB_NAME", "limrose_email_pipeline")
DB_USER = os.getenv("DB_USER", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")

# LLM Configuration
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "GEMINI")
LLM_API_KEY = os.getenv("LLM_API_KEY")
LLM_MODEL = os.getenv("LLM_MODEL", "gemini-2.0-flash-lite")  # Configurable model

if LLM_PROVIDER == "GEMINI":
    LLM_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{LLM_MODEL}:generateContent?key={LLM_API_KEY}"

@dataclass
class CustomerIssue:
    email_id: int
    thread_id: str
    issue_type: str
    issue_summary: str
    has_resolution: bool
    resolution_summary: Optional[str]
    fix_instructions: Optional[str]
    issue_fingerprint: str
    created_at: datetime


class CustomerIssueTracker:
    def __init__(self):
        """Initialize the customer issue tracking system"""
        self.db_conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, host=DB_HOST
        )
        self.cursor = self.db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.setup_database()
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
        logger.info("✅ Customer Issue Tracker initialized")
        
    def setup_database(self):
        """Create tables for customer issue tracking"""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS customer_issues (
                id SERIAL PRIMARY KEY,
                email_id INTEGER REFERENCES classified_emails(id),
                thread_id VARCHAR(255),
                issue_type VARCHAR(100),
                issue_category VARCHAR(100),
                issue_summary TEXT,
                has_resolution BOOLEAN DEFAULT FALSE,
                resolution_summary TEXT,
                fix_instructions TEXT,
                issue_fingerprint VARCHAR(64) UNIQUE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_customer_issues_email ON customer_issues(email_id);
            CREATE INDEX IF NOT EXISTS idx_customer_issues_thread ON customer_issues(thread_id);
            CREATE INDEX IF NOT EXISTS idx_customer_issues_type ON customer_issues(issue_type);
            CREATE INDEX IF NOT EXISTS idx_customer_issues_category ON customer_issues(issue_category);
            CREATE INDEX IF NOT EXISTS idx_customer_issues_fingerprint ON customer_issues(issue_fingerprint);
        """)
        
        # Table for issue categories and patterns
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS customer_issue_categories (
                id SERIAL PRIMARY KEY,
                category_name VARCHAR(100) UNIQUE,
                description TEXT,
                example_keywords TEXT[],
                occurrence_count INTEGER DEFAULT 0,
                last_seen TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_issue_categories_name ON customer_issue_categories(category_name);
        """)
        
        # Table for tracking fix effectiveness
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS customer_issue_resolutions (
                id SERIAL PRIMARY KEY,
                issue_fingerprint VARCHAR(64),
                fix_effectiveness VARCHAR(20),
                times_applied INTEGER DEFAULT 0,
                success_rate FLOAT,
                last_updated TIMESTAMP DEFAULT NOW(),
                FOREIGN KEY (issue_fingerprint) REFERENCES customer_issues(issue_fingerprint)
            );
        """)
        
        self.db_conn.commit()
        logger.info("✅ Customer issue tracking tables created/verified")
    
    def get_customer_issue_emails(self, batch_size: int = 10) -> List[Dict]:
        """Get emails classified as customer issues that haven't been analyzed"""
        self.cursor.execute("""
            SELECT DISTINCT ce.id, ce.gmail_id, ce.thread_id, ce.subject, 
                   ce.sender_email, ce.body_text, ce.date_sent
            FROM classified_emails ce
            JOIN email_pipeline_routes epr ON ce.id = epr.email_id
            WHERE epr.pipeline_type IN ('customer_issue', 'customer_complaint', 'customer_service_or_feedback')
            AND NOT EXISTS (
                SELECT 1 FROM customer_issues ci WHERE ci.email_id = ce.id
            )
            ORDER BY ce.date_sent DESC
            LIMIT %s
        """, (batch_size,))
        
        return [dict(row) for row in self.cursor.fetchall()]
    
    def analyze_customer_issue(self, email_data: Dict) -> Dict:
        """Analyze a customer issue email using LLM"""
        prompt = f"""Analyze this customer email and extract the following information:

Email Subject: {email_data.get('subject', '')}
From: {email_data.get('sender_email', '')}
Body: {email_data.get('body_text', '')[:3000]}

Please analyze and return a JSON response with:
1. issue_type: The specific type of issue (e.g., "login_problem", "payment_issue", "feature_request", "bug_report", "account_access", "data_loss", "performance_issue", etc.)
2. issue_category: Broader category (e.g., "technical", "billing", "account", "feature", "service")
3. issue_summary: A clear, concise summary of the customer's issue (2-3 sentences)
4. key_details: Important specifics mentioned (account numbers, error messages, timestamps, etc.)
5. customer_sentiment: "frustrated", "neutral", "satisfied", "angry"

Return JSON only in this format:
{{
    "issue_type": "specific_issue_type",
    "issue_category": "broader_category", 
    "issue_summary": "clear summary",
    "key_details": ["detail1", "detail2"],
    "customer_sentiment": "sentiment"
}}"""

        try:
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.2,
                    "topP": 0.95,
                    "maxOutputTokens": 1024,
                    "responseMimeType": "application/json"
                }
            }
            
            response = self.session.post(LLM_API_URL, json=payload, timeout=30)
            response.raise_for_status()
            result = response.json()
            
            analysis = json.loads(result['candidates'][0]['content']['parts'][0]['text'])
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing customer issue: {e}")
            return {
                "issue_type": "unclassified",
                "issue_category": "general",
                "issue_summary": "Error analyzing issue"
            }
    
    def check_thread_for_resolution(self, thread_id: str, issue_summary: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """Check if the email thread contains a resolution"""
        # Get all emails in thread
        self.cursor.execute("""
            SELECT id, subject, body_text, sender_email, date_sent
            FROM classified_emails
            WHERE thread_id = %s
            ORDER BY date_sent ASC
        """, (thread_id,))
        
        thread_emails = self.cursor.fetchall()
        
        if len(thread_emails) < 2:
            return False, None, None
        
        # Concatenate thread for analysis
        thread_text = "\n\n---EMAIL---\n".join([
            f"From: {email['sender_email']}\nDate: {email['date_sent']}\nSubject: {email['subject']}\n{email['body_text'][:1000]}"
            for email in thread_emails
        ])
        
        # Check for resolution
        prompt = f"""Analyze this email thread to determine if a resolution was provided for the customer issue.

Original Issue Summary: {issue_summary}

Email Thread:
{thread_text[:5000]}

Please analyze and return JSON with:
1. has_resolution: true/false - Was a solution or fix provided?
2. resolution_summary: If yes, summarize what solution was offered (2-3 sentences)
3. fix_instructions: If a fix was provided, write clear step-by-step instructions that could help other customers with the same issue
4. resolution_quality: "complete", "partial", "workaround", or "none"

For fix_instructions, format as numbered steps that are clear and actionable.

Return JSON only:
{{
    "has_resolution": true/false,
    "resolution_summary": "summary or null",
    "fix_instructions": "step-by-step instructions or null",
    "resolution_quality": "quality_rating"
}}"""

        try:
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.3,
                    "topP": 0.95,
                    "maxOutputTokens": 2048,
                    "responseMimeType": "application/json"
                }
            }
            
            response = self.session.post(LLM_API_URL, json=payload, timeout=30)
            response.raise_for_status()
            result = response.json()
            
            resolution_data = json.loads(result['candidates'][0]['content']['parts'][0]['text'])
            
            return (
                resolution_data.get('has_resolution', False),
                resolution_data.get('resolution_summary'),
                resolution_data.get('fix_instructions')
            )
            
        except Exception as e:
            logger.error(f"Error checking for resolution: {e}")
            return False, None, None
    
    def create_issue_fingerprint(self, issue_type: str, issue_summary: str) -> str:
        """Create a fingerprint for similar issue detection"""
        # Normalize the content
        normalized = f"{issue_type.lower()}|{issue_summary.lower()}"
        # Remove common words that don't help with uniqueness
        stopwords = {'the', 'a', 'an', 'is', 'it', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for'}
        words = [w for w in normalized.split() if w not in stopwords]
        normalized = ' '.join(sorted(words)[:10])  # Use first 10 meaningful words
        
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()
    
    def save_customer_issue(self, email_id: int, thread_id: str, analysis: Dict, 
                          has_resolution: bool, resolution_summary: Optional[str], 
                          fix_instructions: Optional[str]):
        """Save the analyzed customer issue to database"""
        issue_fingerprint = self.create_issue_fingerprint(
            analysis['issue_type'], 
            analysis['issue_summary']
        )
        
        try:
            # Check if similar issue exists
            self.cursor.execute("""
                SELECT id, fix_instructions FROM customer_issues 
                WHERE issue_fingerprint = %s AND fix_instructions IS NOT NULL
                LIMIT 1
            """, (issue_fingerprint,))
            
            existing = self.cursor.fetchone()
            
            # If similar issue has fix and this doesn't, use existing fix
            if existing and existing['fix_instructions'] and not fix_instructions:
                fix_instructions = existing['fix_instructions']
                logger.info(f"Using existing fix instructions from similar issue #{existing['id']}")
            
            # Insert the issue
            self.cursor.execute("""
                INSERT INTO customer_issues (
                    email_id, thread_id, issue_type, issue_category,
                    issue_summary, has_resolution,
                    resolution_summary, fix_instructions, issue_fingerprint
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (issue_fingerprint) DO UPDATE SET
                    has_resolution = CASE 
                        WHEN customer_issues.has_resolution = false AND EXCLUDED.has_resolution = true 
                        THEN true 
                        ELSE customer_issues.has_resolution 
                    END,
                    resolution_summary = COALESCE(customer_issues.resolution_summary, EXCLUDED.resolution_summary),
                    fix_instructions = COALESCE(customer_issues.fix_instructions, EXCLUDED.fix_instructions),
                    updated_at = NOW()
            """, (
                email_id,
                thread_id,
                analysis.get('issue_type', 'unclassified'),
                analysis.get('issue_category', 'general'),
                analysis.get('issue_summary', ''),
                has_resolution,
                resolution_summary,
                fix_instructions,
                issue_fingerprint
            ))
            
            # Update category statistics
            self.cursor.execute("""
                INSERT INTO customer_issue_categories (category_name, occurrence_count, last_seen)
                VALUES (%s, 1, NOW())
                ON CONFLICT (category_name) DO UPDATE SET
                    occurrence_count = customer_issue_categories.occurrence_count + 1,
                    last_seen = NOW()
            """, (analysis.get('issue_category', 'general'),))
            
            self.db_conn.commit()
            logger.info(f"✅ Saved customer issue for email {email_id}")
            
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error saving customer issue: {e}")
            raise
    
    def process_customer_issues(self, batch_size: int = 10):
        """Main processing loop for customer issues"""
        emails = self.get_customer_issue_emails(batch_size)
        
        if not emails:
            logger.info("No new customer issue emails to process")
            return
        
        logger.info(f"Processing {len(emails)} customer issue emails")
        
        for email in emails:
            try:
                logger.info(f"Analyzing email {email['id']}: {email['subject']}")
                
                # Analyze the issue
                analysis = self.analyze_customer_issue(email)
                
                # Check thread for resolution
                has_resolution = False
                resolution_summary = None
                fix_instructions = None
                
                if email['thread_id']:
                    has_resolution, resolution_summary, fix_instructions = self.check_thread_for_resolution(
                        email['thread_id'], 
                        analysis['issue_summary']
                    )
                
                # Save to database
                self.save_customer_issue(
                    email['id'],
                    email['thread_id'],
                    analysis,
                    has_resolution,
                    resolution_summary,
                    fix_instructions
                )
                
            except Exception as e:
                logger.error(f"Error processing email {email['id']}: {e}")
                continue
    
    def get_issue_statistics(self) -> Dict:
        """Get statistics about customer issues"""
        # Issue type breakdown
        self.cursor.execute("""
            SELECT issue_type, COUNT(*) as count, 
                   SUM(CASE WHEN has_resolution THEN 1 ELSE 0 END) as resolved_count
            FROM customer_issues
            GROUP BY issue_type
            ORDER BY count DESC
        """)
        issue_types = self.cursor.fetchall()
        
        # Category statistics
        self.cursor.execute("""
            SELECT category_name, occurrence_count, last_seen
            FROM customer_issue_categories
            ORDER BY occurrence_count DESC
            LIMIT 10
        """)
        categories = self.cursor.fetchall()
        
        # Resolution rate
        self.cursor.execute("""
            SELECT 
                COUNT(*) as total_issues,
                SUM(CASE WHEN has_resolution THEN 1 ELSE 0 END) as resolved_issues
            FROM customer_issues
        """)
        summary = self.cursor.fetchone()
        
        # Recent issues needing attention
        self.cursor.execute("""
            SELECT ci.id, ci.issue_summary, ce.subject, ce.sender_email
            FROM customer_issues ci
            JOIN classified_emails ce ON ci.email_id = ce.id
            WHERE ci.has_resolution = false
            ORDER BY ci.created_at DESC
            LIMIT 10
        """)
        unresolved_issues = self.cursor.fetchall()
        
        return {
            'summary': dict(summary),
            'issue_types': [dict(row) for row in issue_types],
            'top_categories': [dict(row) for row in categories],
            'unresolved_issues': [dict(row) for row in unresolved_issues],
            'resolution_rate': (summary['resolved_issues'] / summary['total_issues'] * 100) if summary['total_issues'] > 0 else 0
        }
    
    def export_fix_documentation(self, output_file: str = "customer_fixes.json"):
        """Export all documented fixes for customer self-service"""
        self.cursor.execute("""
            SELECT DISTINCT issue_type, issue_category, issue_summary, 
                   fix_instructions
            FROM customer_issues
            WHERE fix_instructions IS NOT NULL
            ORDER BY issue_category, issue_type
        """)
        
        fixes = []
        for row in self.cursor.fetchall():
            fixes.append({
                'type': row['issue_type'],
                'category': row['issue_category'],
                'problem': row['issue_summary'],
                'solution': row['fix_instructions']
            })
        
        with open(output_file, 'w') as f:
            json.dump({
                'generated_at': datetime.now().isoformat(),
                'total_fixes': len(fixes),
                'fixes': fixes
            }, f, indent=2)
        
        logger.info(f"✅ Exported {len(fixes)} fix instructions to {output_file}")
        return output_file


def main():
    """Run customer issue tracking"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Track and analyze customer issues')
    parser.add_argument('--batch-size', type=int, default=10, help='Number of emails to process')
    parser.add_argument('--stats', action='store_true', help='Show statistics only')
    parser.add_argument('--export-fixes', action='store_true', help='Export fix documentation')
    args = parser.parse_args()
    
    tracker = CustomerIssueTracker()
    
    if args.stats:
        stats = tracker.get_issue_statistics()
        print("\n📊 Customer Issue Statistics")
        print("=" * 50)
        print(f"Total Issues: {stats['summary']['total_issues']}")
        print(f"Resolved: {stats['summary']['resolved_issues']} ({stats['resolution_rate']:.1f}%)")
        
        print("\n🏷️ Top Issue Types:")
        for issue in stats['issue_types'][:10]:
            print(f"  {issue['issue_type']}: {issue['count']} ({issue['resolved_count']} resolved)")
        
        if stats['unresolved_issues']:
            print("\n❓ Recent Unresolved Issues:")
            for issue in stats['unresolved_issues']:
                print(f"  {issue['issue_summary'][:80]}...")
                print(f"    From: {issue['sender_email']}")
    
    elif args.export_fixes:
        output_file = tracker.export_fix_documentation()
        print(f"✅ Fix documentation exported to {output_file}")
    
    else:
        tracker.process_customer_issues(args.batch_size)
        # Show brief stats after processing
        stats = tracker.get_issue_statistics()
        print(f"\n✅ Processed customer issues. Total: {stats['summary']['total_issues']}, Resolution rate: {stats['resolution_rate']:.1f}%")


if __name__ == "__main__":
    main()