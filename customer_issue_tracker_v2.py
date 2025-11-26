#!/usr/bin/env python3
"""
Customer Issue Tracker V2 - With Semantic Vector Search
Analyzes emails classified as customer issues, finds similar past issues,
tracks resolutions, and generates fix documentation using vector similarity.
"""

import os
import json
import psycopg2
import psycopg2.extras
import requests
import numpy as np
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import logging
from dataclasses import dataclass
from sentence_transformers import SentenceTransformer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
DB_NAME = os.getenv("DB_NAME", "email_pipeline")
DB_USER = os.getenv("DB_USER", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
EMBEDDING_MODEL = 'sentence-transformers/all-MiniLM-L6-v2'

# LLM Configuration
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "GEMINI")
LLM_API_KEY = os.getenv("LLM_API_KEY")
if LLM_PROVIDER == "GEMINI":
    LLM_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent?key={LLM_API_KEY}"

@dataclass
class CustomerIssue:
    email_id: int
    thread_id: str
    issue_type: str
    issue_summary: str
    has_resolution: bool
    resolution_summary: Optional[str]
    fix_instructions: Optional[str]
    issue_embedding: Optional[np.ndarray]
    created_at: datetime


class CustomerIssueTrackerV2:
    def __init__(self):
        """Initialize the customer issue tracking system with vector search"""
        self.db_conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, host=DB_HOST
        )
        self.cursor = self.db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Initialize embedding model
        logger.info("Loading embedding model...")
        if os.environ.get('HF_HUB_OFFLINE') == '1':
            snapshot_path = os.path.expanduser("~/.cache/huggingface/hub/models--sentence-transformers--all-MiniLM-L6-v2/snapshots/c9745ed1d9f207416be6d2e6f8de32d1f16199bf")
            if os.path.exists(snapshot_path):
                self.embedding_model = SentenceTransformer(snapshot_path, device='cpu')
            else:
                self.embedding_model = SentenceTransformer(EMBEDDING_MODEL, device='cpu', local_files_only=True)
        else:
            self.embedding_model = SentenceTransformer(EMBEDDING_MODEL, device='cpu')
        
        self.setup_database()
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
        logger.info("✅ Customer Issue Tracker V2 initialized with vector search")
        
    def setup_database(self):
        """Create tables for customer issue tracking with vector support"""
        # Main customer issues table with embeddings
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS customer_issues_v2 (
                id SERIAL PRIMARY KEY,
                email_id INTEGER REFERENCES classified_emails(id),
                thread_id VARCHAR(255),
                issue_type VARCHAR(100),
                issue_category VARCHAR(100),
                issue_summary TEXT,
                has_resolution BOOLEAN DEFAULT FALSE,
                resolution_summary TEXT,
                fix_instructions TEXT,
                issue_embedding VECTOR(384),
                resolution_embedding VECTOR(384),
                similarity_score FLOAT,
                based_on_issues INTEGER[],
                confidence_level VARCHAR(20),
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_customer_issues_v2_email ON customer_issues_v2(email_id);
            CREATE INDEX IF NOT EXISTS idx_customer_issues_v2_thread ON customer_issues_v2(thread_id);
            CREATE INDEX IF NOT EXISTS idx_customer_issues_v2_type ON customer_issues_v2(issue_type);
            CREATE INDEX IF NOT EXISTS idx_customer_issues_v2_category ON customer_issues_v2(issue_category);
            
            -- Vector similarity index for issue embeddings
            CREATE INDEX IF NOT EXISTS idx_customer_issues_v2_embedding 
            ON customer_issues_v2 
            USING hnsw (issue_embedding vector_cosine_ops)
            WITH (m = 16, ef_construction = 64);
        """)
        
        # Resolution effectiveness tracking
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS resolution_feedback (
                id SERIAL PRIMARY KEY,
                issue_id INTEGER REFERENCES customer_issues_v2(id),
                was_effective BOOLEAN,
                feedback_text TEXT,
                feedback_date TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_resolution_feedback_issue ON resolution_feedback(issue_id);
        """)
        
        # Issue similarity cache
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS issue_similarity_cache (
                id SERIAL PRIMARY KEY,
                source_issue_id INTEGER REFERENCES customer_issues_v2(id),
                similar_issue_id INTEGER REFERENCES customer_issues_v2(id),
                similarity_score FLOAT,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(source_issue_id, similar_issue_id)
            );
            
            CREATE INDEX IF NOT EXISTS idx_similarity_cache_source ON issue_similarity_cache(source_issue_id);
        """)
        
        self.db_conn.commit()
        logger.info("✅ Customer issue tracking tables with vector support created/verified")
    
    def get_customer_issue_emails(self, batch_size: int = 10) -> List[Dict]:
        """Get emails classified as customer issues that haven't been analyzed"""
        self.cursor.execute("""
            SELECT DISTINCT ce.id, ce.gmail_id, ce.thread_id, ce.subject, 
                   ce.sender_email, ce.body_text, ce.date_sent
            FROM classified_emails ce
            JOIN email_pipeline_routes epr ON ce.id = epr.email_id
            WHERE epr.pipeline_type IN ('customer_issue', 'customer_complaint', 'customer_service_or_feedback')
            AND NOT EXISTS (
                SELECT 1 FROM customer_issues_v2 ci WHERE ci.email_id = ce.id
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
            
            # Create embedding for the issue
            issue_text = f"{analysis['issue_type']}: {analysis['issue_summary']}"
            issue_embedding = self.embedding_model.encode(issue_text)
            analysis['issue_embedding'] = issue_embedding
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing customer issue: {e}")
            return {
                "issue_type": "unclassified",
                "issue_category": "general",
                "issue_summary": "Error analyzing issue",
                "issue_embedding": self.embedding_model.encode("unclassified issue")
            }
    
    def find_similar_resolved_issues(self, issue_embedding: np.ndarray, threshold: float = 0.75) -> List[Dict]:
        """Find similar issues that have been resolved"""
        
        self.cursor.execute("""
            SELECT 
                id,
                issue_type,
                issue_summary,
                resolution_summary,
                fix_instructions,
                1 - (issue_embedding <=> %s::vector) as similarity
            FROM customer_issues_v2
            WHERE fix_instructions IS NOT NULL
            AND 1 - (issue_embedding <=> %s::vector) > %s
            ORDER BY issue_embedding <=> %s::vector
            LIMIT 5
        """, (issue_embedding.tolist(), issue_embedding.tolist(), threshold, issue_embedding.tolist()))
        
        return [dict(row) for row in self.cursor.fetchall()]
    
    def suggest_resolution(self, issue_analysis: Dict) -> Optional[Dict]:
        """Suggest resolution based on similar past issues"""
        
        similar_issues = self.find_similar_resolved_issues(
            issue_analysis['issue_embedding'],
            threshold=0.70  # Lower threshold to find more potential matches
        )
        
        if not similar_issues:
            logger.info("No similar resolved issues found")
            return None
        
        logger.info(f"Found {len(similar_issues)} similar issues, best match: {similar_issues[0]['similarity']:.2f}")
        
        # If very high similarity, use existing fix directly
        if similar_issues[0]['similarity'] > 0.92:
            return {
                'fix_instructions': similar_issues[0]['fix_instructions'],
                'confidence': 'high',
                'based_on_issue': similar_issues[0]['issue_summary'],
                'similarity': similar_issues[0]['similarity']
            }
        
        # For moderate similarity, synthesize fix from multiple similar issues
        if len(similar_issues) >= 2 and similar_issues[0]['similarity'] > 0.80:
            return self.synthesize_fix_from_similar(similar_issues, issue_analysis)
        
        # For lower similarity, just provide as reference
        if similar_issues[0]['similarity'] > 0.70:
            return {
                'fix_instructions': similar_issues[0]['fix_instructions'],
                'confidence': 'low',
                'based_on_issue': similar_issues[0]['issue_summary'],
                'similarity': similar_issues[0]['similarity'],
                'note': 'This fix is from a somewhat similar issue and may need adaptation'
            }
        
        return None
    
    def synthesize_fix_from_similar(self, similar_issues: List[Dict], current_issue: Dict) -> Dict:
        """Use LLM to synthesize fix from similar issues"""
        
        prompt = f"""
Current customer issue: {current_issue['issue_summary']}

Here are similar issues and their resolutions:

{chr(10).join([
    f"Issue: {issue['issue_summary']} (similarity: {issue['similarity']:.2f})\n"
    f"Fix: {issue['fix_instructions']}\n"
    for issue in similar_issues[:3]
])}

Based on these similar issues, provide the best fix instructions for the current issue.
Adapt the solutions as needed to match the specific problem.

Return JSON:
{{
    "fix_instructions": "step by step instructions",
    "confidence_note": "any caveats about this fix"
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
            
            synthesis = json.loads(result['candidates'][0]['content']['parts'][0]['text'])
            
            return {
                'fix_instructions': synthesis['fix_instructions'],
                'confidence': 'medium',
                'synthesized_from': [issue['id'] for issue in similar_issues[:3]],
                'similarity_scores': [issue['similarity'] for issue in similar_issues[:3]],
                'note': synthesis.get('confidence_note', '')
            }
            
        except Exception as e:
            logger.error(f"Error synthesizing fix: {e}")
            # Fallback to best match
            return {
                'fix_instructions': similar_issues[0]['fix_instructions'],
                'confidence': 'low',
                'based_on_issue': similar_issues[0]['issue_summary'],
                'similarity': similar_issues[0]['similarity']
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
    
    def save_customer_issue(self, email_id: int, thread_id: str, analysis: Dict, 
                          has_resolution: bool, resolution_summary: Optional[str], 
                          fix_instructions: Optional[str], suggested_resolution: Optional[Dict] = None):
        """Save the analyzed customer issue to database with embeddings"""
        
        try:
            # Prepare embedding
            issue_embedding = analysis.get('issue_embedding')
            if issue_embedding is None:
                issue_text = f"{analysis['issue_type']}: {analysis['issue_summary']}"
                issue_embedding = self.embedding_model.encode(issue_text)
            
            # Create resolution embedding if we have fix instructions
            resolution_embedding = None
            if fix_instructions:
                resolution_embedding = self.embedding_model.encode(fix_instructions)
            
            # Track what this resolution was based on
            based_on_issues = None
            confidence_level = 'direct'  # Direct from email thread
            similarity_score = None
            
            if suggested_resolution and not has_resolution:
                # We're using a suggested resolution
                confidence_level = suggested_resolution.get('confidence', 'medium')
                similarity_score = suggested_resolution.get('similarity')
                if 'synthesized_from' in suggested_resolution:
                    based_on_issues = suggested_resolution['synthesized_from']
                elif 'based_on_issue' in suggested_resolution:
                    # Find the ID of the similar issue
                    self.cursor.execute("""
                        SELECT id FROM customer_issues_v2 
                        WHERE issue_summary = %s 
                        LIMIT 1
                    """, (suggested_resolution['based_on_issue'],))
                    result = self.cursor.fetchone()
                    if result:
                        based_on_issues = [result[0]]
            
            # Insert the issue
            self.cursor.execute("""
                INSERT INTO customer_issues_v2 (
                    email_id, thread_id, issue_type, issue_category,
                    issue_summary, has_resolution,
                    resolution_summary, fix_instructions, 
                    issue_embedding, resolution_embedding,
                    similarity_score, based_on_issues, confidence_level
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                email_id,
                thread_id,
                analysis.get('issue_type', 'unclassified'),
                analysis.get('issue_category', 'general'),
                analysis.get('issue_summary', ''),
                has_resolution or (fix_instructions is not None),
                resolution_summary,
                fix_instructions,
                issue_embedding.tolist() if issue_embedding is not None else None,
                resolution_embedding.tolist() if resolution_embedding is not None else None,
                similarity_score,
                based_on_issues,
                confidence_level
            ))
            
            # Get the new issue ID
            result = self.cursor.fetchone()
            new_issue_id = result[0] if result else None
            
            self.db_conn.commit()
            logger.info(f"✅ Saved customer issue for email {email_id} (confidence: {confidence_level})")
            
            # Cache similar issues for faster lookups
            if suggested_resolution and based_on_issues and new_issue_id:
                for similar_id in based_on_issues[:3]:  # Cache top 3
                    try:
                        self.cursor.execute("""
                            INSERT INTO issue_similarity_cache 
                            (source_issue_id, similar_issue_id, similarity_score)
                            VALUES (%s, %s, %s)
                            ON CONFLICT DO NOTHING
                        """, (new_issue_id, similar_id, similarity_score))
                    except:
                        pass
                self.db_conn.commit()
            
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error saving customer issue: {e}")
            raise
    
    def process_customer_issues(self, batch_size: int = 10):
        """Main processing loop for customer issues with vector similarity"""
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
                
                # First, try to find a similar resolved issue
                suggested_resolution = self.suggest_resolution(analysis)
                
                # Check thread for actual resolution
                has_resolution = False
                resolution_summary = None
                fix_instructions = None
                
                if email['thread_id']:
                    has_resolution, resolution_summary, fix_instructions = self.check_thread_for_resolution(
                        email['thread_id'], 
                        analysis['issue_summary']
                    )
                
                # If no resolution in thread but we have a good suggestion, use it
                if not fix_instructions and suggested_resolution:
                    logger.info(f"Using suggested resolution with confidence: {suggested_resolution['confidence']}")
                    fix_instructions = suggested_resolution['fix_instructions']
                    if not resolution_summary:
                        resolution_summary = f"Suggested based on similar issues (confidence: {suggested_resolution['confidence']})"
                
                # Save to database
                self.save_customer_issue(
                    email['id'],
                    email['thread_id'],
                    analysis,
                    has_resolution,
                    resolution_summary,
                    fix_instructions,
                    suggested_resolution
                )
                
            except Exception as e:
                logger.error(f"Error processing email {email['id']}: {e}")
                continue
    
    def track_resolution_effectiveness(self, issue_id: int, was_effective: bool, feedback: str = None):
        """Track if suggested resolutions actually worked"""
        
        self.cursor.execute("""
            INSERT INTO resolution_feedback (issue_id, was_effective, feedback_text)
            VALUES (%s, %s, %s)
        """, (issue_id, was_effective, feedback))
        
        self.db_conn.commit()
        logger.info(f"Tracked feedback for issue {issue_id}: {'Effective' if was_effective else 'Not effective'}")
    
    def get_issue_statistics(self) -> Dict:
        """Get statistics about customer issues"""
        # Overall stats
        self.cursor.execute("""
            SELECT 
                COUNT(*) as total_issues,
                SUM(CASE WHEN has_resolution THEN 1 ELSE 0 END) as resolved_issues,
                SUM(CASE WHEN confidence_level = 'high' THEN 1 ELSE 0 END) as high_confidence,
                SUM(CASE WHEN confidence_level = 'medium' THEN 1 ELSE 0 END) as medium_confidence,
                SUM(CASE WHEN confidence_level = 'low' THEN 1 ELSE 0 END) as low_confidence,
                AVG(similarity_score) as avg_similarity
            FROM customer_issues_v2
        """)
        summary = self.cursor.fetchone()
        
        # Issue types with resolution rates
        self.cursor.execute("""
            SELECT 
                issue_type, 
                COUNT(*) as count,
                SUM(CASE WHEN has_resolution THEN 1 ELSE 0 END) as resolved_count,
                AVG(similarity_score) as avg_similarity
            FROM customer_issues_v2
            GROUP BY issue_type
            ORDER BY count DESC
            LIMIT 10
        """)
        issue_types = self.cursor.fetchall()
        
        # Effectiveness of suggested resolutions
        self.cursor.execute("""
            SELECT 
                ci.confidence_level,
                COUNT(rf.id) as feedback_count,
                SUM(CASE WHEN rf.was_effective THEN 1 ELSE 0 END) as effective_count,
                ROUND(100.0 * SUM(CASE WHEN rf.was_effective THEN 1 ELSE 0 END) / COUNT(rf.id), 1) as effectiveness_rate
            FROM customer_issues_v2 ci
            JOIN resolution_feedback rf ON ci.id = rf.issue_id
            GROUP BY ci.confidence_level
        """)
        effectiveness = self.cursor.fetchall()
        
        # Recent unresolved issues
        self.cursor.execute("""
            SELECT ci.id, ci.issue_summary, ce.subject, ce.sender_email, ci.created_at
            FROM customer_issues_v2 ci
            JOIN classified_emails ce ON ci.email_id = ce.id
            WHERE ci.has_resolution = false
            ORDER BY ci.created_at DESC
            LIMIT 10
        """)
        unresolved_issues = self.cursor.fetchall()
        
        return {
            'summary': dict(summary),
            'issue_types': [dict(row) for row in issue_types],
            'resolution_effectiveness': [dict(row) for row in effectiveness],
            'unresolved_issues': [dict(row) for row in unresolved_issues],
            'resolution_rate': (summary['resolved_issues'] / summary['total_issues'] * 100) if summary['total_issues'] > 0 else 0
        }
    
    def export_fix_documentation(self, output_file: str = "customer_fixes_v2.json"):
        """Export documented fixes with confidence levels"""
        self.cursor.execute("""
            SELECT DISTINCT 
                issue_type, 
                issue_category, 
                issue_summary, 
                fix_instructions,
                confidence_level,
                similarity_score
            FROM customer_issues_v2
            WHERE fix_instructions IS NOT NULL
            ORDER BY confidence_level DESC, similarity_score DESC NULLS LAST
        """)
        
        fixes = []
        for row in self.cursor.fetchall():
            fixes.append({
                'type': row['issue_type'],
                'category': row['issue_category'],
                'problem': row['issue_summary'],
                'solution': row['fix_instructions'],
                'confidence': row['confidence_level'],
                'similarity_score': float(row['similarity_score']) if row['similarity_score'] else None
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
    
    parser = argparse.ArgumentParser(description='Track and analyze customer issues with vector similarity')
    parser.add_argument('--batch-size', type=int, default=10, help='Number of emails to process')
    parser.add_argument('--stats', action='store_true', help='Show statistics only')
    parser.add_argument('--export-fixes', action='store_true', help='Export fix documentation')
    parser.add_argument('--test-similarity', type=int, help='Test similarity search for a specific issue ID')
    args = parser.parse_args()
    
    tracker = CustomerIssueTrackerV2()
    
    if args.stats:
        stats = tracker.get_issue_statistics()
        print("\n📊 Customer Issue Statistics (Vector-Based)")
        print("=" * 60)
        print(f"Total Issues: {stats['summary']['total_issues']}")
        print(f"Resolved: {stats['summary']['resolved_issues']} ({stats['resolution_rate']:.1f}%)")
        print(f"Avg Similarity Score: {stats['summary']['avg_similarity']:.3f}" if stats['summary']['avg_similarity'] else "N/A")
        
        print(f"\n📈 Resolution Confidence:")
        print(f"  High: {stats['summary']['high_confidence']}")
        print(f"  Medium: {stats['summary']['medium_confidence']}")
        print(f"  Low: {stats['summary']['low_confidence']}")
        
        print("\n🏷️ Top Issue Types:")
        for issue in stats['issue_types'][:10]:
            print(f"  {issue['issue_type']}: {issue['count']} ({issue['resolved_count']} resolved)")
        
        if stats['resolution_effectiveness']:
            print("\n✅ Resolution Effectiveness:")
            for eff in stats['resolution_effectiveness']:
                print(f"  {eff['confidence_level']}: {eff['effectiveness_rate']}% effective ({eff['feedback_count']} feedbacks)")
        
        if stats['unresolved_issues']:
            print("\n❓ Recent Unresolved Issues:")
            for issue in stats['unresolved_issues']:
                print(f"  {issue['issue_summary'][:80]}...")
                print(f"    From: {issue['sender_email']}")
    
    elif args.export_fixes:
        output_file = tracker.export_fix_documentation()
        print(f"✅ Fix documentation exported to {output_file}")
    
    elif args.test_similarity:
        # Test similarity search for a specific issue
        tracker.cursor.execute("""
            SELECT issue_embedding, issue_summary 
            FROM customer_issues_v2 
            WHERE id = %s
        """, (args.test_similarity,))
        result = tracker.cursor.fetchone()
        
        if result and result['issue_embedding']:
            print(f"\nTesting similarity for: {result['issue_summary']}")
            similar = tracker.find_similar_resolved_issues(np.array(result['issue_embedding']), threshold=0.5)
            print(f"\nFound {len(similar)} similar issues:")
            for s in similar:
                print(f"  Similarity: {s['similarity']:.3f} - {s['issue_summary'][:80]}...")
                if s['fix_instructions']:
                    print(f"    Fix: {s['fix_instructions'][:100]}...")
        else:
            print("Issue not found or has no embedding")
    
    else:
        tracker.process_customer_issues(args.batch_size)
        # Show brief stats after processing
        stats = tracker.get_issue_statistics()
        print(f"\n✅ Processed customer issues. Total: {stats['summary']['total_issues']}, Resolution rate: {stats['resolution_rate']:.1f}%")


if __name__ == "__main__":
    main()