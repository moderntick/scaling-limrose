#!/usr/bin/env python3
"""
Email Pipeline Router
Multi-classification system that routes emails to appropriate pipelines
"""

import os
import json
from decimal import Decimal
import re
from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple, Optional
import psycopg2
from sentence_transformers import SentenceTransformer
from dataclasses import dataclass
from enum import Enum

# Use same embedding model as article pipeline

class DateTimeJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime and Decimal objects"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

EMBEDDING_MODEL_NAME = 'sentence-transformers/all-MiniLM-L6-v2'

@dataclass
class EmailClassification:
    """Email classification result"""
    email_id: str
    classifications: Set[str]
    confidence_scores: Dict[str, float]
    pipeline_routes: List[str]
    priority_score: float
    requires_human_review: bool

class PipelineType(Enum):
    """Available pipeline types"""
    STORY_PIPELINE = "story_pipeline"
    SALES_PIPELINE = "sales_pipeline"
    PRESS_RELEASE_PIPELINE = "press_release_pipeline"
    COMPLAINT_PIPELINE = "complaint_pipeline"
    EDITOR_FEEDBACK_PIPELINE = "editor_feedback_pipeline"
    EXTERNAL_MARKETING_PIPELINE = "external_marketing_pipeline"
    SPAM_FILTER = "spam_filter"
    ARCHIVE_PIPELINE = "archive_pipeline"

class EmailPipelineRouter:
    """Routes emails to appropriate pipelines based on multi-classification"""
    
    def __init__(self):
        # Check if we're in offline mode
        local_files_only = os.environ.get('HF_HUB_OFFLINE') == '1'
        
        # Load model with explicit local_files_only flag for sentence-transformers v4.x
        if local_files_only:
            # Try loading from the exact snapshot path
            snapshot_path = os.path.expanduser("~/.cache/huggingface/hub/models--sentence-transformers--all-MiniLM-L6-v2/snapshots/c9745ed1d9f207416be6d2e6f8de32d1f16199bf")
            if os.path.exists(snapshot_path):
                self.model = SentenceTransformer(snapshot_path, device='cpu')
            else:
                # Fallback
                self.model = SentenceTransformer(EMBEDDING_MODEL_NAME, device='cpu', local_files_only=True)
        else:
            self.model = SentenceTransformer(EMBEDDING_MODEL_NAME)
        self.db_conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME', 'limrose_email_pipeline'),
            user=os.getenv('DB_USER', 'postgres'),
            host=os.getenv('DB_HOST', 'localhost')
        )
        self.setup_database()
        self.classification_patterns = self._load_classification_patterns()
        
    def setup_database(self):
        """Create email routing tables"""
        cursor = self.db_conn.cursor()
        
        # Multi-classification table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS email_classifications (
                id SERIAL PRIMARY KEY,
                email_id INTEGER REFERENCES classified_emails(id),
                classification_type VARCHAR(50),
                confidence_score FLOAT,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(email_id, classification_type)
            );
            
            CREATE INDEX IF NOT EXISTS idx_email_classifications_email ON email_classifications(email_id);
            CREATE INDEX IF NOT EXISTS idx_email_classifications_type ON email_classifications(classification_type);
        """)
        
        # Pipeline routing table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS email_pipeline_routes (
                id SERIAL PRIMARY KEY,
                email_id INTEGER REFERENCES classified_emails(id),
                pipeline_type VARCHAR(50),
                priority_score FLOAT,
                status VARCHAR(20) DEFAULT 'pending',
                assigned_to VARCHAR(255),
                processing_notes TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_pipeline_routes_email ON email_pipeline_routes(email_id);
            CREATE INDEX IF NOT EXISTS idx_pipeline_routes_type ON email_pipeline_routes(pipeline_type);
            CREATE INDEX IF NOT EXISTS idx_pipeline_routes_status ON email_pipeline_routes(status);
        """)
        
        # Pipeline outcomes tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_outcomes (
                id SERIAL PRIMARY KEY,
                email_id INTEGER REFERENCES classified_emails(id),
                pipeline_type VARCHAR(50),
                outcome_type VARCHAR(50), -- story_published, sale_closed, meeting_scheduled, etc.
                outcome_details JSONB,
                revenue_generated DECIMAL(10,2),
                articles_published INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_pipeline_outcomes_email ON pipeline_outcomes(email_id);
            CREATE INDEX IF NOT EXISTS idx_pipeline_outcomes_type ON pipeline_outcomes(outcome_type);
        """)
        
        # Classification performance tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS classification_performance (
                id SERIAL PRIMARY KEY,
                classification_type VARCHAR(50),
                true_positives INTEGER DEFAULT 0,
                false_positives INTEGER DEFAULT 0,
                false_negatives INTEGER DEFAULT 0,
                precision_score FLOAT,
                recall_score FLOAT,
                f1_score FLOAT,
                last_updated TIMESTAMP DEFAULT NOW()
            );
        """)
        
        self.db_conn.commit()
    
    def _load_classification_patterns(self) -> Dict:
        """Load classification patterns with confidence scoring"""
        return {
            'story_pipeline': {
                'keywords': [
                    'story', 'article', 'pitch', 'breaking', 'news', 'investigation',
                    'interview', 'feature', 'exclusive', 'scoop', 'reporting'
                ],
                'weighted_keywords': {
                    'breaking': 2.0,
                    'exclusive': 1.8,
                    'investigation': 1.6,
                    'story': 1.4,
                    'pitch': 1.2
                },
                'domain_patterns': [r'.*\.gov$', r'.*\.edu$', r'.*media.*'],
                'subject_patterns': [r'story.*', r'article.*', r'pitch.*', r'news.*'],
                'threshold': 0.4
            },
            
            'sales_pipeline': {
                'keywords': [
                    'advertising', 'sponsor', 'partnership', 'marketing', 'rates',
                    'media kit', 'business', 'revenue', 'budget', 'campaign'
                ],
                'weighted_keywords': {
                    'advertising': 2.0,
                    'sponsor': 1.8,
                    'rates': 1.6,
                    'media kit': 1.4,
                    'partnership': 1.2
                },
                'domain_patterns': [r'.*marketing.*', r'.*agency.*', r'.*media.*'],
                'subject_patterns': [r'advertising.*', r'sponsor.*', r'partnership.*'],
                'threshold': 0.3
            },
            
            'press_release_pipeline': {
                'keywords': [
                    'press release', 'announcement', 'embargo', 'media contact',
                    'for immediate release', 'media advisory', 'statement'
                ],
                'weighted_keywords': {
                    'press release': 2.0,
                    'for immediate release': 1.8,
                    'embargo': 1.6,
                    'announcement': 1.4,
                    'media advisory': 1.2
                },
                'domain_patterns': [r'.*\.gov$', r'.*\.org$', r'.*pr\..*'],
                'subject_patterns': [r'press.*release.*', r'announcement.*', r'media.*advisory.*'],
                'threshold': 0.5
            },
            
            'external_marketing_pipeline': {
                'keywords': [
                    'newsletter', 'unsubscribe', 'email preferences', 'marketing',
                    'weekly update', 'monthly digest', 'industry news', 'trends',
                    'best practices', 'case study', 'whitepaper', 'webinar',
                    'roundup', 'digest', 'bulletin'
                ],
                'weighted_keywords': {
                    'unsubscribe': 2.0,
                    'newsletter': 1.8,
                    'email preferences': 1.6,
                    'weekly update': 1.4,
                    'monthly digest': 1.4,
                    'roundup': 1.2
                },
                'domain_patterns': [
                    r'.*mailchimp.*', r'.*constantcontact.*', r'.*campaign.*',
                    r'.*list-manage.*', r'.*sendgrid.*'
                ],
                'subject_patterns': [
                    r'.*newsletter.*', r'.*digest.*', r'.*roundup.*', 
                    r'.*weekly.*', r'.*monthly.*', r'.*update.*'
                ],
                'threshold': 0.35
            },
            
            'complaint_pipeline': {
                'keywords': [
                    'complaint', 'error', 'correction', 'wrong', 'inaccurate',
                    'disappointed', 'unhappy', 'issue', 'problem', 'concern'
                ],
                'weighted_keywords': {
                    'complaint': 2.0,
                    'correction': 1.8,
                    'error': 1.6,
                    'inaccurate': 1.4,
                    'wrong': 1.2
                },
                'domain_patterns': [],
                'subject_patterns': [r'complaint.*', r'error.*', r'correction.*'],
                'threshold': 0.4
            },
            
            'editor_feedback_pipeline': {
                'keywords': [
                    'edit', 'revision', 'feedback', 'draft', 'review', 'changes',
                    'suggestions', 'comments', 'version', 'update'
                ],
                'weighted_keywords': {
                    'edit': 1.8,
                    'revision': 1.6,
                    'feedback': 1.4,
                    'draft': 1.2,
                    'review': 1.0
                },
                'domain_patterns': [],
                'subject_patterns': [r'edit.*', r'revision.*', r'feedback.*', r'draft.*'],
                'threshold': 0.4
            },
            
            'spam_filter': {
                'keywords': [
                    'unsubscribe', 'click here', 'limited time', 'act now',
                    'free', 'guarantee', 'winner', 'congratulations'
                ],
                'weighted_keywords': {
                    'unsubscribe': 2.0,
                    'click here': 1.8,
                    'limited time': 1.6,
                    'act now': 1.4,
                    'free': 1.2
                },
                'domain_patterns': [r'.*noreply.*', r'.*marketing.*'],
                'subject_patterns': [r'unsubscribe.*', r'free.*', r'winner.*'],
                'threshold': 0.6
            }
        }
    
    def classify_email(self, email_data: Dict) -> EmailClassification:
        """Multi-classify email and determine pipeline routes"""
        email_text = self._prepare_email_text(email_data)
        
        # Get all classifications with confidence scores
        classifications = {}
        for classification_type, patterns in self.classification_patterns.items():
            score = self._calculate_classification_score(email_text, email_data, patterns)
            if score >= patterns['threshold']:
                classifications[classification_type] = score
        
        # Determine pipeline routes
        pipeline_routes = self._determine_pipeline_routes(classifications)
        
        # Calculate priority score
        priority_score = self._calculate_priority_score(classifications, email_data)
        
        # Determine if human review is needed
        requires_human_review = self._requires_human_review(classifications, email_data)
        
        return EmailClassification(
            email_id=email_data['gmail_id'],
            classifications=set(classifications.keys()),
            confidence_scores=classifications,
            pipeline_routes=pipeline_routes,
            priority_score=priority_score,
            requires_human_review=requires_human_review
        )
    
    def _prepare_email_text(self, email_data: Dict) -> str:
        """Prepare email text for classification"""
        subject = email_data.get('subject', '')
        body = email_data.get('body_text', '')
        sender = email_data.get('sender_email', '')
        
        return f"Subject: {subject}\nFrom: {sender}\n\n{body}".lower()
    
    def _calculate_classification_score(self, email_text: str, email_data: Dict, patterns: Dict) -> float:
        """Calculate classification confidence score"""
        score = 0.0
        
        # Keyword matching with weights
        for keyword, weight in patterns['weighted_keywords'].items():
            if keyword in email_text:
                score += weight
        
        # Regular keyword matching
        for keyword in patterns['keywords']:
            if keyword in email_text and keyword not in patterns['weighted_keywords']:
                score += 0.5
        
        # Domain pattern matching
        sender_email = email_data.get('sender_email', '')
        for pattern in patterns['domain_patterns']:
            if re.search(pattern, sender_email):
                score += 1.0
        
        # Subject pattern matching
        subject = email_data.get('subject', '').lower()
        for pattern in patterns['subject_patterns']:
            if re.search(pattern, subject):
                score += 1.5
        
        # Normalize score (0-1 range)
        max_possible_score = (
            sum(patterns['weighted_keywords'].values()) +
            len(patterns['keywords']) * 0.5 +
            len(patterns['domain_patterns']) * 1.0 +
            len(patterns['subject_patterns']) * 1.5
        )
        
        return min(score / max_possible_score, 1.0) if max_possible_score > 0 else 0.0
    
    def _determine_pipeline_routes(self, classifications: Dict[str, float]) -> List[str]:
        """Determine which pipelines email should be routed to"""
        routes = []
        
        # Direct mapping from classifications to pipelines
        classification_to_pipeline = {
            'story_pipeline': PipelineType.STORY_PIPELINE.value,
            'sales_pipeline': PipelineType.SALES_PIPELINE.value,
            'press_release_pipeline': PipelineType.PRESS_RELEASE_PIPELINE.value,
            'complaint_pipeline': PipelineType.COMPLAINT_PIPELINE.value,
            'editor_feedback_pipeline': PipelineType.EDITOR_FEEDBACK_PIPELINE.value,
            'external_marketing_pipeline': PipelineType.EXTERNAL_MARKETING_PIPELINE.value,
            'spam_filter': PipelineType.SPAM_FILTER.value
        }
        
        # Add primary routes
        for classification in classifications:
            if classification in classification_to_pipeline:
                routes.append(classification_to_pipeline[classification])
        
        # Add secondary routes based on combinations
        if 'press_release_pipeline' in classifications and 'story_pipeline' not in classifications:
            routes.append(PipelineType.STORY_PIPELINE.value)  # Press releases can become stories
        
        if 'press_release_pipeline' in classifications and classifications['press_release_pipeline'] > 0.7:
            routes.append(PipelineType.SALES_PIPELINE.value)  # PR contacts are sales opportunities
        
        
        # If no routes found, send to archive
        if not routes:
            routes.append(PipelineType.ARCHIVE_PIPELINE.value)
        
        return routes
    
    def _calculate_priority_score(self, classifications: Dict[str, float], email_data: Dict) -> float:
        """Calculate priority score for email processing"""
        priority = 0.5  # Base priority
        
        # High priority classifications
        high_priority = {
            'complaint_pipeline': 0.3,
            'story_pipeline': 0.2,
            'press_release_pipeline': 0.15,
            'sales_pipeline': 0.1
        }
        
        for classification, score in classifications.items():
            if classification in high_priority:
                priority += high_priority[classification] * score
        
        # Time-based priority boost
        if email_data.get('date_sent'):
            now = datetime.now(timezone.utc) if email_data['date_sent'].tzinfo else datetime.now()
            hours_old = (now - email_data['date_sent']).total_seconds() / 3600
            if hours_old < 24:
                priority += 0.1
        
        # Sender-based priority
        sender_email = email_data.get('sender_email', '')
        if '.gov' in sender_email or '.edu' in sender_email:
            priority += 0.1
        
        # Subject urgency indicators
        subject = email_data.get('subject', '').lower()
        urgent_words = ['urgent', 'breaking', 'immediate', 'asap', 'time sensitive']
        for word in urgent_words:
            if word in subject:
                priority += 0.2
                break
        
        return min(priority, 1.0)
    
    def _requires_human_review(self, classifications: Dict[str, float], email_data: Dict) -> bool:
        """Determine if email requires human review"""
        # Low confidence classifications
        if any(score < 0.5 for score in classifications.values()):
            return True
        
        # Multiple conflicting classifications
        if len(classifications) > 3:
            return True
        
        # High priority items
        if 'complaint_pipeline' in classifications and classifications['complaint_pipeline'] > 0.7:
            return True
        
        # Potential spam but not certain
        if 'spam_filter' in classifications and classifications['spam_filter'] < 0.8:
            return True
        
        return False
    
    def route_email(self, email_id: int, classification: EmailClassification):
        """Route email to appropriate pipelines"""
        cursor = self.db_conn.cursor()
        
        # Check if email exists first
        cursor.execute("SELECT id FROM classified_emails WHERE id = %s", (email_id,))
        if not cursor.fetchone():
            print(f"Warning: Email ID {email_id} not found in classified_emails table. Skipping routing.")
            return
        
        try:
            # Save classifications
            for classification_type, confidence in classification.confidence_scores.items():
                cursor.execute("""
                    INSERT INTO email_classifications (email_id, classification_type, confidence_score)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (email_id, classification_type) 
                    DO UPDATE SET confidence_score = EXCLUDED.confidence_score
                """, (email_id, classification_type, confidence))
            
            # Save pipeline routes
            for pipeline_type in classification.pipeline_routes:
                cursor.execute("""
                    INSERT INTO email_pipeline_routes (
                        email_id, pipeline_type, priority_score, status
                    ) VALUES (%s, %s, %s, %s)
                """, (email_id, pipeline_type, classification.priority_score, 'pending'))
            
            self.db_conn.commit()
            
        except Exception as e:
            self.db_conn.rollback()
            print(f"Error routing email: {e}")
    
    def get_pipeline_queue(self, pipeline_type: str, status: str = 'pending', limit: int = 50) -> List[Dict]:
        """Get emails in a specific pipeline queue"""
        cursor = self.db_conn.cursor()
        
        cursor.execute("""
            SELECT 
                e.id,
                e.gmail_id,
                e.subject,
                e.sender_name,
                e.sender_email,
                e.date_sent,
                pr.priority_score,
                pr.status,
                pr.assigned_to,
                ARRAY_AGG(ec.classification_type) as classifications
            FROM email_pipeline_routes pr
            JOIN classified_emails e ON e.id = pr.email_id
            LEFT JOIN email_classifications ec ON e.id = ec.email_id
            WHERE pr.pipeline_type = %s
            AND pr.status = %s
            GROUP BY e.id, e.gmail_id, e.subject, e.sender_name, e.sender_email, 
                     e.date_sent, pr.priority_score, pr.status, pr.assigned_to
            ORDER BY pr.priority_score DESC, e.date_sent DESC
            LIMIT %s
        """, (pipeline_type, status, limit))
        
        return cursor.fetchall()
    
    def update_pipeline_status(self, email_id: int, pipeline_type: str, status: str, assigned_to: str = None, notes: str = None):
        """Update pipeline processing status"""
        cursor = self.db_conn.cursor()
        
        cursor.execute("""
            UPDATE email_pipeline_routes 
            SET status = %s, assigned_to = %s, processing_notes = %s, updated_at = NOW()
            WHERE email_id = %s AND pipeline_type = %s
        """, (status, assigned_to, notes, email_id, pipeline_type))
        
        self.db_conn.commit()
    
    def record_pipeline_outcome(self, email_id: int, pipeline_type: str, outcome_type: str, 
                              outcome_details: Dict = None, revenue: float = None, articles: int = None):
        """Record pipeline outcome for training"""
        cursor = self.db_conn.cursor()
        
        cursor.execute("""
            INSERT INTO pipeline_outcomes (
                email_id, pipeline_type, outcome_type, outcome_details,
                revenue_generated, articles_published
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (email_id, pipeline_type, outcome_type, 
              json.dumps(outcome_details, cls=DateTimeJSONEncoder) if outcome_details else None,
              revenue, articles))
        
        self.db_conn.commit()
    
    def get_routing_stats(self) -> Dict:
        """Get routing and pipeline statistics"""
        cursor = self.db_conn.cursor()
        
        # Pipeline queue sizes
        cursor.execute("""
            SELECT pipeline_type, status, COUNT(*) as count
            FROM email_pipeline_routes
            GROUP BY pipeline_type, status
            ORDER BY pipeline_type, status
        """)
        
        pipeline_stats = {}
        for row in cursor.fetchall():
            pipeline = row[0]
            if pipeline not in pipeline_stats:
                pipeline_stats[pipeline] = {}
            pipeline_stats[pipeline][row[1]] = row[2]
        
        # Classification distribution
        cursor.execute("""
            SELECT classification_type, COUNT(*) as count, AVG(confidence_score) as avg_confidence
            FROM email_classifications
            GROUP BY classification_type
            ORDER BY count DESC
        """)
        
        classification_stats = {}
        for row in cursor.fetchall():
            classification_stats[row[0]] = {
                'count': row[1],
                'avg_confidence': row[2]
            }
        
        # Outcome tracking
        cursor.execute("""
            SELECT pipeline_type, outcome_type, COUNT(*) as count,
                   AVG(revenue_generated) as avg_revenue,
                   SUM(articles_published) as total_articles
            FROM pipeline_outcomes
            GROUP BY pipeline_type, outcome_type
            ORDER BY pipeline_type, count DESC
        """)
        
        outcome_stats = {}
        for row in cursor.fetchall():
            pipeline = row[0]
            if pipeline not in outcome_stats:
                outcome_stats[pipeline] = {}
            outcome_stats[pipeline][row[1]] = {
                'count': row[2],
                'avg_revenue': row[3],
                'total_articles': row[4]
            }
        
        return {
            'pipeline_queues': pipeline_stats,
            'classifications': classification_stats,
            'outcomes': outcome_stats,
            'generated_at': datetime.now().isoformat()
        }


def main():
    """Main function for testing email routing"""
    router = EmailPipelineRouter()
    
    # Test email data
    test_email = {
        'gmail_id': 'test123',
        'subject': 'Breaking: Major development story pitch',
        'sender_email': 'reporter@example.com',
        'body_text': 'I have an exclusive story about city development that I think would be perfect for your publication...',
        'date_sent': datetime.now()
    }
    
    # Classify and route
    classification = router.classify_email(test_email)
    print(f"Classifications: {classification.classifications}")
    print(f"Pipeline routes: {classification.pipeline_routes}")
    print(f"Priority score: {classification.priority_score}")
    print(f"Human review needed: {classification.requires_human_review}")
    
    # Get routing stats
    stats = router.get_routing_stats()
    print(f"\nRouting statistics: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()